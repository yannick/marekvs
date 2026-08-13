//! Leader election over a `coordination.k8s.io/Lease` (T2-16).
//!
//! Two controller replicas would fight: both server-side-apply the same
//! children under the same field manager, and both run the scale stepper, so
//! one could scale down while the other scales up. Today's mitigation is
//! "run 1 replica" by convention (`k8s/operator/deployment.yaml`) — real, but
//! self-inflicted the moment someone edits the Deployment.
//!
//! The contract is deliberately blunt: acquire or wait, and **exit the process
//! on any loss of the lease**. A follower that keeps running while unsure is
//! exactly the split-brain this prevents, and the Deployment restarts the
//! process as a follower within seconds. Fail-fast beats fail-ambiguous.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::coordination::v1::Lease;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::Client;
use serde_json::json;

/// How long a lease is honoured after its last renewal.
pub const LEASE_DURATION: Duration = Duration::from_secs(15);
/// How often the holder renews. Comfortably inside `LEASE_DURATION` so a
/// single slow API call cannot cost the lease.
pub const RENEW_INTERVAL: Duration = Duration::from_secs(10);
/// How often a follower re-checks whether the lease went stale.
pub const RETRY_INTERVAL: Duration = Duration::from_secs(2);

pub const LEASE_NAME: &str = "marekvs-operator-leader";

/// True when `renew_time` is older than `LEASE_DURATION` — i.e. the recorded
/// holder stopped renewing and the lease may be taken.
///
/// Split out and pure so the expiry rule is unit-testable without a cluster.
pub fn lease_expired(renew_epoch: Option<i64>, now_epoch: i64, duration_secs: i64) -> bool {
    match renew_epoch {
        None => true,
        Some(t) => now_epoch.saturating_sub(t) >= duration_secs,
    }
}

fn now_micros_rfc3339() -> String {
    k8s_openapi::jiff::Timestamp::now().to_string()
}

/// Parse a `microTime`/RFC3339 stamp back to epoch seconds.
fn epoch_of(stamp: &str) -> Option<i64> {
    stamp
        .parse::<k8s_openapi::jiff::Timestamp>()
        .ok()
        .map(|t| t.as_second())
}

/// Block until this process holds the lease, then keep renewing it in a
/// background task. Returns once leadership is acquired.
///
/// `identity` must be unique per replica — the pod name is the natural choice.
pub async fn acquire_and_keep(client: Client, ns: &str, identity: &str) -> anyhow::Result<()> {
    let leases: Api<Lease> = Api::namespaced(client, ns);

    loop {
        match try_acquire(&leases, identity).await {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => tracing::warn!(%e, "lease acquisition attempt failed"),
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
    tracing::info!(identity, "acquired leadership");

    let leases = Arc::new(leases);
    let identity = identity.to_string();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(RENEW_INTERVAL).await;
            match try_acquire(&leases, &identity).await {
                Ok(true) => {}
                Ok(false) => {
                    // Someone else holds it: we were partitioned or paused
                    // long enough to lose it. Never keep reconciling.
                    tracing::error!(identity, "lost leadership; exiting");
                    std::process::exit(0);
                }
                Err(e) => {
                    tracing::error!(%e, identity, "lease renewal failed; exiting");
                    std::process::exit(1);
                }
            }
        }
    });
    Ok(())
}

/// One acquire/renew attempt. `Ok(true)` = we hold the lease.
async fn try_acquire(leases: &Api<Lease>, identity: &str) -> anyhow::Result<bool> {
    let now = k8s_openapi::jiff::Timestamp::now().as_second();
    let existing = leases.get_opt(LEASE_NAME).await?;

    let Some(lease) = existing else {
        // No lease yet: create it. A concurrent creator makes this fail with
        // AlreadyExists, and the next pass sees theirs — no lost update.
        let body: Lease = serde_json::from_value(json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": LEASE_NAME},
            "spec": {
                "holderIdentity": identity,
                "leaseDurationSeconds": LEASE_DURATION.as_secs() as i32,
                "acquireTime": now_micros_rfc3339(),
                "renewTime": now_micros_rfc3339(),
            }
        }))?;
        return match leases.create(&PostParams::default(), &body).await {
            Ok(_) => Ok(true),
            Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
            Err(e) => Err(e.into()),
        };
    };

    let spec = lease.spec.clone().unwrap_or_default();
    let holder = spec.holder_identity.clone().unwrap_or_default();
    let duration = i64::from(
        spec.lease_duration_seconds
            .unwrap_or(LEASE_DURATION.as_secs() as i32),
    );
    let renewed = spec
        .renew_time
        .as_ref()
        .and_then(|t| epoch_of(&t.0.to_string()));

    let ours = holder == identity;
    if !ours && !lease_expired(renewed, now, duration) {
        return Ok(false);
    }

    // Ours to renew, or expired and free to take. resourceVersion makes this
    // a compare-and-swap: a competing replica that took it since our GET makes
    // this patch fail with a conflict rather than silently stealing it back.
    let mut patch = json!({
        "spec": {
            "holderIdentity": identity,
            "leaseDurationSeconds": duration,
            "renewTime": now_micros_rfc3339(),
        }
    });
    if !ours {
        patch["spec"]["acquireTime"] = json!(now_micros_rfc3339());
    }
    patch["metadata"] = json!({"resourceVersion": lease.metadata.resource_version});

    match leases
        .patch(LEASE_NAME, &PatchParams::default(), &Patch::Merge(&patch))
        .await
    {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_never_renewed_is_expired() {
        assert!(lease_expired(None, 1000, 15));
    }

    #[test]
    fn a_freshly_renewed_lease_is_held() {
        assert!(!lease_expired(Some(1000), 1005, 15));
    }

    /// Exactly at the duration the lease is takeable — a follower must not
    /// have to wait an extra tick past a holder that is definitively gone.
    #[test]
    fn expiry_is_inclusive_at_the_duration() {
        assert!(lease_expired(Some(1000), 1015, 15));
        assert!(!lease_expired(Some(1000), 1014, 15));
    }

    /// Clock skew that puts `now` before the renewal must not be read as an
    /// expired lease — that would let a follower steal an active one.
    #[test]
    fn backwards_clock_does_not_expire_a_live_lease() {
        assert!(!lease_expired(Some(2000), 1000, 15));
    }
}
