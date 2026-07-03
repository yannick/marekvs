//! Minimal Prometheus text-format reading — just enough to sum counters
//! and read gauges from marekvs's /metrics endpoint.

/// Sum every sample of `name` (all label sets) in one exposition body.
/// Returns None when the metric is absent entirely.
pub fn sum_metric(body: &str, name: &str) -> Option<f64> {
    let mut sum = 0.0;
    let mut seen = false;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // "<name>{labels} <value>" or "<name> <value>"
        let rest = match line.strip_prefix(name) {
            Some(r) => r,
            None => continue,
        };
        // Guard against prefix collisions (foo vs foo_total).
        let rest = match rest.chars().next() {
            Some('{') => match rest.find('}') {
                Some(i) => &rest[i + 1..],
                None => continue,
            },
            Some(' ') | Some('\t') => rest,
            _ => continue,
        };
        if let Some(v) = rest
            .split_whitespace()
            .next()
            .and_then(|t| t.parse::<f64>().ok())
        {
            sum += v;
            seen = true;
        }
    }
    seen.then_some(sum)
}

/// Single-sample gauge read (first occurrence).
pub fn gauge(body: &str, name: &str) -> Option<i64> {
    sum_metric(body, name).map(|v| v as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = r#"
# HELP marekvs_commands_total Commands processed
# TYPE marekvs_commands_total counter
marekvs_commands_total{cmd="get"} 1200
marekvs_commands_total{cmd="set"} 800
marekvs_commands_total_created 1.7e9
marekvs_cluster_underreplicated_partitions 0
marekvs_cluster_members 3
"#;

    #[test]
    fn sums_labeled_counter() {
        assert_eq!(sum_metric(BODY, "marekvs_commands_total"), Some(2000.0));
    }

    #[test]
    fn no_prefix_collision() {
        // _created must not leak into the _total sum (checked above), and
        // asking for a substring name must not match longer metrics.
        assert_eq!(sum_metric(BODY, "marekvs_commands"), None);
    }

    #[test]
    fn reads_gauges() {
        assert_eq!(
            gauge(BODY, "marekvs_cluster_underreplicated_partitions"),
            Some(0)
        );
        assert_eq!(gauge(BODY, "marekvs_cluster_members"), Some(3));
        assert_eq!(gauge(BODY, "nope"), None);
    }
}
