//! Local pub/sub registry (design/04 §Pub/Sub). Cluster fan-out is wired in
//! by the server through `set_cluster_hook`; messages arriving from peers are
//! delivered with `publish_local`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone)]
pub struct PubMessage {
    /// Set when delivered via a pattern subscription (pmessage frame).
    pub pattern: Option<Vec<u8>>,
    pub channel: Vec<u8>,
    pub payload: Vec<u8>,
}

type Subscribers = HashMap<u64, UnboundedSender<PubMessage>>;
type ClusterHook = Box<dyn Fn(&[u8], &[u8]) + Send + Sync>;

#[derive(Default)]
pub struct PubSub {
    channels: Mutex<HashMap<Vec<u8>, Subscribers>>,
    patterns: Mutex<HashMap<Vec<u8>, Subscribers>>,
    cluster: Mutex<Option<ClusterHook>>,
    next_id: AtomicU64,
}

impl PubSub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn new_subscriber_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Server installs this to forward publishes to matching peers.
    pub fn set_cluster_hook(&self, hook: ClusterHook) {
        *self.cluster.lock() = Some(hook);
    }

    pub fn subscribe(&self, id: u64, channel: &[u8], tx: UnboundedSender<PubMessage>) {
        self.channels
            .lock()
            .entry(channel.to_vec())
            .or_default()
            .insert(id, tx);
    }

    pub fn unsubscribe(&self, id: u64, channel: &[u8]) {
        let mut chans = self.channels.lock();
        if let Some(subs) = chans.get_mut(channel) {
            subs.remove(&id);
            if subs.is_empty() {
                chans.remove(channel);
            }
        }
    }

    pub fn psubscribe(&self, id: u64, pattern: &[u8], tx: UnboundedSender<PubMessage>) {
        self.patterns
            .lock()
            .entry(pattern.to_vec())
            .or_default()
            .insert(id, tx);
    }

    pub fn punsubscribe(&self, id: u64, pattern: &[u8]) {
        let mut pats = self.patterns.lock();
        if let Some(subs) = pats.get_mut(pattern) {
            subs.remove(&id);
            if subs.is_empty() {
                pats.remove(pattern);
            }
        }
    }

    /// Full publish: local delivery + cluster fan-out. Returns local receiver
    /// count (Redis semantics: receivers on *this* node).
    pub fn publish(&self, channel: &[u8], payload: &[u8]) -> usize {
        let n = self.publish_local(channel, payload);
        if let Some(hook) = self.cluster.lock().as_ref() {
            hook(channel, payload);
        }
        n
    }

    /// Local-only delivery (used for messages arriving from peers).
    pub fn publish_local(&self, channel: &[u8], payload: &[u8]) -> usize {
        let mut delivered = 0;
        {
            let chans = self.channels.lock();
            if let Some(subs) = chans.get(channel) {
                for tx in subs.values() {
                    if tx
                        .send(PubMessage {
                            pattern: None,
                            channel: channel.to_vec(),
                            payload: payload.to_vec(),
                        })
                        .is_ok()
                    {
                        delivered += 1;
                    }
                }
            }
        }
        {
            let pats = self.patterns.lock();
            for (pat, subs) in pats.iter() {
                if glob_match(pat, channel) {
                    for tx in subs.values() {
                        if tx
                            .send(PubMessage {
                                pattern: Some(pat.clone()),
                                channel: channel.to_vec(),
                                payload: payload.to_vec(),
                            })
                            .is_ok()
                        {
                            delivered += 1;
                        }
                    }
                }
            }
        }
        delivered
    }

    pub fn channels_matching(&self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        self.channels
            .lock()
            .keys()
            .filter(|c| pattern.is_none_or(|p| glob_match(p, c)))
            .cloned()
            .collect()
    }

    pub fn numsub(&self, channel: &[u8]) -> usize {
        self.channels.lock().get(channel).map_or(0, |s| s.len())
    }

    pub fn numpat(&self) -> usize {
        self.patterns.lock().len()
    }

    /// Drop every subscription of a disconnected session.
    pub fn drop_session(&self, id: u64, channels: &[Vec<u8>], patterns: &[Vec<u8>]) {
        for c in channels {
            self.unsubscribe(id, c);
        }
        for p in patterns {
            self.punsubscribe(id, p);
        }
    }
}

/// Redis-style glob matching: `*`, `?`, `[...]` (with `!` negation), `\` escape.
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    glob_inner(pattern, text)
}

fn glob_inner(p: &[u8], t: &[u8]) -> bool {
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_p, mut star_t) = (usize::MAX, 0usize);
    while ti < t.len() {
        if pi < p.len() {
            match p[pi] {
                b'*' => {
                    star_p = pi;
                    star_t = ti;
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                b'\\' if pi + 1 < p.len() && p[pi + 1] == t[ti] => {
                    pi += 2;
                    ti += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, adv)) = class_match(&p[pi..], t[ti]) {
                        if matched {
                            pi += adv;
                            ti += 1;
                            continue;
                        }
                    }
                }
                c if c == t[ti] => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                _ => {}
            }
        }
        // mismatch: backtrack to the last star if any
        if star_p != usize::MAX {
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

fn class_match(p: &[u8], c: u8) -> Option<(bool, usize)> {
    // p[0] == b'['
    let mut i = 1;
    let negate = p.get(1) == Some(&b'!');
    if negate {
        i = 2;
    }
    let mut matched = false;
    let mut first = true;
    while i < p.len() {
        match p[i] {
            b']' if !first => return Some((matched != negate, i + 1)),
            b'\\' if i + 1 < p.len() => {
                if p[i + 1] == c {
                    matched = true;
                }
                i += 2;
            }
            lo if i + 2 < p.len() && p[i + 1] == b'-' && p[i + 2] != b']' => {
                if lo <= c && c <= p[i + 2] {
                    matched = true;
                }
                i += 3;
            }
            ch => {
                if ch == c {
                    matched = true;
                }
                i += 1;
            }
        }
        first = false;
    }
    None // unterminated class: no match
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globs() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"news.*", b"news.tech"));
        assert!(!glob_match(b"news.*", b"sports.tech"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[!ae]llo", b"hallo"));
        assert!(glob_match(b"h[a-c]llo", b"hbllo"));
        assert!(glob_match(b"a*b*c", b"axxbyyc"));
        assert!(!glob_match(b"a*b*c", b"axxbyy"));
        assert!(glob_match(b"\\*", b"*"));
        assert!(!glob_match(b"\\*", b"x"));
    }
}
