use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Instant;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Budget {
    pub max_candidates: usize,
    pub max_nd: usize,
    pub max_changes: usize,
    pub max_nodes: usize,
    pub max_depth: usize,
    pub max_bytes: usize,
    pub max_leaf_tokens: usize,
}
impl Default for Budget {
    fn default() -> Self {
        Self {
            max_candidates: 50_000,
            max_nd: 2_000_000,
            max_changes: 100_000,
            max_nodes: 50_000,
            max_depth: 64,
            max_bytes: 16 * 1024 * 1024,
            max_leaf_tokens: 8192,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffError {
    Model(crate::model::ModelError),
    Cancelled,
    Timeout {
        stage: &'static str,
    },
    TooBig {
        bound: &'static str,
        n: usize,
        limit: usize,
    },
    Budget {
        limit: &'static str,
    },
    InvalidGraph(String),
}
impl std::fmt::Display for DiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DiffError {}
impl From<crate::model::ModelError> for DiffError {
    fn from(e: crate::model::ModelError) -> Self {
        Self::Model(e)
    }
}
#[derive(Clone, Debug)]
pub struct CancelToken {
    cancelled: Arc<AtomicBool>,
    deadline: Option<Instant>,
}
impl Default for CancelToken {
    fn default() -> Self {
        Self::none()
    }
}
impl CancelToken {
    pub fn none() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            deadline: None,
        }
    }
    pub fn with_deadline(deadline: Instant) -> Self {
        Self {
            deadline: Some(deadline),
            ..Self::none()
        }
    }
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
    pub fn check(&self) -> Result<(), DiffError> {
        if self.cancelled.load(Ordering::Relaxed) {
            Err(DiffError::Cancelled)
        } else if self.deadline.is_some_and(|d| Instant::now() >= d) {
            Err(DiffError::Timeout { stage: "diff" })
        } else {
            Ok(())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn budget_cancel_clone_and_deadline() {
        let c = CancelToken::none();
        assert!(c.check().is_ok());
        c.clone().cancel();
        assert_eq!(c.check(), Err(DiffError::Cancelled));
        assert!(matches!(
            CancelToken::with_deadline(Instant::now()).check(),
            Err(DiffError::Timeout { .. })
        ));
    }
}
