//! Structured document comparison commands. All history keys share one tag.
pub mod cache;
pub mod fork;
pub mod keys;
pub mod pool;
pub mod snapshot;

use crate::reply::Reply;
pub fn error(error: marekvs_diff::DiffError) -> Reply {
    use marekvs_diff::DiffError;
    match error {
        DiffError::Cancelled => Reply::err("DIFFCANCELLED comparison cancelled"),
        DiffError::Timeout { stage } => Reply::err(format!("DIFFTIMEOUT {stage}")),
        DiffError::TooBig { bound, n, limit } => {
            Reply::err(format!("DIFFTOOBIG {bound}={n} limit={limit}"))
        }
        DiffError::Budget { limit } => Reply::err(format!("DIFFTOOBIG {limit}")),
        DiffError::Model(error) => Reply::err(format!("DIFFMODEL {error}")),
        DiffError::InvalidGraph(error) if error.starts_with("DIFFNOSNAPSHOT") => Reply::err(error),
        DiffError::InvalidGraph(error) => Reply::err(format!("DIFFINVALID {error}")),
    }
}
pub mod apply;
pub mod compare;
pub mod decide;
pub mod import;
