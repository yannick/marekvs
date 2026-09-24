//! Where the ondaDB database lives inside a marekvs data directory.
//!
//! Since 0.3.5 the database is `<data_dir>/db`, not `<data_dir>` itself. Every
//! deployment mounts a volume *at* the data directory (`/data`), and ondaDB's
//! format upgrades rebuild a database in a sibling directory and swap it in by
//! renaming the database directory — impossible for a mount point, and the
//! sibling of `/data` is the read-only image root. One level down, the sibling
//! of the database is the data directory itself: same filesystem, writable.
//!
//! A flat (≤ 0.3.4) data directory is moved into `db/` on open. The move is a
//! set of same-filesystem renames into a staging directory, then one rename of
//! the staging directory to `db`, so a crash at any point leaves either the
//! flat layout, a staging directory the next open finishes filling, or the
//! final layout. Nothing is copied and no file is rewritten.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// The database directory, relative to the data directory.
pub const DB_SUBDIR: &str = "db";
/// Where a flat layout is gathered before it becomes [`DB_SUBDIR`].
const STAGING: &str = "db.migrating";
/// Data-directory entries that are never part of a flat database: our own two
/// names, and the directory `mkfs.ext4` puts at the root of every volume.
const NOT_DB: &[&str] = &[DB_SUBDIR, STAGING, "lost+found"];
/// Present in every ondaDB database directory, so it identifies a flat layout.
const DB_MARKER: &str = "MANIFEST";

/// Resolve the database directory for `data_dir`, first moving a flat
/// layout into it. Returns `<data_dir>/db`.
pub fn prepare(data_dir: &Path) -> io::Result<PathBuf> {
    fs::create_dir_all(data_dir)?;
    let db = data_dir.join(DB_SUBDIR);
    let staging = data_dir.join(STAGING);
    let flat = data_dir.join(DB_MARKER).exists();

    if !staging.exists() {
        if !flat {
            return Ok(db); // already migrated, or a fresh directory
        }
        if db.exists() {
            // Two databases: refuse rather than guess which one is live.
            return Err(io::Error::other(format!(
                "{}: holds both a flat database ({DB_MARKER}) and {DB_SUBDIR}/; \
                 move one of them away before starting",
                data_dir.display()
            )));
        }
    } else if db.exists() {
        return Err(io::Error::other(format!(
            "{}: holds both {STAGING}/ and {DB_SUBDIR}/; an interrupted layout \
             migration cannot be finished safely — resolve by hand",
            data_dir.display()
        )));
    }

    // A 0.3.4 process still running on the flat layout holds ondaDB's advisory
    // lock on LOCK; moving its files underneath it would be a split brain.
    // The lock travels with the inode through the rename and is released when
    // `_lock` drops, before the database is opened.
    // After an interrupted migration the lock file may already be staged.
    let lock_path = [data_dir.join("LOCK"), staging.join("LOCK")]
        .into_iter()
        .find(|p| p.exists())
        .unwrap_or_else(|| data_dir.join("LOCK"));
    let _lock = match fs::File::open(lock_path) {
        Ok(f) => {
            f.try_lock().map_err(|e| {
                io::Error::other(format!(
                    "{}: the database is in use by another process ({e}); \
                     not migrating its layout",
                    data_dir.display()
                ))
            })?;
            Some(f)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };

    if !staging.exists() {
        fs::create_dir(&staging)?;
        sync_dir(data_dir)?;
    }
    let mut moved = 0usize;
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if NOT_DB.iter().any(|n| name == *n) {
            continue;
        }
        fs::rename(entry.path(), staging.join(&name))?;
        moved += 1;
    }
    sync_dir(&staging)?;
    sync_dir(data_dir)?;
    fs::rename(&staging, &db)?;
    sync_dir(data_dir)?;
    tracing::info!(
        data_dir = %data_dir.display(),
        db = %db.display(),
        moved,
        "moved the flat ondaDB layout into its own subdirectory"
    );
    Ok(db)
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(p: &Path) {
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, p.file_name().unwrap().as_encoded_bytes()).unwrap();
    }

    fn flat(root: &Path) {
        touch(&root.join("MANIFEST"));
        touch(&root.join("MANIFEST-EDITS"));
        touch(&root.join("LOCK"));
        touch(&root.join("cf-data/1.klog"));
        touch(&root.join("cf-meta/wal-3.log"));
        fs::create_dir_all(root.join("lost+found")).unwrap();
    }

    fn assert_migrated(root: &Path, db: &Path) {
        assert_eq!(db, root.join("db"));
        for f in [
            "MANIFEST",
            "MANIFEST-EDITS",
            "LOCK",
            "cf-data/1.klog",
            "cf-meta/wal-3.log",
        ] {
            let p = db.join(f);
            assert!(p.is_file(), "{f} missing from db/");
            // Moved, not recreated: the content is the original.
            assert_eq!(
                fs::read(&p).unwrap(),
                Path::new(f).file_name().unwrap().as_encoded_bytes()
            );
            assert!(!root.join(f).exists(), "{f} left in the data dir");
        }
        assert!(root.join("lost+found").is_dir(), "lost+found must stay put");
        assert!(!root.join(STAGING).exists());
    }

    #[test]
    fn fresh_directory_gets_the_subdir_path_and_nothing_else() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("data");
        let db = prepare(&root).unwrap();
        assert_eq!(db, root.join("db"));
        assert!(root.is_dir());
        assert!(!db.exists(), "ondaDB creates the database directory itself");
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    }

    #[test]
    fn flat_layout_is_moved_into_db() {
        let t = tempfile::tempdir().unwrap();
        flat(t.path());
        let db = prepare(t.path()).unwrap();
        assert_migrated(t.path(), &db);
        // Idempotent: a second open finds the final layout and moves nothing.
        assert_eq!(prepare(t.path()).unwrap(), db);
        assert_migrated(t.path(), &db);
    }

    #[test]
    fn interrupted_migration_is_finished() {
        let t = tempfile::tempdir().unwrap();
        flat(t.path());
        // A crash after two entries reached the staging directory.
        let staging = t.path().join(STAGING);
        fs::create_dir(&staging).unwrap();
        fs::rename(t.path().join("MANIFEST"), staging.join("MANIFEST")).unwrap();
        fs::rename(t.path().join("cf-data"), staging.join("cf-data")).unwrap();
        let db = prepare(t.path()).unwrap();
        assert_migrated(t.path(), &db);
    }

    #[test]
    fn interrupted_before_any_move_is_finished() {
        let t = tempfile::tempdir().unwrap();
        flat(t.path());
        fs::create_dir(t.path().join(STAGING)).unwrap();
        let db = prepare(t.path()).unwrap();
        assert_migrated(t.path(), &db);
    }

    #[test]
    fn flat_and_subdir_together_is_refused() {
        let t = tempfile::tempdir().unwrap();
        flat(t.path());
        fs::create_dir(t.path().join("db")).unwrap();
        let err = prepare(t.path()).unwrap_err().to_string();
        assert!(err.contains("both a flat database"), "{err}");
        assert!(t.path().join("MANIFEST").exists(), "nothing may move");
    }

    #[test]
    fn staging_and_subdir_together_is_refused() {
        let t = tempfile::tempdir().unwrap();
        fs::create_dir(t.path().join("db")).unwrap();
        fs::create_dir(t.path().join(STAGING)).unwrap();
        assert!(prepare(t.path()).is_err());
    }

    #[test]
    fn a_locked_flat_database_is_not_moved() {
        let t = tempfile::tempdir().unwrap();
        flat(t.path());
        let holder = fs::File::open(t.path().join("LOCK")).unwrap();
        holder.lock().unwrap();
        let err = prepare(t.path()).unwrap_err().to_string();
        assert!(err.contains("in use"), "{err}");
        assert!(t.path().join("MANIFEST").exists(), "nothing may move");
        assert!(!t.path().join(STAGING).exists());
    }
}
