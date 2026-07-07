//! protox compilation of `.proto` source and FileDescriptorSet validation
//! (design/17). Compilation is CPU-bound — handlers MUST call it inside
//! `tokio::task::spawn_blocking`, never on shard threads.

use std::collections::HashMap;
use std::path::Path;

use prost::Message;
use prost_reflect::DescriptorPool;
use prost_types::FileDescriptorProto;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

use super::err::ProtoErr;
use super::ProtoLimits;

/// One registry-resolved import dependency.
#[derive(Debug, Clone)]
pub enum DepFile {
    /// Original `.proto` source (schema uploaded with SOURCE).
    Source(String),
    /// Pre-compiled files (schema uploaded with DESCRIPTOR): every file of
    /// its self-contained FileDescriptorSet.
    Files(Vec<FileDescriptorProto>),
}

/// Compilation result: the SELF-CONTAINED FileDescriptorSet (imports
/// inlined) plus the message type names the schema defines.
#[derive(Debug, Clone)]
pub struct CompileOutput {
    pub fds: Vec<u8>,
    pub types: Vec<String>,
}

/// The canonical protox file name for a registry schema name: used both to
/// open the main file and to key the in-memory resolver map.
pub fn file_name(schema: &str) -> String {
    if schema.ends_with(".proto") {
        schema.to_string()
    } else {
        format!("{schema}.proto")
    }
}

/// Extract the file names of top-level `import "x";` statements (including
/// `import public` / `import weak`), ignoring comments and string literals
/// elsewhere. Purely lexical — protox does the real parse; this only feeds
/// the registry BFS.
pub fn extract_imports(source: &str) -> Vec<String> {
    let stripped = strip_comments(source);
    let mut out = Vec::new();
    let bytes = stripped.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Match the `import` keyword at a token boundary.
        if stripped[i..].starts_with("import")
            && (i == 0 || !is_ident_byte(bytes[i - 1]))
            && !is_ident_byte(*bytes.get(i + 6).unwrap_or(&b' '))
        {
            let mut j = i + 6;
            // optional `public` / `weak`
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            for kw in ["public", "weak"] {
                if stripped[j..].starts_with(kw)
                    && !is_ident_byte(*bytes.get(j + kw.len()).unwrap_or(&b' '))
                {
                    j += kw.len();
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    break;
                }
            }
            if j < bytes.len() && bytes[j] == b'"' {
                if let Some(end) = stripped[j + 1..].find('"') {
                    let name = &stripped[j + 1..j + 1 + end];
                    if !name.is_empty() && !out.iter().any(|n| n == name) {
                        out.push(name.to_string());
                    }
                    i = j + 1 + end + 1;
                    continue;
                }
            }
        }
        i += 1;
    }
    out
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Remove `//`, `/* */` comments and skip string literals.
fn strip_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'/' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
            out.push(b' ');
        } else if b[i] == b'"' {
            // copy string literals verbatim (imports need them)
            out.push(b[i]);
            i += 1;
            while i < b.len() && b[i] != b'"' {
                if b[i] == b'\\' {
                    out.push(b[i]);
                    i += 1;
                    if i >= b.len() {
                        break;
                    }
                }
                out.push(b[i]);
                i += 1;
            }
            if i < b.len() {
                out.push(b[i]);
                i += 1;
            }
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// In-memory resolver over registry-fetched dependencies.
struct MapResolver {
    files: HashMap<String, DepFile>,
}

impl FileResolver for MapResolver {
    fn resolve_path(&self, _path: &Path) -> Option<String> {
        None
    }

    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.files.get(name) {
            Some(DepFile::Source(src)) => File::from_source(name, src),
            Some(DepFile::Files(fdps)) => fdps
                .iter()
                .find(|f| f.name() == name)
                .map(|f| File::from_file_descriptor_proto(f.clone()))
                .ok_or_else(|| protox::Error::file_not_found(name)),
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

/// Enforce the per-file source-size bound.
pub fn check_source_size(source: &str, limits: &ProtoLimits) -> Result<(), ProtoErr> {
    if source.len() > limits.max_source {
        return Err(ProtoErr::Schema(format!(
            "source too large ({} bytes, limit {})",
            source.len(),
            limits.max_source
        )));
    }
    Ok(())
}

/// Compile `.proto` source with its registry-resolved dependencies into a
/// self-contained FileDescriptorSet. `deps` maps protox file names (see
/// [`file_name`]) to their content; well-known `google/protobuf/*` imports
/// resolve via protox's bundled GoogleFileResolver.
pub fn compile_source(
    schema: &str,
    source: &str,
    deps: HashMap<String, DepFile>,
    limits: &ProtoLimits,
) -> Result<CompileOutput, ProtoErr> {
    check_source_size(source, limits)?;
    let main = file_name(schema);

    // The dep map plus the main file, all in-memory.
    let mut files = deps;
    files.insert(main.clone(), DepFile::Source(source.to_string()));

    let mut resolver = ChainFileResolver::new();
    resolver.add(GoogleFileResolver::new());
    resolver.add(MapResolver { files });

    let mut compiler = protox::Compiler::with_file_resolver(resolver);
    compiler.include_imports(true).include_source_info(false);
    compiler.open_file(&main).map_err(|e| {
        // protox renders missing imports as `import 'x' not found`.
        if e.is_file_not_found() {
            ProtoErr::Schema(format!("{e} (upload it first or use DESCRIPTOR)"))
        } else {
            ProtoErr::Schema(format!("{e}"))
        }
    })?;
    if compiler.files().len() > limits.max_files {
        return Err(ProtoErr::Schema(format!(
            "too many files in compilation unit ({}, limit {})",
            compiler.files().len(),
            limits.max_files
        )));
    }

    let fds = compiler.encode_file_descriptor_set();
    if fds.len() > limits.max_fds {
        return Err(ProtoErr::Schema(format!(
            "compiled descriptor set too large ({} bytes, limit {})",
            fds.len(),
            limits.max_fds
        )));
    }
    let types = pool_types(&compiler.descriptor_pool());
    Ok(CompileOutput { fds, types })
}

/// Validate an uploaded FileDescriptorSet: must decode, be self-contained,
/// and respect the size bound. Returns the type names it defines.
pub fn compile_descriptor(fds: &[u8], limits: &ProtoLimits) -> Result<CompileOutput, ProtoErr> {
    if fds.len() > limits.max_fds {
        return Err(ProtoErr::Schema(format!(
            "descriptor set too large ({} bytes, limit {})",
            fds.len(),
            limits.max_fds
        )));
    }
    let set = prost_types::FileDescriptorSet::decode(fds)
        .map_err(|e| ProtoErr::Schema(format!("invalid FileDescriptorSet: {e}")))?;
    if set.file.is_empty() {
        return Err(ProtoErr::Schema("empty FileDescriptorSet".into()));
    }
    if set.file.len() > limits.max_files {
        return Err(ProtoErr::Schema(format!(
            "too many files in descriptor set ({}, limit {})",
            set.file.len(),
            limits.max_files
        )));
    }
    // DescriptorPool::decode resolves all cross-references — a set with
    // missing imports fails here (self-containment check).
    let pool = DescriptorPool::decode(fds)
        .map_err(|e| ProtoErr::Schema(format!("descriptor set is not self-contained: {e}")))?;
    Ok(CompileOutput {
        fds: fds.to_vec(),
        types: pool_types(&pool),
    })
}

/// Build a DescriptorPool from stored (already-validated) FDS bytes.
pub fn pool_from_fds(fds: &[u8]) -> Result<DescriptorPool, ProtoErr> {
    DescriptorPool::decode(fds).map_err(|e| ProtoErr::Schema(format!("corrupt schema record: {e}")))
}

/// Message type names a pool defines, excluding the bundled well-known
/// `google/protobuf/*` files.
pub fn pool_types(pool: &DescriptorPool) -> Vec<String> {
    let mut types: Vec<String> = pool
        .all_messages()
        .filter(|m| !m.parent_file().name().starts_with("google/protobuf/"))
        .map(|m| m.full_name().to_string())
        .collect();
    types.sort();
    types
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ProtoLimits {
        ProtoLimits {
            max_source: 1024 * 1024,
            max_fds: 4 * 1024 * 1024,
            max_value: 4 * 1024 * 1024,
            max_files: 64,
            max_depth: 16,
        }
    }

    const ORDER: &str = r#"
        syntax = "proto3";
        package shop.v1;
        message Order {
            string id = 1;
            uint64 total_cents = 2;
            repeated Item items = 3;
        }
        message Item {
            string sku = 1;
            uint32 qty = 2;
        }
    "#;

    #[test]
    fn simple_compile_lists_types() {
        let out = compile_source("orders", ORDER, HashMap::new(), &limits()).unwrap();
        assert_eq!(out.types, vec!["shop.v1.Item", "shop.v1.Order"]);
        assert!(!out.fds.is_empty());
        // The FDS is self-contained and round-trips through a pool.
        let pool = pool_from_fds(&out.fds).unwrap();
        assert!(pool.get_message_by_name("shop.v1.Order").is_some());
    }

    #[test]
    fn import_chain_via_deps_map() {
        let common = r#"
            syntax = "proto3";
            package shop.common;
            message Money { int64 cents = 1; string currency = 2; }
        "#;
        let main = r#"
            syntax = "proto3";
            package shop.v1;
            import "common.proto";
            message Invoice { shop.common.Money total = 1; }
        "#;
        let mut deps = HashMap::new();
        deps.insert("common.proto".to_string(), DepFile::Source(common.into()));
        let out = compile_source("invoices", main, deps, &limits()).unwrap();
        assert!(out.types.contains(&"shop.v1.Invoice".to_string()));
        // Imported types are part of the self-contained set too.
        assert!(out.types.contains(&"shop.common.Money".to_string()));
        let pool = pool_from_fds(&out.fds).unwrap();
        assert!(pool.get_message_by_name("shop.common.Money").is_some());
    }

    #[test]
    fn descriptor_dep_resolves_import() {
        // A dependency uploaded as DESCRIPTOR (compiled elsewhere) satisfies
        // a SOURCE import.
        let common = r#"
            syntax = "proto3";
            package shop.common;
            message Money { int64 cents = 1; }
        "#;
        let compiled = compile_source("common", common, HashMap::new(), &limits()).unwrap();
        let set = prost_types::FileDescriptorSet::decode(&compiled.fds[..]).unwrap();
        let main = r#"
            syntax = "proto3";
            import "common.proto";
            message Wallet { shop.common.Money balance = 1; }
        "#;
        let mut deps = HashMap::new();
        deps.insert("common.proto".to_string(), DepFile::Files(set.file));
        let out = compile_source("wallet", main, deps, &limits()).unwrap();
        assert!(out.types.contains(&"Wallet".to_string()));
    }

    #[test]
    fn wkt_import_resolves_without_registry() {
        let src = r#"
            syntax = "proto3";
            package t;
            import "google/protobuf/timestamp.proto";
            message Event { google.protobuf.Timestamp at = 1; }
        "#;
        let out = compile_source("events", src, HashMap::new(), &limits()).unwrap();
        assert!(out.types.contains(&"t.Event".to_string()));
        // WKT helper types are not reported as schema-owned types.
        assert!(!out.types.iter().any(|t| t.starts_with("google.protobuf")));
    }

    #[test]
    fn missing_import_names_the_file() {
        let src = r#"
            syntax = "proto3";
            import "nowhere.proto";
            message X { int32 a = 1; }
        "#;
        let err = compile_source("x", src, HashMap::new(), &limits()).unwrap_err();
        match err {
            ProtoErr::Schema(msg) => {
                assert!(msg.contains("nowhere.proto"), "{msg}");
                assert!(msg.contains("upload it first"), "{msg}");
            }
            other => panic!("expected Schema err, got {other:?}"),
        }
    }

    #[test]
    fn parse_error_is_schemaerr() {
        let err = compile_source("bad", "message {", HashMap::new(), &limits()).unwrap_err();
        assert!(matches!(err, ProtoErr::Schema(_)));
    }

    #[test]
    fn oversize_source_rejected() {
        let mut l = limits();
        l.max_source = 64;
        let src = format!("syntax = \"proto3\"; // {}", "x".repeat(100));
        let err = compile_source("big", &src, HashMap::new(), &l).unwrap_err();
        match err {
            ProtoErr::Schema(msg) => assert!(msg.contains("too large"), "{msg}"),
            other => panic!("expected Schema err, got {other:?}"),
        }
    }

    #[test]
    fn bad_descriptor_rejected() {
        let err = compile_descriptor(b"\xFF\xFF\xFFnot-an-fds", &limits()).unwrap_err();
        assert!(matches!(err, ProtoErr::Schema(_)));
        let err = compile_descriptor(b"", &limits()).unwrap_err();
        assert!(matches!(err, ProtoErr::Schema(_)));
    }

    #[test]
    fn good_descriptor_roundtrips() {
        let out = compile_source("orders", ORDER, HashMap::new(), &limits()).unwrap();
        let again = compile_descriptor(&out.fds, &limits()).unwrap();
        assert_eq!(again.types, out.types);
    }

    #[test]
    fn extract_imports_lexical() {
        let src = r#"
            // import "commented.proto";
            /* import "block.proto"; */
            syntax = "proto3";
            import "a.proto";
            import public "b.proto";
            import weak "c.proto";
            import "a.proto"; // duplicate ignored
            message NotAnImport { string import_name = 1; }
        "#;
        assert_eq!(extract_imports(src), vec!["a.proto", "b.proto", "c.proto"]);
    }

    #[test]
    fn file_name_normalizes() {
        assert_eq!(file_name("orders"), "orders.proto");
        assert_eq!(file_name("orders.proto"), "orders.proto");
    }
}
