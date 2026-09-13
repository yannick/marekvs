use marekvs_diff::*;
fn showcase() -> (Tree, Tree) {
    (
        Tree::from_json(&serde_json::from_str(include_str!("corpus/showcase/a.json")).unwrap())
            .unwrap(),
        Tree::from_json(&serde_json::from_str(include_str!("corpus/showcase/b.json")).unwrap())
            .unwrap(),
    )
}
#[test]
fn showcase_has_independent_move_and_word_change() {
    let (a, b) = showcase();
    let g = diff(&a, &b, &Options::default()).unwrap();
    let json = serde_json::to_value(&g).unwrap();
    let changes = json["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 2, "{json:#}");
    assert_eq!(changes.iter().filter(|c| c["op"] == "move").count(), 1);
    assert_eq!(changes.iter().filter(|c| c["op"] == "modify").count(), 1);
    assert_eq!(
        apply(&a, &plan(&a, &g, &Accepted::all(&g)).unwrap()).to_json(),
        b.to_json()
    );
    for c in &g.changes {
        let mut accepted = Accepted::none();
        accepted.0.insert(c.id.clone());
        let result = apply(&a, &plan(&a, &g, &accepted).unwrap());
        assert!(Tree::from_json(&result.to_json()).is_ok());
        assert_ne!(result.sid, a.sid);
        assert_ne!(result.sid, b.sid);
    }
}

#[test]
fn adversarial_fixture_roundtrips_and_operation_counts() {
    for case in [
        "moves/rotation_abc_cab",
        "format/one_bold_word",
        "attrs/title_only",
        "attrs/level_only",
        "repeats/identical_clauses",
        "import/wholesale_rewrite",
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/corpus")
            .join(case);
        let read = |name: &str| {
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(path.join(name)).unwrap())
                .unwrap()
        };
        let a = Tree::from_json(&read("a.json")).unwrap();
        let b = Tree::from_json(&read("b.json")).unwrap();
        let g = diff(&a, &b, &Options::default()).unwrap();
        assert_eq!(
            apply(&a, &plan(&a, &g, &Accepted::all(&g)).unwrap()).to_json(),
            b.to_json(),
            "{case}"
        );
        let v = serde_json::to_value(&g).unwrap();
        for (op, count) in read("expect.json")["ops"].as_object().unwrap() {
            assert_eq!(
                v["changes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|c| c["op"].as_str() == Some(op))
                    .count(),
                count.as_u64().unwrap() as usize,
                "{case}: {v}"
            );
        }
    }
}

#[test]
fn huge_leaf_and_deep_tree_are_rejected_before_diff() {
    let huge = serde_json::json!({"t":"doc","c":[{"t":"sen","x":vec!["word";8193].join(" ")}]});
    assert!(Tree::from_json(&huge).is_err());
    let mut deep = serde_json::json!({"t":"sen","x":"leaf"});
    for _ in 0..65 {
        deep = serde_json::json!({"t":"sec","c":[deep]});
    }
    assert!(Tree::from_json(&serde_json::json!({"t":"doc","c":[deep]})).is_err());
}
