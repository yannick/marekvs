use marekvs_diff::*;
use proptest::prelude::*;
use serde_json::{json, Value};

fn tree(values: &[u8]) -> Tree {
    Tree::from_json(&json!({"t":"doc","c":[{"t":"sec","a":{"title":"Terms"},"c":[{"t":"par","c":values.iter().map(|v|json!({"t":"sen","x":format!("Clause {v} shall apply.")})).collect::<Vec<Value>>()}]}]})).unwrap()
}
proptest! {
    #![proptest_config(ProptestConfig {cases:256,..ProptestConfig::default()})]
    #[test]
    fn all_and_none_roundtrip(a in prop::collection::vec(0u8..20,0..12), b in prop::collection::vec(0u8..20,0..12)) {
        let a=tree(&a);let b=tree(&b);let graph=diff(&a,&b,&Options::default()).unwrap();
        prop_assert_eq!(apply(&a,&plan(&a,&graph,&Accepted::all(&graph)).unwrap()).to_json(),b.to_json());
        prop_assert_eq!(apply(&a,&plan(&a,&graph,&Accepted::none()).unwrap()).to_json(),a.to_json());
        prop_assert!(diff(&a,&a,&Options::default()).unwrap().changes.is_empty());
        let again=diff(&a,&Tree::from_json(&b.to_json()).unwrap(),&Options::default()).unwrap();
        prop_assert_eq!(serde_json::to_vec(&graph).unwrap(),serde_json::to_vec(&again).unwrap());
    }
    #[test]
    fn text_roundtrip_unicode(a in "[a-zαé \n\t]{0,50}", b in "[a-zαé \n\t]{0,50}") {
        for mode in [Level::Word,Level::Char,Level::Sen] {
            let edits=textdiff::diff_text(&a,&b,mode);
            prop_assert_eq!(textdiff::apply_edits(&a,&edits).unwrap(),b.clone());
        }
    }
}

fn structured(seed: &[u8]) -> Value {
    json!({"t":"doc","c":(0..3).map(|s|json!({"t":"sec","a":{"title":format!("Section {s}")},"c":[{"t":"par","c":(0..3).map(|i|json!({"t":"sen","x":format!("The clause {} shall apply to party {}.",seed[(s*3+i)%seed.len()],s*3+i)})).collect::<Vec<_>>()}]})).collect::<Vec<_>>()})
}
proptest! {
    #![proptest_config(ProptestConfig {cases:256,..ProptestConfig::default()})]
    #[test]
    fn structural_scripts_roundtrip(seed in prop::collection::vec(any::<u8>(),9), steps in prop::collection::vec((0u8..6,0usize..3,0usize..3),0..8)) {
        let v=structured(&seed);let a=Tree::from_json(&v).unwrap();let mut v=v;
        for (op,s,t) in steps {
            match op {
                0=>{let list=v["c"][s]["c"][0]["c"].as_array_mut().unwrap();if let Some(node)=list.pop(){v["c"][t]["c"][0]["c"].as_array_mut().unwrap().insert(0,node);}},
                1=>{v["c"][s]["a"]["title"]=json!(format!("Revised section {t}"));},
                2=>{v["c"][s]["c"][0]["c"].as_array_mut().unwrap().push(json!({"t":"sen","x":format!("Additional clause {t} applies.")}));},
                3=>{v["c"][s]["c"][0]["c"].as_array_mut().unwrap().pop();},
                4=>{if let Some(node)=v["c"][s]["c"][0]["c"].as_array_mut().unwrap().first_mut(){node["x"]=json!(node["x"].as_str().unwrap().replace("shall","may"));}},
                _=>{if let Some(node)=v["c"][s]["c"][0]["c"].as_array_mut().unwrap().first_mut(){node["f"]=json!([[0,3,{"b":true}]]);}},
            }
        }
        let b=Tree::from_json(&v).unwrap();let g=diff(&a,&b,&Options::default()).unwrap();
        prop_assert_eq!(apply(&a,&plan(&a,&g,&Accepted::all(&g)).unwrap()).to_json(),b.to_json());
        prop_assert_eq!(apply(&a,&plan(&a,&g,&Accepted::none()).unwrap()).to_json(),a.to_json());
        if g.changes.len()<=8 {
            for mask in 0usize..(1<<g.changes.len()) {
                let selected:Accepted=g.changes.iter().enumerate().filter(|(i,_)|mask&(1<<i)!=0).map(|(_,c)|c.id.clone()).collect();
                if let Ok(p)=plan(&a,&g,&selected) {
                    let result=apply(&a,&p);
                    prop_assert!(Tree::from_json(&result.to_json()).is_ok());
                    prop_assert_eq!(result.to_json(),apply(&a,&plan(&a,&g,&selected).unwrap()).to_json());
                }
            }
        }
    }
}
