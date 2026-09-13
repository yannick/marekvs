//! Offline interface to the same bounded algorithms used by DIFF commands.
use marekvs_diff::{apply, diff, plan, Accepted, ChangeId, Graph, Level, Options, Tree};
use std::{error::Error, fs};
fn tree(path: &str) -> Result<Tree, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if bytes.len() > Options::default().budget.max_bytes {
        return Err("input exceeds byte budget".into());
    }
    Ok(Tree::from_json(&serde_json::from_slice(&bytes)?)?)
}
fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let mut options = Options::default();
    let result=match args.first().map(String::as_str) {
        Some("compare") if args.len()>=3=>{
            let mut i=3;while i<args.len(){match args[i].as_str(){"--theta"=>{i+=1;options.theta=args.get(i).ok_or("missing theta")?.parse()?},"--level"=>{i+=1;options.level=match args.get(i).map(String::as_str){Some("sen")=>Level::Sen,Some("word")=>Level::Word,Some("char")=>Level::Char,_=>return Err("invalid level".into())}},"--json"=>{},_=>return Err("unknown compare option".into())}i+=1;}
            serde_json::to_value(diff(&tree(&args[1])?,&tree(&args[2])?,&options)?)?
        }
        Some("merge3") if args.len()==4=>serde_json::to_value(marekvs_diff::merge3(&tree(&args[1])?,&tree(&args[2])?,&tree(&args[3])?,&options)?)?,
        Some("apply") if args.len()==5 && args[3]=="--accept"=>{
            let a=tree(&args[1])?;let graph:Graph=serde_json::from_slice(&fs::read(&args[2])?)?;
            let accepted=match args[4].as_str(){"all"=>Accepted::all(&graph),"none"=>Accepted::none(),s=>s.split(',').map(|s|ChangeId(s.into())).collect()};
            apply(&a,&plan(&a,&graph,&accepted)?).to_json()
        }
        _=>return Err("usage: diffcli compare A.json B.json [--theta N] [--level sen|word|char] [--json]\n       diffcli apply A.json graph.json --accept all|none|id,id".into()),
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("diffcli: {error}");
        std::process::exit(1);
    }
}
