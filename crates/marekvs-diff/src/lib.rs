//! Structured document comparison over a canonical arena tree.
pub mod budget;
pub mod canonical;
pub mod hash;
pub mod model;
pub use budget::{Budget, CancelToken, DiffError};
pub use model::{Attrs, Kind, Lid, Node, NodeId, Run, Sid, Text, Tree};
pub const ALGO_VERSION: u32 = 2;

pub mod records;
pub mod textdiff;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Sen,
    Word,
    Char,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Weights {
    pub text: f32,
    pub children: f32,
    pub context: f32,
    pub format: f32,
    pub size: f32,
}
impl Default for Weights {
    fn default() -> Self {
        Self {
            text: 0.45,
            children: 0.25,
            context: 0.15,
            format: 0.05,
            size: 0.10,
        }
    }
}
#[derive(Clone, Debug)]
pub struct Options {
    pub level: Level,
    pub theta: f32,
    pub weights: Weights,
    pub bands: (u8, u8),
    pub budget: Budget,
    pub cancel: CancelToken,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            level: Level::Word,
            theta: 0.5,
            weights: Weights::default(),
            bands: (32, 4),
            budget: Budget::default(),
            cancel: CancelToken::none(),
        }
    }
}
impl Options {
    pub fn validate(&self) -> Result<(), DiffError> {
        let ws = [
            self.weights.text,
            self.weights.children,
            self.weights.context,
            self.weights.format,
            self.weights.size,
        ];
        if !self.theta.is_finite()
            || !(0.0..=1.0).contains(&self.theta)
            || ws.iter().any(|w| !w.is_finite() || *w < 0.0)
            || ws.iter().sum::<f32>() <= 0.0
            || ws.iter().sum::<f32>().is_infinite()
            || self.bands.0 == 0
            || self.bands.1 == 0
            || usize::from(self.bands.0) * usize::from(self.bands.1) != 128
        {
            return Err(DiffError::InvalidGraph("invalid comparison options".into()));
        }
        Ok(())
    }
    pub fn digest(&self) -> String {
        let sum = self.weights.text
            + self.weights.children
            + self.weights.context
            + self.weights.format
            + self.weights.size;
        let value = serde_json::json!({"level":self.level,"theta":self.theta,"weights":[self.weights.text/sum,self.weights.children/sum,self.weights.context/sum,self.weights.format/sum,self.weights.size/sum],"bands":self.bands,"max_candidates":self.budget.max_candidates,"max_nd":self.budget.max_nd,"max_changes":self.budget.max_changes});
        format!(
            "{:032x}",
            xxhash_rust::xxh3::xxh3_128(&serde_json::to_vec(&value).expect("options serialize"))
        )
    }
}
pub mod align;
pub mod classify;
pub mod exact;
pub mod graph;
pub mod matching;
pub mod near;
pub mod plan;
pub use graph::{Change, ChangeId, Gid, Graph, Op};
pub use plan::{apply, plan, Accepted, Plan, PlanError};

/// Compare immutable canonical trees. Timing/cancellation state never enters
/// serialized graph identity; algorithmic fallbacks and evidence do.
pub fn diff(a: &Tree, b: &Tree, o: &Options) -> Result<Graph, DiffError> {
    diff_with_matching(a, b, o).map(|(graph, _)| graph)
}

/// Comparison plus scan-local correspondence for identity-preserving import.
pub fn diff_with_matching(
    a: &Tree,
    b: &Tree,
    o: &Options,
) -> Result<(Graph, matching::Matching), DiffError> {
    o.validate()?;
    o.cancel.check()?;
    let mut matching = matching::Matching::new(a, b);
    exact::run(a, b, &mut matching, &o.cancel)?;
    let near = near::run(a, b, &mut matching, o)?;
    let aligned = align::run(a, b, &mut matching, &o.budget, &o.cancel)?;
    let mut graph = graph::build(a, b, &matching, o)?;
    for (a, _) in matching.pairs() {
        match matching.layer(a) {
            Some(matching::Layer::I0) => graph.stats.i0 += 1,
            Some(matching::Layer::I1Exact | matching::Layer::I1Content) => graph.stats.i1 += 1,
            Some(matching::Layer::I2) => graph.stats.i2 += 1,
            Some(matching::Layer::I3) => graph.stats.i3 += 1,
            None => {}
        }
    }
    graph.stats.candidates = near.candidates;
    graph.stats.fallbacks.extend(near.fallbacks);
    graph.stats.fallbacks.extend(aligned.fallbacks);
    graph.ensure_bounded(o)?;
    graph.rehash();
    o.cancel.check()?;
    Ok((graph, matching))
}
pub mod merge3;
pub use merge3::{merge3, MergeGraph, Side};
