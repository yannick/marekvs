#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_offsets_and_hard_break_tokens() {
        assert_eq!(
            tokenize("Héllo wörld\nnext"),
            vec![
                Token { start: 0, end: 5 },
                Token { start: 6, end: 11 },
                Token { start: 11, end: 12 },
                Token { start: 12, end: 16 }
            ]
        );
    }
    #[test]
    fn exact_text_roundtrips_in_all_modes() {
        for (a, b) in [
            (
                "This Agreement shall commence.",
                "This Agreement may commence.",
            ),
            ("a b c d", "a x c d e"),
            ("  α\tβ\n", " α\tγ\n\n"),
            ("", "héllo"),
            ("abc", ""),
            ("café", "cafés"),
            (" a ", "a"),
        ] {
            for level in [Level::Sen, Level::Word, Level::Char] {
                assert_eq!(apply_edits(a, &diff_text(a, b, level)).unwrap(), b);
            }
        }
    }
    #[test]
    fn char_refinement_and_disjoint_word_ranges() {
        assert_eq!(
            diff_text("café", "cafés", Level::Char),
            vec![Edit {
                at: 4,
                del: 0,
                ins: "s".into()
            }]
        );
        let edits = diff_text("a b c d", "a x c y", Level::Word);
        assert_eq!(edits.len(), 2);
        assert_eq!(apply_edits("a b c d", &edits).unwrap(), "a x c y");
    }
    #[test]
    fn rejects_overlapping_and_out_of_bounds_edits() {
        assert!(apply_edits(
            "ab",
            &[Edit {
                at: 3,
                del: 0,
                ins: String::new()
            }]
        )
        .is_err());
        assert!(apply_edits(
            "abc",
            &[
                Edit {
                    at: 0,
                    del: 2,
                    ins: String::new()
                },
                Edit {
                    at: 1,
                    del: 1,
                    ins: String::new()
                }
            ]
        )
        .is_err());
    }
    #[test]
    fn nd_exhaustion_is_lossless_and_cancellation_is_observed() {
        let b = Budget {
            max_nd: 1,
            ..Budget::default()
        };
        let edits =
            diff_text_bounded("a b c", "x y z", Level::Word, &b, &CancelToken::none()).unwrap();
        assert_eq!(apply_edits("a b c", &edits).unwrap(), "x y z");
        let cancel = CancelToken::none();
        cancel.cancel();
        assert!(diff_text_bounded("a", "b", Level::Char, &b, &cancel).is_err());
    }
}

use crate::{Attrs, Budget, CancelToken, DiffError, Level, Run};
use serde::{Deserialize, Serialize};

/// Display token coordinates are Unicode scalar values, including hard breaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Token {
    pub start: u32,
    pub end: u32,
}
/// Executable edits use Unicode scalar coordinates in the immutable base text.
/// Unlike token indices these can exactly represent code whitespace and char mode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edit {
    pub at: u32,
    pub del: u32,
    pub ins: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextError(pub &'static str);
impl std::fmt::Display for TextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for TextError {}

pub fn tokenize(text: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, ch) in text.chars().enumerate() {
        let i = i as u32;
        if ch.is_whitespace() {
            if let Some(s) = start.take() {
                out.push(Token { start: s, end: i });
            }
            if ch == '\n' {
                out.push(Token {
                    start: i,
                    end: i + 1,
                });
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(s) = start {
        out.push(Token {
            start: s,
            end: text.chars().count() as u32,
        });
    }
    out
}

fn units(chars: &[char], level: Level) -> Vec<(usize, usize)> {
    if level == Level::Char {
        return (0..chars.len()).map(|i| (i, i + 1)).collect();
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let start = i;
        i += 1;
        if chars[start] != '\n' {
            while i < chars.len()
                && chars[i] != '\n'
                && chars[i].is_whitespace() == chars[start].is_whitespace()
            {
                i += 1;
            }
        }
        out.push((start, i));
    }
    out
}

/// Bounded Myers shortest-edit-path. A None result requests a lossless coarse
/// replacement when the trace/work budget is exhausted.
fn matching_units(
    a: &[char],
    b: &[char],
    ua: &[(usize, usize)],
    ub: &[(usize, usize)],
    budget: &Budget,
    cancel: &CancelToken,
) -> Result<Option<Vec<(usize, usize)>>, DiffError> {
    let max = ua.len() + ub.len();
    if max == 0 {
        return Ok(Some(Vec::new()));
    }
    if max.saturating_mul(2).saturating_add(3) > budget.max_nd {
        return Ok(None);
    }
    let offset = max as isize + 1;
    let mut v = vec![0isize; 2 * max + 3];
    let mut trace = Vec::new();
    let mut work = 0usize;
    for d in 0..=max {
        cancel.check()?;
        // The trace itself is charged; no O(ND) unbounded allocation.
        if work.saturating_add(v.len()) > budget.max_nd {
            return Ok(None);
        }
        work += v.len();
        trace.push(v.clone());
        let di = d as isize;
        for k in (-di..=di).step_by(2) {
            cancel.check()?;
            let idx = (offset + k) as usize;
            let mut x = if k == -di || (k != di && v[idx - 1] < v[idx + 1]) {
                v[idx + 1]
            } else {
                v[idx - 1] + 1
            };
            let mut y = x - k;
            while x < (ua.len() as isize)
                && y < (ub.len() as isize)
                && a[ua[x as usize].0..ua[x as usize].1] == b[ub[y as usize].0..ub[y as usize].1]
            {
                x += 1;
                y += 1;
                work += 1;
                if work > budget.max_nd {
                    return Ok(None);
                }
                if work.is_multiple_of(1024) {
                    cancel.check()?;
                }
            }
            v[idx] = x;
            if x >= ua.len() as isize && y >= ub.len() as isize {
                let (mut x, mut y) = (ua.len() as isize, ub.len() as isize);
                let mut pairs = Vec::new();
                for depth in (0..=d).rev() {
                    cancel.check()?;
                    let old = &trace[depth];
                    let k = x - y;
                    let dd = depth as isize;
                    let idx = (offset + k) as usize;
                    let prev_k = if k == -dd || (k != dd && old[idx - 1] < old[idx + 1]) {
                        k + 1
                    } else {
                        k - 1
                    };
                    let prev_x = old[(offset + prev_k) as usize];
                    let prev_y = prev_x - prev_k;
                    while x > prev_x && y > prev_y {
                        x -= 1;
                        y -= 1;
                        pairs.push((x as usize, y as usize));
                    }
                    if depth > 0 {
                        x = prev_x;
                        y = prev_y;
                    }
                }
                pairs.reverse();
                return Ok(Some(pairs));
            }
        }
    }
    Ok(None)
}

pub fn diff_text(old: &str, new: &str, level: Level) -> Vec<Edit> {
    // Inputs to this convenience API are already model-bounded in production.
    diff_text_bounded(old, new, level, &Budget::default(), &CancelToken::none()).unwrap_or_else(
        |_| {
            vec![Edit {
                at: 0,
                del: old.chars().count() as u32,
                ins: new.into(),
            }]
        },
    )
}

pub fn diff_text_bounded(
    old: &str,
    new: &str,
    level: Level,
    budget: &Budget,
    cancel: &CancelToken,
) -> Result<Vec<Edit>, DiffError> {
    cancel.check()?;
    for text in [old, new] {
        if text.len() > budget.max_bytes {
            return Err(DiffError::TooBig {
                bound: "text_bytes",
                n: text.len(),
                limit: budget.max_bytes,
            });
        }
    }
    if old == new {
        return Ok(Vec::new());
    }
    let old_chars = old.chars().count();
    let new_chars = new.chars().count();
    let full = || {
        vec![Edit {
            at: 0,
            del: old_chars as u32,
            ins: new.into(),
        }]
    };
    // Charge the worst-case characters, token coordinates and frontier before
    // allocating any of them. Large single-token code/text inputs otherwise
    // evade the token-count limit and expand into gigabytes in char mode.
    if level == Level::Sen || old_chars.saturating_add(new_chars) > budget.max_nd / 8 {
        return Ok(full());
    }
    cancel.check()?;
    let a: Vec<char> = old.chars().collect();
    let b: Vec<char> = new.chars().collect();
    let ua = units(&a, level);
    let ub = units(&b, level);
    let Some(pairs) = matching_units(&a, &b, &ua, &ub, budget, cancel)? else {
        return Ok(full());
    };
    let (mut pa, mut pb) = (0, 0);
    let mut edits = Vec::new();
    for (ai, bi) in pairs
        .into_iter()
        .chain(std::iter::once((ua.len(), ub.len())))
    {
        let start_a = ua.get(ai).map_or(a.len(), |u| u.0);
        let start_b = ub.get(bi).map_or(b.len(), |u| u.0);
        if start_a > pa || start_b > pb {
            edits.push(Edit {
                at: pa as u32,
                del: (start_a - pa) as u32,
                ins: b[pb..start_b].iter().collect(),
            });
        }
        pa = ua.get(ai).map_or(a.len(), |u| u.1);
        pb = ub.get(bi).map_or(b.len(), |u| u.1);
    }
    Ok(edits)
}

pub fn apply_edits(old: &str, edits: &[Edit]) -> Result<String, TextError> {
    let mut chars = old.chars();
    let mut out = String::new();
    let mut cursor = 0usize;
    let mut previous = None;
    for edit in edits {
        let at = edit.at as usize;
        let end = at
            .checked_add(edit.del as usize)
            .ok_or(TextError("edit overflow"))?;
        if at < cursor || previous == Some(at) {
            return Err(TextError("overlapping edits"));
        }
        for _ in cursor..at {
            out.push(chars.next().ok_or(TextError("out-of-bounds edit"))?);
        }
        for _ in at..end {
            chars.next().ok_or(TextError("out-of-bounds edit"))?;
        }
        out.push_str(&edit.ins);
        cursor = end;
        previous = Some(at);
    }
    out.extend(chars);
    Ok(out)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FmtDelta {
    pub old: Vec<Run>,
    pub new: Vec<Run>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenSpan {
    pub first: u32,
    pub last: u32,
    pub start_offset: u32,
    pub end_offset: u32,
    pub approx: bool,
    pub attrs: Attrs,
}
impl FmtDelta {
    pub fn token_spans(&self, new_text: &str) -> Vec<TokenSpan> {
        let tokens = tokenize(new_text);
        self.new
            .iter()
            .filter_map(|r| {
                let first = tokens
                    .iter()
                    .position(|t| t.end > r.start && t.start < r.end)?;
                let last = tokens
                    .iter()
                    .rposition(|t| t.end > r.start && t.start < r.end)?;
                Some(TokenSpan {
                    first: first as u32,
                    last: last as u32 + 1,
                    start_offset: r.start.saturating_sub(tokens[first].start),
                    end_offset: r.end.saturating_sub(tokens[last].start),
                    approx: r.start > tokens[first].start || r.end < tokens[last].end,
                    attrs: r.attrs.clone(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod allocation_regressions {
    use super::*;
    #[test]
    fn char_work_limit_selects_coarse_edit_before_expansion() {
        let old = "a".repeat(1000);
        let new = format!("{old}b");
        let budget = Budget {
            max_nd: 10000,
            ..Default::default()
        };
        let edits =
            diff_text_bounded(&old, &new, Level::Char, &budget, &CancelToken::none()).unwrap();
        assert_eq!(
            edits,
            vec![Edit {
                at: 0,
                del: 1000,
                ins: new
            }]
        );
    }
}
