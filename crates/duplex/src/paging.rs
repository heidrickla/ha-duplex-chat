//! Paging for tools that hand a session text or lists it reads once.
//!
//! A result cap cuts a field to fit and the cut data is gone. A tool that consumes data (a
//! message marked read, a task result delivered) must not hand it over to be cut, so it pages
//! itself inside the budget: what does not fit is named with an offset, and the data stays
//! readable in full from that offset through a tool that takes one. Strings and lists are
//! paged alike: a long string by characters, a long list by items.
//!
//! `fits` says whether a value goes out whole; the caller builds it from the final result size
//! over the redacted value, so a page that fits here is never cut afterwards. Text is paged
//! after redaction, so a secret-shaped value cannot be split across two pages.

use crate::cap::dumps;
use serde_json::{Map, Value, json};

pub const PREVIEW_CHARS: usize = 200;
pub const PAGING_KEY: &str = "text_paging";
/// What the paged field is left of the room, before other lists take what they can.
const TARGET_ROOM: usize = 2000;

/// Why a page cannot be made; the text is the refusal for the session.
#[derive(Debug, PartialEq)]
pub struct PageError(pub String);

impl std::fmt::Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PageError {}

fn no_room(what: &str) -> PageError {
    PageError(what.to_owned())
}

fn chars_of(text: &[char], from: usize, to: usize) -> String {
    text[from.min(text.len())..to.min(text.len())]
        .iter()
        .collect()
}

/// The longest page of `text[offset..]` that fits, and where the next one starts (None at the
/// end). Offsets count characters.
pub fn page(
    text: &str,
    offset: usize,
    fits: &dyn Fn(&str) -> bool,
) -> Result<(String, Option<usize>), PageError> {
    let chars: Vec<char> = text.chars().collect();
    let rest = chars.len().saturating_sub(offset);
    let whole = chars_of(&chars, offset, chars.len());
    if fits(&whole) {
        return Ok((whole, None));
    }
    if !fits("") {
        return Err(no_room("the result has no room for text"));
    }
    let (mut low, mut high) = (0usize, rest.saturating_sub(1));
    while low < high {
        let middle = (low + high).div_ceil(2);
        if fits(&chars_of(&chars, offset, offset + middle)) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    if low == 0 {
        return Err(no_room("the result has no room for a character of text"));
    }
    Ok((chars_of(&chars, offset, offset + low), Some(offset + low)))
}

/// `entry` with its `text` cut to the first page that fits, marked with the text's length and
/// where the next page starts. The rest is read by offset elsewhere.
pub fn paged_entry(entry: &Value, fits: &dyn Fn(&Value) -> bool) -> Result<Value, PageError> {
    let text = entry["text"].as_str().unwrap_or("").to_owned();
    let total = text.chars().count();
    let marked = |chunk: &str, following: Option<usize>| -> Value {
        let mut out = entry.as_object().cloned().unwrap_or_default();
        out.insert("text".into(), json!(chunk));
        out.insert("total_chars".into(), json!(total));
        out.insert("text_offset".into(), json!(0));
        if let Some(next) = following {
            out.insert("next_offset".into(), json!(next));
        }
        Value::Object(out)
    };
    let (chunk, following) = page(&text, 0, &|c| fits(&marked(c, Some(total))))?;
    Ok(marked(&chunk, following))
}

#[derive(Clone, Debug, PartialEq)]
enum Part {
    Key(String),
    Index(usize),
}

type Path = Vec<Part>;

fn field_name(path: &[Part]) -> String {
    let mut out = String::new();
    for part in path {
        match part {
            Part::Index(i) => out.push_str(&format!("[{i}]")),
            Part::Key(k) if out.is_empty() => out.push_str(k),
            Part::Key(k) => {
                out.push('.');
                out.push_str(k);
            }
        }
    }
    out
}

/// Every string and every list in `node`, with its path.
fn leaves(node: &Value, path: &Path, out: &mut Vec<(Path, Value)>) {
    match node {
        Value::String(_) => out.push((path.clone(), node.clone())),
        Value::Object(map) => {
            for (key, value) in map {
                let mut next = path.clone();
                next.push(Part::Key(key.clone()));
                leaves(value, &next, out);
            }
        }
        Value::Array(items) => {
            out.push((path.clone(), node.clone()));
            for (index, value) in items.iter().enumerate() {
                let mut next = path.clone();
                next.push(Part::Index(index));
                leaves(value, &next, out);
            }
        }
        _ => {}
    }
}

fn size(value: &Value) -> usize {
    dumps(value, None).len()
}

/// How one window of a view is drawn: the target (a string cut from `start` to `stop`, or a
/// list's items from `start` to `stop`), how long a string may be and how many items a list
/// may keep elsewhere.
struct Plan<'a> {
    view: &'a Value,
    target: &'a Path,
    preview: usize,
    ceiling: usize,
    start: usize,
    stop: usize,
}

impl Plan<'_> {
    /// The drawn copy and the notes naming everything shown in part.
    fn render(&self) -> (Value, Vec<Value>) {
        let mut notes = Vec::new();
        let drawn = self.draw(self.view, &Vec::new(), &mut notes);
        (drawn, notes)
    }

    fn draw(&self, node: &Value, path: &Path, notes: &mut Vec<Value>) -> Value {
        let name = field_name(path);
        match node {
            Value::String(text) => {
                let chars: Vec<char> = text.chars().collect();
                if path == self.target {
                    return json!(chars_of(&chars, self.start, self.stop));
                }
                if chars.len() > self.preview {
                    notes.push(
                        json!({"field": name, "total_chars": chars.len(), "shown": self.preview}),
                    );
                    return json!(chars_of(&chars, 0, self.preview));
                }
                node.clone()
            }
            Value::Object(map) => {
                let mut out = Map::new();
                for (key, value) in map {
                    let mut next = path.clone();
                    next.push(Part::Key(key.clone()));
                    out.insert(key.clone(), self.draw(value, &next, notes));
                }
                Value::Object(out)
            }
            Value::Array(items) => {
                let kept: Vec<usize> = if path == self.target {
                    (self.start..self.stop.min(items.len())).collect()
                } else if self.target.starts_with(path) && self.target.len() > path.len() {
                    // Holds the target: only its item.
                    let Part::Index(index) = self.target[path.len()] else {
                        return node.clone();
                    };
                    notes.push(
                        json!({"field": name, "total_items": items.len(), "only_item": index}),
                    );
                    vec![index]
                } else {
                    if items.len() > self.ceiling {
                        notes.push(json!({
                            "field": name,
                            "total_items": items.len(),
                            "shown": self.ceiling,
                            "next_offset": self.ceiling
                        }));
                    }
                    (0..self.ceiling.min(items.len())).collect()
                };
                Value::Array(
                    kept.into_iter()
                        .map(|i| {
                            let mut next = path.clone();
                            next.push(Part::Index(i));
                            self.draw(&items[i], &next, notes)
                        })
                        .collect(),
                )
            }
            other => other.clone(),
        }
    }
}

/// The largest n in `[low, high]` with `fits(n)`, assuming `fits(low)`; fits is taken to hold
/// up to some n and fail beyond.
fn largest(mut low: usize, mut high: usize, fits: &dyn Fn(usize) -> bool) -> usize {
    while low < high {
        let middle = (low + high).div_ceil(2);
        if fits(middle) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}

/// `view` as it fits. Whole when it fits and no offset is asked. Otherwise one field (`field`,
/// else the largest string or list) is shown from `offset` (a character of a string, an item
/// of a list) as far as the room goes, every other long string by its first characters and
/// every other long list by its first items; `text_paging` names each one shown in part, with
/// its total and where to continue. An error for an unknown field, an offset past the end and
/// a view with no room.
pub fn window(
    view: &Value,
    fits: &dyn Fn(&Value) -> bool,
    field: Option<&str>,
    offset: usize,
) -> Result<Value, PageError> {
    if offset == 0 && fits(view) {
        return Ok(view.clone());
    }
    let mut found = Vec::new();
    leaves(view, &Vec::new(), &mut found);
    let named: Vec<(String, Path, Value)> = found
        .into_iter()
        .map(|(path, value)| (field_name(&path), path, value))
        .collect();
    let chosen = match field {
        Some(name) => name.to_owned(),
        None => {
            // The first of the largest.
            let mut best: Option<(&str, usize)> = None;
            for (name, _, value) in &named {
                let s = size(value);
                if best.is_none_or(|(_, b)| s > b) {
                    best = Some((name, s));
                }
            }
            best.ok_or_else(|| no_room("the result has no text to page"))?
                .0
                .to_owned()
        }
    };
    // A repeated name is the last one, as a dict keyed by name keeps it.
    let (_, target, value) = named
        .iter()
        .rev()
        .find(|(name, _, _)| *name == chosen)
        .ok_or_else(|| {
            PageError(format!(
                "{chosen} is not a text or list field of this result."
            ))
        })?;
    let (is_text, total) = match value {
        Value::String(s) => (true, s.chars().count()),
        Value::Array(items) => (false, items.len()),
        _ => (false, 0),
    };
    if offset > total || (offset == total && offset != 0) {
        return Err(PageError(format!(
            "offset {offset} is at or past the end of {chosen}."
        )));
    }
    let longest = named
        .iter()
        .filter_map(|(_, _, v)| v.as_array().map(Vec::len))
        .max()
        .unwrap_or(0);

    let drawn = |plan: &Plan<'_>, widest: bool| -> Value {
        let (mut out, mut notes) = plan.render();
        let mut here = Map::new();
        here.insert("field".into(), json!(chosen));
        here.insert(
            (if is_text {
                "total_chars"
            } else {
                "total_items"
            })
            .into(),
            json!(total),
        );
        here.insert(
            (if is_text {
                "text_offset"
            } else {
                "item_offset"
            })
            .into(),
            json!(offset),
        );
        if plan.stop < total || widest {
            here.insert(
                "next_offset".into(),
                json!(if widest { total } else { plan.stop }),
            );
        }
        let only_this = notes.is_empty();
        notes.push(Value::Object(here));
        if !widest && plan.stop >= total && offset == 0 && only_this {
            return out; // the one long field whole, and nothing else cut
        }
        if let Value::Object(map) = &mut out {
            map.insert(PAGING_KEY.into(), Value::Array(notes));
        }
        out
    };
    let plan = |preview: usize, ceiling: usize, count: usize| Plan {
        view,
        target,
        preview,
        ceiling,
        start: offset,
        stop: offset + count,
    };

    let rest = total - offset;
    let mut picked: Option<(usize, usize)> = None;
    'reserve: for reserve in [TARGET_ROOM, 1] {
        // What the field itself must be left.
        for preview in [PREVIEW_CHARS, 0] {
            if fits(&drawn(&plan(preview, 0, rest.min(reserve)), true)) {
                picked = Some((preview, rest.min(reserve)));
                break 'reserve;
            }
        }
    }
    let Some((preview, least)) = picked else {
        return Err(no_room("the result has no room for what it must say"));
    };
    let ceiling = largest(0, longest, &|c| {
        fits(&drawn(&plan(preview, c, least), true))
    });
    let count = largest(least, rest, &|n| {
        fits(&drawn(&plan(preview, ceiling, n), true))
    });
    Ok(drawn(&plan(preview, ceiling, count), false))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A budget in the compact dump's length, standing in for the wire cap.
    fn within(limit: usize) -> impl Fn(&Value) -> bool {
        move |v| dumps(v, None).len() <= limit
    }

    #[test]
    fn text_is_cut_to_the_longest_page_that_fits_and_read_on_by_offset() {
        let text = "abcdefghij".repeat(30);
        let fits = |s: &str| s.len() <= 100;
        let (first, next) = page(&text, 0, &fits).unwrap();
        assert_eq!((first.len(), next), (100, Some(100)));
        let (last, end) = page(&text, 250, &fits).unwrap();
        assert_eq!((last.len(), end), (50, None));
        assert!(
            page("abc", 0, &|_| false).is_err(),
            "no room even for no text"
        );
        assert_eq!(
            page("abc", 0, &|s| s.is_empty()).unwrap_err().0,
            "the result has no room for a character of text"
        );
    }

    #[test]
    fn offsets_count_characters_not_bytes() {
        let text = "é".repeat(50);
        let (page_one, next) = page(&text, 0, &|s| s.chars().count() <= 20).unwrap();
        assert_eq!((page_one.chars().count(), next), (20, Some(20)));
    }

    #[test]
    fn a_long_entry_comes_as_its_first_page_with_its_length() {
        let entry = json!({"message_id": "m", "text": "x".repeat(500)});
        let out = paged_entry(&entry, &|e| dumps(e, None).len() <= 300).unwrap();
        assert_eq!(out["total_chars"], 500);
        assert_eq!(out["text_offset"], 0);
        let shown = out["text"].as_str().unwrap().len();
        assert_eq!(out["next_offset"], shown);
        assert!(dumps(&out, None).len() <= 300);
    }

    #[test]
    fn a_view_that_fits_is_returned_whole() {
        let view = json!({"a": "short", "b": [1, 2]});
        assert_eq!(window(&view, &within(1000), None, 0).unwrap(), view);
    }

    #[test]
    fn the_largest_field_is_paged_and_the_others_shown_by_their_start() {
        let view = json!({"note": "n".repeat(400), "text": "t".repeat(3000), "id": "1"});
        let out = window(&view, &within(2600), None, 0).unwrap();
        let paging = out["text_paging"].as_array().unwrap();
        assert!(
            paging
                .iter()
                .any(|n| n["field"] == "note" && n["shown"] == 200)
        );
        let main = paging.iter().find(|n| n["field"] == "text").unwrap();
        assert_eq!(main["total_chars"], 3000);
        let next = main["next_offset"].as_u64().unwrap() as usize;
        assert_eq!(out["text"].as_str().unwrap().len(), next);
        assert!(dumps(&out, None).len() <= 2600);
        // Read on from the offset until the end.
        let more = window(&view, &within(2600), Some("text"), next).unwrap();
        assert!(!more["text"].as_str().unwrap().is_empty());
        let mut at = next;
        let mut guard = 0;
        loop {
            let again = window(&view, &within(2600), Some("text"), at).unwrap();
            let main = again["text_paging"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["field"] == "text")
                .unwrap()
                .clone();
            match main.get("next_offset").and_then(Value::as_u64) {
                Some(n) => at = n as usize,
                None => break,
            }
            guard += 1;
            assert!(guard < 10);
        }
    }

    #[test]
    fn a_long_list_is_paged_by_items() {
        let items: Vec<Value> = (0..200)
            .map(|i| json!({"n": i, "pad": "p".repeat(40)}))
            .collect();
        let view = json!({"tasks": items, "total": 200});
        let out = window(&view, &within(3000), None, 0).unwrap();
        let shown = out["tasks"].as_array().unwrap().len();
        assert!(shown > 0 && shown < 200);
        let note = out["text_paging"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["field"] == "tasks")
            .unwrap()
            .clone();
        assert_eq!(note["total_items"], 200);
        assert_eq!(note["next_offset"], shown);
        let next = window(&view, &within(3000), Some("tasks"), shown).unwrap();
        assert_eq!(next["tasks"][0]["n"], shown);
    }

    #[test]
    fn refusals_name_the_problem() {
        let view = json!({"text": "t".repeat(3000)});
        assert_eq!(
            window(&view, &within(500), Some("nope"), 0).unwrap_err().0,
            "nope is not a text or list field of this result."
        );
        assert_eq!(
            window(&view, &within(500), Some("text"), 3000)
                .unwrap_err()
                .0,
            "offset 3000 is at or past the end of text."
        );
        assert!(window(&json!({"n": 1}), &|_| false, None, 0).is_err());
        assert!(
            window(&view, &within(5), None, 0).is_err(),
            "no room for what it must say"
        );
    }
}
