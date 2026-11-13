//! Message and conversation IDs as adapters see them (broker `ids.py`).

fn is_hex(c: char) -> bool {
    matches!(c, '0'..='9' | 'a'..='f')
}

/// 32 lowercase hex digits, the form the broker stores.
fn is_addressable(value: &str) -> bool {
    value.len() == 32 && value.chars().all(is_hex)
}

/// `8-4-4-4-12` lowercase hex, the form adapters show.
fn is_display(value: &str) -> bool {
    let parts: Vec<&str> = value.split('-').collect();
    parts.len() == 5
        && parts
            .iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(p, n)| p.len() == n)
        && value.chars().all(|c| c == '-' || is_hex(c))
}

/// The ID an adapter shows and takes back: a stored ID hyphenated like a UUID, which
/// older MCP redactors leave intact. Anything else is returned as it came.
pub fn reply_id(value: &str) -> String {
    if is_addressable(value) {
        format!(
            "{}-{}-{}-{}-{}",
            &value[..8],
            &value[8..12],
            &value[12..16],
            &value[16..20],
            &value[20..]
        )
    } else {
        value.to_owned()
    }
}

/// The inverse of `reply_id`: the broker's own form of an ID an adapter was shown.
pub fn stored_id(value: &str) -> String {
    if is_display(value) {
        value.replace('-', "")
    } else {
        value.to_owned()
    }
}

/// The indexes of the sessions a typed name picks out, from their (id, title) pairs. An id and
/// a title are different namespaces: the name is normalized as an id (a displayed ID loses its
/// hyphens) for the id match, and compared as typed for the title match, so a title shaped like
/// a UUID is still a title. More than one match is the caller's ambiguity to refuse.
pub fn match_sessions(sessions: &[(String, String)], name: &str) -> Vec<usize> {
    let as_id = stored_id(name);
    sessions
        .iter()
        .enumerate()
        .filter(|(_, (id, title))| *id == as_id || title == name)
        .map(|(i, _)| i)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_typed_name_is_an_id_or_a_title_and_a_uuid_shaped_title_is_still_a_title() {
        let id = "6058b30915944089b93e6d61672f5134".to_owned();
        let sessions = vec![
            (id.clone(), "Builder".to_owned()),
            (
                "11111111111111111111111111111111".to_owned(),
                "6058b309-1594-4089-b93e-6d61672f5134".to_owned(),
            ),
        ];
        assert_eq!(match_sessions(&sessions, "Builder"), [0]);
        assert_eq!(match_sessions(&sessions, &id), [0]);
        assert_eq!(
            match_sessions(&sessions, "6058b309-1594-4089-b93e-6d61672f5134"),
            [0, 1],
            "an id shown hyphenated, and a title spelled the same"
        );
        assert!(
            match_sessions(&sessions, "builder").is_empty(),
            "titles match as typed"
        );
    }

    const STORED: &str = "6058b30915944089b93e6d61672f5134";
    const SHOWN: &str = "6058b309-1594-4089-b93e-6d61672f5134";

    #[test]
    fn a_stored_id_is_hyphenated_and_back() {
        assert_eq!(reply_id(STORED), SHOWN);
        assert_eq!(stored_id(SHOWN), STORED);
        assert_eq!(stored_id(&reply_id(STORED)), STORED);
    }

    #[test]
    fn anything_else_passes_unchanged() {
        for other in [
            "",
            "abc",
            "6058B30915944089B93E6D61672F5134",
            "g058b30915944089b93e6d61672f5134",
        ] {
            assert_eq!(reply_id(other), other);
            assert_eq!(stored_id(other), other);
        }
        assert_eq!(
            stored_id("6058b309-1594-4089-b93e-6d61672f513"),
            "6058b309-1594-4089-b93e-6d61672f513"
        );
    }
}
