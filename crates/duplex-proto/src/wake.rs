//! The line that wakes a session. Every component that wakes one sends this text.

/// The MCP server name `duplex setup` registers; the wake line names it.
pub const MCP_SERVER_NAME: &str = "duplex";

/// The fixed wake line: ids only, never message text or sender names, so the message
/// reaches the session through `bridge_receive` as tool data, not as a prompt.
pub fn wake_line(session: &str, message: &str, kind: &str) -> String {
    format!(
        "[duplex-chat] You've got mail for session {session}: message {message} is waiting in a \
         {kind} conversation. Read it with the {MCP_SERVER_NAME} tool bridge_receive and reply \
         with bridge_send."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_line_carries_only_ids_and_the_kind() {
        assert_eq!(
            wake_line("s1", "m1", "group"),
            "[duplex-chat] You've got mail for session s1: message m1 is waiting in a group \
             conversation. Read it with the duplex tool bridge_receive and reply with bridge_send."
        );
    }
}
