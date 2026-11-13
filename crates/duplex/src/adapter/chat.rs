//! What a session does with the broker: read its messages, send, look back, and work tasks.

use super::{Adapter, AdapterError, Pending, now};
use crate::log;
use crate::paging::paged_entry;
use crate::policy::{
    describe, receipt, recipients_seen, resolve_recipients, stale_note, task_rules,
};
use duplex_proto::BrokerError;
use duplex_proto::client::quote;
use duplex_proto::contract::DeliveryState;
use serde_json::{Map, Value, json};

/// Does this list of entries go out whole.
pub type Fits<'a> = &'a dyn Fn(&[Value]) -> bool;

fn str_of(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned()
}

fn truthy(value: Option<&Value>) -> bool {
    value.is_some_and(|v| !matches!(v, Value::Null | Value::Bool(false)) && v.as_str() != Some(""))
}

impl Adapter {
    /// What a delivery is to the session reading it.
    fn entry(&self, item: &Pending) -> Value {
        let actor = item.message["actor"].clone();
        let mut detail = item.conversation.clone();
        if str_of(&detail, "kind") == "group" {
            match self.conversation(&str_of(&detail, "id")) {
                Ok(fresh) => detail = fresh,
                Err(err) => log::warn(format!("conversation detail unavailable: {err}")),
            }
        }
        let title_of = |id: &str| -> Value {
            detail
                .get("members")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .find(|m| m.get("id").and_then(Value::as_str) == Some(id))
                .and_then(|m| m.get("title").cloned())
                .filter(|t| !t.is_null())
                .unwrap_or_else(|| json!(id))
        };
        let from = match item.message.get("from") {
            Some(from) if truthy(Some(from)) => from.clone(),
            _ => {
                let mut with_title = actor.as_object().cloned().unwrap_or_default();
                with_title.insert("title".into(), title_of(&str_of(&actor, "id")));
                Value::Object(with_title)
            }
        };
        let mut entry = Map::new();
        entry.insert(
            "conversation".into(),
            json!({"id": detail["id"], "kind": detail["kind"], "title": detail["title"]}),
        );
        entry.insert("message_id".into(), item.message["id"].clone());
        entry.insert("from".into(), from);
        entry.insert(
            "to".into(),
            item.message.get("to").cloned().unwrap_or(Value::Null),
        );
        entry.insert(
            "sent_at".into(),
            item.message.get("sent_at").cloned().unwrap_or(Value::Null),
        );
        entry.insert("text".into(), item.message["text"].clone());
        entry.insert(
            "reply_to".into(),
            item.message
                .get("parent_id")
                .cloned()
                .unwrap_or(Value::Null),
        );
        let notice = item
            .message
            .get("notice")
            .filter(|n| truthy(Some(n)))
            .cloned();
        if let Some(notice) = &notice {
            entry.insert("notice".into(), notice.clone());
        }
        let owner = self.owner_name();
        if detail.get("mode").is_some() {
            entry.insert(
                "policy".into(),
                describe(&detail, &self.binding_id(), now(), &owner),
            );
        }
        let kind = notice
            .as_ref()
            .map(|n| str_of(n, "kind"))
            .unwrap_or_default();
        if let Some(rules) = task_rules(&kind, &owner) {
            let mut policy = entry
                .get("policy")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            policy.insert("rules".into(), json!(rules));
            entry.insert("policy".into(), Value::Object(policy));
        }
        Value::Object(entry)
    }

    /// The waiting deliveries, each marked read as it is handed over. `annotate` sees each
    /// entry first. With `fits` (does this list go out whole), a delivery that would overflow
    /// stays waiting and unread for the next call; one that overflows alone is handed over as
    /// its first page, the rest of its text readable by offset. A delivery is reported read
    /// only after its entry has been composed and measured with everything handed over with
    /// it, so what the caller then sends is what was measured.
    pub fn receive(
        &self,
        fits: Option<Fits<'_>>,
        annotate: Option<&dyn Fn(&mut Value)>,
    ) -> Result<Vec<Value>, AdapterError> {
        let items: Vec<Pending> = {
            let _hand = self
                .handover
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut self.st().pending)
        };
        let whole = |shown: &[Value]| fits.is_none_or(|f| f(shown));
        let mut out: Vec<Value> = Vec::new();
        loop {
            let held = self.st().entries_held.first().cloned();
            let Some(held) = held else { break };
            let mut trial = out.clone();
            trial.push(held.clone());
            if !whole(&trial) {
                break;
            }
            self.st().entries_held.remove(0);
            out.push(held);
        }
        for (index, original) in items.iter().enumerate() {
            let mut item = original.clone();
            let step = (|| -> Result<Option<Value>, AdapterError> {
                let mut entry = self.entry(&item);
                if let Some(annotate) = annotate {
                    annotate(&mut entry);
                }
                let mut trial = out.clone();
                trial.push(entry.clone());
                if !whole(&trial) {
                    if !out.is_empty() {
                        return Ok(None);
                    }
                    if let Some(f) = fits {
                        entry = paged_entry(&entry, &|one| f(std::slice::from_ref(one)))?;
                    }
                }
                let closed = self.report_read(&item.delivery_id, &mut item.accepted)?;
                match closed {
                    Some(err) => {
                        // Closed unread (the wake ladder ran out): a pointer, never the text.
                        let pointer = self.stale(&item, &err);
                        let mut with = out.clone();
                        with.push(pointer.clone());
                        if whole(&with) {
                            out.push(pointer);
                        } else {
                            self.st().entries_held.push(pointer);
                        }
                        Ok(Some(Value::Null))
                    }
                    None => Ok(Some(entry)),
                }
            })();
            match step {
                Ok(Some(Value::Null)) => {}
                Ok(Some(entry)) => {
                    let actor = str_of(&item.message["actor"], "id");
                    self.st().consumed.insert(
                        str_of(&item.message, "id"),
                        (item.delivery_id.clone(), actor),
                    );
                    out.push(entry);
                }
                Ok(None) => {
                    self.keep(&items[index..]);
                    return Ok(out);
                }
                Err(err) => {
                    // The broker did not confirm the read: the delivery is still ours to hand
                    // over. Entries already read are returned first; the next call surfaces
                    // the error.
                    let rest: Vec<Pending> = std::iter::once(item.clone())
                        .chain(items[index + 1..].iter().cloned())
                        .collect();
                    self.keep(&rest);
                    if out.is_empty() {
                        return Err(err);
                    }
                    self.st().last_error = format!("read not confirmed: {err}");
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    /// Who has read this session's messages since the last call, each reported once. Never a
    /// wake: they are fetched when the session reads its messages. A broker without receipts,
    /// or one that cannot be reached, gives none. With `fits` the ones that would overflow wait
    /// in memory for the next call.
    pub fn receipts(&self, fits: Option<Fits<'_>>) -> Vec<Value> {
        let taken = match self.post("/api/adapter/receipts", None) {
            Ok(Value::Array(rows)) => rows,
            Ok(_) => Vec::new(),
            Err(err) => {
                log::info(format!("receipts unavailable: {err}"));
                Vec::new()
            }
        };
        let mut waiting: Vec<Value> = std::mem::take(&mut self.st().receipts_held);
        waiting.extend(taken);
        let mut out: Vec<Value> = Vec::new();
        while let Some(first) = waiting.first() {
            let mut trial = out.clone();
            trial.push(first.clone());
            if !fits.is_none_or(|f| f(&trial)) {
                break;
            }
            out.push(waiting.remove(0));
        }
        self.st().receipts_held = waiting;
        out
    }

    /// Puts deliveries back in line for the next receive, ahead of newer ones.
    fn keep(&self, items: &[Pending]) {
        let mut st = self.st();
        let newer = std::mem::take(&mut st.pending);
        let mut kept: Vec<Pending> = Vec::new();
        for item in items.iter().cloned().chain(newer) {
            if !kept.iter().any(|k| k.delivery_id == item.delivery_id) {
                kept.push(item);
            }
        }
        st.pending = kept;
    }

    /// Where a closed delivery's message is: id, sender and age, from the broker's late-read
    /// answer, or from what the adapter holds when the broker has none.
    fn stale(&self, item: &Pending, err: &BrokerError) -> Value {
        let (message, conversation) = (&item.message, &item.conversation);
        let actor = &message["actor"];
        let sender_default = {
            let mut with = actor.as_object().cloned().unwrap_or_default();
            with.insert(
                "title".into(),
                actor.get("id").cloned().unwrap_or(Value::Null),
            );
            Value::Object(with)
        };
        let age = message
            .get("created_at")
            .and_then(Value::as_f64)
            .map(|c| (now() - c).round() as i64);
        let mut pointer = Map::new();
        pointer.insert("message_id".into(), message["id"].clone());
        pointer.insert("conversation_id".into(), conversation["id"].clone());
        pointer.insert("conversation_kind".into(), conversation["kind"].clone());
        pointer.insert(
            "conversation_title".into(),
            conversation.get("title").cloned().unwrap_or(Value::Null),
        );
        pointer.insert(
            "from".into(),
            message
                .get("from")
                .filter(|f| truthy(Some(f)))
                .cloned()
                .unwrap_or(sender_default),
        );
        pointer.insert(
            "to".into(),
            message.get("to").cloned().unwrap_or(Value::Null),
        );
        pointer.insert(
            "sent_at".into(),
            message.get("sent_at").cloned().unwrap_or(Value::Null),
        );
        pointer.insert("age_seconds".into(), age.map_or(Value::Null, |a| json!(a)));
        pointer.insert("state".into(), json!("closed"));
        pointer.insert("reason".into(), json!(err.detail()));
        pointer.insert("history_after".into(), json!(0));
        match self.post(
            &format!("/api/adapter/deliveries/{}/late-read", item.delivery_id),
            None,
        ) {
            Ok(Value::Object(late)) => pointer.extend(late),
            Ok(_) => {}
            Err(late) => log::warn(format!("late read of {} refused: {late}", item.delivery_id)),
        }
        let pointer = Value::Object(pointer);
        let sender = &pointer["from"];
        let sender_name = sender
            .get("title")
            .filter(|t| truthy(Some(t)))
            .or_else(|| sender.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        json!({
            "stale": true,
            "conversation": {"id": pointer["conversation_id"], "kind": pointer["conversation_kind"], "title": pointer["conversation_title"]},
            "message_id": pointer["message_id"],
            "from": sender,
            "to": pointer["to"],
            "sent_at": pointer["sent_at"],
            "age_seconds": pointer["age_seconds"],
            "state": pointer["state"],
            "reason": pointer["reason"],
            "note": stale_note(pointer["state"].as_str().unwrap_or("closed"), pointer["conversation_id"].as_str().unwrap_or(""), &pointer["history_after"], &sender_name),
        })
    }

    /// Posts into a conversation. `to` names sessions to address (consult or collaborate
    /// only); without it the message is a reply to the group. `mode` is how the recipients
    /// take it: auto, queue, steer or restart.
    pub fn send(
        &self,
        conversation_id: &str,
        text: &str,
        reply_to: Option<&str>,
        to: Option<&[String]>,
        mode: Option<&str>,
    ) -> Result<Value, AdapterError> {
        let recipients: Option<Vec<String>> = match to {
            Some(names) => {
                let detail = self.conversation(conversation_id)?;
                let members = detail
                    .get("members")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                Some(
                    resolve_recipients(&members, names, &self.binding_id())
                        .map_err(AdapterError::Invalid)?,
                )
            }
            None => None,
        };
        let mut body = json!({
            "conversation_id": conversation_id,
            "client_message_id": client_message_id(),
            "text": text,
            "parent_id": reply_to,
        });
        if let Some(recipients) = recipients.as_ref().filter(|r| !r.is_empty()) {
            body["recipients"] = json!(recipients);
        }
        if let Some(mode) = mode {
            body["mode"] = json!(mode);
        }
        let result = self.post("/api/adapter/messages", Some(&body))?;
        // The delivery is answered when the reply reaches its sender: the owner sees every
        // group reply, and a session sees only what is addressed to it.
        let held = reply_to.and_then(|id| self.st().consumed.get(id).cloned());
        let listed = recipients.clone().unwrap_or_default();
        let answered = held
            .as_ref()
            .is_some_and(|(_, author)| listed.is_empty() || listed.contains(author));
        if let (Some((delivery, _)), true, Some(reply)) = (&held, answered, reply_to) {
            self.st().consumed.remove(reply);
            self.state(delivery, DeliveryState::Responded, None)?;
        }
        let mut sent = json!({
            "message_id": result["message"]["id"],
            "answered": answered,
            "recipients": listed,
        });
        if result.get("recipients_seen").is_some() {
            sent["delivered_to"] = json!(recipients_seen(&result, now()));
        }
        Ok(sent)
    }

    /// One message by id with each recipient's delivery: the receipt check.
    pub fn message(&self, message_id: &str) -> Result<Value, AdapterError> {
        let record = self.get(&format!("/api/adapter/messages/{message_id}"))?;
        Ok(receipt(&record, now()))
    }

    /// The messages among the next `limit` events after `after`, and the sequence number to ask
    /// again from when the broker may hold more (the batch was full) or None when it ended.
    pub fn history_batch(
        &self,
        conversation_id: &str,
        after: u64,
        limit: u64,
    ) -> Result<(Vec<Value>, Option<u64>), AdapterError> {
        let events = self.get(&format!(
            "/api/adapter/conversations/{conversation_id}/events?after={after}&limit={limit}"
        ))?;
        let events = events.as_array().cloned().unwrap_or_default();
        let more = (events.len() as u64 >= limit)
            .then(|| events.last().and_then(|e| e["seq"].as_u64()))
            .flatten();
        let messages = events
            .iter()
            .filter(|e| e["type"] == "message.accepted")
            .map(|e| {
                let payload = &e["payload"];
                json!({
                    "seq": e["seq"],
                    "from": payload.get("from").filter(|f| truthy(Some(f))).unwrap_or(&e["actor"]),
                    "to": payload.get("to"),
                    "sent_at": payload.get("sent_at"),
                    "text": payload.get("text"),
                    "message_id": e["message_id"],
                })
            })
            .collect();
        Ok((messages, more))
    }

    /// Hands a task to another session. `to` is its title or id.
    pub fn delegate(
        &self,
        to: &str,
        task: &str,
        done_when: &str,
        parent_task_id: Option<&str>,
        mode: Option<&str>,
    ) -> Result<Value, AdapterError> {
        let mut body = json!({"to": to, "task": task, "done_when": done_when});
        if let Some(parent) = parent_task_id {
            body["parent_task_id"] = json!(parent);
        }
        if let Some(mode) = mode {
            body["mode"] = json!(mode);
        }
        Ok(self.post("/api/adapter/tasks", Some(&body))?)
    }

    /// This session's open tasks in both roles, without results: a result is read with `task`,
    /// which is what marks it delivered.
    pub fn tasks(&self) -> Result<Vec<Value>, AdapterError> {
        let rows = self.get("/api/adapter/tasks?role=both&open=1")?;
        Ok(rows
            .as_array()
            .into_iter()
            .flatten()
            .map(|row| {
                let mut out: Map<String, Value> = row.as_object().cloned().unwrap_or_default();
                let has_result = out.remove("result").is_some_and(|r| !r.is_null());
                out.insert("has_result".into(), json!(has_result));
                Value::Object(out)
            })
            .collect())
    }

    pub fn task(&self, task_id: &str) -> Result<Value, AdapterError> {
        Ok(self.get(&format!("/api/adapter/tasks/{}", quote(task_id, "")))?)
    }

    fn task_post(&self, path: &str, body: Option<&Value>) -> Result<Value, AdapterError> {
        Ok(self.post(&format!("/api/adapter/{path}"), body)?)
    }

    pub fn cancel_task(&self, task_id: &str, reason: Option<&str>) -> Result<Value, AdapterError> {
        let body = reason.map(|r| json!({"reason": r}));
        self.task_post(
            &format!("tasks/{}/cancel", quote(task_id, "")),
            body.as_ref(),
        )
    }

    pub fn report_task(
        &self,
        task_id: &str,
        outcome: &str,
        result: &str,
    ) -> Result<Value, AdapterError> {
        self.task_post(
            &format!("tasks/{}/report", quote(task_id, "")),
            Some(&json!({"outcome": outcome, "result": result})),
        )
    }

    pub fn ask(&self, task_id: &str, question: &str) -> Result<Value, AdapterError> {
        self.task_post(
            &format!("tasks/{}/questions", quote(task_id, "")),
            Some(&json!({"question": question})),
        )
    }

    pub fn answer(&self, question_id: &str, answer: &str) -> Result<Value, AdapterError> {
        self.task_post(
            &format!("questions/{}/answer", quote(question_id, "")),
            Some(&json!({"answer": answer})),
        )
    }

    pub fn withdraw(&self, question_id: &str) -> Result<Value, AdapterError> {
        self.task_post(
            &format!("questions/{}/withdraw", quote(question_id, "")),
            None,
        )
    }
}

/// A fresh id for a sent message: 32 hex digits, like a UUID without hyphens.
fn client_message_id() -> String {
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seed = format!(
        "{nanos}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    Sha256::digest(seed.as_bytes())
        .iter()
        .take(16)
        .map(|b| format!("{b:02x}"))
        .collect()
}
