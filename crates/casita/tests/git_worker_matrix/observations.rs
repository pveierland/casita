//! Linux parent-process observations, excluding fixture subprocesses.
use std::collections::BTreeMap;

// All threads, excluding children. Raw clock ticks avoid false precision.
// The command name may contain spaces or parentheses.
pub(super) fn process_cpu() -> Option<BTreeMap<String, u64>> {
    let text = std::fs::read_to_string("/proc/self/stat").ok()?;
    let (_, fields) = text.rsplit_once(") ")?;
    let mut fields = fields.split_whitespace();
    Some(BTreeMap::from([
        ("user_ticks".to_owned(), fields.nth(11)?.parse().ok()?),
        ("system_ticks".to_owned(), fields.next()?.parse().ok()?),
    ]))
}

pub(super) fn cpu_delta(
    before: Option<BTreeMap<String, u64>>,
    after: Option<BTreeMap<String, u64>>,
) -> Option<BTreeMap<String, u64>> {
    let (before, after) = before.zip(after)?;
    after
        .into_iter()
        .map(|(key, value)| {
            let difference = value.checked_sub(*before.get(&key)?)?;
            Some((key, difference))
        })
        .collect()
}
