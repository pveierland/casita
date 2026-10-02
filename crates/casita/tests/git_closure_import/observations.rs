// Process counters bracket only import. Logical character counts include all
// import threads and these tiny snapshots; physical I/O can lag writeback.
// The fixture and audit run outside this interval.
pub(super) fn process_io() -> Option<std::collections::BTreeMap<String, u64>> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    let mut values = std::collections::BTreeMap::new();
    for line in text.lines() {
        let (name, value) = line.split_once(':')?;
        if matches!(
            name,
            "rchar" | "wchar" | "read_bytes" | "write_bytes" | "cancelled_write_bytes"
        ) {
            values.insert(name.to_owned(), value.trim().parse().ok()?);
        }
    }
    (values.len() == 5).then_some(values)
}

pub(super) fn io_delta(
    before: Option<std::collections::BTreeMap<String, u64>>,
    after: Option<std::collections::BTreeMap<String, u64>>,
) -> Option<std::collections::BTreeMap<String, u64>> {
    let (before, after) = before.zip(after)?;
    after
        .into_iter()
        .map(|(name, value)| {
            let difference = value.checked_sub(*before.get(&name)?)?;
            Some((name, difference))
        })
        .collect()
}

// Linux process totals include all threads, excluding child processes. Keep raw
// ticks: the runner records SC_CLK_TCK and must not imply finer resolution.
// Parse after the final ')' because the command name may contain spaces or ')'.
pub(super) fn process_cpu() -> Option<std::collections::BTreeMap<String, u64>> {
    let text = std::fs::read_to_string("/proc/self/stat").ok()?;
    let (_, fields) = text.rsplit_once(") ")?;
    let mut fields = fields.split_whitespace();
    let user = fields.nth(11)?.parse().ok()?;
    let system = fields.next()?.parse().ok()?;
    Some(std::collections::BTreeMap::from([
        ("user_ticks".to_owned(), user),
        ("system_ticks".to_owned(), system),
    ]))
}
