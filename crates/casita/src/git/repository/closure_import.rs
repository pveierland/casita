//! Inventory-free Git closure ingestion. Stored records prove their own native
//! identities; only a validated closure proves it is safe to stop descending.

use std::collections::{BTreeSet, VecDeque};

use futures::TryStreamExt;

use crate::blob::BlobStore;
use crate::git::git_key_parts;
use crate::importers::{GitClosureImport, GitClosureImportError, GitClosureImportReport};
use crate::metadata::MetadataStore;
use crate::repository::{MutationSession, RepositoryError};
use crate::spill::{SpillSet, TraversalQueue};

const FRONTIER: usize = 256;

type Result<T> = std::result::Result<T, GitClosureImportError>;

fn source_error(error: impl std::fmt::Display) -> GitClosureImportError {
    GitClosureImportError::Source(error.to_string())
}

mod workers;
use workers::SourcePool;

pub(crate) async fn import<PS: BlobStore, SS: MetadataStore>(
    session: &MutationSession<'_, PS, SS>,
    request: &GitClosureImport,
) -> Result<GitClosureImportReport> {
    let repository = session.repository();
    let limits = repository.limits();
    if limits.max_batch_objects == 0 {
        return Err(RepositoryError::LimitExceeded(
            "Git closure import requires a nonzero publication batch limit".into(),
        )
        .into());
    }
    if request.roots.len() > limits.max_traversal_objects {
        return Err(RepositoryError::LimitExceeded(
            "Git root selection exceeds traversal limit".into(),
        )
        .into());
    }
    let mut format = None;
    for key in &request.roots {
        let (current, _, _) = git_key_parts(key)?;
        if format.is_some_and(|previous| previous != current) {
            return Err(GitClosureImportError::InvalidSelection(
                "mixed Git hash formats",
            ));
        }
        format = Some(current);
    }
    let mut report = GitClosureImportReport {
        roots: request.roots.clone(),
        ..Default::default()
    };
    let Some(format) = format else {
        return Ok(report);
    };
    // Protect the existing graph before inspecting records. New objects are
    // protected by the mutation's staging pin; both protections overlap until
    // selected roots and all their now-complete links have been retained.
    let hold = repository.retention_hold().await?;
    let snapshot = hold.snapshot();
    let area = repository.spill_area();
    let mut visited = SpillSet::new(area.clone(), "git-import-visited");
    let mut unsettled = SpillSet::new(area.clone(), "git-import-unsettled");
    let mut queue = TraversalQueue::new(area.clone());
    for root in &request.roots {
        queue
            .push((None, root.clone()))
            .await
            .map_err(RepositoryError::from)?;
    }
    let mut source: Option<SourcePool> = None;
    let mut staged = Vec::new();
    let mut staged_links = 0usize;
    loop {
        let mut keys = Vec::new();
        while keys.len() < FRONTIER {
            let Some((_, key)) = queue.pop().await.map_err(RepositoryError::from)? else {
                break;
            };
            keys.push(key);
        }
        if keys.is_empty() {
            break;
        }
        let fresh = visited
            .insert_batch(&keys)
            .await
            .map_err(RepositoryError::from)?;
        keys = keys
            .into_iter()
            .zip(fresh)
            .filter_map(|(key, fresh)| fresh.then_some(key))
            .collect();
        if visited.len() > limits.max_traversal_objects {
            return Err(RepositoryError::LimitExceeded(format!(
                "Git closure traversal exceeded {} objects",
                limits.max_traversal_objects
            ))
            .into());
        }
        // Complete boundaries need only a presence/witness probe, not their
        // potentially huge link arrays. Persistent backends answer this from
        // payload-summary columns without decoding the object record.
        let complete = snapshot
            .validated_payload_batch(&keys)
            .await
            .map_err(RepositoryError::from)?;
        let mut unresolved = Vec::new();
        for (key, complete) in keys.into_iter().zip(complete) {
            if complete.is_some() {
                report.reused_objects += 1;
            } else {
                unresolved.push(key);
            }
        }
        let records = snapshot
            .object_batch(&unresolved)
            .await
            .map_err(RepositoryError::from)?;
        let mut missing = VecDeque::new();
        for (key, record) in unresolved.into_iter().zip(records) {
            if let Some(record) = record {
                report.reused_objects += 1;
                for child in record.links() {
                    queue
                        .push((None, child.clone()))
                        .await
                        .map_err(RepositoryError::from)?;
                }
            } else {
                missing.push_back(key.clone());
            }
            unsettled.insert(key).await.map_err(RepositoryError::from)?;
        }
        while !missing.is_empty() {
            let window = workers::stage_window(
                session,
                source.take(),
                missing,
                request,
                format,
                limits.clone(),
                area.clone(),
            )
            .await?;
            source = Some(window.source);
            missing = window.pending;
            report.peak_source_bytes = report.peak_source_bytes.max(window.bytes);
            report.peak_decode_workers = report.peak_decode_workers.max(window.peak_workers);
            report.imported_objects += window.objects.len();
            report.spilled_delta_objects += window.spilled_delta_objects;
            report.source_bytes = report
                .source_bytes
                .checked_add(window.bytes)
                .ok_or_else(|| source_error("imported byte count overflow"))?;
            // All window writers have drained before publication, which can
            // flush payloads and must not prevent an active writer being polled.
            for object in window.objects {
                for child in object.record().links() {
                    queue
                        .push((None, child.clone()))
                        .await
                        .map_err(RepositoryError::from)?;
                }
                staged_links += object.record().links().len();
                staged.push(object);
                if staged.len() == limits.max_batch_objects
                    || staged_links >= super::MAX_GIT_IMPORT_BATCH_LINKS
                {
                    session
                        .publish_unrooted(std::mem::take(&mut staged))
                        .await?;
                    staged_links = 0;
                }
            }
        }
    }
    if !staged.is_empty() {
        session.publish_unrooted(staged).await?;
    }
    // Only now does every discovered native record have all its canonical
    // dependencies. Native Git formats have no additional verify_links rules.
    // Mark in bounded batches without rereading payloads or retaining an O(N)
    // in-memory inventory. A failed discovery cannot publish any false marks.
    session
        .retain_objects(request.roots.iter().cloned())
        .await?;
    let mut keys = Box::pin(unsettled.into_stream());
    loop {
        let mut batch = BTreeSet::new();
        while batch.len() < limits.max_batch_objects {
            let Some(key) = keys.try_next().await.map_err(RepositoryError::from)? else {
                break;
            };
            batch.insert(key);
        }
        if batch.is_empty() {
            break;
        }
        session.publish_git_closure_witnesses(batch).await?;
    }
    report.peak_spill_bytes = area.metrics().peak_bytes;
    Ok(report)
}
