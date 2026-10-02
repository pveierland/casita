//! The only API adapter changed when measuring the pre-worker revision.
use casita::GitClosureImportReport;
use casita::import::GitClosureImport;

pub(super) fn select_workers(request: GitClosureImport, workers: usize) -> GitClosureImport {
    request.with_decode_workers(workers.try_into().unwrap())
}

pub(super) fn source_metrics(report: &GitClosureImportReport) -> (Option<u64>, Option<usize>) {
    (
        Some(report.peak_source_bytes),
        Some(report.peak_decode_workers),
    )
}
