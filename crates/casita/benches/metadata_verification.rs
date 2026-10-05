//! Portable bounded metadata verification, with the previous helper as a control.
use casita::ObjectKey;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Bounded metadata materialization uses the same path as directory verification.
/// Include empty payloads and both sides of its 64 KiB scratch-buffer boundary.
fn metadata_verification_buffers(c: &mut Criterion) {
    use casita::experimental::{BlobId, FormatError, PayloadReader, VerificationContext};

    async fn direct_metadata(
        context: &mut VerificationContext<'_>,
        limit: u64,
    ) -> Result<Vec<u8>, FormatError> {
        const MAX_READ: usize = 64 * 1024;
        let hint = context.exact_len();
        let initial = hint
            .unwrap_or(MAX_READ as u64)
            .min(limit)
            .min(MAX_READ as u64) as usize;
        // Read into the returned allocation directly. Small known payloads
        // need neither a 64 KiB scratch allocation nor a second copy.
        let mut output = vec![0; initial];
        let mut used = 0;
        loop {
            if used == output.len() {
                // A length hint is only an allocation hint. Probe EOF before
                // growing, even when the declared length or limit is reached.
                let mut probe = [0];
                if context.read(&mut probe).await? == 0 {
                    return Ok(output);
                }
                let remaining = limit.saturating_sub(used as u64);
                if remaining == 0 {
                    return Err(FormatError::MetadataLimit { limit });
                }
                let hinted_remaining = hint.unwrap_or(0).saturating_sub(used as u64);
                let growth = if hinted_remaining > 0 {
                    hinted_remaining.min(MAX_READ as u64)
                } else {
                    used.clamp(1, MAX_READ) as u64
                }
                .min(remaining) as usize;
                let new_len = used
                    .checked_add(growth)
                    .ok_or(FormatError::PayloadSizeOverflow)?;
                if new_len > output.capacity() {
                    // Bound geometric growth explicitly: Vec's usual reserve
                    // may cross the limit, including its minimum allocation.
                    let capacity = output
                        .capacity()
                        .saturating_mul(2)
                        .max(new_len)
                        .min(usize::try_from(limit).unwrap_or(usize::MAX));
                    output.reserve_exact(capacity - output.len());
                }
                output.resize(new_len, 0);
                output[used] = probe[0];
                used += 1;
            } else {
                let end = output.len().min(used.saturating_add(MAX_READ));
                let read = context.read(&mut output[used..end]).await?;
                if read == 0 {
                    output.truncate(used);
                    return Ok(output);
                }
                used += read;
            }
        }
    }

    async fn hybrid_metadata(
        context: &mut VerificationContext<'_>,
        limit: u64,
    ) -> Result<Vec<u8>, FormatError> {
        fn append(output: &mut Vec<u8>, bytes: &[u8], limit: u64) -> Result<(), FormatError> {
            let length = output
                .len()
                .checked_add(bytes.len())
                .ok_or(FormatError::PayloadSizeOverflow)?;
            if length as u64 > limit {
                return Err(FormatError::MetadataLimit { limit });
            }
            if length > output.capacity() {
                let capacity = output
                    .capacity()
                    .saturating_mul(2)
                    .max(length)
                    .min(usize::try_from(limit).unwrap_or(usize::MAX));
                output.reserve_exact(capacity - output.len());
            }
            output.extend_from_slice(bytes);
            Ok(())
        }
        const MAX_READ: u64 = 64 * 1024;
        let mut output = Vec::new();
        if let Some(length) = context.exact_len().filter(|length| *length <= MAX_READ) {
            output = vec![0; length.min(limit) as usize];
            let mut used = 0;
            while used < output.len() {
                let read = context.read(&mut output[used..]).await?;
                if read == 0 {
                    output.truncate(used);
                    return Ok(output);
                }
                used += read;
            }
            let mut probe = [0];
            if context.read(&mut probe).await? == 0 {
                return Ok(output);
            }
            append(&mut output, &probe, limit)?;
        }
        let scratch = MAX_READ.min(limit.saturating_sub(output.len() as u64).saturating_add(1));
        let mut buffer = vec![0; scratch as usize];
        loop {
            let read = context.read(&mut buffer).await?;
            if read == 0 {
                return Ok(output);
            }
            append(&mut output, &buffer[..read], limit)?;
        }
    }

    // The former production helper, retained as a same-binary control.
    async fn scratch_metadata(
        context: &mut VerificationContext<'_>,
        limit: u64,
    ) -> Result<Vec<u8>, FormatError> {
        let mut output = Vec::new();
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = context.read(&mut buffer).await?;
            if read == 0 {
                return Ok(output);
            }
            let new_len = (output.len() as u64)
                .checked_add(read as u64)
                .ok_or(FormatError::PayloadSizeOverflow)?;
            if new_len > limit {
                return Err(FormatError::MetadataLimit { limit });
            }
            output.extend_from_slice(&buffer[..read]);
        }
    }

    async fn hinted_scratch_metadata(
        context: &mut VerificationContext<'_>,
        limit: u64,
    ) -> Result<Vec<u8>, FormatError> {
        let mut output = Vec::new();
        let size = context
            .exact_len()
            .unwrap_or(64 * 1024)
            .clamp(1, 64 * 1024)
            .min(limit.saturating_add(1));
        let mut buffer = vec![0u8; size as usize];
        loop {
            let read = context.read(&mut buffer).await?;
            if read == 0 {
                return Ok(output);
            }
            let new_len = (output.len() as u64)
                .checked_add(read as u64)
                .ok_or(FormatError::PayloadSizeOverflow)?;
            if new_len > limit {
                return Err(FormatError::MetadataLimit { limit });
            }
            if new_len > output.capacity() as u64 {
                let capacity = (output.capacity().saturating_mul(2) as u64)
                    .max(new_len)
                    .min(limit);
                let capacity =
                    usize::try_from(capacity).map_err(|_| FormatError::PayloadSizeOverflow)?;
                output.reserve_exact(capacity - output.len());
            }
            output.extend_from_slice(&buffer[..read]);
        }
    }

    struct Reader<'a> {
        bytes: &'a [u8],
        length: Option<u64>,
        largest_buffer: usize,
        eof: bool,
    }

    #[async_trait::async_trait]
    impl PayloadReader for Reader<'_> {
        fn exact_len(&self) -> Option<u64> {
            self.length
        }

        async fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            assert!(!buffer.is_empty());
            self.largest_buffer = self.largest_buffer.max(buffer.len());
            let count = std::io::Read::read(&mut self.bytes, buffer)?;
            self.eof |= count == 0;
            Ok(count)
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("metadata_verification_buffers");
    group.sample_size(10);
    for size in [0, 1, 128, 1024, 16384, 65535, 65536, 65537, 262144] {
        let mut data = vec![0; size];
        blake3::Hasher::new()
            .update(b"metadata-verification-benchmark")
            .finalize_xof()
            .fill(&mut data);
        let digest = blake3::hash(&data).into();
        let key = ObjectKey::blob(BlobId::new(digest));
        group.throughput(Throughput::Bytes(size as u64));
        for (kind, length) in [("known", Some(size as u64)), ("unknown", None)] {
            let mut implementations = [
                "scratch_64k",
                "direct",
                "hybrid",
                "hinted_scratch",
                "production",
            ];
            if std::env::var_os("CASITA_METADATA_READ_REVERSE").is_some() {
                implementations.reverse();
            }
            for implementation in implementations {
                let read = || async {
                    let mut reader = Reader {
                        bytes: &data,
                        length,
                        largest_buffer: 0,
                        eof: false,
                    };
                    let mut context = VerificationContext::new(&key, &mut reader);
                    let decoded = if implementation == "scratch_64k" {
                        scratch_metadata(&mut context, 256 * 1024 * 1024)
                            .await
                            .unwrap()
                    } else if implementation == "hinted_scratch" {
                        hinted_scratch_metadata(&mut context, 256 * 1024 * 1024)
                            .await
                            .unwrap()
                    } else if implementation == "hybrid" {
                        hybrid_metadata(&mut context, 256 * 1024 * 1024)
                            .await
                            .unwrap()
                    } else if implementation == "direct" {
                        direct_metadata(&mut context, 256 * 1024 * 1024)
                            .await
                            .unwrap()
                    } else {
                        context
                            .read_to_end_bounded(256 * 1024 * 1024)
                            .await
                            .unwrap()
                    };
                    assert_eq!(decoded, data);
                    assert_eq!(context.observed_digest(), digest);
                    assert_eq!(
                        context.finish(Vec::new()).unwrap().record().payload_size(),
                        size as u64
                    );
                    assert!(reader.eof);
                    assert!(reader.largest_buffer <= 65536);
                    reader.largest_buffer
                };
                let largest_buffer = runtime.block_on(read());
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "case": format!("metadata_verification_buffers/{kind}/{size}/{implementation}"),
                        "largest_read_buffer_bytes": largest_buffer, "correctness": "passed",
                    })
                );
                group.bench_function(
                    BenchmarkId::new(format!("{kind}/{size}"), implementation),
                    |b| b.to_async(&runtime).iter(read),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, metadata_verification_buffers);
criterion_main!(benches);
