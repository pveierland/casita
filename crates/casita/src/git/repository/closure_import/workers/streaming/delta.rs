//! Best-effort, file-backed blob delta reconstruction. No complete base,
//! instruction stream or result is materialized in a Vec.

use super::{BUFFER_BYTES, Control, HEADER_BYTES, Inflater, Input, Locator, State};
use crate::git::{GitObjectFormat, NativeHasher};
use crate::spill::{SpillArea, SpillPayload};
use gix::odb::pack::data::entry::Header;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub(in crate::git::repository::closure_import::workers) const MAX_SOURCE_FILES: usize = 128;
#[derive(Debug)]
struct HandleWindowFull;
impl std::fmt::Display for HandleWindowFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Git delta source-handle window is full")
    }
}
impl std::error::Error for HandleWindowFull {}
pub(in crate::git::repository::closure_import::workers) fn handle_window_full(
    error: &io::Error,
) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.is::<HandleWindowFull>())
}
const MAX_DELTAS: usize = 64;
const MAX_WORK_BYTES: u64 = 64 * 1024 * 1024 * 1024;

fn limit(message: impl Into<String>) -> io::Error {
    io::Error::other(crate::error::Error::LimitExceeded(message.into()))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

struct Work {
    control: Arc<Control>,
    remaining: AtomicU64,
}
impl Work {
    fn charge(&self, bytes: u64) -> io::Result<()> {
        if self.control.cancelled.load(Ordering::Relaxed) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "Git delta reconstruction cancelled",
            ));
        }
        self.remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(bytes)
            })
            .map_err(|_| limit("Git delta reconstruction exceeded its aggregate work limit"))?;
        Ok(())
    }
}

struct Reader {
    state: State,
    work: Arc<Work>,
}
impl Read for Reader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            self.work.charge(0)?;
            if self.state.cursor < self.state.filled {
                let count = output.len().min(self.state.filled - self.state.cursor);
                output[..count].copy_from_slice(
                    &self.state.output[self.state.cursor..self.state.cursor + count],
                );
                self.state.cursor += count;
                return Ok(count);
            }
            if self.state.inflater.finished {
                return Ok(0);
            }
            let before = self.state.inflater.inflate.total_in();
            self.state.step()?;
            self.work.charge(
                self.state.inflater.inflate.total_in() - before + self.state.filled as u64,
            )?;
        }
    }
}

// A plan's nodes share stable source ownership. Separate plans never share
// seek positions; a reader duplicates at most one handle per active source job.
struct SourceFile {
    file: File,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
struct Files {
    slots: Arc<tokio::sync::Semaphore>,
    open: BTreeMap<PathBuf, Arc<SourceFile>>,
}
impl Files {
    fn get(&mut self, path: &std::path::Path) -> io::Result<Option<Arc<SourceFile>>> {
        if let Some(file) = self.open.get(path) {
            return Ok(Some(file.clone()));
        }
        match path.metadata() {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        }
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, HandleWindowFull))?;
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if !file.metadata()?.is_file() {
            return Ok(None);
        }
        let file = Arc::new(SourceFile {
            file,
            _permit: permit,
        });
        self.open.insert(path.to_owned(), file.clone());
        Ok(Some(file))
    }
}

struct Node {
    file: Arc<SourceFile>,
    path: PathBuf,
    offset: u64,
    data_offset: u64,
    end: u64,
    header: Header,
    inflated_size: u64,
    result_size: u64,
    base_size: Option<u64>,
    loose: bool,
    oid: Option<gix::ObjectId>,
}
impl Node {
    fn packed(
        source: Arc<SourceFile>,
        path: PathBuf,
        offset: u64,
        hash_len: usize,
        oid: Option<gix::ObjectId>,
    ) -> io::Result<Self> {
        let mut file = source.file.try_clone()?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(invalid("Git pack is not a regular file"));
        }
        let end = metadata
            .len()
            .checked_sub(hash_len as u64)
            .ok_or_else(|| invalid("truncated Git pack"))?;
        if offset < 12 || offset >= end {
            return Err(invalid("Git delta pack offset is outside its pack"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0; 12];
        file.read_exact(&mut header)?;
        gix::odb::pack::data::header::decode(&header).map_err(io::Error::other)?;
        file.seek(SeekFrom::Start(offset))?;
        let entry = gix::odb::pack::data::Entry::from_read(
            &mut (&mut file).take((end - offset).min(HEADER_BYTES as u64)),
            offset,
            hash_len,
        )?;
        if entry.data_offset >= end {
            return Err(invalid("Git delta entry has no compressed payload"));
        }
        Ok(Self {
            file: source,
            path,
            offset,
            data_offset: entry.data_offset,
            end,
            header: entry.header,
            inflated_size: entry.decompressed_size,
            result_size: entry.decompressed_size,
            base_size: None,
            loose: false,
            oid,
        })
    }

    fn validate_prefix(&self, parents: &[Node], declared_work: u64) -> io::Result<()> {
        if declared_work > MAX_WORK_BYTES {
            return Err(limit("Git delta declared work exceeds 64 GiB"));
        }
        if let Some(parent) = parents.last()
            && parent.base_size != Some(self.result_size)
        {
            return Err(invalid("Git delta base length mismatch"));
        }
        Ok(())
    }

    fn reader(&self, work: Arc<Work>) -> io::Result<Reader> {
        // Clones share a seek position, but a plan is used sequentially by one
        // source job. Every reader resets its offset and dies before the next.
        let mut file = self.file.file.try_clone()?;
        file.seek(SeekFrom::Start(self.data_offset))?;
        Ok(Reader {
            state: State::new(
                Input::Compressed(file.take(self.end - self.data_offset)),
                self.inflated_size,
                self.loose,
            ),
            work,
        })
    }

    fn hasher(&self) -> io::Result<Option<NativeHasher>> {
        self.oid
            .map(|oid| {
                let format = match oid.kind() {
                    gix::hash::Kind::Sha1 => GitObjectFormat::Sha1,
                    gix::hash::Kind::Sha256 => GitObjectFormat::Sha256,
                    _ => return Err(invalid("unsupported Git delta object hash")),
                };
                Ok(NativeHasher::new(
                    format,
                    format!("blob {}\0", self.result_size).as_bytes(),
                ))
            })
            .transpose()
    }

    fn verify(&self, hasher: Option<NativeHasher>) -> io::Result<()> {
        if let (Some(oid), Some(hasher)) = (self.oid, hasher) {
            let actual = hasher.finish().map_err(io::Error::other)?;
            if actual != oid.as_bytes() {
                return Err(invalid("Git delta base or result native identity mismatch"));
            }
        }
        Ok(())
    }
}

fn packed(locator: &Locator, oid: &gix::ObjectId, files: &mut Files) -> io::Result<Option<Node>> {
    for index in locator.indexes() {
        let Some(position) = index.lookup(oid) else {
            continue;
        };
        let path = index.path().with_extension("pack");
        let Some(file) = files.get(&path)? else {
            continue;
        };
        // A stale optional hint before selection remains a miss.
        match Node::packed(
            file,
            path,
            index.pack_offset_at_index(position),
            oid.as_bytes().len(),
            Some(*oid),
        ) {
            Ok(node) => return Ok(Some(node)),
            Err(error) if error.raw_os_error().is_some() => return Err(error),
            Err(_) => {} // stale or malformed optional index/pack hint
        }
    }
    Ok(None)
}

fn loose(
    locator: &Locator,
    oid: &gix::ObjectId,
    work: Arc<Work>,
    files: &mut Files,
) -> io::Result<Option<Node>> {
    let hex = oid.to_string();
    for root in &locator.roots {
        let path = root.join(&hex[..2]).join(&hex[2..]);
        let Some(file) = files.get(&path)? else {
            continue;
        };
        let metadata = file.file.metadata()?;
        let end = metadata.len();
        let mut inflate = Inflater {
            source: Input::Compressed(file.file.try_clone()?.take(end)),
            inflate: gix::features::zlib::Decompress::new(),
            input: vec![0; BUFFER_BYTES].into_boxed_slice(),
            start: 0,
            end: 0,
            finished: false,
        };
        let mut header = [0; HEADER_BYTES];
        let mut count = 0;
        let size = loop {
            work.charge(0)?;
            let before = inflate.inflate.total_in();
            let got = inflate.step(&mut header[count..])?;
            work.charge(inflate.inflate.total_in() - before + got as u64)?;
            count += got;
            if let Some(nul) = header[..count].iter().position(|b| *b == 0) {
                let text = std::str::from_utf8(&header[..nul]).map_err(io::Error::other)?;
                let size: u64 = text
                    .strip_prefix("blob ")
                    .ok_or_else(|| invalid("Git delta base is not a blob"))?
                    .parse()
                    .map_err(io::Error::other)?;
                if text != format!("blob {size}") {
                    return Err(invalid("noncanonical Git loose base header"));
                }
                break size;
            }
            if count == HEADER_BYTES || inflate.finished {
                return Err(invalid("truncated Git loose base header"));
            }
        };
        return Ok(Some(Node {
            file,
            path,
            offset: 0,
            data_offset: 0,
            end,
            header: Header::Blob,
            inflated_size: size,
            result_size: size,
            base_size: None,
            loose: true,
            oid: Some(*oid),
        }));
    }
    Ok(None)
}

fn varint(reader: &mut impl Read) -> io::Result<u64> {
    let mut value = 0u64;
    for shift in (0..=63).step_by(7) {
        let mut byte = [0];
        reader.read_exact(&mut byte)?;
        let low = u64::from(byte[0] & 0x7f);
        if low > (u64::MAX >> shift) {
            return Err(invalid("Git delta size varint overflows u64"));
        }
        value |= low << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid("Git delta size varint is too long"))
}

pub(in crate::git::repository::closure_import::workers) struct Plan {
    nodes: Vec<Node>,
    work: Arc<Work>,
}
impl Plan {
    pub(in crate::git::repository::closure_import::workers) fn probe(
        locator: &Locator,
        oid: &gix::ObjectId,
        payload_limit: u64,
        control: Arc<Control>,
        source_files: Arc<tokio::sync::Semaphore>,
    ) -> io::Result<Option<Self>> {
        let hex = oid.to_string();
        if locator.roots.iter().any(|root| {
            root.join(&hex[..2])
                .join(&hex[2..])
                .metadata()
                .is_ok_and(|m| m.is_file())
        }) {
            return Ok(None);
        }
        let mut files = Files {
            slots: source_files,
            open: BTreeMap::new(),
        };
        let Some(mut node) = packed(locator, oid, &mut files)? else {
            return Ok(None);
        };
        if !node.header.is_delta() {
            return Ok(None);
        }
        let work = Arc::new(Work {
            control,
            remaining: AtomicU64::new(MAX_WORK_BYTES),
        });
        let mut nodes = Vec::new();
        let mut locations = BTreeSet::new();
        let mut ids = BTreeSet::from([*oid]);
        let mut declared_work = 0u64;
        loop {
            work.charge(0)?;
            if !locations.insert((node.path.clone(), node.offset)) {
                return Err(invalid("Git delta cycle"));
            }
            if node.inflated_size > payload_limit {
                return Err(limit("Git delta source exceeds payload limit"));
            }
            declared_work = declared_work
                .checked_add(node.inflated_size)
                .ok_or_else(|| limit("Git delta work overflow"))?;
            let base = match node.header {
                Header::Blob => {
                    node.validate_prefix(&nodes, declared_work)?;
                    None
                }
                Header::OfsDelta { .. } | Header::RefDelta { .. } => {
                    if nodes.len() == MAX_DELTAS {
                        return Err(limit("Git delta chain exceeds 64 deltas"));
                    }
                    let mut reader = node.reader(work.clone())?;
                    let base_size = varint(&mut reader)?;
                    let result_size = varint(&mut reader)?;
                    drop(reader);
                    if base_size > payload_limit || result_size > payload_limit {
                        return Err(limit("Git delta base or result exceeds payload limit"));
                    }
                    node.base_size = Some(base_size);
                    node.result_size = result_size;
                    declared_work = declared_work
                        .checked_add(result_size)
                        .ok_or_else(|| limit("Git delta work overflow"))?;
                    // Reject facts already established about this node before
                    // a missing later hint can return to the gix fallback.
                    node.validate_prefix(&nodes, declared_work)?;
                    match node.header {
                        Header::OfsDelta { base_distance } => {
                            let offset =
                                Header::verified_base_pack_offset(node.offset, base_distance)
                                    .ok_or_else(|| invalid("invalid Git delta base offset"))?;
                            Some(Node::packed(
                                node.file.clone(),
                                node.path.clone(),
                                offset,
                                oid.as_bytes().len(),
                                None,
                            )?)
                        }
                        Header::RefDelta { base_id } => {
                            if !ids.insert(base_id) {
                                return Err(invalid("Git delta reference cycle"));
                            }
                            let base = match loose(locator, &base_id, work.clone(), &mut files)? {
                                Some(base) => Some(base),
                                None => packed(locator, &base_id, &mut files)?,
                            };
                            // A genuinely absent optional hint is a best-effort
                            // miss. Rejected chains never return through here.
                            let Some(base) = base else { return Ok(None) };
                            Some(base)
                        }
                        _ => unreachable!(),
                    }
                }
                _ => return Err(invalid("Git delta chain has a non-blob base")),
            };
            nodes.push(node);
            match base {
                Some(base) => node = base,
                None => break,
            }
        }
        Ok(Some(Self { nodes, work }))
    }

    pub(in crate::git::repository::closure_import::workers) fn size(&self) -> u64 {
        self.nodes[0].result_size
    }

    pub(in crate::git::repository::closure_import::workers) fn reconstruct(
        mut self,
        area: &SpillArea,
    ) -> io::Result<SpillPayload> {
        let base = self.nodes.pop().expect("a plan contains a terminal base");
        let mut current = area.payload(base.result_size).map_err(io::Error::other)?;
        let mut reader = base.reader(self.work.clone())?;
        let mut hasher = base.hasher()?;
        let mut buffer = vec![0; BUFFER_BYTES].into_boxed_slice();
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            current.append(&buffer[..count])?;
            if let Some(hasher) = &mut hasher {
                hasher.update(&buffer[..count]);
            }
        }
        base.verify(hasher)?;
        current.rewind()?;
        drop(reader);
        for node in self.nodes.into_iter().rev() {
            self.work.charge(0)?;
            let mut result = area.payload(node.result_size).map_err(io::Error::other)?;
            let mut reader = node.reader(self.work.clone())?;
            if varint(&mut reader)? != current.len() || varint(&mut reader)? != node.result_size {
                return Err(invalid("Git delta header changed since planning"));
            }
            let mut hasher = node.hasher()?;
            loop {
                let mut opcode = [0];
                if reader.read(&mut opcode)? == 0 {
                    break;
                }
                if opcode[0] == 0 {
                    return Err(invalid("Git delta opcode zero is reserved"));
                }
                let (mut offset, mut count) = (0u64, 0u64);
                if opcode[0] & 0x80 != 0 {
                    for bit in 0..7 {
                        if opcode[0] & (1 << bit) != 0 {
                            let mut byte = [0];
                            reader.read_exact(&mut byte)?;
                            if bit < 4 {
                                offset |= u64::from(byte[0]) << (bit * 8);
                            } else {
                                count |= u64::from(byte[0]) << ((bit - 4) * 8);
                            }
                        }
                    }
                    if count == 0 {
                        count = 65536;
                    }
                    if offset > current.len() || count > current.len() - offset {
                        return Err(invalid("Git delta copy outside base"));
                    }
                } else {
                    count = u64::from(opcode[0]);
                }
                if count > node.result_size - result.len() {
                    return Err(invalid("Git delta instructions exceed result size"));
                }
                while count != 0 {
                    let length = count.min(BUFFER_BYTES as u64) as usize;
                    self.work.charge(length as u64)?;
                    if opcode[0] & 0x80 != 0 {
                        current.read_exact_at(offset, &mut buffer[..length])?;
                        offset += length as u64;
                    } else {
                        reader.read_exact(&mut buffer[..length])?;
                    }
                    result.append(&buffer[..length])?;
                    if let Some(hasher) = &mut hasher {
                        hasher.update(&buffer[..length]);
                    }
                    count -= length as u64;
                }
            }
            node.verify(hasher)?;
            result.rewind()?;
            current = result;
        }
        Ok(current)
    }
}

#[cfg(test)]
mod tests;
