//! Regular-file archives spanning the decoder/storage buffering window.
pub fn archive(size: usize) -> Vec<u8> {
    let contents: Vec<u8> = (0..size)
        .map(|index| ((index * 37 + index / 251) % 256) as u8)
        .collect();
    let mut bytes = Vec::new();
    nix_archive::nar::encode_tree(
        &mut bytes,
        &nix_archive::nar::Node::Regular {
            executable: false,
            contents: &contents,
        },
    )
    .unwrap();
    bytes
}
