//! Newtype for chunk SHA-1 identifiers.

use std::fmt;

/// SHA-1 hash of a chunk's plaintext content. Also serves as the chunk's
/// address on Steam's CDN and as the key in [`crate::ChunkStore`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkSha(pub [u8; 20]);

impl ChunkSha {
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(40);
        for b in &self.0 {
            write!(&mut s, "{:02x}", b).unwrap();
        }
        s
    }
}

impl From<[u8; 20]> for ChunkSha {
    fn from(b: [u8; 20]) -> Self {
        Self(b)
    }
}

impl AsRef<[u8; 20]> for ChunkSha {
    fn as_ref(&self) -> &[u8; 20] {
        &self.0
    }
}

impl fmt::Display for ChunkSha {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ChunkSha {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkSha({})", self.to_hex())
    }
}
