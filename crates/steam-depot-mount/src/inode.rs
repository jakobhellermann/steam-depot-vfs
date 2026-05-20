// TODO(ai-review): review for correctness/style
//! Inode encoding (option A): pack a 16-bit snapshot id into the high bits
//! of a 64-bit inode, leaving 48 bits for a within-snapshot index.
//!
//! - snapshot_id 0 reserved for synthetic directories (root + per-app +
//!   per-depot dirs). Their internal layout is owned by [`tree::MountTree`].
//! - snapshot_id 1..=u16::MAX names a mounted depot snapshot. Within it,
//!   index 0 is the snapshot's own root directory (the manifest_gid dir),
//!   and index N (N ≥ 1) refers to `manifest.files[N - 1]`.
//!
//! 16 bits = 65 535 concurrent snapshots is generous; 48 bits of file
//! index gives 281 T files per snapshot, which Steam will never hand us.

use fuser::INodeNo;

pub(crate) type SnapshotId = u16;

pub(crate) const SYNTHETIC: SnapshotId = 0;
pub(crate) const ROOT: INodeNo = INodeNo::ROOT;

const SNAPSHOT_SHIFT: u32 = 48;
const INDEX_MASK: u64 = (1 << SNAPSHOT_SHIFT) - 1;

pub(crate) fn pack(snapshot: SnapshotId, index: u64) -> INodeNo {
    debug_assert!(index <= INDEX_MASK, "index {index} overflows 48 bits");
    INodeNo(((snapshot as u64) << SNAPSHOT_SHIFT) | index)
}

pub(crate) fn unpack(ino: INodeNo) -> (SnapshotId, u64) {
    ((ino.0 >> SNAPSHOT_SHIFT) as SnapshotId, ino.0 & INDEX_MASK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_synthetic_index_zero() {
        assert_eq!(unpack(ROOT), (SYNTHETIC, 1));
    }

    #[test]
    fn roundtrip() {
        for (s, i) in [(0, 0), (0, 1), (1, 0), (1, 42), (u16::MAX, INDEX_MASK)] {
            let ino = pack(s, i);
            assert_eq!(unpack(ino), (s, i), "ino={:#x}", ino.0);
        }
    }
}
