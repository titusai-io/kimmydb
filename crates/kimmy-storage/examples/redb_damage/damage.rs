// Damage to a redb 4.x store's primary commit slot, for proving that a start
// refuses it. Shared, through `include!`, by the `redb_damage` example, which
// a tester runs on a real store, and by kimmy-storage's tests, so the two
// fixtures cannot diverge. Plain comments: an included file cannot carry
// inner doc comments.
//
// The layout is redb 4.x's file format 3, and nothing else is accepted: the
// magic, then two 128-byte commit slots at 64 and 192. Bit 0 of the god byte
// names the primary. In a slot:
// - byte 0 is the format byte;
// - bytes 40..48 are the system root's page number, whose top five bits are
//   the page order;
// - bytes 112..128 are the XXH3-128 checksum of bytes 0..112.

/// redb's magic.
pub const MAGIC: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];
/// Where the two commit slots start.
pub const SLOT_OFFSETS: [usize; 2] = [64, 192];
/// The god byte, and its flags.
pub const GOD_BYTE: usize = MAGIC.len();
pub const PRIMARY_BIT: u8 = 1;
pub const RECOVERY_REQUIRED: u8 = 2;
pub const TWO_PHASE_COMMIT: u8 = 4;
/// The only file format this knows the layout of.
pub const FILE_FORMAT: u8 = 3;
/// The header: the magic and god byte, then both slots.
pub const HEADER_LEN: usize = 320;
const SLOT_LEN: usize = 128;
/// The top byte of the system root's page number, within a slot.
const SYSTEM_ROOT_TOP_BYTE: usize = 47;
const CHECKSUM: usize = 112;

/// One slot's bytes before and after a change, and where it starts.
pub struct SlotChange {
    pub offset: usize,
    pub before: Vec<u8>,
    pub after: Vec<u8>,
}

/// The primary slot's offset, once `bytes` is known to be a format-3 store.
pub fn primary_slot(bytes: &[u8]) -> Result<usize, String> {
    if bytes.len() < HEADER_LEN {
        return Err(format!("{} bytes is shorter than redb's header", bytes.len()));
    }
    if bytes[..MAGIC.len()] != MAGIC {
        return Err("the file does not begin with redb's magic".into());
    }
    for offset in SLOT_OFFSETS {
        if bytes[offset] != FILE_FORMAT {
            return Err(format!(
                "the slot at {offset} is file format {}, not {FILE_FORMAT}",
                bytes[offset]
            ));
        }
    }
    Ok(SLOT_OFFSETS[usize::from(bytes[GOD_BYTE] & PRIMARY_BIT)])
}

/// Set the primary slot's system-root page order to `order` (0..=31): a root
/// page of 2^order pages. With `valid_checksum`, the slot's checksum is
/// recomputed, so only a check of the page order itself can refuse the
/// store; without it, the checksum no longer matches.
pub fn set_primary_page_order(
    bytes: &mut [u8],
    order: u8,
    valid_checksum: bool,
) -> Result<SlotChange, String> {
    if order > 31 {
        return Err(format!("a page order is five bits; {order} does not fit"));
    }
    let offset = primary_slot(bytes)?;
    let before = bytes[offset..offset + SLOT_LEN].to_vec();
    let top = offset + SYSTEM_ROOT_TOP_BYTE;
    bytes[top] = (bytes[top] & 0x07) | (order << 3);
    if valid_checksum {
        let checksum = xxhash_rust::xxh3::xxh3_128(&bytes[offset..offset + CHECKSUM]);
        bytes[offset + CHECKSUM..offset + SLOT_LEN].copy_from_slice(&checksum.to_le_bytes());
    }
    Ok(SlotChange { offset, before, after: bytes[offset..offset + SLOT_LEN].to_vec() })
}

/// Whether the slot at `offset` carries the checksum of its own bytes.
pub fn slot_checksum_valid(bytes: &[u8], offset: usize) -> bool {
    let stored = u128::from_le_bytes(
        bytes[offset + CHECKSUM..offset + SLOT_LEN].try_into().expect("16 bytes"),
    );
    stored == xxhash_rust::xxh3::xxh3_128(&bytes[offset..offset + CHECKSUM])
}
