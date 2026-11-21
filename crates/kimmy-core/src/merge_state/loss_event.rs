//! What a stream and the metrics name a loss record by (Q7 Step 4, C8): the label of a kind and whether a value was cut. (The
//! `eventId` of a `conflictResolved` event is in `kimmy-storage`, `conflict_event.rs`, where the document key it names is encoded.)

use super::types::{LossKind, LossValue};

impl LossKind {
    /// Every kind, in the order of [`LossKind::index`] and of the metric's labels.
    pub const ALL: [LossKind; 6] = [
        LossKind::Clash,
        LossKind::DeleteWon,
        LossKind::InsertLost,
        LossKind::RetiredWriter,
        LossKind::TypeClash,
        LossKind::ArrayClash,
    ];

    /// The name a client reads (`kind` of an event, the `kind` label of the metric). A chosen representation, not a derive's:
    /// a rename of a variant must not change what a client branches on.
    pub const fn as_str(self) -> &'static str {
        match self {
            LossKind::Clash => "clash",
            LossKind::DeleteWon => "deleteWon",
            LossKind::InsertLost => "insertLost",
            LossKind::RetiredWriter => "retiredWriter",
            LossKind::TypeClash => "typeClash",
            LossKind::ArrayClash => "arrayClash",
        }
    }

    /// The position of the kind in [`LossKind::ALL`].
    pub const fn index(self) -> usize {
        match self {
            LossKind::Clash => 0,
            LossKind::DeleteWon => 1,
            LossKind::InsertLost => 2,
            LossKind::RetiredWriter => 3,
            LossKind::TypeClash => 4,
            LossKind::ArrayClash => 5,
        }
    }
}

impl LossValue {
    /// Whether the record kept only the part of the lost value that fit the collection's cut (`truncated` of an event).
    pub const fn is_cut(&self) -> bool {
        matches!(self, LossValue::CutValue(_) | LossValue::CutFields(_))
    }
}
