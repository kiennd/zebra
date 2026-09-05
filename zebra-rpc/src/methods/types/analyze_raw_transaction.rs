//! Types for the `analyzerawtransaction` RPC.

use derive_getters::Getters;
use derive_new::new;

use super::transaction::TransactionObject;

/// A stateless analysis of a serialized transaction.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct AnalyzeRawTransactionResponse {
    /// The decoded transaction, without any chain or mempool context.
    pub(crate) transaction: Box<TransactionObject>,

    /// The ZIP-317 conventional fee calculation for the transaction.
    pub(crate) zip317: Zip317FeeAnalysis,
}

/// The ZIP-317 conventional fee calculation for a serialized transaction.
#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct Zip317FeeAnalysis {
    /// The implemented ZIP-317 specification revision.
    #[getter(copy)]
    pub(crate) zip317_revision: u32,

    /// The number of conventional actions after applying the grace action minimum.
    #[getter(copy)]
    pub(crate) conventional_actions: u32,

    /// The conventional fee in zatoshis.
    #[getter(copy)]
    pub(crate) conventional_fee_zat: u64,

    /// The marginal fee in zatoshis per logical action.
    #[getter(copy)]
    pub(crate) marginal_fee_zat: u64,

    /// The minimum number of chargeable logical actions.
    #[getter(copy)]
    pub(crate) grace_actions: u32,
}
