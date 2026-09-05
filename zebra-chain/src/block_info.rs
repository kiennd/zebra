//! Extra per-block info tracked in the state.
use crate::{
    amount::{Amount, NonNegative},
    value_balance::ValueBalance,
};

/// Extra per-block info tracked in the state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockInfo {
    /// The pool balances after the block.
    value_pools: ValueBalance<NonNegative>,
    /// The size of the block in bytes.
    size: u32,
    /// The number of transactions in the block, when indexed by this Zebra version.
    transaction_count: Option<u32>,
    /// The total fees paid by non-coinbase transactions, when indexed by this Zebra version.
    total_fee: Option<Amount<NonNegative>>,
}

impl BlockInfo {
    /// Creates a new [`BlockInfo`] with the given value pools.
    pub fn new(value_pools: ValueBalance<NonNegative>, size: u32) -> Self {
        BlockInfo {
            value_pools,
            size,
            transaction_count: None,
            total_fee: None,
        }
    }

    /// Creates a new [`BlockInfo`] with lightweight explorer metrics.
    pub fn with_metrics(
        value_pools: ValueBalance<NonNegative>,
        size: u32,
        transaction_count: u32,
        total_fee: Amount<NonNegative>,
    ) -> Self {
        BlockInfo {
            value_pools,
            size,
            transaction_count: Some(transaction_count),
            total_fee: Some(total_fee),
        }
    }

    /// Returns the value pools of this block.
    pub fn value_pools(&self) -> &ValueBalance<NonNegative> {
        &self.value_pools
    }

    /// Returns the size of this block.
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Returns the number of transactions in this block, if it was indexed.
    pub fn transaction_count(&self) -> Option<u32> {
        self.transaction_count
    }

    /// Returns the total fees paid by this block, if they were indexed.
    pub fn total_fee(&self) -> Option<Amount<NonNegative>> {
        self.total_fee
    }
}
