//! Snapshot data storage and retrieval for block height snapshots.
//!
//! This module provides functionality to store and retrieve snapshot data
//! at the first block of each UTC day, including:
//! - Funded transparent address counts (not people or shielded holders)
//! - Pool values
//! - Mining difficulty
//! - ZEC issuance and inflation rate
//! - Block timestamps
//! - Transaction counts per pool
//! - Pool inflow/outflow
//! - Average block time, fees, and sizes

use std::{collections::HashMap, fmt};

use rocksdb::ColumnFamily;
use zebra_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::{
        subsidy::{
            block_subsidy, founders_reward, funding_stream_values, halving, height_for_halving,
        },
        Network, NetworkUpgrade,
    },
    transparent,
    value_balance::ValueBalance,
    work::difficulty::{ParameterDifficulty, U256},
};

use crate::{
    request::FinalizedBlock,
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        zebra_db::ZebraDb,
    },
    BoxError, FromDisk, IntoDisk,
};

use super::{super::TypedColumnFamily, transaction_facts::FinalizedTransactionFacts};

/// The name of the snapshot data by date column family.
/// Stores funded transparent address count, pool values, difficulty, issuance, inflation rate,
/// and timestamp.
/// Key format: YY:MM:DD (year, month, day) as 3 bytes: [year, month, day]
/// This is used for daily snapshots only.
pub const SNAPSHOT_DATA_BY_DATE: &str = "snapshot_data_by_date";

/// The name of the realtime snapshot data column family.
/// Stores the same data as daily snapshots, but for realtime snapshots (taken when fully synced).
/// Keeps the active realtime snapshot and its daily anchor under separate constant keys.
/// This is used for realtime snapshots only, to prevent them from interfering with daily snapshot calculations.
pub const REALTIME_SNAPSHOT_DATA: &str = "realtime_snapshot_data";

/// The version of the persistent incremental snapshot accumulator format.
const SNAPSHOT_ACCUMULATOR_VERSION: u32 = 3;

/// The number of mutually-exclusive transaction classes stored in the accumulator.
const SNAPSHOT_TRANSACTION_CLASS_COUNT: usize = 7;

/// The number of directional pool flows stored in the accumulator.
const SNAPSHOT_POOL_FLOW_COUNT: usize = 10;

/// Independent Ironwood counters. These overlap intentionally and must not be confused with the
/// mutually-exclusive primary transaction classes above.
const SNAPSHOT_IRONWOOD_COUNTER_COUNT: usize = 12;
const IRONWOOD_V6_TX_INDEX: usize = 0;
const IRONWOOD_BUNDLE_TX_INDEX: usize = 1;
const ORCHARD_BUNDLE_TX_INDEX: usize = 2;
const ORCHARD_IRONWOOD_TX_INDEX: usize = 3;
const ORCHARD_ACTION_INDEX: usize = 4;
const IRONWOOD_ACTION_INDEX: usize = 5;
const IRONWOOD_ACTIVE_BLOCK_INDEX: usize = 6;
const OBSERVABLE_ORCHARD_TO_IRONWOOD_TX_INDEX: usize = 7;
const ZIP318_ACTION_SHAPE_TX_INDEX: usize = 8;
const ZIP318_DENOMINATION_TX_INDEX: usize = 9;
const ZIP318_FEE_TX_INDEX: usize = 10;
const ZIP318_SCHEDULE_TX_INDEX: usize = 11;

/// The current ZIP-318 Draft expiry bucket: 30 days at the 75-second target spacing.
const ZIP318_EXPIRY_MODULUS: u32 = 34_560;

/// The current ZIP-318 Draft anchor-height bucket: about three hours at the 75-second target
/// spacing.
const ZIP318_ANCHOR_MODULUS: u32 = 144;

/// ZIP-318's current draft denomination set, in zatoshis: 1/2/5 times powers of ten from 0.01 to
/// 10,000 ZEC. This is explicitly versioned in API methodology because ZIP-318 remains a draft.
const IRONWOOD_CANONICAL_DENOMINATIONS_ZAT: [u64; 19] = [
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    20_000_000,
    50_000_000,
    100_000_000,
    200_000_000,
    500_000_000,
    1_000_000_000,
    2_000_000_000,
    5_000_000_000,
    10_000_000_000,
    20_000_000_000,
    50_000_000_000,
    100_000_000_000,
    200_000_000_000,
    500_000_000_000,
    1_000_000_000_000,
];

/// Returns true when the public locktime and expiry fields are compatible with the current
/// ZIP-318 Draft schedule at the transaction's inclusion height.
///
/// The scheduled broadcast height is private wallet state, so chain data cannot prove the exact
/// schedule formula. But it can reject expired values and expiry buckets whose possible scheduled
/// heights do not overlap the public inclusion height and ZIP-318's network-wide schedule floor.
fn has_zip318_schedule_shape(
    network: &Network,
    inclusion_height: Height,
    raw_lock_time: u32,
    expiry_height: Option<u32>,
) -> bool {
    let (Some(expiry_height), Some(activation_height)) = (
        expiry_height,
        NetworkUpgrade::Nu6_3.activation_height(network),
    ) else {
        return false;
    };
    let inclusion_height = inclusion_height.0;

    // ZIP-318 lower-bounds the schedule's running height at one complete anchor bucket after the
    // first anchor boundary strictly above NU6.3 activation. A funding note created later can
    // only raise this bound, so this is the earliest schedule height observable chain-wide.
    let earliest_schedule_height = (activation_height.0 / ZIP318_ANCHOR_MODULUS)
        .checked_add(2)
        .and_then(|bucket| bucket.checked_mul(ZIP318_ANCHOR_MODULUS));

    // For a canonical expiry E, its possible private scheduled height S is in
    // [E - 2 * EXPIRY_MODULUS, E - EXPIRY_MODULUS - 1]. The transaction is compatible only when
    // that interval intersects [earliest_schedule_height, inclusion_height].
    let scheduled_height_range = expiry_height
        .checked_sub(2 * ZIP318_EXPIRY_MODULUS)
        .and_then(|start| {
            expiry_height
                .checked_sub(ZIP318_EXPIRY_MODULUS + 1)
                .map(|end| (start, end))
        });

    raw_lock_time == 0
        && expiry_height % ZIP318_EXPIRY_MODULUS == 0
        && expiry_height >= inclusion_height
        && earliest_schedule_height
            .zip(scheduled_height_range)
            .is_some_and(|(earliest, (scheduled_start, scheduled_end))| {
                inclusion_height >= earliest
                    && scheduled_start <= inclusion_height
                    && scheduled_end >= earliest
            })
}

/// The encoded byte length of [`SnapshotMetricTotals`].
const SNAPSHOT_METRIC_TOTALS_LEN: usize = SNAPSHOT_TRANSACTION_CLASS_COUNT * 8
    + SNAPSHOT_POOL_FLOW_COUNT * 16
    + SNAPSHOT_IRONWOOD_COUNTER_COUNT * 8
    + IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len() * 8
    + 16
    + 16
    + 8
    + 8
    + 8
    + 8
    + 16
    + 6 * 16
    + 5 * 16;

/// The encoded byte length of the two active header-time ranges.
const SNAPSHOT_TIME_RANGES_LEN: usize = 4 * 8;

/// The encoded byte length of [`SnapshotMetricAnchor`].
const SNAPSHOT_METRIC_ANCHOR_LEN: usize = 1 + 4 + 8 + SNAPSHOT_METRIC_TOTALS_LEN;

/// The encoded byte length of [`SnapshotAccumulator`].
const SNAPSHOT_ACCUMULATOR_LEN: usize = 4
    + 4
    + 8
    + 48
    + 8
    + 8
    + 8
    + SNAPSHOT_METRIC_TOTALS_LEN
    + SNAPSHOT_TIME_RANGES_LEN
    + 2 * SNAPSHOT_METRIC_ANCHOR_LEN;

const TRANSPARENT_TX_INDEX: usize = 0;
const TRANSPARENT_COINBASE_TX_INDEX: usize = 1;
const SHIELDED_COINBASE_MIGRATION_TX_INDEX: usize = 2;
const SPROUT_TX_INDEX: usize = 3;
const SAPLING_TX_INDEX: usize = 4;
const ORCHARD_TX_INDEX: usize = 5;
const IRONWOOD_TX_INDEX: usize = 6;

const TRANSPARENT_INFLOW_INDEX: usize = 0;
const TRANSPARENT_OUTFLOW_INDEX: usize = 1;
const SPROUT_INFLOW_INDEX: usize = 2;
const SPROUT_OUTFLOW_INDEX: usize = 3;
const SAPLING_INFLOW_INDEX: usize = 4;
const SAPLING_OUTFLOW_INDEX: usize = 5;
const ORCHARD_INFLOW_INDEX: usize = 6;
const ORCHARD_OUTFLOW_INDEX: usize = 7;
const IRONWOOD_INFLOW_INDEX: usize = 8;
const IRONWOOD_OUTFLOW_INDEX: usize = 9;

/// Key type for the realtime snapshot.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RealtimeSnapshotKey;

impl IntoDisk for RealtimeSnapshotKey {
    type Bytes = [u8; 1];

    fn as_bytes(&self) -> Self::Bytes {
        [0]
    }
}

impl FromDisk for RealtimeSnapshotKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let _ = bytes.as_ref();
        RealtimeSnapshotKey
    }
}

/// Constant key for the replaceable realtime snapshot.
const REALTIME_SNAPSHOT_KEY: RealtimeSnapshotKey = RealtimeSnapshotKey;

/// Key type for the daily anchor metadata stored beside a realtime snapshot.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RealtimeSnapshotAnchorKey;

impl IntoDisk for RealtimeSnapshotAnchorKey {
    type Bytes = [u8; 1];

    fn as_bytes(&self) -> Self::Bytes {
        [1]
    }
}

impl FromDisk for RealtimeSnapshotAnchorKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let _ = bytes.as_ref();
        RealtimeSnapshotAnchorKey
    }
}

/// Constant key for the daily snapshot replaced by the realtime snapshot.
///
/// Keeping the anchor beside the realtime value avoids scanning the complete daily snapshot
/// column family on every bounded dashboard read. Legacy realtime records without this metadata
/// use the scan fallback until the next refresh.
const REALTIME_SNAPSHOT_ANCHOR_KEY: RealtimeSnapshotAnchorKey = RealtimeSnapshotAnchorKey;

/// Key type for the persistent incremental snapshot accumulator.
///
/// Snapshot values and metadata intentionally share one column family, but use disjoint constant
/// keys. This avoids adding another column family solely for a single value.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SnapshotAccumulatorKey;

impl IntoDisk for SnapshotAccumulatorKey {
    type Bytes = [u8; 1];

    fn as_bytes(&self) -> Self::Bytes {
        [2]
    }
}

impl FromDisk for SnapshotAccumulatorKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        assert_eq!(
            bytes.as_ref(),
            [2],
            "snapshot accumulator key must be the constant singleton key"
        );
        SnapshotAccumulatorKey
    }
}

/// Constant key for the persistent incremental snapshot accumulator.
const SNAPSHOT_ACCUMULATOR_KEY: SnapshotAccumulatorKey = SnapshotAccumulatorKey;

/// Errors while incrementally maintaining snapshot metrics.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotAccumulatorError {
    /// A non-genesis database is missing its accumulator.
    #[error(
        "snapshot accumulator is missing while committing non-genesis height {height:?}; \
         rebuild the snapshot accumulator or resync the database"
    )]
    MissingAtNonGenesis {
        /// The block whose commit found the missing accumulator.
        height: Height,
    },

    /// Snapshot materialization was attempted before the accumulator was initialized.
    #[error("snapshot accumulator is missing at requested snapshot height {height:?}")]
    MissingForSnapshot {
        /// The requested snapshot height.
        height: Height,
    },

    /// The accumulator does not end immediately before the new block.
    #[error(
        "snapshot accumulator is not sequential: latest height {latest_height:?}, \
         attempted height {attempted_height:?}"
    )]
    NonSequential {
        /// The height persisted in the accumulator.
        latest_height: Height,
        /// The block height being committed.
        attempted_height: Height,
    },

    /// The caller requested a snapshot for a height other than the accumulator tip.
    #[error(
        "snapshot accumulator tip is {accumulator_height:?}, but snapshot height is \
         {requested_height:?}"
    )]
    HeightMismatch {
        /// The height persisted in the accumulator.
        accumulator_height: Height,
        /// The height requested by the snapshot scheduler.
        requested_height: Height,
    },

    /// A checked integer operation failed.
    #[error("snapshot accumulator overflow or underflow calculating {metric}")]
    Arithmetic {
        /// The metric being calculated.
        metric: &'static str,
    },

    /// A block input was not present in the verifier-provided spent UTXO map.
    #[error("snapshot accumulator is missing spent UTXO {outpoint:?}")]
    MissingSpentUtxo {
        /// The unresolved transparent outpoint.
        outpoint: transparent::OutPoint,
    },

    /// A chain amount or value balance could not be calculated.
    #[error("snapshot accumulator could not calculate {metric}: {reason}")]
    InvalidValue {
        /// The metric being calculated.
        metric: &'static str,
        /// The underlying calculation error.
        reason: String,
    },

    /// A block has an invalid compact difficulty threshold.
    #[error("snapshot accumulator found an invalid difficulty threshold at {height:?}")]
    InvalidDifficulty {
        /// The block containing the invalid threshold.
        height: Height,
    },

    /// The latest daily snapshot required for realtime metadata was not found.
    #[error(
        "snapshot accumulator daily anchor at height {height:?} and date {date} was not found"
    )]
    MissingDailyAnchor {
        /// The expected daily snapshot height.
        height: Height,
        /// The expected daily snapshot date.
        date: SnapshotDateKey,
    },
}

/// Monotonic metrics accumulated from genesis through a finalized block.
///
/// Cumulative values let daily and realtime intervals be calculated with constant-time
/// subtraction. Wider internal counters avoid overflowing over the lifetime of the chain; the
/// existing daily wire format remains unchanged and is range-checked when materialized.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct SnapshotMetricTotals {
    transaction_counts: [u64; SNAPSHOT_TRANSACTION_CLASS_COUNT],
    pool_flows: [u128; SNAPSHOT_POOL_FLOW_COUNT],
    ironwood_counts: [u64; SNAPSHOT_IRONWOOD_COUNTER_COUNT],
    ironwood_denomination_counts: [u64; IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len()],
    observable_orchard_to_ironwood_value_zat: u128,
    total_fees_zat: u128,
    total_block_size: u64,
    block_count: u64,
    transaction_count: u64,
    empty_block_count: u64,
    accepted_work: u128,
    total_subsidy_zat: u128,
    miner_subsidy_zat: u128,
    founders_reward_zat: u128,
    funding_streams_zat: u128,
    deferred_subsidy_zat: u128,
    lockbox_disbursement_zat: u128,
    coinbase_output_transparent_zat: u128,
    coinbase_output_sapling_zat: u128,
    coinbase_output_orchard_zat: u128,
    coinbase_output_ironwood_zat: u128,
    coinbase_unclaimed_zat: u128,
}

impl SnapshotMetricTotals {
    fn from_block(
        transaction_facts: &[FinalizedTransactionFacts],
        spent_utxos: &HashMap<transparent::OutPoint, transparent::Utxo>,
        network: &Network,
        block_height: Height,
        block_size: u32,
    ) -> Result<Self, SnapshotAccumulatorError> {
        fn add_count(
            totals: &mut SnapshotMetricTotals,
            index: usize,
        ) -> Result<(), SnapshotAccumulatorError> {
            totals.transaction_counts[index] = totals.transaction_counts[index]
                .checked_add(1)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "per-block transaction count",
                })?;
            Ok(())
        }

        fn add_flow(
            totals: &mut SnapshotMetricTotals,
            index: usize,
            value: u128,
        ) -> Result<(), SnapshotAccumulatorError> {
            totals.pool_flows[index] = totals.pool_flows[index].checked_add(value).ok_or(
                SnapshotAccumulatorError::Arithmetic {
                    metric: "per-block pool flow",
                },
            )?;
            Ok(())
        }

        fn add_ironwood_count(
            totals: &mut SnapshotMetricTotals,
            index: usize,
            value: u64,
            metric: &'static str,
        ) -> Result<(), SnapshotAccumulatorError> {
            totals.ironwood_counts[index] = totals.ironwood_counts[index]
                .checked_add(value)
                .ok_or(SnapshotAccumulatorError::Arithmetic { metric })?;
            Ok(())
        }

        fn add_signed_pool_flow(
            totals: &mut SnapshotMetricTotals,
            value_zat: i64,
            inflow_index: usize,
            outflow_index: usize,
        ) -> Result<(), SnapshotAccumulatorError> {
            if value_zat < 0 {
                let inflow =
                    value_zat
                        .checked_neg()
                        .ok_or(SnapshotAccumulatorError::Arithmetic {
                            metric: "shielded pool inflow",
                        })? as u128;
                add_flow(totals, inflow_index, inflow)?;
            } else if value_zat > 0 {
                add_flow(totals, outflow_index, value_zat as u128)?;
            }
            Ok(())
        }

        let mut totals = Self {
            total_block_size: u64::from(block_size),
            block_count: 1,
            transaction_count: u64::try_from(transaction_facts.len()).map_err(|_| {
                SnapshotAccumulatorError::Arithmetic {
                    metric: "per-block transaction count",
                }
            })?,
            empty_block_count: u64::from(transaction_facts.len() <= 1),
            ..Self::default()
        };
        totals.ironwood_counts[IRONWOOD_ACTIVE_BLOCK_INDEX] = u64::from(
            transaction_facts
                .iter()
                .any(|facts| facts.ironwood_action_count > 0),
        );

        for facts in transaction_facts {
            let has_transparent = facts.has_transparent_inputs || facts.has_transparent_outputs();
            let mut spends_coinbase_output = false;
            let mut transparent_input_zat = 0i128;
            let mut transparent_output_zat = 0i128;
            let mut sprout_inflow_zat = 0i128;
            let mut sprout_outflow_zat = 0i128;

            add_ironwood_count(
                &mut totals,
                IRONWOOD_V6_TX_INDEX,
                u64::from(facts.is_v6),
                "v6 transaction count",
            )?;
            add_ironwood_count(
                &mut totals,
                IRONWOOD_BUNDLE_TX_INDEX,
                u64::from(facts.ironwood_action_count > 0),
                "Ironwood bundle transaction count",
            )?;
            add_ironwood_count(
                &mut totals,
                ORCHARD_BUNDLE_TX_INDEX,
                u64::from(facts.orchard_action_count > 0),
                "Orchard bundle transaction count",
            )?;
            add_ironwood_count(
                &mut totals,
                ORCHARD_IRONWOOD_TX_INDEX,
                u64::from(facts.orchard_action_count > 0 && facts.ironwood_action_count > 0),
                "Orchard and Ironwood transaction count",
            )?;
            add_ironwood_count(
                &mut totals,
                ORCHARD_ACTION_INDEX,
                u64::from(facts.orchard_action_count),
                "Orchard action count",
            )?;
            add_ironwood_count(
                &mut totals,
                IRONWOOD_ACTION_INDEX,
                u64::from(facts.ironwood_action_count),
                "Ironwood action count",
            )?;

            // The verifier already resolved every spent output, including same-block spends. Use
            // that map for flows, migration classification, and fees rather than reading old
            // transactions back from RocksDB.
            for outpoint in &facts.transparent_input_outpoints {
                let utxo = spent_utxos.get(outpoint).ok_or(
                    SnapshotAccumulatorError::MissingSpentUtxo {
                        outpoint: *outpoint,
                    },
                )?;
                add_flow(
                    &mut totals,
                    TRANSPARENT_OUTFLOW_INDEX,
                    utxo.output.value().zatoshis() as u128,
                )?;
                transparent_input_zat = transparent_input_zat
                    .checked_add(i128::from(utxo.output.value().zatoshis()))
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "transparent input value",
                    })?;
                spends_coinbase_output |= utxo.from_coinbase;
            }

            // These classes are intentionally exclusive and ordered from most-specific to
            // least-specific, so their sum never exceeds the transaction count.
            if !facts.is_coinbase && spends_coinbase_output && facts.has_shielded_outputs {
                add_count(&mut totals, SHIELDED_COINBASE_MIGRATION_TX_INDEX)?;
            } else if facts.has_ironwood {
                add_count(&mut totals, IRONWOOD_TX_INDEX)?;
            } else if facts.has_orchard {
                add_count(&mut totals, ORCHARD_TX_INDEX)?;
            } else if facts.has_sapling {
                add_count(&mut totals, SAPLING_TX_INDEX)?;
            } else if facts.has_sprout {
                add_count(&mut totals, SPROUT_TX_INDEX)?;
            } else if facts.is_coinbase {
                add_count(&mut totals, TRANSPARENT_COINBASE_TX_INDEX)?;
            } else if has_transparent {
                add_count(&mut totals, TRANSPARENT_TX_INDEX)?;
            }

            for value_zat in facts.transparent_output_values_zat.iter().copied() {
                add_flow(&mut totals, TRANSPARENT_INFLOW_INDEX, value_zat as u128)?;
                transparent_output_zat = transparent_output_zat
                    .checked_add(i128::from(value_zat))
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "transparent output value",
                    })?;
            }

            // Sprout vpub_old enters the shielded pool and vpub_new leaves it.
            for vpub_old_zat in facts.sprout_inflow_values_zat.iter().copied() {
                let vpub_old = Amount::<NonNegative>::try_from(vpub_old_zat).map_err(|error| {
                    SnapshotAccumulatorError::InvalidValue {
                        metric: "Sprout inflow",
                        reason: error.to_string(),
                    }
                })?;
                add_flow(
                    &mut totals,
                    SPROUT_INFLOW_INDEX,
                    vpub_old.zatoshis() as u128,
                )?;
                sprout_inflow_zat = sprout_inflow_zat
                    .checked_add(i128::from(vpub_old.zatoshis()))
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "Sprout inflow value",
                    })?;
            }
            for vpub_new_zat in facts.sprout_outflow_values_zat.iter().copied() {
                let vpub_new = Amount::<NonNegative>::try_from(vpub_new_zat).map_err(|error| {
                    SnapshotAccumulatorError::InvalidValue {
                        metric: "Sprout outflow",
                        reason: error.to_string(),
                    }
                })?;
                add_flow(
                    &mut totals,
                    SPROUT_OUTFLOW_INDEX,
                    vpub_new.zatoshis() as u128,
                )?;
                sprout_outflow_zat = sprout_outflow_zat
                    .checked_add(i128::from(vpub_new.zatoshis()))
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "Sprout outflow value",
                    })?;
            }

            let sapling_value_zat = facts.sapling_value_balance_zat;
            let orchard_value_zat = facts.orchard_value_balance_zat;
            let ironwood_value_zat = facts.ironwood_value_balance_zat;
            add_signed_pool_flow(
                &mut totals,
                sapling_value_zat,
                SAPLING_INFLOW_INDEX,
                SAPLING_OUTFLOW_INDEX,
            )?;
            add_signed_pool_flow(
                &mut totals,
                orchard_value_zat,
                ORCHARD_INFLOW_INDEX,
                ORCHARD_OUTFLOW_INDEX,
            )?;
            add_signed_pool_flow(
                &mut totals,
                ironwood_value_zat,
                IRONWOOD_INFLOW_INDEX,
                IRONWOOD_OUTFLOW_INDEX,
            )?;

            if !facts.is_coinbase {
                // Consensus fee formula, using values already visited for flows above:
                // transparent inputs - outputs + Sprout vpub_new - vpub_old + each modern
                // shielded value balance. Semantically verified transactions cannot be negative.
                let fee_zat = transparent_input_zat
                    .checked_sub(transparent_output_zat)
                    .and_then(|value| value.checked_add(sprout_outflow_zat))
                    .and_then(|value| value.checked_sub(sprout_inflow_zat))
                    .and_then(|value| value.checked_add(i128::from(sapling_value_zat)))
                    .and_then(|value| value.checked_add(i128::from(orchard_value_zat)))
                    .and_then(|value| value.checked_add(i128::from(ironwood_value_zat)))
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "transaction fee",
                    })?;
                if fee_zat < 0 {
                    return Err(SnapshotAccumulatorError::InvalidValue {
                        metric: "transaction fee",
                        reason: format!("verified transaction has negative fee {fee_zat}"),
                    });
                }
                totals.total_fees_zat = totals.total_fees_zat.checked_add(fee_zat as u128).ok_or(
                    SnapshotAccumulatorError::Arithmetic {
                        metric: "per-block fees",
                    },
                )?;

                // A direct crossing has a single publicly observable net source (Orchard) and
                // destination (Ironwood). This does not identify a wallet, user, or intent.
                let is_observable_orchard_to_ironwood = facts.orchard_action_count > 0
                    && facts.ironwood_action_count > 0
                    && orchard_value_zat > 0
                    && ironwood_value_zat < 0
                    && !has_transparent
                    && !facts.has_sprout
                    && !facts.has_sapling;
                if is_observable_orchard_to_ironwood {
                    add_ironwood_count(
                        &mut totals,
                        OBSERVABLE_ORCHARD_TO_IRONWOOD_TX_INDEX,
                        1,
                        "observable Orchard-to-Ironwood transaction count",
                    )?;
                    let ironwood_credit_zat = ironwood_value_zat.unsigned_abs();
                    totals.observable_orchard_to_ironwood_value_zat = totals
                        .observable_orchard_to_ironwood_value_zat
                        .checked_add(u128::from(ironwood_credit_zat))
                        .ok_or(SnapshotAccumulatorError::Arithmetic {
                            metric: "observable Orchard-to-Ironwood value",
                        })?;

                    // These are cumulative public-field checks from ZIP-318 Draft. Anchor-root
                    // membership is intentionally not claimed here, so the API exposes these as
                    // individual funnel stages rather than a binary compliance result.
                    let has_action_shape = facts.is_v6
                        && facts.orchard_action_count == 2
                        && facts.ironwood_action_count == 1
                        && facts.orchard_spends_enabled
                        && facts.orchard_outputs_enabled
                        && !facts.ironwood_spends_enabled
                        && facts.ironwood_outputs_enabled;
                    if has_action_shape {
                        add_ironwood_count(
                            &mut totals,
                            ZIP318_ACTION_SHAPE_TX_INDEX,
                            1,
                            "ZIP-318 action-shape transaction count",
                        )?;

                        if let Some(denomination_index) = IRONWOOD_CANONICAL_DENOMINATIONS_ZAT
                            .iter()
                            .position(|denomination| *denomination == ironwood_credit_zat)
                        {
                            add_ironwood_count(
                                &mut totals,
                                ZIP318_DENOMINATION_TX_INDEX,
                                1,
                                "ZIP-318 denomination transaction count",
                            )?;
                            totals.ironwood_denomination_counts[denomination_index] = totals
                                .ironwood_denomination_counts[denomination_index]
                                .checked_add(1)
                                .ok_or(SnapshotAccumulatorError::Arithmetic {
                                    metric: "Ironwood denomination count",
                                })?;

                            if fee_zat == i128::from(facts.conventional_fee_zat) {
                                add_ironwood_count(
                                    &mut totals,
                                    ZIP318_FEE_TX_INDEX,
                                    1,
                                    "ZIP-318 conventional-fee transaction count",
                                )?;

                                let has_schedule_shape = has_zip318_schedule_shape(
                                    network,
                                    block_height,
                                    facts.raw_lock_time,
                                    facts.expiry_height,
                                );
                                if has_schedule_shape {
                                    add_ironwood_count(
                                        &mut totals,
                                        ZIP318_SCHEDULE_TX_INDEX,
                                        1,
                                        "ZIP-318 schedule-shape transaction count",
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(totals)
    }

    /// Adds deterministic proof-of-work, subsidy, and coinbase output-route facts for one block.
    ///
    /// Output routes use aggregate coinbase value balances, not recipient heuristics. They include
    /// every coinbase recipient, including consensus funding recipients.
    fn add_mining_accounting(
        &mut self,
        finalized: &FinalizedBlock,
        network: &Network,
        subsidy: Amount<NonNegative>,
        block_fees: Amount<NonNegative>,
    ) -> Result<(), SnapshotAccumulatorError> {
        fn shielded_coinbase_credit(
            value_balance_zat: i64,
            pool: &'static str,
        ) -> Result<u128, SnapshotAccumulatorError> {
            if value_balance_zat > 0 {
                return Err(SnapshotAccumulatorError::InvalidValue {
                    metric: "coinbase output route",
                    reason: format!(
                        "verified coinbase withdraws {value_balance_zat} zatoshis from {pool}"
                    ),
                });
            }

            Ok(u128::from(value_balance_zat.unsigned_abs()))
        }

        let height = finalized.height;
        let work = finalized
            .block
            .header
            .difficulty_threshold
            .to_work()
            .ok_or(SnapshotAccumulatorError::InvalidDifficulty { height })?
            .as_u128();
        let founder_reward = founders_reward(network, height);
        let funding_stream_values =
            funding_stream_values(height, network, subsidy).map_err(|error| {
                SnapshotAccumulatorError::InvalidValue {
                    metric: "funding streams",
                    reason: error.to_string(),
                }
            })?;
        let mut direct_funding_streams_zat = 0u128;
        let mut deferred_subsidy_zat = 0u128;
        for (receiver, value) in funding_stream_values {
            let value = u128::from(value.zatoshis() as u64);
            if receiver.is_deferred() {
                deferred_subsidy_zat = deferred_subsidy_zat.checked_add(value).ok_or(
                    SnapshotAccumulatorError::Arithmetic {
                        metric: "deferred subsidy",
                    },
                )?;
            } else {
                direct_funding_streams_zat = direct_funding_streams_zat.checked_add(value).ok_or(
                    SnapshotAccumulatorError::Arithmetic {
                        metric: "direct funding streams",
                    },
                )?;
            }
        }

        let subsidy_zat = u128::from(subsidy.zatoshis() as u64);
        let founders_reward_zat = u128::from(founder_reward.zatoshis() as u64);
        let protocol_subsidy_zat = founders_reward_zat
            .checked_add(direct_funding_streams_zat)
            .and_then(|value| value.checked_add(deferred_subsidy_zat))
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "subsidy composition",
            })?;
        let miner_subsidy_zat = subsidy_zat.checked_sub(protocol_subsidy_zat).ok_or(
            SnapshotAccumulatorError::InvalidValue {
                metric: "subsidy composition",
                reason: format!(
                    "protocol subsidy {protocol_subsidy_zat} exceeds total subsidy {subsidy_zat}"
                ),
            },
        )?;

        let (transparent_output_zat, sapling_output_zat, orchard_output_zat, ironwood_output_zat) =
            if let Some(coinbase) = finalized.block.transactions.first() {
                let transparent_output_zat =
                    coinbase.outputs().iter().try_fold(0u128, |total, output| {
                        total
                            .checked_add(u128::from(output.value().zatoshis() as u64))
                            .ok_or(SnapshotAccumulatorError::Arithmetic {
                                metric: "coinbase transparent outputs",
                            })
                    })?;
                (
                    transparent_output_zat,
                    shielded_coinbase_credit(
                        coinbase.sapling_value_balance().sapling_amount().zatoshis(),
                        "Sapling",
                    )?,
                    shielded_coinbase_credit(
                        coinbase.orchard_value_balance().orchard_amount().zatoshis(),
                        "Orchard",
                    )?,
                    shielded_coinbase_credit(
                        coinbase
                            .ironwood_value_balance()
                            .ironwood_amount()
                            .zatoshis(),
                        "Ironwood",
                    )?,
                )
            } else {
                (0, 0, 0, 0)
            };

        let observed_coinbase_output_zat = transparent_output_zat
            .checked_add(sapling_output_zat)
            .and_then(|value| value.checked_add(orchard_output_zat))
            .and_then(|value| value.checked_add(ironwood_output_zat))
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "observed coinbase output",
            })?;
        // Deferred contributions are not transaction outputs. Conversely, a one-time lockbox
        // disbursement is an output funded by the existing deferred pool, not new subsidy.
        // Any remainder after subtracting every observed coinbase output is permitted coinbase
        // input value that was not claimed in an output; it is not necessarily miner allocation.
        let lockbox_disbursement_zat =
            u128::from(network.lockbox_disbursement_total_amount(height).zatoshis() as u64);
        let allowed_coinbase_output_zat = subsidy_zat
            .checked_sub(deferred_subsidy_zat)
            .and_then(|value| value.checked_add(u128::from(block_fees.zatoshis() as u64)))
            .and_then(|value| value.checked_add(lockbox_disbursement_zat))
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "allowed coinbase output",
            })?;
        let unclaimed_zat = allowed_coinbase_output_zat
            .checked_sub(observed_coinbase_output_zat)
            .ok_or(SnapshotAccumulatorError::InvalidValue {
                metric: "coinbase output",
                reason: format!(
                    "observed coinbase output {observed_coinbase_output_zat} exceeds allowed output {allowed_coinbase_output_zat}"
                ),
            })?;

        self.accepted_work = work;
        self.total_subsidy_zat = subsidy_zat;
        self.miner_subsidy_zat = miner_subsidy_zat;
        self.founders_reward_zat = founders_reward_zat;
        self.funding_streams_zat = direct_funding_streams_zat;
        self.deferred_subsidy_zat = deferred_subsidy_zat;
        self.lockbox_disbursement_zat = lockbox_disbursement_zat;
        self.coinbase_output_transparent_zat = transparent_output_zat;
        self.coinbase_output_sapling_zat = sapling_output_zat;
        self.coinbase_output_orchard_zat = orchard_output_zat;
        self.coinbase_output_ironwood_zat = ironwood_output_zat;
        self.coinbase_unclaimed_zat = unclaimed_zat;

        Ok(())
    }

    fn checked_add_assign(
        &mut self,
        block: &SnapshotMetricTotals,
    ) -> Result<(), SnapshotAccumulatorError> {
        for (total, value) in self
            .transaction_counts
            .iter_mut()
            .zip(block.transaction_counts)
        {
            *total = total
                .checked_add(value)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "transaction count",
                })?;
        }

        for (total, value) in self.pool_flows.iter_mut().zip(block.pool_flows) {
            *total = total
                .checked_add(value)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "pool flow",
                })?;
        }

        for (total, value) in self.ironwood_counts.iter_mut().zip(block.ironwood_counts) {
            *total = total
                .checked_add(value)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "Ironwood observatory count",
                })?;
        }

        for (total, value) in self
            .ironwood_denomination_counts
            .iter_mut()
            .zip(block.ironwood_denomination_counts)
        {
            *total = total
                .checked_add(value)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "Ironwood denomination count",
                })?;
        }

        self.observable_orchard_to_ironwood_value_zat = self
            .observable_orchard_to_ironwood_value_zat
            .checked_add(block.observable_orchard_to_ironwood_value_zat)
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "observable Orchard-to-Ironwood value",
            })?;

        self.total_fees_zat = self
            .total_fees_zat
            .checked_add(block.total_fees_zat)
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "total fees",
            })?;
        self.total_block_size = self
            .total_block_size
            .checked_add(block.total_block_size)
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "total block size",
            })?;
        self.block_count = self.block_count.checked_add(block.block_count).ok_or(
            SnapshotAccumulatorError::Arithmetic {
                metric: "block count",
            },
        )?;
        self.transaction_count = self
            .transaction_count
            .checked_add(block.transaction_count)
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "transaction count",
            })?;
        self.empty_block_count = self
            .empty_block_count
            .checked_add(block.empty_block_count)
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "empty block count",
            })?;
        self.accepted_work = self.accepted_work.checked_add(block.accepted_work).ok_or(
            SnapshotAccumulatorError::Arithmetic {
                metric: "accepted work",
            },
        )?;

        macro_rules! checked_add_mining_total {
            ($field:ident, $metric:literal) => {
                self.$field = self
                    .$field
                    .checked_add(block.$field)
                    .ok_or(SnapshotAccumulatorError::Arithmetic { metric: $metric })?;
            };
        }

        checked_add_mining_total!(total_subsidy_zat, "total subsidy");
        checked_add_mining_total!(miner_subsidy_zat, "miner subsidy");
        checked_add_mining_total!(founders_reward_zat, "founders reward");
        checked_add_mining_total!(funding_streams_zat, "funding streams");
        checked_add_mining_total!(deferred_subsidy_zat, "deferred subsidy");
        checked_add_mining_total!(lockbox_disbursement_zat, "lockbox disbursement");
        checked_add_mining_total!(
            coinbase_output_transparent_zat,
            "transparent coinbase output"
        );
        checked_add_mining_total!(coinbase_output_sapling_zat, "Sapling coinbase output");
        checked_add_mining_total!(coinbase_output_orchard_zat, "Orchard coinbase output");
        checked_add_mining_total!(coinbase_output_ironwood_zat, "Ironwood coinbase output");
        checked_add_mining_total!(coinbase_unclaimed_zat, "unclaimed coinbase value");

        Ok(())
    }

    fn checked_sub(self, anchor: SnapshotMetricTotals) -> Result<Self, SnapshotAccumulatorError> {
        let mut transaction_counts = [0; SNAPSHOT_TRANSACTION_CLASS_COUNT];
        for (difference, (total, anchor)) in transaction_counts.iter_mut().zip(
            self.transaction_counts
                .into_iter()
                .zip(anchor.transaction_counts),
        ) {
            *difference =
                total
                    .checked_sub(anchor)
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "transaction count interval",
                    })?;
        }

        let mut pool_flows = [0; SNAPSHOT_POOL_FLOW_COUNT];
        for (difference, (total, anchor)) in pool_flows
            .iter_mut()
            .zip(self.pool_flows.into_iter().zip(anchor.pool_flows))
        {
            *difference =
                total
                    .checked_sub(anchor)
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "pool flow interval",
                    })?;
        }

        let mut ironwood_counts = [0; SNAPSHOT_IRONWOOD_COUNTER_COUNT];
        for (difference, (total, anchor)) in ironwood_counts
            .iter_mut()
            .zip(self.ironwood_counts.into_iter().zip(anchor.ironwood_counts))
        {
            *difference =
                total
                    .checked_sub(anchor)
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "Ironwood observatory count interval",
                    })?;
        }

        let mut ironwood_denomination_counts = [0; IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len()];
        for (difference, (total, anchor)) in ironwood_denomination_counts.iter_mut().zip(
            self.ironwood_denomination_counts
                .into_iter()
                .zip(anchor.ironwood_denomination_counts),
        ) {
            *difference =
                total
                    .checked_sub(anchor)
                    .ok_or(SnapshotAccumulatorError::Arithmetic {
                        metric: "Ironwood denomination count interval",
                    })?;
        }

        Ok(Self {
            transaction_counts,
            pool_flows,
            ironwood_counts,
            ironwood_denomination_counts,
            observable_orchard_to_ironwood_value_zat: self
                .observable_orchard_to_ironwood_value_zat
                .checked_sub(anchor.observable_orchard_to_ironwood_value_zat)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "observable Orchard-to-Ironwood value interval",
                })?,
            total_fees_zat: self
                .total_fees_zat
                .checked_sub(anchor.total_fees_zat)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "fee interval",
                })?,
            total_block_size: self
                .total_block_size
                .checked_sub(anchor.total_block_size)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "block size interval",
                })?,
            block_count: self.block_count.checked_sub(anchor.block_count).ok_or(
                SnapshotAccumulatorError::Arithmetic {
                    metric: "block count interval",
                },
            )?,
            transaction_count: self
                .transaction_count
                .checked_sub(anchor.transaction_count)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "transaction count interval",
                })?,
            empty_block_count: self
                .empty_block_count
                .checked_sub(anchor.empty_block_count)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "empty block count interval",
                })?,
            accepted_work: self.accepted_work.checked_sub(anchor.accepted_work).ok_or(
                SnapshotAccumulatorError::Arithmetic {
                    metric: "accepted work interval",
                },
            )?,
            total_subsidy_zat: checked_sub_mining_total(
                self.total_subsidy_zat,
                anchor.total_subsidy_zat,
                "total subsidy interval",
            )?,
            miner_subsidy_zat: checked_sub_mining_total(
                self.miner_subsidy_zat,
                anchor.miner_subsidy_zat,
                "miner subsidy interval",
            )?,
            founders_reward_zat: checked_sub_mining_total(
                self.founders_reward_zat,
                anchor.founders_reward_zat,
                "founders reward interval",
            )?,
            funding_streams_zat: checked_sub_mining_total(
                self.funding_streams_zat,
                anchor.funding_streams_zat,
                "funding streams interval",
            )?,
            deferred_subsidy_zat: checked_sub_mining_total(
                self.deferred_subsidy_zat,
                anchor.deferred_subsidy_zat,
                "deferred subsidy interval",
            )?,
            lockbox_disbursement_zat: checked_sub_mining_total(
                self.lockbox_disbursement_zat,
                anchor.lockbox_disbursement_zat,
                "lockbox disbursement interval",
            )?,
            coinbase_output_transparent_zat: checked_sub_mining_total(
                self.coinbase_output_transparent_zat,
                anchor.coinbase_output_transparent_zat,
                "transparent coinbase output interval",
            )?,
            coinbase_output_sapling_zat: checked_sub_mining_total(
                self.coinbase_output_sapling_zat,
                anchor.coinbase_output_sapling_zat,
                "Sapling coinbase output interval",
            )?,
            coinbase_output_orchard_zat: checked_sub_mining_total(
                self.coinbase_output_orchard_zat,
                anchor.coinbase_output_orchard_zat,
                "Orchard coinbase output interval",
            )?,
            coinbase_output_ironwood_zat: checked_sub_mining_total(
                self.coinbase_output_ironwood_zat,
                anchor.coinbase_output_ironwood_zat,
                "Ironwood coinbase output interval",
            )?,
            coinbase_unclaimed_zat: checked_sub_mining_total(
                self.coinbase_unclaimed_zat,
                anchor.coinbase_unclaimed_zat,
                "unclaimed coinbase value interval",
            )?,
        })
    }

    fn append_bytes(self, bytes: &mut Vec<u8>) {
        for count in self.transaction_counts {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        for flow in self.pool_flows {
            bytes.extend_from_slice(&flow.to_be_bytes());
        }
        for count in self.ironwood_counts {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        for count in self.ironwood_denomination_counts {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        bytes.extend_from_slice(&self.observable_orchard_to_ironwood_value_zat.to_be_bytes());
        bytes.extend_from_slice(&self.total_fees_zat.to_be_bytes());
        bytes.extend_from_slice(&self.total_block_size.to_be_bytes());
        bytes.extend_from_slice(&self.block_count.to_be_bytes());
        bytes.extend_from_slice(&self.transaction_count.to_be_bytes());
        bytes.extend_from_slice(&self.empty_block_count.to_be_bytes());
        bytes.extend_from_slice(&self.accepted_work.to_be_bytes());
        bytes.extend_from_slice(&self.total_subsidy_zat.to_be_bytes());
        bytes.extend_from_slice(&self.miner_subsidy_zat.to_be_bytes());
        bytes.extend_from_slice(&self.founders_reward_zat.to_be_bytes());
        bytes.extend_from_slice(&self.funding_streams_zat.to_be_bytes());
        bytes.extend_from_slice(&self.deferred_subsidy_zat.to_be_bytes());
        bytes.extend_from_slice(&self.lockbox_disbursement_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_output_transparent_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_output_sapling_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_output_orchard_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_output_ironwood_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_unclaimed_zat.to_be_bytes());
    }

    fn take_bytes(bytes: &[u8], offset: &mut usize) -> Self {
        let mut transaction_counts = [0; SNAPSHOT_TRANSACTION_CLASS_COUNT];
        for count in &mut transaction_counts {
            *count = u64::from_be_bytes(take_accumulator_bytes(bytes, offset));
        }

        let mut pool_flows = [0; SNAPSHOT_POOL_FLOW_COUNT];
        for flow in &mut pool_flows {
            *flow = u128::from_be_bytes(take_accumulator_bytes(bytes, offset));
        }

        let mut ironwood_counts = [0; SNAPSHOT_IRONWOOD_COUNTER_COUNT];
        for count in &mut ironwood_counts {
            *count = u64::from_be_bytes(take_accumulator_bytes(bytes, offset));
        }

        let mut ironwood_denomination_counts = [0; IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len()];
        for count in &mut ironwood_denomination_counts {
            *count = u64::from_be_bytes(take_accumulator_bytes(bytes, offset));
        }

        Self {
            transaction_counts,
            pool_flows,
            ironwood_counts,
            ironwood_denomination_counts,
            observable_orchard_to_ironwood_value_zat: u128::from_be_bytes(take_accumulator_bytes(
                bytes, offset,
            )),
            total_fees_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            total_block_size: u64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            block_count: u64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            transaction_count: u64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            empty_block_count: u64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            accepted_work: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            total_subsidy_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            miner_subsidy_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            founders_reward_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            funding_streams_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            deferred_subsidy_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            lockbox_disbursement_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            coinbase_output_transparent_zat: u128::from_be_bytes(take_accumulator_bytes(
                bytes, offset,
            )),
            coinbase_output_sapling_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            coinbase_output_orchard_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            coinbase_output_ironwood_zat: u128::from_be_bytes(take_accumulator_bytes(
                bytes, offset,
            )),
            coinbase_unclaimed_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
        }
    }
}

fn checked_sub_mining_total(
    total: u128,
    anchor: u128,
    metric: &'static str,
) -> Result<u128, SnapshotAccumulatorError> {
    total
        .checked_sub(anchor)
        .ok_or(SnapshotAccumulatorError::Arithmetic { metric })
}

/// The minimum and maximum committed header times observed in an active snapshot interval.
///
/// Each range includes the interval's anchor header, matching the denominator used by Zebra's
/// `getnetworksolps` estimator for a contiguous block window.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct SnapshotHeaderTimeRange {
    min_timestamp: i64,
    max_timestamp: i64,
}

impl SnapshotHeaderTimeRange {
    fn singleton(timestamp: i64) -> Self {
        Self {
            min_timestamp: timestamp,
            max_timestamp: timestamp,
        }
    }

    fn include(&mut self, timestamp: i64) {
        self.min_timestamp = self.min_timestamp.min(timestamp);
        self.max_timestamp = self.max_timestamp.max(timestamp);
    }

    fn elapsed_seconds(self) -> u64 {
        u64::try_from(self.max_timestamp.saturating_sub(self.min_timestamp)).unwrap_or(u64::MAX)
    }

    fn append_bytes(self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.min_timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.max_timestamp.to_be_bytes());
    }

    fn take_bytes(bytes: &[u8], offset: &mut usize) -> Self {
        let min_timestamp = i64::from_be_bytes(take_accumulator_bytes(bytes, offset));
        let max_timestamp = i64::from_be_bytes(take_accumulator_bytes(bytes, offset));
        assert!(
            min_timestamp <= max_timestamp,
            "snapshot header-time range minimum must not exceed its maximum"
        );
        Self {
            min_timestamp,
            max_timestamp,
        }
    }
}

/// A cumulative-metrics anchor at a daily snapshot boundary.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct SnapshotMetricAnchor {
    /// `false` identifies the virtual point before genesis.
    initialized: bool,
    height: u32,
    timestamp: i64,
    totals: SnapshotMetricTotals,
}

impl SnapshotMetricAnchor {
    fn before_genesis(genesis_timestamp: i64) -> Self {
        Self {
            timestamp: genesis_timestamp,
            ..Self::default()
        }
    }

    fn append_bytes(self, bytes: &mut Vec<u8>) {
        bytes.push(u8::from(self.initialized));
        bytes.extend_from_slice(&self.height.to_be_bytes());
        bytes.extend_from_slice(&self.timestamp.to_be_bytes());
        self.totals.append_bytes(bytes);
    }

    fn take_bytes(bytes: &[u8], offset: &mut usize) -> Self {
        let initialized = match take_accumulator_bytes::<1>(bytes, offset)[0] {
            0 => false,
            1 => true,
            value => panic!("invalid snapshot accumulator anchor flag: {value}"),
        };

        Self {
            initialized,
            height: u32::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            timestamp: i64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            totals: SnapshotMetricTotals::take_bytes(bytes, offset),
        }
    }
}

/// Incremental snapshot state persisted atomically with every finalized block.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotAccumulator {
    latest_height: u32,
    latest_timestamp: i64,
    latest_pool_values: ValueBalance<NonNegative>,
    latest_work_difficulty_bits: u64,
    funded_transparent_address_count: u64,
    total_issuance: u64,
    totals: SnapshotMetricTotals,
    /// Header-time range beginning at `daily_anchor`.
    daily_header_time_range: SnapshotHeaderTimeRange,
    /// Header-time range beginning at `realtime_anchor`.
    realtime_header_time_range: SnapshotHeaderTimeRange,
    /// The latest daily snapshot endpoint.
    daily_anchor: SnapshotMetricAnchor,
    /// The endpoint preceding the latest daily snapshot.
    ///
    /// Realtime data replaces the latest daily row, so it spans from this anchor.
    realtime_anchor: SnapshotMetricAnchor,
}

impl SnapshotAccumulator {
    fn new_at_genesis(
        genesis_timestamp: i64,
        genesis_pool_values: ValueBalance<NonNegative>,
    ) -> Self {
        let before_genesis = SnapshotMetricAnchor::before_genesis(genesis_timestamp);
        Self {
            latest_height: Height::MIN.0,
            latest_timestamp: genesis_timestamp,
            latest_pool_values: genesis_pool_values,
            latest_work_difficulty_bits: 0.0f64.to_bits(),
            funded_transparent_address_count: 0,
            total_issuance: 0,
            totals: SnapshotMetricTotals::default(),
            daily_header_time_range: SnapshotHeaderTimeRange::singleton(genesis_timestamp),
            realtime_header_time_range: SnapshotHeaderTimeRange::singleton(genesis_timestamp),
            daily_anchor: before_genesis,
            realtime_anchor: before_genesis,
        }
    }

    fn current_anchor(&self) -> SnapshotMetricAnchor {
        SnapshotMetricAnchor {
            initialized: true,
            height: self.latest_height,
            timestamp: self.latest_timestamp,
            totals: self.totals,
        }
    }

    fn apply_funded_count_delta(&mut self, delta: i64) -> Result<(), SnapshotAccumulatorError> {
        if delta >= 0 {
            self.funded_transparent_address_count = self
                .funded_transparent_address_count
                .checked_add(delta as u64)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "funded transparent address count",
                })?;
        } else {
            let magnitude = delta
                .checked_abs()
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "funded transparent address count delta",
                })? as u64;
            self.funded_transparent_address_count = self
                .funded_transparent_address_count
                .checked_sub(magnitude)
                .ok_or(SnapshotAccumulatorError::Arithmetic {
                    metric: "funded transparent address count",
                })?;
        }

        Ok(())
    }
}

impl IntoDisk for SnapshotAccumulator {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = Vec::with_capacity(SNAPSHOT_ACCUMULATOR_LEN);
        bytes.extend_from_slice(&SNAPSHOT_ACCUMULATOR_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.latest_height.to_be_bytes());
        bytes.extend_from_slice(&self.latest_timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.latest_pool_values.as_bytes());
        bytes.extend_from_slice(&self.latest_work_difficulty_bits.to_be_bytes());
        bytes.extend_from_slice(&self.funded_transparent_address_count.to_be_bytes());
        bytes.extend_from_slice(&self.total_issuance.to_be_bytes());
        self.totals.append_bytes(&mut bytes);
        self.daily_header_time_range.append_bytes(&mut bytes);
        self.realtime_header_time_range.append_bytes(&mut bytes);
        self.daily_anchor.append_bytes(&mut bytes);
        self.realtime_anchor.append_bytes(&mut bytes);
        debug_assert_eq!(bytes.len(), SNAPSHOT_ACCUMULATOR_LEN);
        bytes
    }
}

impl FromDisk for SnapshotAccumulator {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        assert_eq!(
            bytes.len(),
            SNAPSHOT_ACCUMULATOR_LEN,
            "SnapshotAccumulator deserialization error: expected \
             {SNAPSHOT_ACCUMULATOR_LEN} bytes, got {} bytes",
            bytes.len()
        );

        let mut offset = 0;
        let version = u32::from_be_bytes(take_accumulator_bytes(bytes, &mut offset));
        assert_eq!(
            version, SNAPSHOT_ACCUMULATOR_VERSION,
            "unsupported SnapshotAccumulator version {version}"
        );
        let latest_height = u32::from_be_bytes(take_accumulator_bytes(bytes, &mut offset));
        let latest_timestamp = i64::from_be_bytes(take_accumulator_bytes(bytes, &mut offset));
        let pool_end = offset + 48;
        let latest_pool_values = ValueBalance::<NonNegative>::from_bytes(&bytes[offset..pool_end])
            .expect("snapshot accumulator pool values must be valid");
        offset = pool_end;
        let latest_work_difficulty_bits =
            u64::from_be_bytes(take_accumulator_bytes(bytes, &mut offset));
        let funded_transparent_address_count =
            u64::from_be_bytes(take_accumulator_bytes(bytes, &mut offset));
        let total_issuance = u64::from_be_bytes(take_accumulator_bytes(bytes, &mut offset));
        let totals = SnapshotMetricTotals::take_bytes(bytes, &mut offset);
        let daily_header_time_range = SnapshotHeaderTimeRange::take_bytes(bytes, &mut offset);
        let realtime_header_time_range = SnapshotHeaderTimeRange::take_bytes(bytes, &mut offset);
        let daily_anchor = SnapshotMetricAnchor::take_bytes(bytes, &mut offset);
        let realtime_anchor = SnapshotMetricAnchor::take_bytes(bytes, &mut offset);

        assert_eq!(
            offset,
            bytes.len(),
            "snapshot accumulator decoder must consume the entire record"
        );

        Self {
            latest_height,
            latest_timestamp,
            latest_pool_values,
            latest_work_difficulty_bits,
            funded_transparent_address_count,
            total_issuance,
            totals,
            daily_header_time_range,
            realtime_header_time_range,
            daily_anchor,
            realtime_anchor,
        }
    }
}

fn take_accumulator_bytes<const N: usize>(bytes: &[u8], offset: &mut usize) -> [u8; N] {
    let end = offset
        .checked_add(N)
        .expect("snapshot accumulator byte offset must not overflow");
    let value = bytes
        .get(*offset..end)
        .unwrap_or_else(|| panic!("snapshot accumulator is truncated at byte offset {offset}"));
    *offset = end;
    value
        .try_into()
        .expect("checked snapshot accumulator field must have the requested length")
}

/// Snapshot records written before the Ironwood value pool was added.
const LEGACY_SNAPSHOT_DATA_LEN: usize = 184;
/// Snapshot records written after the Ironwood pool was added, but before its metrics were stored.
const TRANSITIONAL_SNAPSHOT_DATA_LEN: usize = 192;
/// Date-indexed snapshot records written by early versions of the fork, with a 32-byte expanded
/// difficulty. (The older 196-byte records used a separate height-indexed column family.)
const EXPANDED_DIFFICULTY_SNAPSHOT_DATA_LEN: usize = 208;
/// Snapshot records with the Ironwood pool and its transaction/flow metrics, but no mining totals.
const IRONWOOD_SNAPSHOT_DATA_LEN: usize = 212;
/// The encoded byte length of [`MiningIntervalData`] before Ironwood observatory counters.
const LEGACY_MINING_INTERVAL_DATA_LEN: usize = 256;
/// The encoded Ironwood observatory suffix in [`MiningIntervalData`].
const IRONWOOD_OBSERVATORY_INTERVAL_DATA_LEN: usize =
    SNAPSHOT_IRONWOOD_COUNTER_COUNT * 8 + IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len() * 8 + 16;
/// The encoded byte length of the current [`MiningIntervalData`].
const MINING_INTERVAL_DATA_LEN: usize =
    LEGACY_MINING_INTERVAL_DATA_LEN + IRONWOOD_OBSERVATORY_INTERVAL_DATA_LEN;
/// Snapshot records with exact interval mining totals but no Ironwood observatory counters.
const MINING_SNAPSHOT_DATA_LEN: usize =
    IRONWOOD_SNAPSHOT_DATA_LEN + LEGACY_MINING_INTERVAL_DATA_LEN;
/// Current snapshot records, including exact interval mining and Ironwood observatory totals.
const CURRENT_SNAPSHOT_DATA_LEN: usize = IRONWOOD_SNAPSHOT_DATA_LEN + MINING_INTERVAL_DATA_LEN;

/// The target duration represented by the first date-indexed snapshot.
///
/// Upgraded databases can have no compatible daily anchor. Limiting that first metrics interval
/// prevents a synchronous scan from genesis to the current tip on the sole state writer thread.
const INITIAL_SNAPSHOT_METRICS_SECONDS: u32 = 24 * 60 * 60;

/// Calculates the annual issuance rate as a percentage of the current pool supply.
fn inflation_rate_percent(
    height: Height,
    network: &Network,
    total_supply: Amount<NonNegative>,
) -> Result<f64, BoxError> {
    let blocks_per_year =
        if let Some(blossom_height) = NetworkUpgrade::Blossom.activation_height(network) {
            if height >= blossom_height {
                420_480.0
            } else {
                210_240.0
            }
        } else {
            210_240.0
        };

    let block_subsidy_amount = block_subsidy(height, network)
        .map_err(|error| format!("failed to get block subsidy: {error}"))?;
    if total_supply == Amount::<NonNegative>::zero() {
        return Ok(0.0);
    }

    Ok(
        (block_subsidy_amount.zatoshis() as f64 * blocks_per_year / total_supply.zatoshis() as f64)
            * 100.0,
    )
}

/// Identifies the source disk layout of a decoded snapshot.
///
/// This metadata is not serialized. It is used to normalize fields whose historical meaning or
/// representation changed, and to incrementally correct issuance from old snapshots.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SnapshotDiskFormat {
    Legacy,
    Transitional,
    ExpandedDifficulty,
    /// Current mining totals, written before independent Ironwood counters were added.
    Mining,
    /// A historical layout whose legacy-only fields have already been normalized in memory.
    ResolvedLegacy,
    Current,
}

/// Exact on-chain mining totals for the height interval represented by a snapshot.
///
/// These fields intentionally contain additive integer facts. Consumers can safely combine daily
/// rows before deriving averages. `accepted_work / elapsed_header_time_seconds` is an estimated
/// network solution rate, not directly-observed hashrate.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MiningIntervalData {
    block_count: u64,
    transaction_count: u64,
    empty_block_count: u64,
    min_header_timestamp: i64,
    max_header_timestamp: i64,
    accepted_work: u128,
    total_fees_zat: u128,
    total_block_size_bytes: u64,
    total_subsidy_zat: u128,
    miner_subsidy_zat: u128,
    founders_reward_zat: u128,
    funding_streams_zat: u128,
    deferred_subsidy_zat: u128,
    lockbox_disbursement_zat: u128,
    coinbase_output_transparent_zat: u128,
    coinbase_output_sapling_zat: u128,
    coinbase_output_orchard_zat: u128,
    coinbase_output_ironwood_zat: u128,
    coinbase_unclaimed_zat: u128,
    ironwood_counts: [u64; SNAPSHOT_IRONWOOD_COUNTER_COUNT],
    ironwood_denomination_counts: [u64; IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len()],
    observable_orchard_to_ironwood_value_zat: u128,
}

impl MiningIntervalData {
    fn from_interval(
        interval: SnapshotMetricTotals,
        header_time_range: SnapshotHeaderTimeRange,
    ) -> Self {
        Self {
            block_count: interval.block_count,
            transaction_count: interval.transaction_count,
            empty_block_count: interval.empty_block_count,
            min_header_timestamp: header_time_range.min_timestamp,
            max_header_timestamp: header_time_range.max_timestamp,
            accepted_work: interval.accepted_work,
            total_fees_zat: interval.total_fees_zat,
            total_block_size_bytes: interval.total_block_size,
            total_subsidy_zat: interval.total_subsidy_zat,
            miner_subsidy_zat: interval.miner_subsidy_zat,
            founders_reward_zat: interval.founders_reward_zat,
            funding_streams_zat: interval.funding_streams_zat,
            deferred_subsidy_zat: interval.deferred_subsidy_zat,
            lockbox_disbursement_zat: interval.lockbox_disbursement_zat,
            coinbase_output_transparent_zat: interval.coinbase_output_transparent_zat,
            coinbase_output_sapling_zat: interval.coinbase_output_sapling_zat,
            coinbase_output_orchard_zat: interval.coinbase_output_orchard_zat,
            coinbase_output_ironwood_zat: interval.coinbase_output_ironwood_zat,
            coinbase_unclaimed_zat: interval.coinbase_unclaimed_zat,
            ironwood_counts: interval.ironwood_counts,
            ironwood_denomination_counts: interval.ironwood_denomination_counts,
            observable_orchard_to_ironwood_value_zat: interval
                .observable_orchard_to_ironwood_value_zat,
        }
    }

    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    pub fn transaction_count(&self) -> u64 {
        self.transaction_count
    }

    pub fn empty_block_count(&self) -> u64 {
        self.empty_block_count
    }

    pub fn min_header_timestamp(&self) -> i64 {
        self.min_header_timestamp
    }

    pub fn max_header_timestamp(&self) -> i64 {
        self.max_header_timestamp
    }

    pub fn elapsed_header_time_seconds(&self) -> u64 {
        SnapshotHeaderTimeRange {
            min_timestamp: self.min_header_timestamp,
            max_timestamp: self.max_header_timestamp,
        }
        .elapsed_seconds()
    }

    pub fn accepted_work(&self) -> u128 {
        self.accepted_work
    }

    pub fn estimated_network_solution_rate(&self) -> Option<u128> {
        let elapsed = self.elapsed_header_time_seconds();
        (elapsed > 0).then(|| self.accepted_work / u128::from(elapsed))
    }

    pub fn total_fees_zat(&self) -> u128 {
        self.total_fees_zat
    }

    pub fn total_block_size_bytes(&self) -> u64 {
        self.total_block_size_bytes
    }

    pub fn total_subsidy_zat(&self) -> u128 {
        self.total_subsidy_zat
    }

    pub fn miner_subsidy_zat(&self) -> u128 {
        self.miner_subsidy_zat
    }

    pub fn founders_reward_zat(&self) -> u128 {
        self.founders_reward_zat
    }

    /// Returns direct funding-stream outputs, excluding deferred-pool contributions.
    pub fn funding_streams_zat(&self) -> u128 {
        self.funding_streams_zat
    }

    /// Returns new subsidy routed into the deferred pool, not later lockbox disbursements.
    pub fn deferred_subsidy_zat(&self) -> u128 {
        self.deferred_subsidy_zat
    }

    /// Returns output value released from the existing deferred pool, not new issuance.
    pub fn lockbox_disbursement_zat(&self) -> u128 {
        self.lockbox_disbursement_zat
    }

    pub fn coinbase_output_transparent_zat(&self) -> u128 {
        self.coinbase_output_transparent_zat
    }

    pub fn coinbase_output_sapling_zat(&self) -> u128 {
        self.coinbase_output_sapling_zat
    }

    pub fn coinbase_output_orchard_zat(&self) -> u128 {
        self.coinbase_output_orchard_zat
    }

    pub fn coinbase_output_ironwood_zat(&self) -> u128 {
        self.coinbase_output_ironwood_zat
    }

    /// Returns consensus-permitted coinbase value left unclaimed in outputs.
    ///
    /// This remainder was historically possible before NU6 and is not necessarily miner
    /// allocation.
    pub fn coinbase_unclaimed_zat(&self) -> u128 {
        self.coinbase_unclaimed_zat
    }

    pub fn v6_transaction_count(&self) -> u64 {
        self.ironwood_counts[IRONWOOD_V6_TX_INDEX]
    }

    pub fn ironwood_bundle_transaction_count(&self) -> u64 {
        self.ironwood_counts[IRONWOOD_BUNDLE_TX_INDEX]
    }

    pub fn orchard_bundle_transaction_count(&self) -> u64 {
        self.ironwood_counts[ORCHARD_BUNDLE_TX_INDEX]
    }

    pub fn orchard_ironwood_transaction_count(&self) -> u64 {
        self.ironwood_counts[ORCHARD_IRONWOOD_TX_INDEX]
    }

    pub fn orchard_action_count(&self) -> u64 {
        self.ironwood_counts[ORCHARD_ACTION_INDEX]
    }

    pub fn ironwood_action_count(&self) -> u64 {
        self.ironwood_counts[IRONWOOD_ACTION_INDEX]
    }

    pub fn ironwood_active_block_count(&self) -> u64 {
        self.ironwood_counts[IRONWOOD_ACTIVE_BLOCK_INDEX]
    }

    pub fn observable_orchard_to_ironwood_transaction_count(&self) -> u64 {
        self.ironwood_counts[OBSERVABLE_ORCHARD_TO_IRONWOOD_TX_INDEX]
    }

    pub fn observable_orchard_to_ironwood_value_zat(&self) -> u128 {
        self.observable_orchard_to_ironwood_value_zat
    }

    pub fn zip318_action_shape_transaction_count(&self) -> u64 {
        self.ironwood_counts[ZIP318_ACTION_SHAPE_TX_INDEX]
    }

    pub fn zip318_denomination_transaction_count(&self) -> u64 {
        self.ironwood_counts[ZIP318_DENOMINATION_TX_INDEX]
    }

    pub fn zip318_fee_transaction_count(&self) -> u64 {
        self.ironwood_counts[ZIP318_FEE_TX_INDEX]
    }

    pub fn zip318_schedule_transaction_count(&self) -> u64 {
        self.ironwood_counts[ZIP318_SCHEDULE_TX_INDEX]
    }

    pub fn ironwood_denomination_counts(
        &self,
    ) -> [u64; IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len()] {
        self.ironwood_denomination_counts
    }

    fn append_bytes(self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.block_count.to_be_bytes());
        bytes.extend_from_slice(&self.transaction_count.to_be_bytes());
        bytes.extend_from_slice(&self.empty_block_count.to_be_bytes());
        bytes.extend_from_slice(&self.min_header_timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.max_header_timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.accepted_work.to_be_bytes());
        bytes.extend_from_slice(&self.total_fees_zat.to_be_bytes());
        bytes.extend_from_slice(&self.total_block_size_bytes.to_be_bytes());
        bytes.extend_from_slice(&self.total_subsidy_zat.to_be_bytes());
        bytes.extend_from_slice(&self.miner_subsidy_zat.to_be_bytes());
        bytes.extend_from_slice(&self.founders_reward_zat.to_be_bytes());
        bytes.extend_from_slice(&self.funding_streams_zat.to_be_bytes());
        bytes.extend_from_slice(&self.deferred_subsidy_zat.to_be_bytes());
        bytes.extend_from_slice(&self.lockbox_disbursement_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_output_transparent_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_output_sapling_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_output_orchard_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_output_ironwood_zat.to_be_bytes());
        bytes.extend_from_slice(&self.coinbase_unclaimed_zat.to_be_bytes());
        for count in self.ironwood_counts {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        for count in self.ironwood_denomination_counts {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        bytes.extend_from_slice(&self.observable_orchard_to_ironwood_value_zat.to_be_bytes());
    }

    fn take_bytes(bytes: &[u8], offset: &mut usize, has_ironwood_observatory: bool) -> Self {
        let mut value = Self {
            block_count: u64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            transaction_count: u64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            empty_block_count: u64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            min_header_timestamp: i64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            max_header_timestamp: i64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            accepted_work: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            total_fees_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            total_block_size_bytes: u64::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            total_subsidy_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            miner_subsidy_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            founders_reward_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            funding_streams_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            deferred_subsidy_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            lockbox_disbursement_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            coinbase_output_transparent_zat: u128::from_be_bytes(take_accumulator_bytes(
                bytes, offset,
            )),
            coinbase_output_sapling_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            coinbase_output_orchard_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            coinbase_output_ironwood_zat: u128::from_be_bytes(take_accumulator_bytes(
                bytes, offset,
            )),
            coinbase_unclaimed_zat: u128::from_be_bytes(take_accumulator_bytes(bytes, offset)),
            ironwood_counts: [0; SNAPSHOT_IRONWOOD_COUNTER_COUNT],
            ironwood_denomination_counts: [0; IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len()],
            observable_orchard_to_ironwood_value_zat: 0,
        };

        if has_ironwood_observatory {
            for count in &mut value.ironwood_counts {
                *count = u64::from_be_bytes(take_accumulator_bytes(bytes, offset));
            }
            for count in &mut value.ironwood_denomination_counts {
                *count = u64::from_be_bytes(take_accumulator_bytes(bytes, offset));
            }
            value.observable_orchard_to_ironwood_value_zat =
                u128::from_be_bytes(take_accumulator_bytes(bytes, offset));
        }

        value
    }
}

/// Snapshot date key in format YY:MM:DD (year, month, day)
/// Stored as 3 bytes: [year (0-99), month (1-12), day (1-31)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotDateKey {
    /// Year (0-99, representing 2000-2099)
    pub year: u8,
    /// Month (1-12)
    pub month: u8,
    /// Day (1-31)
    pub day: u8,
}

impl SnapshotDateKey {
    /// Create a new SnapshotDateKey from year, month, day
    pub fn new(year: u8, month: u8, day: u8) -> Self {
        SnapshotDateKey { year, month, day }
    }

    /// Create a SnapshotDateKey from a timestamp
    pub fn from_timestamp(timestamp: i64) -> Self {
        let datetime = chrono::DateTime::from_timestamp(timestamp, 0)
            .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap());
        let date = datetime.date_naive();
        // Use format to extract date components
        let year_str = date.format("%y").to_string();
        let month_str = date.format("%m").to_string();
        let day_str = date.format("%d").to_string();
        SnapshotDateKey {
            year: year_str.parse::<u8>().unwrap_or(0),
            month: month_str.parse::<u8>().unwrap_or(1),
            day: day_str.parse::<u8>().unwrap_or(1),
        }
    }
}

impl fmt::Display for SnapshotDateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:02}:{:02}:{:02}",
            self.year, self.month, self.day
        )
    }
}

impl IntoDisk for SnapshotDateKey {
    type Bytes = [u8; 3];

    fn as_bytes(&self) -> Self::Bytes {
        [self.year, self.month, self.day]
    }
}

impl FromDisk for SnapshotDateKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        SnapshotDateKey {
            year: bytes[0],
            month: bytes[1],
            day: bytes[2],
        }
    }
}

/// Snapshot data stored in RocksDB containing funded transparent address count, pool values,
/// difficulty, issuance, timestamp, and transaction counts.
///
/// Floating-point metrics are stored as their IEEE bit patterns. This makes equality match the
/// on-disk representation and remain reflexive for NaN values; different NaN payloads and signed
/// zeroes remain distinct.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SnapshotData {
    /// The disk layout this value was decoded from, or `Current` for newly-created snapshots.
    disk_format: SnapshotDiskFormat,
    /// The old 32-byte expanded target, resolved to `work_difficulty_bits` by `ZebraDb` reads.
    legacy_expanded_difficulty: Option<[u8; 32]>,
    /// Number of transparent addresses with non-zero finalized balances.
    ///
    /// This is not a count of people or shielded holders.
    funded_transparent_address_count: u64,
    /// Pool values (value balance) at this height.
    pool_values: ValueBalance<NonNegative>,
    /// Mining work difficulty (as a multiple of the minimum difficulty, stored as f64).
    /// This matches the value returned by the `getdifficulty` RPC method.
    work_difficulty_bits: u64,
    /// Total ZEC issuance up to this height (in zatoshis).
    total_issuance: u64,
    /// Inflation rate per year (in basis points, e.g., 200 = 2.00%).
    /// Stored as u32: 0 = 0%, 10000 = 100.00%.
    inflation_rate_bps: u32,
    /// Block timestamp (Unix timestamp in seconds).
    block_timestamp: i64,
    /// Block height at which this snapshot was taken.
    block_height: u32,
    /// Number of regular transparent transactions (from previous snapshot to this snapshot).
    /// Excludes coinbase transactions.
    transparent_tx_count: u32,
    /// Number of transparent coinbase transactions (from previous snapshot to this snapshot).
    /// Excludes shielded coinbase transactions.
    transparent_coinbase_tx_count: u32,
    /// Number of shielded coinbase migration transactions (from previous snapshot to this snapshot).
    /// These are transactions that spend coinbase UTXOs and send to shielded addresses.
    shielded_coinbase_migration_tx_count: u32,
    /// Number of sprout transactions (from previous snapshot to this snapshot).
    /// Excludes shielded coinbase migration transactions to avoid double counting.
    sprout_tx_count: u32,
    /// Number of sapling transactions (from previous snapshot to this snapshot).
    sapling_tx_count: u32,
    /// Number of orchard transactions (from previous snapshot to this snapshot).
    orchard_tx_count: u32,
    /// Number of Ironwood transactions (from previous snapshot to this snapshot).
    ironwood_tx_count: u32,
    /// Transparent pool inflow (from previous snapshot to this snapshot, in zatoshis).
    transparent_inflow: u64,
    /// Transparent pool outflow (from previous snapshot to this snapshot, in zatoshis).
    transparent_outflow: u64,
    /// Sprout pool inflow (from previous snapshot to this snapshot, in zatoshis).
    sprout_inflow: u64,
    /// Sprout pool outflow (from previous snapshot to this snapshot, in zatoshis).
    sprout_outflow: u64,
    /// Sapling pool inflow (from previous snapshot to this snapshot, in zatoshis).
    sapling_inflow: u64,
    /// Sapling pool outflow (from previous snapshot to this snapshot, in zatoshis).
    sapling_outflow: u64,
    /// Orchard pool inflow (from previous snapshot to this snapshot, in zatoshis).
    orchard_inflow: u64,
    /// Orchard pool outflow (from previous snapshot to this snapshot, in zatoshis).
    orchard_outflow: u64,
    /// Ironwood pool inflow (from previous snapshot to this snapshot, in zatoshis).
    ironwood_inflow: u64,
    /// Ironwood pool outflow (from previous snapshot to this snapshot, in zatoshis).
    ironwood_outflow: u64,
    /// Average block time in seconds (from previous snapshot to this snapshot).
    /// Stored as f32 (4 bytes) to save space, precision is sufficient for block time.
    average_block_time_bits: u32,
    /// Average transaction fee in zatoshis per block (from previous snapshot to this snapshot).
    average_block_fee_zat: u64,
    /// Average block size in bytes (from previous snapshot to this snapshot).
    average_block_size: u32,
    /// Exact additive mining metrics for this interval, or `None` for historical layouts.
    mining_interval: Option<MiningIntervalData>,
}

impl SnapshotData {
    /// Creates a new SnapshotData.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        funded_transparent_address_count: u64,
        pool_values: ValueBalance<NonNegative>,
        work_difficulty: f64,
        total_issuance: Amount<NonNegative>,
        inflation_rate_percent: f64,
        block_timestamp: i64,
        block_height: u32,
        transparent_tx_count: u32,
        transparent_coinbase_tx_count: u32,
        shielded_coinbase_migration_tx_count: u32,
        sprout_tx_count: u32,
        sapling_tx_count: u32,
        orchard_tx_count: u32,
        ironwood_tx_count: u32,
        transparent_inflow: u64,
        transparent_outflow: u64,
        sprout_inflow: u64,
        sprout_outflow: u64,
        sapling_inflow: u64,
        sapling_outflow: u64,
        orchard_inflow: u64,
        orchard_outflow: u64,
        ironwood_inflow: u64,
        ironwood_outflow: u64,
        average_block_time: f32,
        average_block_fee_zat: Amount<NonNegative>,
        average_block_size: u32,
        mining_interval: Option<MiningIntervalData>,
    ) -> Self {
        // Convert inflation rate to basis points (hundredths of a percent)
        let inflation_rate_bps = (inflation_rate_percent * 100.0).round() as u32;

        SnapshotData {
            disk_format: SnapshotDiskFormat::Current,
            legacy_expanded_difficulty: None,
            funded_transparent_address_count,
            pool_values,
            work_difficulty_bits: work_difficulty.to_bits(),
            total_issuance: total_issuance.zatoshis() as u64,
            inflation_rate_bps,
            block_timestamp,
            block_height,
            transparent_tx_count,
            transparent_coinbase_tx_count,
            shielded_coinbase_migration_tx_count,
            sprout_tx_count,
            sapling_tx_count,
            orchard_tx_count,
            ironwood_tx_count,
            transparent_inflow,
            transparent_outflow,
            sprout_inflow,
            sprout_outflow,
            sapling_inflow,
            sapling_outflow,
            orchard_inflow,
            orchard_outflow,
            ironwood_inflow,
            ironwood_outflow,
            average_block_time_bits: average_block_time.to_bits(),
            average_block_fee_zat: average_block_fee_zat.zatoshis() as u64,
            average_block_size,
            mining_interval,
        }
    }

    fn from_accumulator(
        accumulator: &SnapshotAccumulator,
        interval_anchor: SnapshotMetricAnchor,
        header_time_range: SnapshotHeaderTimeRange,
        network: &Network,
    ) -> Result<Self, SnapshotAccumulatorError> {
        fn count(
            interval: &SnapshotMetricTotals,
            index: usize,
            metric: &'static str,
        ) -> Result<u32, SnapshotAccumulatorError> {
            u32::try_from(interval.transaction_counts[index])
                .map_err(|_| SnapshotAccumulatorError::Arithmetic { metric })
        }

        fn flow(
            interval: &SnapshotMetricTotals,
            index: usize,
            metric: &'static str,
        ) -> Result<u64, SnapshotAccumulatorError> {
            u64::try_from(interval.pool_flows[index])
                .map_err(|_| SnapshotAccumulatorError::Arithmetic { metric })
        }

        let interval = accumulator.totals.checked_sub(interval_anchor.totals)?;
        let mining_interval = MiningIntervalData::from_interval(interval, header_time_range);
        let time_interval_count = if interval_anchor.initialized {
            interval.block_count
        } else {
            // The virtual pre-genesis anchor uses the genesis timestamp. There is no block-time
            // interval before genesis, so N blocks contain N-1 measurable intervals.
            interval.block_count.saturating_sub(1)
        };
        let average_block_time = if time_interval_count == 0 {
            0.0
        } else {
            let elapsed = accumulator
                .latest_timestamp
                .saturating_sub(interval_anchor.timestamp)
                .max(0);
            elapsed as f32 / time_interval_count as f32
        };

        let average_fee_zat = if interval.block_count == 0 {
            0
        } else {
            u64::try_from(interval.total_fees_zat / u128::from(interval.block_count)).map_err(
                |_| SnapshotAccumulatorError::Arithmetic {
                    metric: "average block fee",
                },
            )?
        };
        let average_fee = Amount::<NonNegative>::try_from(average_fee_zat).map_err(|error| {
            SnapshotAccumulatorError::InvalidValue {
                metric: "average block fee",
                reason: error.to_string(),
            }
        })?;
        let average_block_size = if interval.block_count == 0 {
            0
        } else {
            u32::try_from(interval.total_block_size / interval.block_count).map_err(|_| {
                SnapshotAccumulatorError::Arithmetic {
                    metric: "average block size",
                }
            })?
        };

        let pool_values = accumulator.latest_pool_values;
        let total_supply = (pool_values.transparent_amount()
            + pool_values.sprout_amount()
            + pool_values.sapling_amount()
            + pool_values.orchard_amount()
            + pool_values.deferred_amount()
            + pool_values.ironwood_amount())
        .map_err(|error| SnapshotAccumulatorError::InvalidValue {
            metric: "total supply",
            reason: error.to_string(),
        })?;
        let inflation_rate =
            inflation_rate_percent(Height(accumulator.latest_height), network, total_supply)
                .map_err(|error| SnapshotAccumulatorError::InvalidValue {
                    metric: "inflation rate",
                    reason: error.to_string(),
                })?;
        let total_issuance =
            Amount::<NonNegative>::try_from(accumulator.total_issuance).map_err(|error| {
                SnapshotAccumulatorError::InvalidValue {
                    metric: "total issuance",
                    reason: error.to_string(),
                }
            })?;

        Ok(Self::new(
            accumulator.funded_transparent_address_count,
            pool_values,
            f64::from_bits(accumulator.latest_work_difficulty_bits),
            total_issuance,
            inflation_rate,
            accumulator.latest_timestamp,
            accumulator.latest_height,
            count(&interval, TRANSPARENT_TX_INDEX, "transparent tx count")?,
            count(
                &interval,
                TRANSPARENT_COINBASE_TX_INDEX,
                "transparent coinbase tx count",
            )?,
            count(
                &interval,
                SHIELDED_COINBASE_MIGRATION_TX_INDEX,
                "shielded coinbase migration tx count",
            )?,
            count(&interval, SPROUT_TX_INDEX, "Sprout tx count")?,
            count(&interval, SAPLING_TX_INDEX, "Sapling tx count")?,
            count(&interval, ORCHARD_TX_INDEX, "Orchard tx count")?,
            count(&interval, IRONWOOD_TX_INDEX, "Ironwood tx count")?,
            flow(&interval, TRANSPARENT_INFLOW_INDEX, "transparent inflow")?,
            flow(&interval, TRANSPARENT_OUTFLOW_INDEX, "transparent outflow")?,
            flow(&interval, SPROUT_INFLOW_INDEX, "Sprout inflow")?,
            flow(&interval, SPROUT_OUTFLOW_INDEX, "Sprout outflow")?,
            flow(&interval, SAPLING_INFLOW_INDEX, "Sapling inflow")?,
            flow(&interval, SAPLING_OUTFLOW_INDEX, "Sapling outflow")?,
            flow(&interval, ORCHARD_INFLOW_INDEX, "Orchard inflow")?,
            flow(&interval, ORCHARD_OUTFLOW_INDEX, "Orchard outflow")?,
            flow(&interval, IRONWOOD_INFLOW_INDEX, "Ironwood inflow")?,
            flow(&interval, IRONWOOD_OUTFLOW_INDEX, "Ironwood outflow")?,
            average_block_time,
            average_fee,
            average_block_size,
            Some(mining_interval),
        ))
    }

    /// Returns the number of transparent addresses whose finalized balance is non-zero.
    ///
    /// This is not a count of people or shielded holders: shielded ownership is encrypted and
    /// cannot be derived from chain data.
    pub fn funded_transparent_address_count(&self) -> u64 {
        self.funded_transparent_address_count
    }

    /// Legacy compatibility alias. Prefer [`Self::funded_transparent_address_count`], because this
    /// value does not include shielded ownership and must not be interpreted as a person count.
    pub fn holder_count(&self) -> u64 {
        self.funded_transparent_address_count()
    }

    pub fn pool_values(&self) -> ValueBalance<NonNegative> {
        self.pool_values
    }

    pub fn work_difficulty(&self) -> f64 {
        f64::from_bits(self.work_difficulty_bits)
    }

    /// Normalizes fields whose historical representation or calculation differed.
    fn resolve_legacy_fields(mut self, network: &Network) -> Self {
        if let Some(expanded_difficulty) = self.legacy_expanded_difficulty.take() {
            self.work_difficulty_bits = Self::relative_work_difficulty(
                U256::from_big_endian(&expanded_difficulty),
                network,
            )
            .to_bits();
        }

        if matches!(
            self.disk_format,
            SnapshotDiskFormat::Legacy
                | SnapshotDiskFormat::Transitional
                | SnapshotDiskFormat::ExpandedDifficulty
        ) {
            use std::ops::Add;

            let slow_start_issuance = Self::slow_start_issuance_through(self.block_height, network)
                .expect("configured slow-start issuance must be valid");
            let corrected_issuance = self
                .total_issuance()
                .add(slow_start_issuance)
                .expect("corrected historical issuance must be valid");
            self.total_issuance = corrected_issuance.zatoshis() as u64;

            // Early writers forced both issuance and inflation to zero during slow start. The
            // pool values in those rows are valid, so restore the derived rate while normalizing.
            if self.block_height < network.slow_start_interval().0 {
                let total_supply = (self.pool_values.transparent_amount()
                    + self.pool_values.sprout_amount()
                    + self.pool_values.sapling_amount()
                    + self.pool_values.orchard_amount()
                    + self.pool_values.deferred_amount()
                    + self.pool_values.ironwood_amount())
                .expect("stored snapshot pool values must have a valid total");
                let inflation_rate =
                    inflation_rate_percent(Height(self.block_height), network, total_supply)
                        .expect("stored snapshot inflation inputs must be valid");
                self.inflation_rate_bps = (inflation_rate * 100.0).round() as u32;
            }

            // All legacy-only fields have now been converted to the current in-memory meaning.
            self.disk_format = SnapshotDiskFormat::ResolvedLegacy;
        }

        self
    }

    /// Returns cumulative slow-start issuance through `height` in constant time.
    ///
    /// Historical snapshot writers started summing at the end of slow start. During slow start,
    /// subsidy is `rate * height`, plus one rate unit in the second half. Summing that arithmetic
    /// sequence avoids calling `block_subsidy` up to 20,000 times for every legacy row read.
    fn slow_start_issuance_through(
        height: u32,
        network: &Network,
    ) -> Result<Amount<NonNegative>, BoxError> {
        let interval_height = network.slow_start_interval().0;
        if interval_height == 0 {
            return Ok(Amount::zero());
        }

        let last_slow_start_subsidy = block_subsidy(Height(interval_height - 1), network)
            .map_err(|error| format!("failed to calculate slow-start subsidy: {error}"))?;
        let interval = u128::from(interval_height);
        let last_subsidy = last_slow_start_subsidy.zatoshis() as u128;
        if last_subsidy % interval != 0 {
            return Err("last slow-start subsidy is not an exact multiple of its interval".into());
        }
        let rate = last_subsidy / interval;

        let end = u128::from(height.min(interval_height - 1));
        let triangular = end
            .checked_mul(end + 1)
            .and_then(|value| value.checked_div(2))
            .ok_or("overflow summing slow-start subsidy factors")?;
        let shift = u128::from(network.slow_start_shift().0);
        let second_half_count = if end >= shift { end - shift + 1 } else { 0 };
        let subsidy_factor = triangular
            .checked_add(second_half_count)
            .ok_or("overflow summing slow-start subsidy factors")?;
        let issuance = rate
            .checked_mul(subsidy_factor)
            .ok_or("overflow calculating slow-start issuance")?;
        let issuance =
            u64::try_from(issuance).map_err(|_| "slow-start issuance does not fit in u64")?;

        Amount::try_from(issuance)
            .map_err(|error| format!("invalid cumulative slow-start issuance: {error}").into())
    }

    /// Calculates work difficulty from an expanded target using the same approximation as the RPC.
    fn relative_work_difficulty(expanded_difficulty: U256, network: &Network) -> f64 {
        let pow_limit: U256 = network.target_difficulty_limit().into();
        let pow_limit = (pow_limit >> 128).as_u128() as f64;
        let difficulty = (expanded_difficulty >> 128).as_u128() as f64;

        if difficulty == 0.0 {
            0.0
        } else {
            pow_limit / difficulty
        }
    }

    pub fn total_issuance(&self) -> Amount<NonNegative> {
        Amount::try_from(self.total_issuance).expect("total_issuance should be valid")
    }

    pub fn inflation_rate_percent(&self) -> f64 {
        self.inflation_rate_bps as f64 / 100.0
    }

    pub fn block_timestamp(&self) -> i64 {
        self.block_timestamp
    }

    pub fn block_height(&self) -> u32 {
        self.block_height
    }

    pub fn transparent_tx_count(&self) -> u32 {
        self.transparent_tx_count
    }

    pub fn transparent_coinbase_tx_count(&self) -> u32 {
        self.transparent_coinbase_tx_count
    }

    pub fn shielded_coinbase_migration_tx_count(&self) -> u32 {
        self.shielded_coinbase_migration_tx_count
    }

    pub fn sprout_tx_count(&self) -> u32 {
        self.sprout_tx_count
    }

    pub fn sapling_tx_count(&self) -> u32 {
        self.sapling_tx_count
    }

    pub fn orchard_tx_count(&self) -> u32 {
        self.orchard_tx_count
    }

    pub fn ironwood_tx_count(&self) -> u32 {
        self.ironwood_tx_count
    }

    pub fn transparent_inflow(&self) -> u64 {
        self.transparent_inflow
    }

    pub fn transparent_outflow(&self) -> u64 {
        self.transparent_outflow
    }

    pub fn sprout_inflow(&self) -> u64 {
        self.sprout_inflow
    }

    pub fn sprout_outflow(&self) -> u64 {
        self.sprout_outflow
    }

    pub fn sapling_inflow(&self) -> u64 {
        self.sapling_inflow
    }

    pub fn sapling_outflow(&self) -> u64 {
        self.sapling_outflow
    }

    pub fn orchard_inflow(&self) -> u64 {
        self.orchard_inflow
    }

    pub fn orchard_outflow(&self) -> u64 {
        self.orchard_outflow
    }

    pub fn ironwood_inflow(&self) -> u64 {
        self.ironwood_inflow
    }

    pub fn ironwood_outflow(&self) -> u64 {
        self.ironwood_outflow
    }

    pub fn average_block_time(&self) -> f32 {
        f32::from_bits(self.average_block_time_bits)
    }

    pub fn average_block_fee_zat(&self) -> Amount<NonNegative> {
        Amount::try_from(self.average_block_fee_zat).expect("average_block_fee_zat should be valid")
    }

    pub fn average_block_size(&self) -> u32 {
        self.average_block_size
    }

    /// Returns exact additive on-chain mining totals, if this row uses the current disk layout.
    pub fn mining_interval(&self) -> Option<MiningIntervalData> {
        self.mining_interval
    }

    /// Returns independent Ironwood observatory totals when the snapshot was written by a format
    /// that actually indexed them. Older rows must report these metrics as unavailable rather
    /// than as misleading zeroes.
    pub fn ironwood_observatory_interval(&self) -> Option<MiningIntervalData> {
        (self.disk_format == SnapshotDiskFormat::Current)
            .then_some(self.mining_interval)
            .flatten()
    }
}

impl IntoDisk for SnapshotData {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        // Keep the original fields as a 192-byte prefix, then append Ironwood metrics.
        // This lets the decoder continue reading legacy 184-byte records and transitional
        // 192-byte records that contain the Ironwood value pool but not its metrics.
        let mut bytes = Vec::with_capacity(CURRENT_SNAPSHOT_DATA_LEN);
        bytes.extend_from_slice(&self.funded_transparent_address_count.to_be_bytes());
        bytes.extend_from_slice(&self.pool_values.as_bytes());
        bytes.extend_from_slice(&self.work_difficulty_bits.to_be_bytes());
        bytes.extend_from_slice(&self.total_issuance.to_be_bytes());
        bytes.extend_from_slice(&self.inflation_rate_bps.to_be_bytes());
        bytes.extend_from_slice(&self.block_timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.block_height.to_be_bytes());
        bytes.extend_from_slice(&self.transparent_tx_count.to_be_bytes());
        bytes.extend_from_slice(&self.transparent_coinbase_tx_count.to_be_bytes());
        bytes.extend_from_slice(&self.shielded_coinbase_migration_tx_count.to_be_bytes());
        bytes.extend_from_slice(&self.sprout_tx_count.to_be_bytes());
        bytes.extend_from_slice(&self.sapling_tx_count.to_be_bytes());
        bytes.extend_from_slice(&self.orchard_tx_count.to_be_bytes());
        bytes.extend_from_slice(&self.transparent_inflow.to_be_bytes());
        bytes.extend_from_slice(&self.transparent_outflow.to_be_bytes());
        bytes.extend_from_slice(&self.sprout_inflow.to_be_bytes());
        bytes.extend_from_slice(&self.sprout_outflow.to_be_bytes());
        bytes.extend_from_slice(&self.sapling_inflow.to_be_bytes());
        bytes.extend_from_slice(&self.sapling_outflow.to_be_bytes());
        bytes.extend_from_slice(&self.orchard_inflow.to_be_bytes());
        bytes.extend_from_slice(&self.orchard_outflow.to_be_bytes());
        bytes.extend_from_slice(&self.average_block_time_bits.to_be_bytes());
        bytes.extend_from_slice(&self.average_block_fee_zat.to_be_bytes());
        bytes.extend_from_slice(&self.average_block_size.to_be_bytes());
        bytes.extend_from_slice(&self.ironwood_tx_count.to_be_bytes());
        bytes.extend_from_slice(&self.ironwood_inflow.to_be_bytes());
        bytes.extend_from_slice(&self.ironwood_outflow.to_be_bytes());
        if let Some(mining_interval) = self.mining_interval {
            mining_interval.append_bytes(&mut bytes);
            debug_assert_eq!(bytes.len(), CURRENT_SNAPSHOT_DATA_LEN);
        } else {
            debug_assert_eq!(bytes.len(), IRONWOOD_SNAPSHOT_DATA_LEN);
        }
        bytes
    }
}

impl FromDisk for SnapshotData {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();

        fn take<const N: usize>(bytes: &[u8], offset: &mut usize, field: &str) -> [u8; N] {
            let end = offset
                .checked_add(N)
                .unwrap_or_else(|| panic!("SnapshotData {field} offset overflow"));
            let field_bytes = bytes.get(*offset..end).unwrap_or_else(|| {
                panic!("SnapshotData {field} is truncated at byte offset {offset}")
            });
            *offset = end;
            field_bytes
                .try_into()
                .expect("the checked snapshot field slice has the requested length")
        }

        // Early versions of this fork stored the expanded target rather than relative work
        // difficulty. Preserve that target here; ZebraDb read methods resolve it using their
        // configured network before returning the snapshot to callers.
        if bytes.len() == EXPANDED_DIFFICULTY_SNAPSHOT_DATA_LEN {
            let mut offset = 0;
            let funded_transparent_address_count =
                u64::from_be_bytes(take(bytes, &mut offset, "funded transparent address count"));

            let pool_values_end = offset + 40;
            let pool_values = ValueBalance::<NonNegative>::from_bytes(
                bytes
                    .get(offset..pool_values_end)
                    .expect("legacy snapshot pool values must not be truncated"),
            )
            .expect("legacy snapshot pool values must contain valid non-negative amounts");
            offset = pool_values_end;

            let legacy_expanded_difficulty = Some(take(bytes, &mut offset, "expanded difficulty"));
            let total_issuance = u64::from_be_bytes(take(bytes, &mut offset, "total issuance"));
            let inflation_rate_bps = u32::from_be_bytes(take(bytes, &mut offset, "inflation rate"));
            let block_timestamp = i64::from_be_bytes(take(bytes, &mut offset, "block timestamp"));
            let block_height = u32::from_be_bytes(take(bytes, &mut offset, "block height"));

            let transparent_tx_count =
                u32::from_be_bytes(take(bytes, &mut offset, "transparent tx count"));
            let transparent_coinbase_tx_count =
                u32::from_be_bytes(take(bytes, &mut offset, "transparent coinbase tx count"));
            let shielded_coinbase_migration_tx_count = u32::from_be_bytes(take(
                bytes,
                &mut offset,
                "shielded coinbase migration tx count",
            ));
            let sprout_tx_count = u32::from_be_bytes(take(bytes, &mut offset, "sprout tx count"));
            let sapling_tx_count = u32::from_be_bytes(take(bytes, &mut offset, "sapling tx count"));
            let orchard_tx_count = u32::from_be_bytes(take(bytes, &mut offset, "orchard tx count"));

            let transparent_inflow =
                u64::from_be_bytes(take(bytes, &mut offset, "transparent inflow"));
            let transparent_outflow =
                u64::from_be_bytes(take(bytes, &mut offset, "transparent outflow"));
            let stored_sprout_inflow =
                u64::from_be_bytes(take(bytes, &mut offset, "sprout inflow"));
            let stored_sprout_outflow =
                u64::from_be_bytes(take(bytes, &mut offset, "sprout outflow"));
            let sapling_inflow = u64::from_be_bytes(take(bytes, &mut offset, "sapling inflow"));
            let sapling_outflow = u64::from_be_bytes(take(bytes, &mut offset, "sapling outflow"));
            let orchard_inflow = u64::from_be_bytes(take(bytes, &mut offset, "orchard inflow"));
            let orchard_outflow = u64::from_be_bytes(take(bytes, &mut offset, "orchard outflow"));

            let average_block_time_bits =
                u32::from_be_bytes(take(bytes, &mut offset, "average block time"));
            let average_block_fee_zat =
                u64::from_be_bytes(take(bytes, &mut offset, "average block fee"));
            let average_block_size =
                u32::from_be_bytes(take(bytes, &mut offset, "average block size"));

            assert_eq!(
                offset,
                bytes.len(),
                "legacy expanded-difficulty snapshot decoder must consume the entire record"
            );

            return SnapshotData {
                disk_format: SnapshotDiskFormat::ExpandedDifficulty,
                legacy_expanded_difficulty,
                funded_transparent_address_count,
                pool_values,
                work_difficulty_bits: 0.0f64.to_bits(),
                total_issuance,
                inflation_rate_bps,
                block_timestamp,
                block_height,
                transparent_tx_count,
                transparent_coinbase_tx_count,
                shielded_coinbase_migration_tx_count,
                sprout_tx_count,
                sapling_tx_count,
                orchard_tx_count,
                ironwood_tx_count: 0,
                transparent_inflow,
                transparent_outflow,
                // All pre-current fork writers stored the Sprout directions backwards.
                sprout_inflow: stored_sprout_outflow,
                sprout_outflow: stored_sprout_inflow,
                sapling_inflow,
                sapling_outflow,
                orchard_inflow,
                orchard_outflow,
                ironwood_inflow: 0,
                ironwood_outflow: 0,
                average_block_time_bits,
                average_block_fee_zat,
                average_block_size,
                mining_interval: None,
            };
        }

        let (disk_format, pool_values_len) = match bytes.len() {
            LEGACY_SNAPSHOT_DATA_LEN => (SnapshotDiskFormat::Legacy, 40),
            TRANSITIONAL_SNAPSHOT_DATA_LEN => (SnapshotDiskFormat::Transitional, 48),
            IRONWOOD_SNAPSHOT_DATA_LEN => (SnapshotDiskFormat::Current, 48),
            MINING_SNAPSHOT_DATA_LEN => (SnapshotDiskFormat::Mining, 48),
            CURRENT_SNAPSHOT_DATA_LEN => (SnapshotDiskFormat::Current, 48),
            actual_len => panic!(
                "SnapshotData deserialization error: expected {LEGACY_SNAPSHOT_DATA_LEN}, \
                 {TRANSITIONAL_SNAPSHOT_DATA_LEN}, {EXPANDED_DIFFICULTY_SNAPSHOT_DATA_LEN}, \
                 {IRONWOOD_SNAPSHOT_DATA_LEN}, {MINING_SNAPSHOT_DATA_LEN}, or \
                 {CURRENT_SNAPSHOT_DATA_LEN} bytes, got \
                 {actual_len} bytes"
            ),
        };

        let mut offset = 0;
        let funded_transparent_address_count =
            u64::from_be_bytes(take(bytes, &mut offset, "funded transparent address count"));

        let pool_values_end = offset
            .checked_add(pool_values_len)
            .expect("SnapshotData pool value offset must not overflow");
        let pool_values = ValueBalance::<NonNegative>::from_bytes(
            bytes
                .get(offset..pool_values_end)
                .expect("SnapshotData pool values must not be truncated"),
        )
        .expect("SnapshotData pool values must contain valid non-negative amounts");
        offset = pool_values_end;

        let work_difficulty_bits = u64::from_be_bytes(take(bytes, &mut offset, "work difficulty"));
        let total_issuance = u64::from_be_bytes(take(bytes, &mut offset, "total issuance"));
        let inflation_rate_bps = u32::from_be_bytes(take(bytes, &mut offset, "inflation rate"));
        let block_timestamp = i64::from_be_bytes(take(bytes, &mut offset, "block timestamp"));
        let block_height = u32::from_be_bytes(take(bytes, &mut offset, "block height"));

        let transparent_tx_count =
            u32::from_be_bytes(take(bytes, &mut offset, "transparent tx count"));
        let transparent_coinbase_tx_count =
            u32::from_be_bytes(take(bytes, &mut offset, "transparent coinbase tx count"));
        let shielded_coinbase_migration_tx_count = u32::from_be_bytes(take(
            bytes,
            &mut offset,
            "shielded coinbase migration tx count",
        ));
        let sprout_tx_count = u32::from_be_bytes(take(bytes, &mut offset, "sprout tx count"));
        let sapling_tx_count = u32::from_be_bytes(take(bytes, &mut offset, "sapling tx count"));
        let orchard_tx_count = u32::from_be_bytes(take(bytes, &mut offset, "orchard tx count"));

        let transparent_inflow = u64::from_be_bytes(take(bytes, &mut offset, "transparent inflow"));
        let transparent_outflow =
            u64::from_be_bytes(take(bytes, &mut offset, "transparent outflow"));
        let stored_sprout_inflow = u64::from_be_bytes(take(bytes, &mut offset, "sprout inflow"));
        let stored_sprout_outflow = u64::from_be_bytes(take(bytes, &mut offset, "sprout outflow"));
        // The fork writers used by both pre-current layouts labelled vpub_new as inflow and
        // vpub_old as outflow, which is the reverse of the pool's actual direction. Correct those
        // records while decoding.
        let (sprout_inflow, sprout_outflow) = if matches!(
            bytes.len(),
            LEGACY_SNAPSHOT_DATA_LEN | TRANSITIONAL_SNAPSHOT_DATA_LEN
        ) {
            (stored_sprout_outflow, stored_sprout_inflow)
        } else {
            (stored_sprout_inflow, stored_sprout_outflow)
        };
        let sapling_inflow = u64::from_be_bytes(take(bytes, &mut offset, "sapling inflow"));
        let sapling_outflow = u64::from_be_bytes(take(bytes, &mut offset, "sapling outflow"));
        let orchard_inflow = u64::from_be_bytes(take(bytes, &mut offset, "orchard inflow"));
        let orchard_outflow = u64::from_be_bytes(take(bytes, &mut offset, "orchard outflow"));

        let average_block_time_bits =
            u32::from_be_bytes(take(bytes, &mut offset, "average block time"));
        let average_block_fee_zat =
            u64::from_be_bytes(take(bytes, &mut offset, "average block fee"));
        let average_block_size = u32::from_be_bytes(take(bytes, &mut offset, "average block size"));

        let has_ironwood_metrics = matches!(
            bytes.len(),
            IRONWOOD_SNAPSHOT_DATA_LEN | MINING_SNAPSHOT_DATA_LEN | CURRENT_SNAPSHOT_DATA_LEN
        );
        let (ironwood_tx_count, ironwood_inflow, ironwood_outflow) = if has_ironwood_metrics {
            (
                u32::from_be_bytes(take(bytes, &mut offset, "ironwood tx count")),
                u64::from_be_bytes(take(bytes, &mut offset, "ironwood inflow")),
                u64::from_be_bytes(take(bytes, &mut offset, "ironwood outflow")),
            )
        } else {
            (0, 0, 0)
        };

        let mining_interval = matches!(
            bytes.len(),
            MINING_SNAPSHOT_DATA_LEN | CURRENT_SNAPSHOT_DATA_LEN
        )
        .then(|| {
            MiningIntervalData::take_bytes(
                bytes,
                &mut offset,
                bytes.len() == CURRENT_SNAPSHOT_DATA_LEN,
            )
        });

        assert_eq!(
            offset,
            bytes.len(),
            "SnapshotData decoder must consume the entire record"
        );

        SnapshotData {
            disk_format,
            legacy_expanded_difficulty: None,
            funded_transparent_address_count,
            pool_values,
            work_difficulty_bits,
            total_issuance,
            inflation_rate_bps,
            block_timestamp,
            block_height,
            transparent_tx_count,
            transparent_coinbase_tx_count,
            shielded_coinbase_migration_tx_count,
            sprout_tx_count,
            sapling_tx_count,
            orchard_tx_count,
            ironwood_tx_count,
            transparent_inflow,
            transparent_outflow,
            sprout_inflow,
            sprout_outflow,
            sapling_inflow,
            sapling_outflow,
            orchard_inflow,
            orchard_outflow,
            ironwood_inflow,
            ironwood_outflow,
            average_block_time_bits,
            average_block_fee_zat,
            average_block_size,
            mining_interval,
        }
    }
}

impl DiskWriteBatch {
    /// Incrementally updates snapshot metrics in the same atomic batch as `finalized`.
    ///
    /// `spent_utxos` must be the verifier-resolved map already used for value-pool updates. The
    /// caller also passes the resulting chain value pool and serialized block size, avoiding
    /// duplicate state reads and block serialization.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn prepare_snapshot_accumulator_batch(
        &mut self,
        db: &ZebraDb,
        network: &Network,
        finalized: &FinalizedBlock,
        transaction_facts: &[FinalizedTransactionFacts],
        spent_utxos: &HashMap<transparent::OutPoint, transparent::Utxo>,
        funded_transparent_address_count_delta: i64,
        new_pool_values: ValueBalance<NonNegative>,
        block_size: u32,
    ) -> Result<Amount<NonNegative>, SnapshotAccumulatorError> {
        let height = finalized.height;
        let timestamp = finalized.block.header.time.timestamp();
        let mut block_metrics = SnapshotMetricTotals::from_block(
            transaction_facts,
            spent_utxos,
            network,
            height,
            block_size,
        )?;
        let block_total_fee_zat = u64::try_from(block_metrics.total_fees_zat).map_err(|_| {
            SnapshotAccumulatorError::Arithmetic {
                metric: "per-block fees",
            }
        })?;
        let block_total_fee =
            Amount::<NonNegative>::try_from(block_total_fee_zat).map_err(|error| {
                SnapshotAccumulatorError::InvalidValue {
                    metric: "per-block fees",
                    reason: error.to_string(),
                }
            })?;
        if let Some(verified_fee) = finalized.block_miner_fees {
            if verified_fee != block_total_fee {
                return Err(SnapshotAccumulatorError::InvalidValue {
                    metric: "per-block fees",
                    reason: format!(
                        "semantic verifier calculated {} zatoshis, snapshot index calculated {} zatoshis",
                        u64::from(verified_fee),
                        block_total_fee_zat,
                    ),
                });
            }
        }

        let mut accumulator = match db.snapshot_accumulator() {
            Some(accumulator) => {
                let expected_height = accumulator.latest_height.checked_add(1);
                if expected_height != Some(height.0) {
                    return Err(SnapshotAccumulatorError::NonSequential {
                        latest_height: Height(accumulator.latest_height),
                        attempted_height: height,
                    });
                }
                accumulator
            }
            None if height.is_min() => {
                SnapshotAccumulator::new_at_genesis(timestamp, new_pool_values)
            }
            None => return Err(SnapshotAccumulatorError::MissingAtNonGenesis { height }),
        };

        let expanded_difficulty = finalized
            .block
            .header
            .difficulty_threshold
            .to_expanded()
            .ok_or(SnapshotAccumulatorError::InvalidDifficulty { height })?;
        let work_difficulty =
            SnapshotData::relative_work_difficulty(U256::from(expanded_difficulty), network);
        let subsidy = block_subsidy(height, network).map_err(|error| {
            SnapshotAccumulatorError::InvalidValue {
                metric: "block subsidy",
                reason: error.to_string(),
            }
        })?;
        block_metrics.add_mining_accounting(finalized, network, subsidy, block_total_fee)?;

        accumulator.totals.checked_add_assign(&block_metrics)?;
        accumulator.daily_header_time_range.include(timestamp);
        accumulator.realtime_header_time_range.include(timestamp);
        accumulator.apply_funded_count_delta(funded_transparent_address_count_delta)?;
        accumulator.total_issuance = accumulator
            .total_issuance
            .checked_add(subsidy.zatoshis() as u64)
            .ok_or(SnapshotAccumulatorError::Arithmetic {
                metric: "total issuance",
            })?;
        accumulator.latest_height = height.0;
        accumulator.latest_timestamp = timestamp;
        accumulator.latest_pool_values = new_pool_values;
        accumulator.latest_work_difficulty_bits = work_difficulty.to_bits();

        self.zs_insert(
            db.realtime_snapshot_data_cf(),
            SNAPSHOT_ACCUMULATOR_KEY,
            accumulator,
        );

        Ok(block_total_fee)
    }
}

impl ZebraDb {
    /// Returns a handle to the `snapshot_data_by_date` RocksDB column family.
    /// This is used for daily snapshots only.
    pub fn snapshot_data_by_date_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(SNAPSHOT_DATA_BY_DATE).unwrap()
    }

    /// Returns a handle to the `realtime_snapshot_data` RocksDB column family.
    /// This only keeps the active interval and its daily anchor metadata.
    pub fn realtime_snapshot_data_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(REALTIME_SNAPSHOT_DATA).unwrap()
    }

    /// Returns the persistent incremental snapshot accumulator, if it has been seeded at genesis.
    pub(crate) fn snapshot_accumulator(&self) -> Option<SnapshotAccumulator> {
        self.db
            .zs_get(self.realtime_snapshot_data_cf(), &SNAPSHOT_ACCUMULATOR_KEY)
    }

    /// Returns the funded transparent address count at the finalized tip in constant time.
    ///
    /// `None` means the accumulator has not been seeded and must be rebuilt or the database
    /// resynced. Shielded ownership is encrypted and is intentionally not represented here.
    pub fn funded_transparent_address_count(&self) -> Option<u64> {
        self.snapshot_accumulator()
            .map(|accumulator| accumulator.funded_transparent_address_count)
    }

    /// Returns the funded transparent address count for a given block height, if it was stored in
    /// a snapshot.
    ///
    /// Returns `None` if no snapshot was stored at that height.
    /// This reads from the snapshot data column family.
    pub fn funded_transparent_address_count_at_height(&self, height: Height) -> Option<u64> {
        let snapshot_data = self.snapshot_data_at_height(height)?;
        Some(snapshot_data.funded_transparent_address_count())
    }

    /// Legacy compatibility alias for [`Self::funded_transparent_address_count_at_height`].
    pub fn holder_count_at_height(&self, height: Height) -> Option<u64> {
        self.funded_transparent_address_count_at_height(height)
    }

    /// Returns the most recent funded transparent address count snapshots, limited to the
    /// specified count.
    ///
    /// Returns `(date, funded_transparent_address_count)` pairs sorted by date (ascending).
    /// Uses reverse iteration to efficiently get only the most recent snapshots.
    /// This reads from the snapshot data column family.
    ///
    /// # Parameters
    ///
    /// - `limit`: Maximum number of snapshots to return
    ///
    /// # Performance
    ///
    /// This method uses reverse iteration to only read the last N snapshots,
    /// avoiding a full scan of the column family.
    pub fn recent_funded_transparent_address_count_snapshots(
        &self,
        limit: usize,
    ) -> Vec<(SnapshotDateKey, u64)> {
        self.recent_snapshot_data(limit)
            .into_iter()
            .map(|(date_key, snapshot_data)| {
                (date_key, snapshot_data.funded_transparent_address_count())
            })
            .collect()
    }

    /// Legacy compatibility alias for
    /// [`Self::recent_funded_transparent_address_count_snapshots`].
    pub fn recent_holder_count_snapshots(&self, limit: usize) -> Vec<(SnapshotDateKey, u64)> {
        self.recent_funded_transparent_address_count_snapshots(limit)
    }

    /// Resolves fields that need the database network after generic disk decoding.
    fn resolve_snapshot(&self, snapshot: SnapshotData) -> SnapshotData {
        snapshot.resolve_legacy_fields(&self.network())
    }

    /// Returns the daily snapshot with the greatest height lower than `height`.
    ///
    /// Block header timestamps are not monotonic, so date-key order cannot be used as a proxy for
    /// height order here. Metrics are height intervals and must always anchor at the latest earlier
    /// snapshot in the chain.
    #[allow(clippy::unwrap_in_result)]
    fn latest_daily_snapshot_before_height(
        &self,
        height: Height,
    ) -> Option<(SnapshotDateKey, SnapshotData)> {
        let typed_cf = TypedColumnFamily::<SnapshotDateKey, SnapshotData>::new(
            &self.db,
            SNAPSHOT_DATA_BY_DATE,
        )
        .expect("column family was created when database was created");

        typed_cf
            .zs_forward_range_iter(..)
            .filter(|(_, snapshot)| snapshot.block_height() < height.0)
            .max_by_key(|(_, snapshot)| snapshot.block_height())
            .map(|(date, snapshot)| (date, self.resolve_snapshot(snapshot)))
    }

    /// Returns the daily snapshot with the greatest stored block height.
    #[allow(clippy::unwrap_in_result)]
    pub(crate) fn latest_daily_snapshot_by_height(
        &self,
    ) -> Option<(SnapshotDateKey, SnapshotData)> {
        let typed_cf = TypedColumnFamily::<SnapshotDateKey, SnapshotData>::new(
            &self.db,
            SNAPSHOT_DATA_BY_DATE,
        )
        .expect("column family was created when database was created");

        typed_cf
            .zs_forward_range_iter(..)
            .max_by_key(|(_, snapshot)| snapshot.block_height())
            .map(|(date, snapshot)| (date, self.resolve_snapshot(snapshot)))
    }

    /// Returns a bounded start height for metrics in the first compatible daily snapshot.
    ///
    /// The interval is one target day at the upgrade active at `height` (576 blocks before
    /// Blossom, 1,152 afterwards), including `height` itself.
    fn initial_snapshot_metrics_start_height(height: Height, network: &Network) -> Height {
        let target_spacing_seconds =
            NetworkUpgrade::target_spacing_for_height(network, height).num_seconds();
        let target_spacing_seconds = u32::try_from(target_spacing_seconds)
            .expect("network target spacing must be a positive u32 number of seconds");
        let blocks_per_day = INITIAL_SNAPSHOT_METRICS_SECONDS / target_spacing_seconds;

        Height(height.0.saturating_sub(blocks_per_day.saturating_sub(1)))
    }

    /// Returns the interval end used to derive a bounded start when no earlier anchor exists.
    fn initial_snapshot_metrics_end_height(
        height: Height,
        store_as_realtime: bool,
        active_daily_height: Option<Height>,
    ) -> Height {
        if store_as_realtime {
            active_daily_height.unwrap_or(height)
        } else {
            height
        }
    }

    /// Calculate total ZEC issuance up to and including a given height.
    ///
    /// When a previous daily snapshot exists, only subsidies after that snapshot are summed.
    fn calculate_total_issuance(
        height: Height,
        network: &Network,
        previous_snapshot: Option<&SnapshotData>,
    ) -> Result<Amount<NonNegative>, BoxError> {
        let (mut total_zat, start_height) = if let Some(previous_snapshot) = previous_snapshot {
            let start_height = previous_snapshot
                .block_height()
                .checked_add(1)
                .ok_or("previous snapshot height has no successor")?;
            (
                u128::from(previous_snapshot.total_issuance().zatoshis() as u64),
                start_height,
            )
        } else {
            (0, Height::MIN.0)
        };

        if start_height > height.0 {
            let total_zat =
                u64::try_from(total_zat).map_err(|_| "total issuance does not fit in u64")?;
            return Amount::try_from(total_zat)
                .map_err(|error| format!("invalid total issuance: {error}").into());
        }

        let slow_start_interval = u64::from(network.slow_start_interval().0);
        let mut cursor = u64::from(start_height);

        // Sum any remaining slow-start range using the closed-form cumulative helper.
        if cursor < slow_start_interval {
            let slow_end = u64::from(height.0).min(slow_start_interval - 1);
            let slow_end_height = u32::try_from(slow_end)
                .map_err(|_| "slow-start height does not fit in a block height")?;
            let through_end = SnapshotData::slow_start_issuance_through(slow_end_height, network)?;
            let before_start = if cursor == 0 {
                Amount::zero()
            } else {
                let previous_height = u32::try_from(cursor - 1)
                    .map_err(|_| "slow-start height does not fit in a block height")?;
                SnapshotData::slow_start_issuance_through(previous_height, network)?
            };
            let slow_range = (through_end - before_start)
                .map_err(|error| format!("invalid slow-start issuance range: {error}"))?;
            total_zat = total_zat
                .checked_add(slow_range.zatoshis() as u128)
                .ok_or("overflow calculating slow-start issuance")?;
            cursor = slow_end + 1;
        }

        // Outside slow start, subsidy is constant between Blossom and halving boundaries. Sum
        // each era in one operation instead of calling `block_subsidy` once per historical block.
        let end_exclusive = u64::from(height.0) + 1;
        let blossom_height = NetworkUpgrade::Blossom
            .activation_height(network)
            .map(|height| u64::from(height.0));

        while cursor < end_exclusive {
            let cursor_height = Height(
                u32::try_from(cursor)
                    .map_err(|_| "issuance cursor does not fit in a block height")?,
            );
            let subsidy = block_subsidy(cursor_height, network).map_err(|error| {
                format!("failed to calculate block subsidy at height {cursor}: {error}")
            })?;
            if subsidy == Amount::<NonNegative>::zero() {
                break;
            }

            let next_halving_height = halving(cursor_height, network)
                .checked_add(1)
                .and_then(|next_halving| height_for_halving(next_halving, network))
                .map(|height| u64::from(height.0));
            let next_change = [Some(end_exclusive), blossom_height, next_halving_height]
                .into_iter()
                .flatten()
                .filter(|change_height| *change_height > cursor)
                .min()
                .unwrap_or(end_exclusive);
            let block_count = next_change - cursor;
            let era_issuance = (subsidy.zatoshis() as u128)
                .checked_mul(u128::from(block_count))
                .ok_or("overflow calculating issuance for a subsidy era")?;
            total_zat = total_zat
                .checked_add(era_issuance)
                .ok_or("overflow calculating total issuance")?;

            cursor = next_change;
        }

        let total_zat =
            u64::try_from(total_zat).map_err(|_| "total issuance does not fit in u64")?;
        Amount::try_from(total_zat)
            .map_err(|error| format!("invalid total issuance: {error}").into())
    }

    /// Calculate annual inflation rate at a given height.
    ///
    /// Formula: (block_subsidy_at_height * blocks_per_year / total_supply) * 100
    ///
    /// Blocks per year:
    /// - Before Blossom: 210240 blocks/year (150 seconds per block)
    /// - After Blossom: 420480 blocks/year (75 seconds per block)
    fn calculate_inflation_rate(
        &self,
        height: Height,
        network: &Network,
        total_supply: Amount<NonNegative>,
    ) -> Result<f64, BoxError> {
        inflation_rate_percent(height, network, total_supply)
    }

    /// Returns the value and coinbase status of the transparent output referenced by `outpoint`.
    ///
    /// Snapshot metrics are calculated after blocks have been committed, so spent outputs are no
    /// longer present in the UTXO column family. Fall back to the persisted source transaction
    /// instead of treating those spent outputs as missing.
    fn transparent_output_value_and_coinbase_status(
        &self,
        outpoint: zebra_chain::transparent::OutPoint,
        current_block_outputs: &HashMap<
            zebra_chain::transparent::OutPoint,
            (Amount<NonNegative>, bool),
        >,
    ) -> Result<(Amount<NonNegative>, bool), BoxError> {
        if let Some(output) = current_block_outputs.get(&outpoint) {
            return Ok(*output);
        }

        let (source_transaction, _, _) = self.transaction(outpoint.hash).ok_or_else(|| {
            format!(
                "source transaction {:?} for transparent input was not found",
                outpoint.hash
            )
        })?;
        let source_outputs = source_transaction.outputs();
        let output = source_outputs.get(outpoint.index as usize).ok_or_else(|| {
            format!(
                "transparent output index {} was not found in source transaction {:?}",
                outpoint.index, outpoint.hash
            )
        })?;
        Ok((output.value(), source_transaction.is_coinbase()))
    }

    /// Combined calculation of transaction counts, pool flows, and block metrics in a single pass.
    /// This combines count_transactions_by_pool, calculate_pool_flows, and calculate_block_metrics
    /// into one iteration for better performance, while maintaining identical calculation logic.
    fn calculate_all_metrics_combined(
        &self,
        start_height: Height,
        end_height: Height,
        end_timestamp: i64,
    ) -> Result<
        (
            // Transaction counts: (transparent_count, transparent_coinbase_count, shielded_coinbase_migration_count, sprout_count, sapling_count, orchard_count, ironwood_count)
            (u32, u32, u32, u32, u32, u32, u32),
            // Pool flows: (transparent_inflow, transparent_outflow, sprout_inflow, sprout_outflow,
            //              sapling_inflow, sapling_outflow, orchard_inflow, orchard_outflow,
            //              ironwood_inflow, ironwood_outflow)
            (u64, u64, u64, u64, u64, u64, u64, u64, u64, u64),
            // Block metrics: (average_block_time_seconds, average_block_fee_zat, average_block_size_bytes)
            (f32, Amount<NonNegative>, u32),
        ),
        BoxError,
    > {
        use std::ops::Add;
        use zebra_chain::serialization::ZcashSerialize;

        fn checked_add_flow(
            total: &mut u64,
            value: u64,
            metric: &'static str,
        ) -> Result<(), BoxError> {
            *total = total
                .checked_add(value)
                .ok_or_else(|| format!("overflow calculating {metric}"))?;
            Ok(())
        }

        // Transaction counts
        let mut transparent_count = 0u32;
        let mut transparent_coinbase_count = 0u32;
        let mut shielded_coinbase_migration_count = 0u32;
        let mut sprout_count = 0u32;
        let mut sapling_count = 0u32;
        let mut orchard_count = 0u32;
        let mut ironwood_count = 0u32;

        // Pool flows use u64 because gross turnover can exceed the maximum ZEC supply.
        let mut transparent_inflow_zat = 0u64;
        let mut transparent_outflow_zat = 0u64;
        let mut sprout_inflow_zat = 0u64;
        let mut sprout_outflow_zat = 0u64;
        let mut sapling_inflow_zat = 0u64;
        let mut sapling_outflow_zat = 0u64;
        let mut orchard_inflow_zat = 0u64;
        let mut orchard_outflow_zat = 0u64;
        let mut ironwood_inflow_zat = 0u64;
        let mut ironwood_outflow_zat = 0u64;

        // Block metrics
        let mut total_fees_zat = 0u128;
        let mut total_block_size = 0u64;
        let mut block_count = 0u32;

        // Include the interval from the previous snapshot block to the first block in this range.
        // At genesis there is no previous block, so only intervals between blocks are available.
        let interval_start_height = start_height.previous().unwrap_or(start_height);
        let interval_start_timestamp = self
            .block(interval_start_height.into())
            .ok_or_else(|| format!("block at height {:?} not found", interval_start_height))?
            .header
            .time
            .timestamp();

        // Single pass through all blocks
        for h in start_height.0..=end_height.0 {
            let block_height = Height(h);
            let block = match self.block(block_height.into()) {
                Some(b) => b,
                None => return Err(format!("block at height {block_height:?} not found").into()),
            };

            // Calculate block size (same as calculate_block_metrics)
            let block_size = block.zcash_serialize_to_vec().map_err(|e| {
                format!(
                    "failed to serialize block at height {:?}: {}",
                    block_height, e
                )
            })?;
            total_block_size = total_block_size
                .checked_add(block_size.len() as u64)
                .ok_or("overflow calculating total block size")?;

            // Build an output map for this block (for same-block references).
            // This is used in both pool flows and fee calculations
            let mut current_block_outputs = HashMap::new();
            for tx in &block.transactions {
                let is_coinbase = tx.is_coinbase();
                for (out_idx, output) in tx.outputs().iter().enumerate() {
                    let outpoint = zebra_chain::transparent::OutPoint {
                        hash: tx.hash(),
                        index: out_idx as u32,
                    };
                    current_block_outputs.insert(outpoint, (output.value(), is_coinbase));
                }
            }

            // Process all transactions in this block
            for transaction in &block.transactions {
                let is_coinbase = transaction.is_coinbase();

                // === TRANSACTION COUNTING (same logic as count_transactions_by_pool) ===
                // Check for transparent activity
                let has_transparent =
                    transaction.has_transparent_inputs() || transaction.has_transparent_outputs();

                // Pool transaction counts include spends and outputs. Coinbase migration
                // classification below uses the stricter output-only predicate.
                let has_sapling_data = transaction.has_sapling_shielded_data();
                let has_orchard_data = transaction.has_orchard_shielded_data();
                let has_ironwood_data = transaction.has_ironwood_shielded_data();
                let has_shielded_outputs = transaction.has_shielded_outputs();

                // Check for sprout joinsplit
                let has_sprout = transaction.has_sprout_joinsplit_data();

                // Read source outputs once for transparent outflow, fee calculations, and exact
                // shielded coinbase migration classification.
                let mut transparent_input_value = Amount::<NonNegative>::zero();
                let mut spends_coinbase_output = false;
                for input in transaction.inputs() {
                    if let Some(outpoint) = input.outpoint() {
                        let (value, source_is_coinbase) = self
                            .transparent_output_value_and_coinbase_status(
                                outpoint,
                                &current_block_outputs,
                            )?;
                        let value_zat = value.zatoshis() as u64;
                        checked_add_flow(
                            &mut transparent_outflow_zat,
                            value_zat,
                            "transparent outflow",
                        )?;
                        transparent_input_value =
                            transparent_input_value.add(value).map_err(|e| {
                                format!("overflow calculating transparent input value: {e}")
                            })?;
                        spends_coinbase_output |= source_is_coinbase;
                    }
                }

                // Count each transaction exactly once using priority order
                if !is_coinbase && spends_coinbase_output && has_shielded_outputs {
                    // Priority 1: Shielded coinbase migration (coinbase outputs -> shielded pool)
                    shielded_coinbase_migration_count = shielded_coinbase_migration_count
                        .checked_add(1)
                        .ok_or("shielded coinbase migration transaction count overflow")?;
                } else if has_ironwood_data {
                    // Priority 2: Ironwood transactions, including direct shielded coinbase.
                    ironwood_count = ironwood_count
                        .checked_add(1)
                        .ok_or("ironwood transaction count overflow")?;
                } else if has_orchard_data {
                    // Priority 3: Orchard transactions, including direct shielded coinbase.
                    orchard_count = orchard_count
                        .checked_add(1)
                        .ok_or("orchard transaction count overflow")?;
                } else if has_sapling_data {
                    // Priority 4: Sapling transactions, including direct shielded coinbase.
                    sapling_count = sapling_count
                        .checked_add(1)
                        .ok_or("sapling transaction count overflow")?;
                } else if has_sprout {
                    // Priority 5: Sprout transactions
                    sprout_count = sprout_count
                        .checked_add(1)
                        .ok_or("sprout transaction count overflow")?;
                } else if is_coinbase {
                    // Priority 6: Transparent coinbase. All shielded coinbase transactions were
                    // classified by their shielded pool above.
                    transparent_coinbase_count = transparent_coinbase_count
                        .checked_add(1)
                        .ok_or("transparent coinbase transaction count overflow")?;
                } else if has_transparent {
                    // Priority 7: Regular transparent transactions
                    transparent_count = transparent_count
                        .checked_add(1)
                        .ok_or("transparent transaction count overflow")?;
                }

                // === POOL FLOWS (same logic as calculate_pool_flows) ===
                // Transparent pool: sum outputs (inflow) and inputs (outflow)
                for output in transaction.outputs() {
                    let value_zat = output.value().zatoshis() as u64;
                    checked_add_flow(&mut transparent_inflow_zat, value_zat, "transparent inflow")?;
                }

                // Sprout pool: vpub_old (inflow) and vpub_new (outflow)
                for vpub_old_zat in transaction.output_values_to_sprout() {
                    let vpub_old = Amount::<NonNegative>::try_from(vpub_old_zat)
                        .map_err(|e| format!("invalid sprout vpub_old amount: {}", e))?;
                    let value_zat = u64::try_from(vpub_old.zatoshis())
                        .map_err(|e| format!("sprout vpub_old does not fit in u64: {e}"))?;
                    checked_add_flow(&mut sprout_inflow_zat, value_zat, "sprout inflow")?;
                }

                for vpub_new_zat in transaction.input_values_from_sprout() {
                    let vpub_new = Amount::<NonNegative>::try_from(vpub_new_zat)
                        .map_err(|e| format!("invalid sprout vpub_new amount: {}", e))?;
                    let value_zat = u64::try_from(vpub_new.zatoshis())
                        .map_err(|e| format!("sprout vpub_new does not fit in u64: {e}"))?;
                    checked_add_flow(&mut sprout_outflow_zat, value_zat, "sprout outflow")?;
                }

                // Sapling pool: value_balance represents net change
                // Negative value_balance = net inflow, positive = net outflow
                let sapling_vb = transaction.sapling_value_balance();
                let sapling_net = sapling_vb.sapling_amount();
                let sapling_zatoshis = sapling_net.zatoshis();
                if sapling_zatoshis < 0 {
                    // Net inflow: value entering sapling pool
                    // Convert negative to positive
                    let value_zat = (-sapling_zatoshis) as u64;
                    checked_add_flow(&mut sapling_inflow_zat, value_zat, "sapling inflow")?;
                } else if sapling_zatoshis > 0 {
                    // Net outflow: value leaving sapling pool
                    let value_zat = sapling_zatoshis as u64;
                    checked_add_flow(&mut sapling_outflow_zat, value_zat, "sapling outflow")?;
                }

                // Orchard pool: value_balance represents net change
                // Negative value_balance = net inflow, positive = net outflow
                let orchard_vb = transaction.orchard_value_balance();
                let orchard_net = orchard_vb.orchard_amount();
                let orchard_zatoshis = orchard_net.zatoshis();
                if orchard_zatoshis < 0 {
                    // Net inflow: value entering orchard pool
                    let value_zat = (-orchard_zatoshis) as u64;
                    checked_add_flow(&mut orchard_inflow_zat, value_zat, "orchard inflow")?;
                } else if orchard_zatoshis > 0 {
                    // Net outflow: value leaving orchard pool
                    let value_zat = orchard_zatoshis as u64;
                    checked_add_flow(&mut orchard_outflow_zat, value_zat, "orchard outflow")?;
                }

                // Ironwood pool: value_balance represents net change
                // Negative value_balance = net inflow, positive = net outflow
                let ironwood_vb = transaction.ironwood_value_balance();
                let ironwood_net = ironwood_vb.ironwood_amount();
                let ironwood_zatoshis = ironwood_net.zatoshis();
                if ironwood_zatoshis < 0 {
                    // Net inflow: value entering the Ironwood pool
                    let value_zat = (-ironwood_zatoshis) as u64;
                    checked_add_flow(&mut ironwood_inflow_zat, value_zat, "ironwood inflow")?;
                } else if ironwood_zatoshis > 0 {
                    // Net outflow: value leaving the Ironwood pool
                    let value_zat = ironwood_zatoshis as u64;
                    checked_add_flow(&mut ironwood_outflow_zat, value_zat, "ironwood outflow")?;
                }

                // === FEE CALCULATION (same logic as calculate_block_metrics) ===
                // Calculate fees for each transaction (excluding coinbase)
                if !is_coinbase {
                    // Calculate transparent outputs value
                    let mut output_value = Amount::<NonNegative>::zero();
                    for output in transaction.outputs() {
                        let value = output.value();
                        output_value = output_value
                            .add(value)
                            .map_err(|e| format!("overflow calculating output value: {}", e))?;
                    }

                    // Calculate fee for all transaction types
                    // General formula: fee = transparent_inputs - transparent_outputs - value_balance
                    // Where value_balance can be negative (shielding) or positive (deshielding)
                    //
                    // Note: transparent_inputs - transparent_outputs can be negative if outputs > inputs,
                    // but this is valid when there's shielding (negative value_balance) that compensates.

                    // Calculate sprout value balance: vpub_new (outputs from sprout) - vpub_old (inputs to sprout)
                    // This matches JoinSplit::value_balance() in zebra-chain/src/sprout/joinsplit.rs
                    // Positive = value leaving sprout (deshielding), Negative = value entering sprout (shielding)
                    let mut sprout_vpub_old = Amount::<NonNegative>::zero();
                    for vpub_old_zat in transaction.output_values_to_sprout() {
                        let vpub_old = Amount::<NonNegative>::try_from(vpub_old_zat)
                            .map_err(|e| format!("invalid sprout vpub_old amount: {}", e))?;
                        sprout_vpub_old = sprout_vpub_old
                            .add(vpub_old)
                            .map_err(|e| format!("overflow calculating sprout vpub_old: {}", e))?;
                    }

                    let mut sprout_vpub_new = Amount::<NonNegative>::zero();
                    for vpub_new_zat in transaction.input_values_from_sprout() {
                        let vpub_new = Amount::<NonNegative>::try_from(vpub_new_zat)
                            .map_err(|e| format!("invalid sprout vpub_new amount: {}", e))?;
                        sprout_vpub_new = sprout_vpub_new
                            .add(vpub_new)
                            .map_err(|e| format!("overflow calculating sprout vpub_new: {}", e))?;
                    }

                    // Sprout value balance (signed): vpub_new - vpub_old
                    // Positive = value leaving sprout (deshielding), Negative = value entering sprout (shielding)
                    let sprout_value_balance_zatoshis = if sprout_vpub_new > sprout_vpub_old {
                        // Value leaving sprout (positive)
                        (sprout_vpub_new - sprout_vpub_old)
                            .map_err(|e| format!("error calculating sprout value balance: {}", e))?
                            .zatoshis()
                    } else if sprout_vpub_old > sprout_vpub_new {
                        // Value entering sprout (negative)
                        -((sprout_vpub_old - sprout_vpub_new)
                            .map_err(|e| format!("error calculating sprout value balance: {}", e))?
                            .zatoshis())
                    } else {
                        0i64
                    };

                    // Sapling value balance: can be negative (shielding) or positive (deshielding)
                    let sapling_vb_fee = transaction.sapling_value_balance();
                    let sapling_value_balance = sapling_vb_fee.sapling_amount();
                    let sapling_zatoshis = sapling_value_balance.zatoshis();

                    // Orchard value balance: can be negative (shielding) or positive (deshielding)
                    let orchard_vb_fee = transaction.orchard_value_balance();
                    let orchard_value_balance = orchard_vb_fee.orchard_amount();
                    let orchard_zatoshis = orchard_value_balance.zatoshis();

                    // Ironwood value balance: can be negative (shielding) or positive (deshielding)
                    let ironwood_vb_fee = transaction.ironwood_value_balance();
                    let ironwood_value_balance = ironwood_vb_fee.ironwood_amount();
                    let ironwood_zatoshis = ironwood_value_balance.zatoshis();

                    // Calculate transparent value balance (signed)
                    // This is: transparent_inputs - transparent_outputs
                    let transparent_value_balance_zatoshis =
                        if transparent_input_value > output_value {
                            // Positive: more inputs than outputs
                            (transparent_input_value - output_value)
                                .map_err(|e| {
                                    format!("error calculating transparent value balance: {}", e)
                                })?
                                .zatoshis()
                        } else if output_value > transparent_input_value {
                            // Negative: more outputs than inputs (valid when there's shielding)
                            -((output_value - transparent_input_value)
                                .map_err(|e| {
                                    format!("error calculating transparent value balance: {}", e)
                                })?
                                .zatoshis())
                        } else {
                            // Zero: inputs equal outputs
                            0i64
                        };

                    // Calculate fee using the consensus formula from value_balance.rs:
                    // remaining_transaction_value = transparent + sprout + sapling + orchard + ironwood
                    //
                    // This is the sum of all value balances:
                    // - transparent: transparent_inputs - transparent_outputs
                    // - sprout: vpub_new - vpub_old
                    // - sapling: value_balance (negative = shielding, positive = deshielding)
                    // - orchard: value_balance (negative = shielding, positive = deshielding)
                    // - ironwood: value_balance (negative = shielding, positive = deshielding)
                    //
                    // The remaining value is the transaction fee (for non-coinbase transactions)
                    let fee_zatoshis = transparent_value_balance_zatoshis
                        .saturating_add(sprout_value_balance_zatoshis)
                        .saturating_add(sapling_zatoshis)
                        .saturating_add(orchard_zatoshis)
                        .saturating_add(ironwood_zatoshis);

                    // Only add positive fees
                    if fee_zatoshis > 0 {
                        total_fees_zat = total_fees_zat
                            .checked_add(fee_zatoshis as u128)
                            .ok_or_else(|| "overflow calculating total fees".to_string())?;
                    }
                }
            }

            block_count = block_count.checked_add(1).ok_or("block count overflow")?;
        }

        // Calculate averages (same logic as calculate_block_metrics)
        let interval_count = if start_height == Height::MIN {
            block_count.saturating_sub(1)
        } else {
            block_count
        };
        let average_block_time = if interval_count > 0 {
            // Header times are consensus-valid within a range and are not required to increase.
            // A backwards endpoint must not become a negative duration in the dashboard.
            let time_diff = end_timestamp
                .saturating_sub(interval_start_timestamp)
                .max(0);
            (time_diff as f32) / (interval_count as f32)
        } else {
            0.0
        };

        let average_fee = if block_count > 0 {
            let avg_fee_zat = total_fees_zat / u128::from(block_count);
            let avg_fee_zat =
                u64::try_from(avg_fee_zat).map_err(|_| "average block fee does not fit in u64")?;
            Amount::try_from(avg_fee_zat)
                .map_err(|e| format!("failed to create average fee amount: {}", e))?
        } else {
            Amount::zero()
        };

        let average_block_size = if block_count > 0 {
            (total_block_size / (block_count as u64)) as u32
        } else {
            0
        };

        Ok((
            (
                transparent_count,
                transparent_coinbase_count,
                shielded_coinbase_migration_count,
                sprout_count,
                sapling_count,
                orchard_count,
                ironwood_count,
            ),
            (
                transparent_inflow_zat,
                transparent_outflow_zat,
                sprout_inflow_zat,
                sprout_outflow_zat,
                sapling_inflow_zat,
                sapling_outflow_zat,
                orchard_inflow_zat,
                orchard_outflow_zat,
                ironwood_inflow_zat,
                ironwood_outflow_zat,
            ),
            (average_block_time, average_fee, average_block_size),
        ))
    }

    /// Materializes a daily or realtime snapshot from the persistent cumulative accumulator.
    ///
    /// This performs a constant number of RocksDB reads and writes, regardless of chain height.
    fn store_snapshot_data_incremental(
        &self,
        height: Height,
        network: &Network,
        store_as_realtime: bool,
    ) -> Result<(), BoxError> {
        let mut accumulator = self
            .snapshot_accumulator()
            .ok_or(SnapshotAccumulatorError::MissingForSnapshot { height })?;
        if accumulator.latest_height != height.0 {
            return Err(SnapshotAccumulatorError::HeightMismatch {
                accumulator_height: Height(accumulator.latest_height),
                requested_height: height,
            }
            .into());
        }

        let date_key = SnapshotDateKey::from_timestamp(accumulator.latest_timestamp);
        // Restarting within the same UTC date must replace that date's row with the complete
        // interval, not just the blocks committed since restart. In that case use the same anchor
        // as realtime replacement data and do not advance it.
        let replaces_same_daily_date = accumulator.daily_anchor.initialized
            && SnapshotDateKey::from_timestamp(accumulator.daily_anchor.timestamp) == date_key;
        let interval_anchor = if store_as_realtime || replaces_same_daily_date {
            accumulator.realtime_anchor
        } else {
            accumulator.daily_anchor
        };
        let header_time_range = if store_as_realtime || replaces_same_daily_date {
            accumulator.realtime_header_time_range
        } else {
            accumulator.daily_header_time_range
        };
        let snapshot_data = SnapshotData::from_accumulator(
            &accumulator,
            interval_anchor,
            header_time_range,
            network,
        )?;

        let mut batch = DiskWriteBatch::new();
        let realtime_cf = self.realtime_snapshot_data_cf();
        if store_as_realtime {
            batch.zs_insert(realtime_cf, REALTIME_SNAPSHOT_KEY, snapshot_data);

            if accumulator.daily_anchor.initialized {
                let anchor_date =
                    SnapshotDateKey::from_timestamp(accumulator.daily_anchor.timestamp);
                let anchor_snapshot: SnapshotData = self
                    .db
                    .zs_get(self.snapshot_data_by_date_cf(), &anchor_date)
                    .map(|snapshot| self.resolve_snapshot(snapshot))
                    .filter(|snapshot| snapshot.block_height() == accumulator.daily_anchor.height)
                    .ok_or(SnapshotAccumulatorError::MissingDailyAnchor {
                        height: Height(accumulator.daily_anchor.height),
                        date: anchor_date,
                    })?;
                batch.zs_insert(realtime_cf, REALTIME_SNAPSHOT_ANCHOR_KEY, anchor_snapshot);
            } else {
                batch.zs_delete(realtime_cf, REALTIME_SNAPSHOT_ANCHOR_KEY);
            }
        } else {
            batch.zs_insert(self.snapshot_data_by_date_cf(), date_key, snapshot_data);
            batch.zs_delete(realtime_cf, REALTIME_SNAPSHOT_KEY);
            batch.zs_delete(realtime_cf, REALTIME_SNAPSHOT_ANCHOR_KEY);

            let previous_daily_anchor = accumulator.daily_anchor;
            let completed_daily_header_time_range = accumulator.daily_header_time_range;
            accumulator.daily_anchor = accumulator.current_anchor();
            accumulator.daily_header_time_range =
                SnapshotHeaderTimeRange::singleton(accumulator.latest_timestamp);
            if !replaces_same_daily_date {
                accumulator.realtime_anchor = previous_daily_anchor;
                accumulator.realtime_header_time_range = completed_daily_header_time_range;
            }
            // The daily value and both anchor shifts are committed atomically. A failed write can
            // therefore be retried without dropping or double-counting an interval.
            batch.zs_insert(realtime_cf, SNAPSHOT_ACCUMULATOR_KEY, accumulator);
        }

        self.db.write(batch)?;

        tracing::info!(
            ?height,
            date_key = %if store_as_realtime { "realtime".to_owned() } else { date_key.to_string() },
            store_as_realtime,
            funded_transparent_address_count = snapshot_data.funded_transparent_address_count(),
            pool_values = ?snapshot_data.pool_values(),
            work_difficulty = snapshot_data.work_difficulty(),
            total_issuance_zat = snapshot_data.total_issuance().zatoshis(),
            inflation_rate_percent = snapshot_data.inflation_rate_percent(),
            block_timestamp = snapshot_data.block_timestamp(),
            transparent_tx_count = snapshot_data.transparent_tx_count(),
            transparent_coinbase_tx_count = snapshot_data.transparent_coinbase_tx_count(),
            shielded_coinbase_migration_tx_count = snapshot_data.shielded_coinbase_migration_tx_count(),
            sprout_tx_count = snapshot_data.sprout_tx_count(),
            sapling_tx_count = snapshot_data.sapling_tx_count(),
            orchard_tx_count = snapshot_data.orchard_tx_count(),
            ironwood_tx_count = snapshot_data.ironwood_tx_count(),
            transparent_inflow_zat = snapshot_data.transparent_inflow(),
            transparent_outflow_zat = snapshot_data.transparent_outflow(),
            sprout_inflow_zat = snapshot_data.sprout_inflow(),
            sprout_outflow_zat = snapshot_data.sprout_outflow(),
            sapling_inflow_zat = snapshot_data.sapling_inflow(),
            sapling_outflow_zat = snapshot_data.sapling_outflow(),
            orchard_inflow_zat = snapshot_data.orchard_inflow(),
            orchard_outflow_zat = snapshot_data.orchard_outflow(),
            ironwood_inflow_zat = snapshot_data.ironwood_inflow(),
            ironwood_outflow_zat = snapshot_data.ironwood_outflow(),
            average_block_time_seconds = snapshot_data.average_block_time(),
            average_block_fee_zat = snapshot_data.average_block_fee_zat().zatoshis(),
            average_block_size_bytes = snapshot_data.average_block_size(),
            "stored snapshot data from incremental accumulator"
        );

        Ok(())
    }

    /// Stores snapshot data (funded transparent address count, pool values, difficulty, issuance,
    /// inflation, timestamp, transaction classes, and flows) to RocksDB at the given block height.
    ///
    /// # Parameters
    ///
    /// - `height`: The block height at which this snapshot is taken
    /// - `network`: The network (mainnet/testnet/regtest) for subsidy calculations
    /// - `store_as_realtime`: Store in the replaceable realtime slot instead of the daily index
    pub fn store_snapshot_data(
        &self,
        height: Height,
        network: &Network,
        store_as_realtime: bool,
    ) -> Result<(), BoxError> {
        self.store_snapshot_data_incremental(height, network, store_as_realtime)
    }

    /// Legacy scanning implementation retained as a reference for rebuild/backfill tooling.
    /// Normal state writes must use the incremental accumulator above.
    #[allow(dead_code)]
    fn store_snapshot_data_by_scanning_legacy(
        &self,
        height: Height,
        network: &Network,
        store_as_realtime: bool,
    ) -> Result<(), BoxError> {
        // 1. Count holders
        let holder_count = self.holder_count();

        // 2. Get pool values
        let pool_values = self.finalized_value_pool();

        // 3. Get block header for difficulty and timestamp
        let block = self
            .block(height.into())
            .ok_or_else(|| format!("block at height {:?} not found", height))?;
        let header = &block.header;

        // 4. Calculate work difficulty (matches RPC getdifficulty)
        // Get expanded difficulty from block header
        let expanded_difficulty = header
            .difficulty_threshold
            .to_expanded()
            .ok_or_else(|| "invalid difficulty threshold".to_string())?;

        let work_difficulty =
            SnapshotData::relative_work_difficulty(U256::from(expanded_difficulty), network);

        // 5. Get block timestamp (Unix timestamp in seconds)
        let block_timestamp = header.time.timestamp();

        // 6. Find the latest earlier daily snapshot by height. Header timestamps are not
        // monotonic, so a date range must not determine the snapshot ordering.
        let latest_daily_snapshot = self.latest_daily_snapshot_before_height(height);
        let realtime_anchor = latest_daily_snapshot
            .as_ref()
            .map(|(_, snapshot)| *snapshot);
        // If there is only one daily record, realtime still replaces that record and must retain
        // the bounded interval it originally represented. Basing the fallback on the advancing
        // realtime height would silently drop the oldest blocks on every refresh.
        let initial_metrics_interval_end = Self::initial_snapshot_metrics_end_height(
            height,
            store_as_realtime,
            realtime_anchor.map(|snapshot| Height(snapshot.block_height())),
        );

        // Calculate total issuance incrementally from the previous daily snapshot.
        let total_issuance = Self::calculate_total_issuance(
            height,
            network,
            latest_daily_snapshot.as_ref().map(|(_, snapshot)| snapshot),
        )?;

        // Pool values represent the actual monetary base (all ZEC in circulation).
        let total_supply = (pool_values.transparent_amount()
            + pool_values.sprout_amount()
            + pool_values.sapling_amount()
            + pool_values.orchard_amount()
            + pool_values.deferred_amount()
            + pool_values.ironwood_amount())
        .map_err(|e| format!("overflow calculating total supply from pool values: {e}"))?;
        let inflation_rate = self.calculate_inflation_rate(height, network, total_supply)?;

        // 8. Select the interval anchor. A realtime record replaces the active daily record in
        // date-indexed reads, so it must include that daily record's interval as well as the new
        // blocks. Anchor realtime metrics at the second-latest daily snapshot by height. Daily
        // records continue to anchor at the latest daily snapshot.
        let interval_anchor = if store_as_realtime {
            latest_daily_snapshot.as_ref().and_then(|(_, snapshot)| {
                self.latest_daily_snapshot_before_height(Height(snapshot.block_height()))
            })
        } else {
            latest_daily_snapshot
        };

        let previous_snapshot_height = {
            if let Some((previous_date_key, previous_snapshot)) = interval_anchor {
                let previous_height = Height(previous_snapshot.block_height());
                tracing::debug!(
                    ?height,
                    ?block_timestamp,
                    ?previous_date_key,
                    ?previous_height,
                    store_as_realtime,
                    "found latest earlier daily snapshot"
                );
                Some(previous_height)
            } else {
                let initial_start_height = Self::initial_snapshot_metrics_start_height(
                    initial_metrics_interval_end,
                    network,
                );
                tracing::debug!(
                    ?height,
                    ?block_timestamp,
                    ?initial_start_height,
                    store_as_realtime,
                    "no earlier daily snapshot found; calculating a bounded initial metrics interval"
                );
                None
            }
        };

        // Calculate range: from (previous_snapshot_height + 1) to height (inclusive)
        // This ensures we don't double count the previous snapshot block
        // For the first snapshot (height 0), start_height will be 0 (no previous snapshot)
        let start_height = if let Some(prev_height) = previous_snapshot_height {
            // We have a previous snapshot, so start from the next block to avoid double counting
            if prev_height.0 < height.0 {
                Height(prev_height.0 + 1)
            } else {
                // Shouldn't happen, but handle it
                height
            }
        } else {
            // A database upgraded from the old height-indexed snapshot format has no compatible
            // daily anchor. Seed it with one target day instead of blocking the state writer on a
            // scan from genesis to the current tip.
            Self::initial_snapshot_metrics_start_height(initial_metrics_interval_end, network)
        };

        // 10-12. Calculate all metrics in a single pass for better performance
        // This combines transaction counting, pool flows, and block metrics
        let (
            (
                transparent_tx_count,
                transparent_coinbase_tx_count,
                shielded_coinbase_migration_tx_count,
                sprout_tx_count,
                sapling_tx_count,
                orchard_tx_count,
                ironwood_tx_count,
            ),
            (
                transparent_inflow,
                transparent_outflow,
                sprout_inflow,
                sprout_outflow,
                sapling_inflow,
                sapling_outflow,
                orchard_inflow,
                orchard_outflow,
                ironwood_inflow,
                ironwood_outflow,
            ),
            (average_block_time, average_block_fee_zat, average_block_size),
        ) = self.calculate_all_metrics_combined(start_height, height, block_timestamp)?;

        // 13. Create snapshot data
        let snapshot_data = SnapshotData::new(
            holder_count as u64,
            pool_values,
            work_difficulty,
            total_issuance,
            inflation_rate,
            block_timestamp,
            height.0,
            transparent_tx_count,
            transparent_coinbase_tx_count,
            shielded_coinbase_migration_tx_count,
            sprout_tx_count,
            sapling_tx_count,
            orchard_tx_count,
            ironwood_tx_count,
            transparent_inflow,
            transparent_outflow,
            sprout_inflow,
            sprout_outflow,
            sapling_inflow,
            sapling_outflow,
            orchard_inflow,
            orchard_outflow,
            ironwood_inflow,
            ironwood_outflow,
            average_block_time,
            average_block_fee_zat,
            average_block_size,
            None,
        );

        // 14. Store in RocksDB
        // Use separate column families for daily vs realtime snapshots
        let mut batch = DiskWriteBatch::new();
        if store_as_realtime {
            // Realtime snapshot: overwrite the active interval and persist its daily anchor in
            // the same atomic batch. Reads can then merge bounded date ranges without scanning
            // every daily record to rediscover that anchor.
            let realtime_snapshot_cf = self.realtime_snapshot_data_cf();
            batch.zs_insert(realtime_snapshot_cf, REALTIME_SNAPSHOT_KEY, snapshot_data);
            if let Some(realtime_anchor) = realtime_anchor {
                batch.zs_insert(
                    realtime_snapshot_cf,
                    REALTIME_SNAPSHOT_ANCHOR_KEY,
                    realtime_anchor,
                );
            } else {
                batch.zs_delete(realtime_snapshot_cf, REALTIME_SNAPSHOT_ANCHOR_KEY);
            }
        } else {
            // Daily snapshot: use date key (YY:MM:DD format), and atomically retire any realtime
            // interval which was based on the previous active daily snapshot.
            let date_key = SnapshotDateKey::from_timestamp(block_timestamp);
            let snapshot_cf = self.snapshot_data_by_date_cf();
            let realtime_snapshot_cf = self.realtime_snapshot_data_cf();
            batch.zs_insert(snapshot_cf, date_key, snapshot_data);
            batch.zs_delete(realtime_snapshot_cf, REALTIME_SNAPSHOT_KEY);
            batch.zs_delete(realtime_snapshot_cf, REALTIME_SNAPSHOT_ANCHOR_KEY);
        }
        self.db.write(batch)?;

        // Prepare date_key string for logging
        let date_key_str = if store_as_realtime {
            "realtime".to_string()
        } else {
            SnapshotDateKey::from_timestamp(block_timestamp).to_string()
        };

        tracing::info!(
            ?height,
            date_key = %date_key_str,
            store_as_realtime,
            holder_count,
            ?pool_values,
            work_difficulty,
            total_issuance_zat = total_issuance.zatoshis(),
            inflation_rate_percent = inflation_rate,
            block_timestamp,
            transparent_tx_count,
            transparent_coinbase_tx_count,
            shielded_coinbase_migration_tx_count,
            sprout_tx_count,
            sapling_tx_count,
            orchard_tx_count,
            ironwood_tx_count,
            transparent_inflow_zat = transparent_inflow,
            transparent_outflow_zat = transparent_outflow,
            sprout_inflow_zat = sprout_inflow,
            sprout_outflow_zat = sprout_outflow,
            sapling_inflow_zat = sapling_inflow,
            sapling_outflow_zat = sapling_outflow,
            orchard_inflow_zat = orchard_inflow,
            orchard_outflow_zat = orchard_outflow,
            ironwood_inflow_zat = ironwood_inflow,
            ironwood_outflow_zat = ironwood_outflow,
            average_block_time_seconds = average_block_time,
            average_block_fee_zat = average_block_fee_zat.zatoshis(),
            average_block_size_bytes = average_block_size,
            "stored snapshot data to RocksDB"
        );

        Ok(())
    }

    /// Returns the snapshot data for a given date key, if it was stored.
    /// Checks both daily and realtime snapshots, with an active realtime snapshot replacing its
    /// anchor daily snapshot.
    ///
    /// Returns `None` if no snapshot was stored for that date.
    pub fn snapshot_data_at_date(&self, date_key: SnapshotDateKey) -> Option<SnapshotData> {
        if let Some((realtime_date_key, realtime_data)) = self.get_realtime_snapshot() {
            if realtime_date_key == date_key {
                return Some(realtime_data);
            }
        }

        let snapshot_cf = self.snapshot_data_by_date_cf();
        self.db
            .zs_get(snapshot_cf, &date_key)
            .map(|snapshot| self.resolve_snapshot(snapshot))
    }

    /// Gets the active realtime snapshot and the daily date key it replaces.
    ///
    /// A realtime entry from an interrupted older write is stale as soon as a daily snapshot has
    /// an equal or greater height. When a daily snapshot exists, its key is also the stable display
    /// key for realtime data; block timestamps are not monotonic and must not reorder the series.
    fn get_realtime_snapshot(&self) -> Option<(SnapshotDateKey, SnapshotData)> {
        let realtime_cf = self.realtime_snapshot_data_cf();
        let realtime_data = self
            .db
            .zs_get(realtime_cf, &REALTIME_SNAPSHOT_KEY)
            .map(|snapshot| self.resolve_snapshot(snapshot))?;

        // New writes persist the anchor atomically with the realtime value. This O(1) lookup is
        // important for limited RPC reads: finding the greatest daily height otherwise requires
        // decoding and scanning the entire date-indexed column family.
        if let Some(anchor_data) = self
            .db
            .zs_get(realtime_cf, &REALTIME_SNAPSHOT_ANCHOR_KEY)
            .map(|snapshot| self.resolve_snapshot(snapshot))
        {
            return (realtime_data.block_height() > anchor_data.block_height()).then(|| {
                (
                    SnapshotDateKey::from_timestamp(anchor_data.block_timestamp()),
                    realtime_data,
                )
            });
        }

        // Compatibility fallback for realtime values written before anchor metadata was added.
        match self.latest_daily_snapshot_by_height() {
            Some((daily_date_key, daily_data))
                if realtime_data.block_height() > daily_data.block_height() =>
            {
                Some((daily_date_key, realtime_data))
            }
            Some(_) => None,
            None => Some((
                SnapshotDateKey::from_timestamp(realtime_data.block_timestamp()),
                realtime_data,
            )),
        }
    }

    /// Returns the height of the active realtime snapshot, if one exists.
    ///
    /// Stale realtime entries at or below the newest daily snapshot are ignored.
    pub fn latest_realtime_snapshot_height(&self) -> Option<Height> {
        self.get_realtime_snapshot()
            .map(|(_, snapshot)| Height(snapshot.block_height()))
    }

    /// Returns the snapshot data for a given block height, if it was stored.
    ///
    /// Returns `None` if no snapshot was stored at that height.
    pub fn snapshot_data_at_height(&self, height: Height) -> Option<SnapshotData> {
        let block = self.block(height.into())?;

        if let Some((_, realtime_data)) = self.get_realtime_snapshot() {
            if realtime_data.block_height() == height.0 {
                return Some(realtime_data);
            }
        }

        let timestamp = block.header.time.timestamp();
        let date_key = SnapshotDateKey::from_timestamp(timestamp);
        let snapshot_cf = self.snapshot_data_by_date_cf();
        self.db
            .zs_get(snapshot_cf, &date_key)
            .map(|snapshot| self.resolve_snapshot(snapshot))
            .filter(|snapshot| snapshot.block_height() == height.0)
    }

    /// Returns the most recent daily snapshot data only (excludes realtime snapshots).
    ///
    /// Returns a vector of (date_key, snapshot_data) pairs, sorted by date (ascending).
    /// Uses reverse iteration to efficiently get only the most recent snapshots.
    ///
    /// # Parameters
    ///
    /// - `limit`: Maximum number of snapshots to return
    ///
    /// # Performance
    ///
    /// This method uses reverse iteration to only read the last N snapshots,
    /// avoiding a full scan of the column family.
    pub fn recent_daily_snapshot_data(&self, limit: usize) -> Vec<(SnapshotDateKey, SnapshotData)> {
        let typed_cf = TypedColumnFamily::<SnapshotDateKey, SnapshotData>::new(
            &self.db,
            SNAPSHOT_DATA_BY_DATE,
        )
        .expect("column family was created when database was created");

        // Use reverse iteration to get the most recent snapshots first
        let mut snapshots: Vec<(SnapshotDateKey, SnapshotData)> = typed_cf
            .zs_reverse_range_iter(..)
            .take(limit)
            .map(|(date, snapshot)| (date, self.resolve_snapshot(snapshot)))
            .collect();

        // Reverse to get ascending order by date
        snapshots.reverse();

        snapshots
    }

    /// Returns the most recent snapshot data, limited to the specified count.
    /// Merges daily and active realtime snapshots, with realtime replacing its anchor daily date.
    ///
    /// Returns a vector of (date_key, snapshot_data) pairs, sorted by date (ascending).
    /// Uses reverse iteration to efficiently get only the most recent snapshots.
    ///
    /// # Parameters
    ///
    /// - `limit`: Maximum number of snapshots to return
    ///
    /// # Performance
    ///
    /// This method uses reverse iteration to only read the last N snapshots,
    /// avoiding a full scan of the column family.
    pub fn recent_snapshot_data(&self, limit: usize) -> Vec<(SnapshotDateKey, SnapshotData)> {
        let typed_cf = TypedColumnFamily::<SnapshotDateKey, SnapshotData>::new(
            &self.db,
            SNAPSHOT_DATA_BY_DATE,
        )
        .expect("column family was created when database was created");

        // Use reverse iteration to get the most recent snapshots first
        let mut snapshots: Vec<(SnapshotDateKey, SnapshotData)> = typed_cf
            .zs_reverse_range_iter(..)
            .take(limit)
            .map(|(date, snapshot)| (date, self.resolve_snapshot(snapshot)))
            .collect();

        // Reverse to get ascending order by date
        snapshots.reverse();

        // Merge with realtime snapshot if it is newer than every daily snapshot.
        if let Some((realtime_date_key, realtime_data)) = self.get_realtime_snapshot() {
            if let Some(existing_idx) = snapshots
                .iter()
                .position(|(key, _)| *key == realtime_date_key)
            {
                snapshots[existing_idx] = (realtime_date_key, realtime_data);
            } else {
                snapshots.push((realtime_date_key, realtime_data));
                snapshots.sort_by_key(|(key, _)| *key);

                if snapshots.len() > limit {
                    snapshots = snapshots.into_iter().rev().take(limit).collect();
                    snapshots.reverse();
                }
            }
        }

        snapshots
    }

    /// Returns snapshot data within a date range.
    /// Merges daily and active realtime snapshots, with realtime replacing its anchor daily date.
    ///
    /// Returns a vector of (date_key, snapshot_data) pairs, sorted by date (ascending).
    ///
    /// # Parameters
    ///
    /// - `start_date`: Optional start date (inclusive). If None, starts from the earliest snapshot.
    /// - `end_date`: Optional end date (inclusive). If None, ends at the latest snapshot.
    pub fn snapshot_data_by_date_range(
        &self,
        start_date: Option<SnapshotDateKey>,
        end_date: Option<SnapshotDateKey>,
    ) -> Vec<(SnapshotDateKey, SnapshotData)> {
        self.snapshot_data_by_date_range_limited(start_date, end_date, usize::MAX)
    }

    /// Returns at most `limit` snapshot records in an inclusive date range.
    ///
    /// The limit is applied directly to the RocksDB iterator, before merging the single active
    /// realtime record, so callers can request one extra record for cursor pagination without
    /// materializing the full range.
    pub fn snapshot_data_by_date_range_limited(
        &self,
        start_date: Option<SnapshotDateKey>,
        end_date: Option<SnapshotDateKey>,
        limit: usize,
    ) -> Vec<(SnapshotDateKey, SnapshotData)> {
        let typed_cf = TypedColumnFamily::<SnapshotDateKey, SnapshotData>::new(
            &self.db,
            SNAPSHOT_DATA_BY_DATE,
        )
        .expect("column family was created when database was created");

        // Get daily snapshots in the range
        let daily_snapshots: Box<dyn Iterator<Item = (SnapshotDateKey, SnapshotData)> + '_> =
            match (start_date, end_date) {
                (Some(start), Some(end)) => Box::new(typed_cf.zs_forward_range_iter(start..=end)),
                (Some(start), None) => Box::new(typed_cf.zs_forward_range_iter(start..)),
                (None, Some(end)) => Box::new(typed_cf.zs_forward_range_iter(..=end)),
                (None, None) => Box::new(typed_cf.zs_forward_range_iter(..)),
            };
        let mut snapshots: Vec<_> = daily_snapshots
            .take(limit)
            .map(|(date, snapshot)| (date, self.resolve_snapshot(snapshot)))
            .collect();

        if let Some((realtime_date_key, realtime_data)) = self.get_realtime_snapshot() {
            let in_range = match (start_date, end_date) {
                (Some(start), Some(end)) => realtime_date_key >= start && realtime_date_key <= end,
                (Some(start), None) => realtime_date_key >= start,
                (None, Some(end)) => realtime_date_key <= end,
                (None, None) => true,
            };

            if in_range {
                if let Some(existing_idx) = snapshots
                    .iter()
                    .position(|(key, _)| *key == realtime_date_key)
                {
                    snapshots[existing_idx] = (realtime_date_key, realtime_data);
                } else {
                    snapshots.push((realtime_date_key, realtime_data));
                    snapshots.sort_by_key(|(key, _)| *key);
                    snapshots.truncate(limit);
                }
            }
        }

        snapshots
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use zebra_chain::{
        amount::DeferredPoolBalanceChange,
        block::{self, Block},
        serialization::ZcashDeserializeInto,
        transaction::{arbitrary::fake_zip318_transaction, LockTime, Transaction},
        transparent::{new_ordered_outputs_with_height, Input, OutPoint, Output, Script},
    };

    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        request::{FinalizedBlock, SemanticallyVerifiedBlock, Treestate},
        service::finalized_state::STATE_COLUMN_FAMILIES_IN_CODE,
        CheckpointVerifiedBlock, Config,
    };

    fn amount(zatoshis: u64) -> Amount<NonNegative> {
        Amount::try_from(zatoshis).expect("test amount must be valid")
    }

    fn zip318_test_facts(
        height: Height,
        denomination_zat: u64,
        expiry_height: Height,
        seed_index: u64,
    ) -> (Vec<FinalizedTransactionFacts>, u64) {
        let transaction = Arc::new(fake_zip318_transaction(
            denomination_zat,
            expiry_height,
            seed_index,
        ));
        let conventional_fee_zat = u64::from(zebra_chain::transaction::zip317::conventional_fee(
            &transaction,
        ));

        let mut header: block::Header = zebra_test::vectors::DUMMY_HEADER
            .as_slice()
            .zcash_deserialize_into()
            .expect("dummy header should deserialize");
        header.difficulty_threshold = Network::Mainnet.target_difficulty_limit().to_compact();
        let block = Arc::new(Block {
            header: Arc::new(header),
            transactions: vec![test_coinbase(height), transaction],
        });
        let transaction_hashes: Arc<[_]> = block.transactions.iter().map(|tx| tx.hash()).collect();
        let new_outputs = new_ordered_outputs_with_height(&block, height, &transaction_hashes);
        let finalized = FinalizedBlock::from_checkpoint_verified(
            CheckpointVerifiedBlock(SemanticallyVerifiedBlock {
                block: block.clone(),
                hash: block.hash(),
                height,
                new_outputs,
                transaction_hashes,
                block_miner_fees: None,
            }),
            Treestate::default(),
            DeferredPoolBalanceChange::zero(),
        );

        (
            FinalizedTransactionFacts::from_block(&finalized),
            conventional_fee_zat,
        )
    }

    fn zip318_metrics(
        transaction_facts: &[FinalizedTransactionFacts],
        height: Height,
    ) -> SnapshotMetricTotals {
        SnapshotMetricTotals::from_block(
            transaction_facts,
            &HashMap::new(),
            &Network::Mainnet,
            height,
            123,
        )
        .expect("the ZIP-318-shaped test facts must produce valid metrics")
    }

    fn assert_zip318_funnel(metrics: &SnapshotMetricTotals, expected: [u64; 5]) {
        assert_eq!(
            [
                metrics.ironwood_counts[OBSERVABLE_ORCHARD_TO_IRONWOOD_TX_INDEX],
                metrics.ironwood_counts[ZIP318_ACTION_SHAPE_TX_INDEX],
                metrics.ironwood_counts[ZIP318_DENOMINATION_TX_INDEX],
                metrics.ironwood_counts[ZIP318_FEE_TX_INDEX],
                metrics.ironwood_counts[ZIP318_SCHEDULE_TX_INDEX],
            ],
            expected,
        );
    }

    #[test]
    fn zip318_schedule_shape_is_compatible_with_the_inclusion_height() {
        let network = Network::Mainnet;
        let activation_height = Height(3_428_143);
        let earliest_schedule_height = Height(3_428_352);
        let canonical_expiry = 3_490_560;

        assert_eq!(
            NetworkUpgrade::Nu6_3.activation_height(&network),
            Some(activation_height)
        );
        assert!(!has_zip318_schedule_shape(
            &network,
            activation_height,
            0,
            Some(canonical_expiry),
        ));
        assert!(!has_zip318_schedule_shape(
            &network,
            Height(earliest_schedule_height.0 - 1),
            0,
            Some(canonical_expiry),
        ));
        assert!(has_zip318_schedule_shape(
            &network,
            earliest_schedule_height,
            0,
            Some(canonical_expiry),
        ));
        assert!(has_zip318_schedule_shape(
            &network,
            Height(3_456_000),
            0,
            Some(canonical_expiry),
        ));
        assert!(has_zip318_schedule_shape(
            &network,
            Height(3_456_000),
            0,
            Some(3_525_120),
        ));
        assert!(has_zip318_schedule_shape(
            &network,
            Height(canonical_expiry),
            0,
            Some(canonical_expiry),
        ));
        assert!(!has_zip318_schedule_shape(
            &network,
            Height(canonical_expiry + 1),
            0,
            Some(canonical_expiry),
        ));
        assert!(!has_zip318_schedule_shape(
            &network,
            earliest_schedule_height,
            1,
            Some(canonical_expiry),
        ));
        assert!(!has_zip318_schedule_shape(
            &network,
            earliest_schedule_height,
            0,
            Some(canonical_expiry + 1),
        ));
        // Its entire scheduled-height bucket ends before ZIP-318's global lower bound.
        assert!(!has_zip318_schedule_shape(
            &network,
            earliest_schedule_height,
            0,
            Some(3_456_000),
        ));
        // Its scheduled-height bucket begins after the transaction's inclusion height.
        assert!(!has_zip318_schedule_shape(
            &network,
            earliest_schedule_height,
            0,
            Some(3_525_120),
        ));
        assert!(!has_zip318_schedule_shape(
            &network,
            earliest_schedule_height,
            0,
            None,
        ));

        let testnet = Network::new_default_testnet();
        let testnet_activation_height = Height(4_134_000);
        let testnet_earliest_schedule_height = Height(4_134_240);
        assert_eq!(
            NetworkUpgrade::Nu6_3.activation_height(&testnet),
            Some(testnet_activation_height)
        );
        assert!(!has_zip318_schedule_shape(
            &testnet,
            testnet_activation_height,
            0,
            Some(4_181_760),
        ));
        assert!(has_zip318_schedule_shape(
            &testnet,
            testnet_earliest_schedule_height,
            0,
            Some(4_181_760),
        ));
    }

    #[test]
    fn v6_zip318_transaction_reaches_every_observable_funnel_stage() {
        let height = Height(3_428_352);
        let denomination_zat = IRONWOOD_CANONICAL_DENOMINATIONS_ZAT[0];
        let (facts, conventional_fee_zat) =
            zip318_test_facts(height, denomination_zat, Height(3_490_560), 0);

        assert_eq!(facts.len(), 2);
        let migration = &facts[1];
        assert!(migration.is_v6);
        assert_eq!(migration.orchard_action_count, 2);
        assert_eq!(migration.ironwood_action_count, 1);
        assert!(migration.orchard_spends_enabled);
        assert!(migration.orchard_outputs_enabled);
        assert!(!migration.ironwood_spends_enabled);
        assert!(migration.ironwood_outputs_enabled);
        assert_eq!(migration.raw_lock_time, 0);
        assert_eq!(migration.expiry_height, Some(3_490_560));

        let metrics = zip318_metrics(&facts, height);
        for index in [
            IRONWOOD_V6_TX_INDEX,
            IRONWOOD_BUNDLE_TX_INDEX,
            ORCHARD_BUNDLE_TX_INDEX,
            ORCHARD_IRONWOOD_TX_INDEX,
            OBSERVABLE_ORCHARD_TO_IRONWOOD_TX_INDEX,
            ZIP318_ACTION_SHAPE_TX_INDEX,
            ZIP318_DENOMINATION_TX_INDEX,
            ZIP318_FEE_TX_INDEX,
            ZIP318_SCHEDULE_TX_INDEX,
        ] {
            assert_eq!(metrics.ironwood_counts[index], 1, "funnel index {index}");
        }
        assert_eq!(metrics.ironwood_counts[ORCHARD_ACTION_INDEX], 2);
        assert_eq!(metrics.ironwood_counts[IRONWOOD_ACTION_INDEX], 1);
        assert_eq!(metrics.ironwood_denomination_counts[0], 1);
        assert_eq!(
            metrics.observable_orchard_to_ironwood_value_zat,
            u128::from(denomination_zat)
        );
        assert_eq!(metrics.total_fees_zat, u128::from(conventional_fee_zat));
    }

    #[test]
    fn zip318_observable_funnel_rejects_each_noncanonical_stage() {
        let height = Height(3_428_352);
        let denomination_zat = IRONWOOD_CANONICAL_DENOMINATIONS_ZAT[0];
        let (facts, conventional_fee_zat) =
            zip318_test_facts(height, denomination_zat, Height(3_490_560), 1);
        let denomination_zat = i64::try_from(denomination_zat).expect("test value fits in i64");
        let conventional_fee_zat =
            i64::try_from(conventional_fee_zat).expect("test fee fits in i64");

        assert_zip318_funnel(&zip318_metrics(&facts, height), [1, 1, 1, 1, 1]);

        // Preserve the fee but reverse the publicly observable crossing direction.
        let mut reversed_direction = facts.clone();
        reversed_direction[1].orchard_value_balance_zat = -denomination_zat;
        reversed_direction[1].ironwood_value_balance_zat = denomination_zat + conventional_fee_zat;
        assert_zip318_funnel(
            &zip318_metrics(&reversed_direction, height),
            [0, 0, 0, 0, 0],
        );

        // Even a zero-valued transparent output is an extra publicly observable component.
        let mut extra_component = facts.clone();
        extra_component[1].transparent_output_values_zat.push(0);
        assert_zip318_funnel(&zip318_metrics(&extra_component, height), [0, 0, 0, 0, 0]);

        let mut wrong_action_count = facts.clone();
        wrong_action_count[1].orchard_action_count = 1;
        assert_zip318_funnel(
            &zip318_metrics(&wrong_action_count, height),
            [1, 0, 0, 0, 0],
        );

        let mut wrong_flags = facts.clone();
        wrong_flags[1].ironwood_spends_enabled = true;
        assert_zip318_funnel(&zip318_metrics(&wrong_flags, height), [1, 0, 0, 0, 0]);

        let mut noncanonical_denomination = facts.clone();
        let noncanonical_denomination_zat = 1_500_000;
        noncanonical_denomination[1].ironwood_value_balance_zat = -noncanonical_denomination_zat;
        noncanonical_denomination[1].orchard_value_balance_zat =
            noncanonical_denomination_zat + conventional_fee_zat;
        assert_zip318_funnel(
            &zip318_metrics(&noncanonical_denomination, height),
            [1, 1, 0, 0, 0],
        );

        let mut noncanonical_fee = facts.clone();
        noncanonical_fee[1].orchard_value_balance_zat += 1;
        assert_zip318_funnel(&zip318_metrics(&noncanonical_fee, height), [1, 1, 1, 0, 0]);

        let mut noncanonical_lock_time = facts.clone();
        noncanonical_lock_time[1].raw_lock_time = 1;
        assert_zip318_funnel(
            &zip318_metrics(&noncanonical_lock_time, height),
            [1, 1, 1, 1, 0],
        );

        // This expiry's possible scheduled heights all precede ZIP-318's global schedule floor.
        let mut pre_zip318_expiry_bucket = facts.clone();
        pre_zip318_expiry_bucket[1].expiry_height = Some(3_456_000);
        assert_zip318_funnel(
            &zip318_metrics(&pre_zip318_expiry_bucket, height),
            [1, 1, 1, 1, 0],
        );

        assert_zip318_funnel(
            &zip318_metrics(&facts, Height(height.0 - 1)),
            [1, 1, 1, 1, 0],
        );
    }

    fn test_pool_values() -> ValueBalance<NonNegative> {
        let mut pool_values = ValueBalance::<NonNegative>::zero();
        pool_values.set_transparent_value_balance(ValueBalance::from_transparent_amount(amount(1)));
        pool_values.set_sprout_value_balance(ValueBalance::from_sprout_amount(amount(2)));
        pool_values.set_sapling_value_balance(ValueBalance::from_sapling_amount(amount(3)));
        pool_values.set_orchard_value_balance(ValueBalance::from_orchard_amount(amount(4)));
        pool_values.set_deferred_amount(amount(5));
        pool_values.set_ironwood_value_balance(ValueBalance::from_ironwood_amount(amount(6)));
        pool_values
    }

    fn new_ephemeral_zebra_db(network: &Network) -> ZebraDb {
        ZebraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening an ephemeral database should succeed")
    }

    fn store_test_block(
        zebra_db: &ZebraDb,
        height: Height,
        timestamp: i64,
        previous_block_hash: block::Hash,
        transactions: Vec<Arc<Transaction>>,
    ) -> Arc<Block> {
        let mut header: block::Header = zebra_test::vectors::DUMMY_HEADER
            .as_slice()
            .zcash_deserialize_into()
            .expect("dummy header should deserialize");
        header.previous_block_hash = previous_block_hash;
        header.time =
            chrono::DateTime::from_timestamp(timestamp, 0).expect("test timestamp should be valid");
        header.difficulty_threshold = Network::Mainnet.target_difficulty_limit().to_compact();

        let block = Arc::new(Block {
            header: Arc::new(header),
            transactions,
        });
        let transaction_hashes: Arc<[_]> = block.transactions.iter().map(|tx| tx.hash()).collect();
        let new_outputs = new_ordered_outputs_with_height(&block, height, &transaction_hashes);
        let semantically_verified = SemanticallyVerifiedBlock {
            block: block.clone(),
            hash: block.hash(),
            height,
            new_outputs,
            transaction_hashes,
            block_miner_fees: None,
        };
        let finalized = FinalizedBlock::from_checkpoint_verified(
            CheckpointVerifiedBlock(semantically_verified),
            Treestate::default(),
            DeferredPoolBalanceChange::zero(),
        );

        let mut batch = DiskWriteBatch::new();
        batch.prepare_block_header_and_transaction_data_batch(zebra_db.db(), &finalized);
        zebra_db
            .write_batch(batch)
            .expect("test block should be written");

        block
    }

    fn test_coinbase(height: Height) -> Arc<Transaction> {
        Arc::new(Transaction::test_v1(
            vec![Input::Coinbase {
                height,
                data: Vec::new(),
                sequence: u32::MAX,
            }],
            Vec::new(),
            LockTime::unlocked(),
        ))
    }

    fn test_snapshot() -> SnapshotData {
        SnapshotData {
            disk_format: SnapshotDiskFormat::Current,
            legacy_expanded_difficulty: None,
            funded_transparent_address_count: 7,
            pool_values: test_pool_values(),
            work_difficulty_bits: 8.5f64.to_bits(),
            total_issuance: 9,
            inflation_rate_bps: 10,
            block_timestamp: 11,
            block_height: 12,
            transparent_tx_count: 13,
            transparent_coinbase_tx_count: 14,
            shielded_coinbase_migration_tx_count: 15,
            sprout_tx_count: 16,
            sapling_tx_count: 17,
            orchard_tx_count: 18,
            ironwood_tx_count: 19,
            transparent_inflow: 20,
            transparent_outflow: 21,
            sprout_inflow: 22,
            sprout_outflow: 23,
            sapling_inflow: 24,
            sapling_outflow: 25,
            orchard_inflow: 26,
            orchard_outflow: 27,
            ironwood_inflow: 28,
            ironwood_outflow: 29,
            average_block_time_bits: 30.5f32.to_bits(),
            average_block_fee_zat: 31,
            average_block_size: 32,
            mining_interval: Some(test_mining_interval()),
        }
    }

    fn test_mining_interval() -> MiningIntervalData {
        MiningIntervalData {
            block_count: 1,
            transaction_count: 2,
            empty_block_count: 3,
            min_header_timestamp: 4,
            max_header_timestamp: 5,
            accepted_work: 6,
            total_fees_zat: 7,
            total_block_size_bytes: 8,
            total_subsidy_zat: 9,
            miner_subsidy_zat: 10,
            founders_reward_zat: 11,
            funding_streams_zat: 12,
            deferred_subsidy_zat: 13,
            lockbox_disbursement_zat: 14,
            coinbase_output_transparent_zat: 15,
            coinbase_output_sapling_zat: 16,
            coinbase_output_orchard_zat: 17,
            coinbase_output_ironwood_zat: 18,
            coinbase_unclaimed_zat: 19,
            ironwood_counts: std::array::from_fn(|index| 20 + index as u64),
            ironwood_denomination_counts: std::array::from_fn(|index| 40 + index as u64),
            observable_orchard_to_ironwood_value_zat: 60,
        }
    }

    #[derive(Debug)]
    struct RawSnapshotBytes(Vec<u8>);

    impl IntoDisk for RawSnapshotBytes {
        type Bytes = Vec<u8>;

        fn as_bytes(&self) -> Self::Bytes {
            self.0.clone()
        }
    }

    /// Encodes the 208-byte layout written by commits 0cbd327df through 9febc0714.
    fn expanded_difficulty_snapshot_bytes(snapshot: SnapshotData, network: &Network) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(EXPANDED_DIFFICULTY_SNAPSHOT_DATA_LEN);
        bytes.extend_from_slice(&snapshot.funded_transparent_address_count.to_be_bytes());

        let pool_values = snapshot.pool_values.as_bytes();
        bytes.extend_from_slice(&pool_values[..40]);

        let expanded_difficulty: U256 = network.target_difficulty_limit().into();
        bytes.extend_from_slice(&expanded_difficulty.to_big_endian());
        bytes.extend_from_slice(&snapshot.total_issuance.to_be_bytes());
        bytes.extend_from_slice(&snapshot.inflation_rate_bps.to_be_bytes());
        bytes.extend_from_slice(&snapshot.block_timestamp.to_be_bytes());
        bytes.extend_from_slice(&snapshot.block_height.to_be_bytes());
        bytes.extend_from_slice(&snapshot.transparent_tx_count.to_be_bytes());
        bytes.extend_from_slice(&snapshot.transparent_coinbase_tx_count.to_be_bytes());
        bytes.extend_from_slice(&snapshot.shielded_coinbase_migration_tx_count.to_be_bytes());
        bytes.extend_from_slice(&snapshot.sprout_tx_count.to_be_bytes());
        bytes.extend_from_slice(&snapshot.sapling_tx_count.to_be_bytes());
        bytes.extend_from_slice(&snapshot.orchard_tx_count.to_be_bytes());
        bytes.extend_from_slice(&snapshot.transparent_inflow.to_be_bytes());
        bytes.extend_from_slice(&snapshot.transparent_outflow.to_be_bytes());
        // The historical writer used the opposite Sprout direction names.
        bytes.extend_from_slice(&snapshot.sprout_outflow.to_be_bytes());
        bytes.extend_from_slice(&snapshot.sprout_inflow.to_be_bytes());
        bytes.extend_from_slice(&snapshot.sapling_inflow.to_be_bytes());
        bytes.extend_from_slice(&snapshot.sapling_outflow.to_be_bytes());
        bytes.extend_from_slice(&snapshot.orchard_inflow.to_be_bytes());
        bytes.extend_from_slice(&snapshot.orchard_outflow.to_be_bytes());
        bytes.extend_from_slice(&snapshot.average_block_time_bits.to_be_bytes());
        bytes.extend_from_slice(&snapshot.average_block_fee_zat.to_be_bytes());
        bytes.extend_from_slice(&snapshot.average_block_size.to_be_bytes());

        assert_eq!(bytes.len(), EXPANDED_DIFFICULTY_SNAPSHOT_DATA_LEN);
        bytes
    }

    fn expected_expanded_difficulty_snapshot(
        snapshot: SnapshotData,
        network: &Network,
    ) -> SnapshotData {
        let mut pool_values = snapshot.pool_values;
        pool_values.set_ironwood_value_balance(ValueBalance::from_ironwood_amount(Amount::zero()));
        let expanded_difficulty: U256 = network.target_difficulty_limit().into();
        let slow_start_issuance =
            SnapshotData::slow_start_issuance_through(snapshot.block_height(), network)
                .expect("slow-start issuance should be valid");
        let corrected_issuance = (snapshot.total_issuance() + slow_start_issuance)
            .expect("corrected issuance should be valid");
        let total_supply = (pool_values.transparent_amount()
            + pool_values.sprout_amount()
            + pool_values.sapling_amount()
            + pool_values.orchard_amount()
            + pool_values.deferred_amount()
            + pool_values.ironwood_amount())
        .expect("test pool values must have a valid total");
        let inflation_rate_bps = if snapshot.block_height() < network.slow_start_interval().0 {
            (inflation_rate_percent(Height(snapshot.block_height()), network, total_supply)
                .expect("test inflation inputs must be valid")
                * 100.0)
                .round() as u32
        } else {
            snapshot.inflation_rate_bps
        };

        SnapshotData {
            disk_format: SnapshotDiskFormat::ResolvedLegacy,
            legacy_expanded_difficulty: None,
            pool_values,
            work_difficulty_bits: SnapshotData::relative_work_difficulty(
                expanded_difficulty,
                network,
            )
            .to_bits(),
            total_issuance: corrected_issuance.zatoshis() as u64,
            inflation_rate_bps,
            ironwood_tx_count: 0,
            ironwood_inflow: 0,
            ironwood_outflow: 0,
            mining_interval: None,
            ..snapshot
        }
    }

    #[test]
    fn current_snapshot_disk_round_trip() {
        let snapshot = test_snapshot();
        let bytes = snapshot.as_bytes();

        assert_eq!(bytes.len(), CURRENT_SNAPSHOT_DATA_LEN);
        assert_eq!(SnapshotData::from_bytes(bytes), snapshot);
    }

    #[test]
    fn pre_mining_snapshot_defaults_mining_interval_to_unavailable() {
        let snapshot = test_snapshot();
        let bytes = snapshot.as_bytes();
        let decoded = SnapshotData::from_bytes(&bytes[..IRONWOOD_SNAPSHOT_DATA_LEN]);

        assert_eq!(
            decoded,
            SnapshotData {
                mining_interval: None,
                ..snapshot
            }
        );
    }

    #[test]
    fn pre_observatory_mining_snapshot_keeps_mining_but_hides_ironwood_totals() {
        let snapshot = test_snapshot();
        let bytes = snapshot.as_bytes();
        let decoded = SnapshotData::from_bytes(&bytes[..MINING_SNAPSHOT_DATA_LEN]);
        let mut legacy_mining = test_mining_interval();
        legacy_mining.ironwood_counts = [0; SNAPSHOT_IRONWOOD_COUNTER_COUNT];
        legacy_mining.ironwood_denomination_counts =
            [0; IRONWOOD_CANONICAL_DENOMINATIONS_ZAT.len()];
        legacy_mining.observable_orchard_to_ironwood_value_zat = 0;

        assert_eq!(
            decoded,
            SnapshotData {
                disk_format: SnapshotDiskFormat::Mining,
                mining_interval: Some(legacy_mining),
                ..snapshot
            }
        );
        assert_eq!(decoded.mining_interval(), Some(legacy_mining));
        assert_eq!(decoded.ironwood_observatory_interval(), None);
    }

    #[test]
    fn snapshot_accumulator_disk_round_trip_is_fixed_and_deterministic() {
        let totals = SnapshotMetricTotals {
            transaction_counts: [1, 2, 3, 4, 5, 6, 7],
            pool_flows: [8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
            total_fees_zat: 18,
            total_block_size: 19,
            block_count: 20,
            ..SnapshotMetricTotals::default()
        };
        let accumulator = SnapshotAccumulator {
            latest_height: 21,
            latest_timestamp: 22,
            latest_pool_values: test_pool_values(),
            latest_work_difficulty_bits: 23.5f64.to_bits(),
            funded_transparent_address_count: 24,
            total_issuance: 25,
            totals,
            daily_header_time_range: SnapshotHeaderTimeRange {
                min_timestamp: 13,
                max_timestamp: 22,
            },
            realtime_header_time_range: SnapshotHeaderTimeRange {
                min_timestamp: 22,
                max_timestamp: 26,
            },
            daily_anchor: SnapshotMetricAnchor {
                initialized: true,
                height: 12,
                timestamp: 13,
                totals: SnapshotMetricTotals {
                    block_count: 14,
                    ..SnapshotMetricTotals::default()
                },
            },
            realtime_anchor: SnapshotMetricAnchor::before_genesis(26),
        };

        let first_encoding = accumulator.as_bytes();
        let second_encoding = accumulator.as_bytes();
        assert_eq!(first_encoding.len(), SNAPSHOT_ACCUMULATOR_LEN);
        assert_eq!(first_encoding, second_encoding);
        assert_eq!(SnapshotAccumulator::from_bytes(first_encoding), accumulator);
    }

    #[test]
    fn accumulator_materializes_daily_and_realtime_intervals_by_subtraction() {
        let network = Network::Mainnet;
        let accumulator = SnapshotAccumulator {
            latest_height: 20,
            latest_timestamp: 1_800,
            latest_pool_values: test_pool_values(),
            latest_work_difficulty_bits: 2.5f64.to_bits(),
            funded_transparent_address_count: 42,
            total_issuance: 1_000,
            totals: SnapshotMetricTotals {
                transaction_counts: [20, 20, 20, 20, 20, 20, 20],
                pool_flows: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200],
                total_fees_zat: 1_000,
                total_block_size: 1_000,
                block_count: 10,
                transaction_count: 20,
                empty_block_count: 3,
                accepted_work: 10_000,
                ..SnapshotMetricTotals::default()
            },
            daily_header_time_range: SnapshotHeaderTimeRange {
                min_timestamp: 1_000,
                max_timestamp: 1_800,
            },
            realtime_header_time_range: SnapshotHeaderTimeRange {
                min_timestamp: 900,
                max_timestamp: 1_800,
            },
            daily_anchor: SnapshotMetricAnchor {
                initialized: true,
                height: 12,
                timestamp: 1_000,
                totals: SnapshotMetricTotals {
                    transaction_counts: [5, 5, 5, 5, 5, 5, 5],
                    pool_flows: [50, 50, 50, 50, 50, 50, 50, 50, 50, 50],
                    total_fees_zat: 200,
                    total_block_size: 200,
                    block_count: 2,
                    transaction_count: 5,
                    empty_block_count: 1,
                    accepted_work: 2_000,
                    ..SnapshotMetricTotals::default()
                },
            },
            realtime_anchor: SnapshotMetricAnchor {
                initialized: true,
                height: 9,
                timestamp: 900,
                totals: SnapshotMetricTotals {
                    transaction_counts: [1, 1, 1, 1, 1, 1, 1],
                    pool_flows: [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
                    total_fees_zat: 100,
                    total_block_size: 100,
                    block_count: 1,
                    transaction_count: 1,
                    empty_block_count: 0,
                    accepted_work: 1_000,
                    ..SnapshotMetricTotals::default()
                },
            },
        };

        let daily = SnapshotData::from_accumulator(
            &accumulator,
            accumulator.daily_anchor,
            accumulator.daily_header_time_range,
            &network,
        )
        .expect("daily interval should materialize");
        assert_eq!(daily.funded_transparent_address_count(), 42);
        assert_eq!(daily.transparent_tx_count(), 15);
        assert_eq!(daily.ironwood_tx_count(), 15);
        assert_eq!(daily.transparent_inflow(), 150);
        assert_eq!(daily.ironwood_outflow(), 150);
        assert_eq!(daily.average_block_time(), 100.0);
        assert_eq!(daily.average_block_fee_zat(), amount(100));
        assert_eq!(daily.average_block_size(), 100);
        let daily_mining = daily.mining_interval().expect("mining totals should exist");
        assert_eq!(daily_mining.block_count(), 8);
        assert_eq!(daily_mining.transaction_count(), 15);
        assert_eq!(daily_mining.empty_block_count(), 2);
        assert_eq!(daily_mining.accepted_work(), 8_000);
        assert_eq!(daily_mining.elapsed_header_time_seconds(), 800);

        let realtime = SnapshotData::from_accumulator(
            &accumulator,
            accumulator.realtime_anchor,
            accumulator.realtime_header_time_range,
            &network,
        )
        .expect("realtime replacement interval should materialize");
        assert_eq!(realtime.transparent_tx_count(), 19);
        assert_eq!(realtime.transparent_inflow(), 190);
        assert_eq!(realtime.average_block_time(), 100.0);
        assert_eq!(realtime.average_block_fee_zat(), amount(100));
        assert_eq!(realtime.average_block_size(), 100);
    }

    #[test]
    fn accumulator_anchor_sequence_preserves_realtime_replacement_intervals() {
        let network = Network::Mainnet;
        let zebra_db = new_ephemeral_zebra_db(&network);
        let genesis_timestamp = 1_700_000_000;
        let genesis_date = SnapshotDateKey::from_timestamp(genesis_timestamp);
        let before_genesis = SnapshotMetricAnchor::before_genesis(genesis_timestamp);
        let mut accumulator = SnapshotAccumulator {
            latest_height: 0,
            latest_timestamp: genesis_timestamp,
            latest_pool_values: test_pool_values(),
            latest_work_difficulty_bits: 1.0f64.to_bits(),
            funded_transparent_address_count: 0,
            total_issuance: 0,
            totals: SnapshotMetricTotals {
                transaction_counts: [0, 1, 0, 0, 0, 0, 0],
                block_count: 1,
                ..SnapshotMetricTotals::default()
            },
            daily_header_time_range: SnapshotHeaderTimeRange::singleton(genesis_timestamp),
            realtime_header_time_range: SnapshotHeaderTimeRange::singleton(genesis_timestamp),
            daily_anchor: before_genesis,
            realtime_anchor: before_genesis,
        };
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            zebra_db.realtime_snapshot_data_cf(),
            SNAPSHOT_ACCUMULATOR_KEY,
            accumulator,
        );
        zebra_db
            .write_batch(batch)
            .expect("genesis accumulator should be written");

        zebra_db
            .store_snapshot_data(Height(0), &network, false)
            .expect("genesis daily snapshot should be stored");
        assert_eq!(
            zebra_db
                .snapshot_data_at_date(genesis_date)
                .expect("genesis daily snapshot should exist")
                .transparent_coinbase_tx_count(),
            1
        );
        accumulator = zebra_db
            .snapshot_accumulator()
            .expect("daily anchor should be persisted");
        assert_eq!(accumulator.daily_anchor.height, 0);
        assert!(accumulator.daily_anchor.initialized);
        assert!(!accumulator.realtime_anchor.initialized);

        // A realtime row replaces the genesis daily row, so its interval still begins at the
        // virtual pre-genesis anchor.
        accumulator.latest_height = 1;
        accumulator.latest_timestamp = genesis_timestamp + 75;
        accumulator
            .daily_header_time_range
            .include(accumulator.latest_timestamp);
        accumulator
            .realtime_header_time_range
            .include(accumulator.latest_timestamp);
        accumulator.totals.transaction_counts[TRANSPARENT_COINBASE_TX_INDEX] = 2;
        accumulator.totals.block_count = 2;
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            zebra_db.realtime_snapshot_data_cf(),
            SNAPSHOT_ACCUMULATOR_KEY,
            accumulator,
        );
        zebra_db
            .write_batch(batch)
            .expect("height-1 accumulator should be written");
        zebra_db
            .store_snapshot_data(Height(1), &network, true)
            .expect("height-1 realtime snapshot should be stored");
        assert_eq!(
            zebra_db
                .get_realtime_snapshot()
                .expect("realtime snapshot should exist")
                .1
                .transparent_coinbase_tx_count(),
            2
        );

        // A next-day daily row advances the daily anchor and retains the prior daily endpoint as
        // the new realtime interval anchor.
        let next_day_timestamp = genesis_timestamp + 24 * 60 * 60;
        let next_day_date = SnapshotDateKey::from_timestamp(next_day_timestamp);
        accumulator.latest_height = 2;
        accumulator.latest_timestamp = next_day_timestamp;
        accumulator
            .daily_header_time_range
            .include(accumulator.latest_timestamp);
        accumulator
            .realtime_header_time_range
            .include(accumulator.latest_timestamp);
        accumulator.totals.transaction_counts[TRANSPARENT_COINBASE_TX_INDEX] = 3;
        accumulator.totals.block_count = 3;
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            zebra_db.realtime_snapshot_data_cf(),
            SNAPSHOT_ACCUMULATOR_KEY,
            accumulator,
        );
        zebra_db
            .write_batch(batch)
            .expect("height-2 accumulator should be written");
        zebra_db
            .store_snapshot_data(Height(2), &network, false)
            .expect("next daily snapshot should be stored");
        assert_eq!(
            zebra_db
                .snapshot_data_at_date(next_day_date)
                .expect("next daily snapshot should exist")
                .transparent_coinbase_tx_count(),
            2
        );
        accumulator = zebra_db
            .snapshot_accumulator()
            .expect("next anchors should be persisted");
        assert_eq!(accumulator.daily_anchor.height, 2);
        assert_eq!(accumulator.realtime_anchor.height, 0);

        // Realtime now replaces the height-2 daily row and includes heights 1 through 3.
        accumulator.latest_height = 3;
        accumulator.latest_timestamp = next_day_timestamp + 75;
        accumulator
            .daily_header_time_range
            .include(accumulator.latest_timestamp);
        accumulator
            .realtime_header_time_range
            .include(accumulator.latest_timestamp);
        accumulator.totals.transaction_counts[TRANSPARENT_COINBASE_TX_INDEX] = 4;
        accumulator.totals.block_count = 4;
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            zebra_db.realtime_snapshot_data_cf(),
            SNAPSHOT_ACCUMULATOR_KEY,
            accumulator,
        );
        zebra_db
            .write_batch(batch)
            .expect("height-3 accumulator should be written");
        zebra_db
            .store_snapshot_data(Height(3), &network, true)
            .expect("height-3 realtime snapshot should be stored");
        assert_eq!(
            zebra_db
                .get_realtime_snapshot()
                .expect("next realtime snapshot should exist")
                .1
                .transparent_coinbase_tx_count(),
            3
        );
    }

    #[test]
    fn per_block_metrics_reuse_resolved_utxos_for_flows_and_fees() {
        let height = Height(1);
        let timestamp = 1_700_000_000;
        let spent_outpoint = OutPoint {
            hash: zebra_chain::transaction::Hash([7; 32]),
            index: 0,
        };
        let spent_utxo =
            transparent::Utxo::new(Output::new(amount(10), Script::new(&[])), Height(0), false);
        let regular_transaction = Arc::new(Transaction::test_v1(
            vec![Input::PrevOut {
                outpoint: spent_outpoint,
                unlock_script: Script::new(&[]),
                sequence: u32::MAX,
            }],
            vec![Output::new(amount(7), Script::new(&[]))],
            LockTime::unlocked(),
        ));
        let coinbase = test_coinbase(height);

        let mut header: block::Header = zebra_test::vectors::DUMMY_HEADER
            .as_slice()
            .zcash_deserialize_into()
            .expect("dummy header should deserialize");
        header.time =
            chrono::DateTime::from_timestamp(timestamp, 0).expect("test timestamp should be valid");
        header.difficulty_threshold = Network::Mainnet.target_difficulty_limit().to_compact();
        let block = Arc::new(Block {
            header: Arc::new(header),
            transactions: vec![coinbase, regular_transaction],
        });
        let transaction_hashes: Arc<[_]> = block.transactions.iter().map(|tx| tx.hash()).collect();
        let new_outputs = new_ordered_outputs_with_height(&block, height, &transaction_hashes);
        let verified = SemanticallyVerifiedBlock {
            block,
            hash: block::Hash([8; 32]),
            height,
            new_outputs,
            transaction_hashes,
            block_miner_fees: None,
        };
        let finalized = FinalizedBlock::from_checkpoint_verified(
            CheckpointVerifiedBlock(verified),
            Treestate::default(),
            DeferredPoolBalanceChange::zero(),
        );
        let spent_utxos = HashMap::from([(spent_outpoint, spent_utxo)]);
        let transaction_facts = FinalizedTransactionFacts::from_block(&finalized);

        let metrics = SnapshotMetricTotals::from_block(
            &transaction_facts,
            &spent_utxos,
            &Network::Mainnet,
            height,
            123,
        )
        .expect("verified block metrics should be valid");
        assert_eq!(metrics.transaction_counts[TRANSPARENT_TX_INDEX], 1);
        assert_eq!(metrics.transaction_counts[TRANSPARENT_COINBASE_TX_INDEX], 1);
        assert_eq!(metrics.pool_flows[TRANSPARENT_INFLOW_INDEX], 7);
        assert_eq!(metrics.pool_flows[TRANSPARENT_OUTFLOW_INDEX], 10);
        assert_eq!(metrics.total_fees_zat, 3);
        assert_eq!(metrics.total_block_size, 123);
        assert_eq!(metrics.block_count, 1);

        let zebra_db = new_ephemeral_zebra_db(&Network::Mainnet);
        let mut batch = DiskWriteBatch::new();
        let error = batch
            .prepare_snapshot_accumulator_batch(
                &zebra_db,
                &Network::Mainnet,
                &finalized,
                &transaction_facts,
                &spent_utxos,
                0,
                ValueBalance::zero(),
                123,
            )
            .expect_err("a non-genesis database must not silently seed an accumulator");
        assert!(matches!(
            error,
            SnapshotAccumulatorError::MissingAtNonGenesis { height: Height(1) }
        ));
    }

    #[test]
    fn mining_accounting_keeps_coinbase_routes_neutral_and_subsidies_separate() {
        let network = Network::Mainnet;
        let height = Height(1);
        let timestamp = 1_700_000_000;
        let subsidy = block_subsidy(height, &network).expect("test subsidy must be valid");
        let coinbase = Arc::new(Transaction::test_v1(
            vec![Input::Coinbase {
                height,
                data: Vec::new(),
                sequence: u32::MAX,
            }],
            vec![Output::new(subsidy, Script::new(&[]))],
            LockTime::unlocked(),
        ));
        let mut header: block::Header = zebra_test::vectors::DUMMY_HEADER
            .as_slice()
            .zcash_deserialize_into()
            .expect("dummy header should deserialize");
        header.time =
            chrono::DateTime::from_timestamp(timestamp, 0).expect("test timestamp should be valid");
        header.difficulty_threshold = network.target_difficulty_limit().to_compact();
        let block = Arc::new(Block {
            header: Arc::new(header),
            transactions: vec![coinbase],
        });
        let transaction_hashes: Arc<[_]> = block.transactions.iter().map(|tx| tx.hash()).collect();
        let new_outputs = new_ordered_outputs_with_height(&block, height, &transaction_hashes);
        let verified = SemanticallyVerifiedBlock {
            block,
            hash: block::Hash([9; 32]),
            height,
            new_outputs,
            transaction_hashes,
            block_miner_fees: Some(Amount::zero()),
        };
        let finalized = FinalizedBlock::from_checkpoint_verified(
            CheckpointVerifiedBlock(verified),
            Treestate::default(),
            DeferredPoolBalanceChange::zero(),
        );
        let transaction_facts = FinalizedTransactionFacts::from_block(&finalized);

        let mut metrics = SnapshotMetricTotals::from_block(
            &transaction_facts,
            &HashMap::new(),
            &network,
            height,
            123,
        )
        .expect("coinbase metrics should be valid");
        metrics
            .add_mining_accounting(&finalized, &network, subsidy, Amount::zero())
            .expect("mining accounting should be valid");

        assert_eq!(metrics.block_count, 1);
        assert_eq!(metrics.transaction_count, 1);
        assert_eq!(metrics.empty_block_count, 1);
        assert_eq!(
            metrics.accepted_work,
            network
                .target_difficulty_limit()
                .to_compact()
                .to_work()
                .expect("test target must have work")
                .as_u128()
        );
        assert_eq!(
            metrics.coinbase_output_transparent_zat,
            subsidy.zatoshis() as u128
        );
        assert_eq!(metrics.coinbase_output_sapling_zat, 0);
        assert_eq!(metrics.coinbase_output_orchard_zat, 0);
        assert_eq!(metrics.coinbase_output_ironwood_zat, 0);
        assert_eq!(metrics.coinbase_unclaimed_zat, 0);
        assert_eq!(
            metrics
                .miner_subsidy_zat
                .checked_add(metrics.founders_reward_zat),
            Some(metrics.total_subsidy_zat)
        );
        assert!(metrics.founders_reward_zat > 0);
        assert!(metrics.coinbase_output_transparent_zat > metrics.miner_subsidy_zat);
    }

    #[test]
    fn genesis_block_and_accumulator_can_be_committed_in_one_batch() {
        let network = Network::Mainnet;
        let zebra_db = new_ephemeral_zebra_db(&network);
        let height = Height(0);
        let timestamp = 1_700_000_000;
        let mut header: block::Header = zebra_test::vectors::DUMMY_HEADER
            .as_slice()
            .zcash_deserialize_into()
            .expect("dummy header should deserialize");
        header.time =
            chrono::DateTime::from_timestamp(timestamp, 0).expect("test timestamp should be valid");
        header.difficulty_threshold = network.target_difficulty_limit().to_compact();
        let block = Arc::new(Block {
            header: Arc::new(header),
            transactions: Vec::new(),
        });
        let transaction_hashes: Arc<[zebra_chain::transaction::Hash]> = Vec::new().into();
        let verified = SemanticallyVerifiedBlock {
            block: block.clone(),
            hash: block.hash(),
            height,
            new_outputs: HashMap::new(),
            transaction_hashes,
            block_miner_fees: None,
        };
        let finalized = FinalizedBlock::from_checkpoint_verified(
            CheckpointVerifiedBlock(verified),
            Treestate::default(),
            DeferredPoolBalanceChange::zero(),
        );
        let transaction_facts = FinalizedTransactionFacts::from_block(&finalized);

        let mut batch = DiskWriteBatch::new();
        batch.prepare_block_header_and_transaction_data_batch(zebra_db.db(), &finalized);
        batch
            .prepare_snapshot_accumulator_batch(
                &zebra_db,
                &network,
                &finalized,
                &transaction_facts,
                &HashMap::new(),
                0,
                ValueBalance::zero(),
                123,
            )
            .expect("genesis accumulator should prepare");

        assert!(zebra_db.block(height.into()).is_none());
        assert!(zebra_db.snapshot_accumulator().is_none());
        zebra_db
            .write_batch(batch)
            .expect("atomic block and accumulator batch should commit");
        assert_eq!(zebra_db.block(height.into()), Some(block));
        let accumulator = zebra_db
            .snapshot_accumulator()
            .expect("accumulator should commit with the block");
        assert_eq!(accumulator.latest_height, height.0);
        assert_eq!(accumulator.latest_timestamp, timestamp);
        assert_eq!(accumulator.totals.block_count, 1);
    }

    #[test]
    fn total_issuance_includes_slow_start() {
        let network = Network::Mainnet;
        let height = Height(1);
        let expected = block_subsidy(height, &network).expect("slow-start subsidy should be valid");

        assert!(expected > Amount::<NonNegative>::zero());
        assert_eq!(
            ZebraDb::calculate_total_issuance(height, &network, None)
                .expect("issuance calculation should succeed"),
            expected,
        );
    }

    #[test]
    fn first_snapshot_metrics_are_limited_to_one_target_day() {
        let network = Network::Mainnet;
        let blossom_height = NetworkUpgrade::Blossom
            .activation_height(&network)
            .expect("mainnet has a Blossom activation height");

        assert_eq!(
            ZebraDb::initial_snapshot_metrics_start_height(Height(575), &network),
            Height(0),
        );
        assert_eq!(
            ZebraDb::initial_snapshot_metrics_start_height(Height(576), &network),
            Height(1),
        );

        let post_blossom_height = Height(blossom_height.0 + 2_000);
        assert_eq!(
            ZebraDb::initial_snapshot_metrics_start_height(post_blossom_height, &network),
            Height(post_blossom_height.0 - 1_151),
        );

        let realtime_height = Height(post_blossom_height.0 + 48);
        assert_eq!(
            ZebraDb::initial_snapshot_metrics_end_height(
                realtime_height,
                true,
                Some(post_blossom_height),
            ),
            post_blossom_height,
            "realtime must preserve the first daily snapshot's bounded interval",
        );
        assert_eq!(
            ZebraDb::initial_snapshot_metrics_end_height(
                realtime_height,
                false,
                Some(post_blossom_height),
            ),
            realtime_height,
        );
    }

    #[test]
    fn grouped_total_issuance_matches_each_regtest_block() {
        let network = Network::new_regtest(Default::default());
        let final_height = Height(1_000);
        let mut expected = Amount::<NonNegative>::zero();

        for height in 0..=final_height.0 {
            expected = (expected
                + block_subsidy(Height(height), &network)
                    .expect("regtest subsidy should be valid"))
            .expect("regtest issuance should be valid");
        }

        assert_eq!(
            ZebraDb::calculate_total_issuance(final_height, &network, None)
                .expect("grouped issuance should be valid"),
            expected,
        );
    }

    #[test]
    fn grouped_total_issuance_matches_mainnet_through_first_halving() {
        let network = Network::Mainnet;
        let first_halving = height_for_halving(1, &network)
            .expect("mainnet first halving height should be defined");
        let final_height = Height(first_halving.0 + 1);
        let mut expected = Amount::<NonNegative>::zero();

        for height in 0..=final_height.0 {
            expected = (expected
                + block_subsidy(Height(height), &network)
                    .expect("mainnet subsidy should be valid"))
            .expect("mainnet issuance should be valid");
        }

        assert_eq!(
            ZebraDb::calculate_total_issuance(final_height, &network, None)
                .expect("grouped issuance should be valid"),
            expected,
        );
    }

    #[test]
    fn closed_form_slow_start_issuance_matches_block_subsidies() {
        let network = Network::Mainnet;
        let interval = network.slow_start_interval().0;
        let shift = network.slow_start_shift().0;
        let checkpoints = [0, 1, shift - 1, shift, interval - 1];
        let mut expected = Amount::<NonNegative>::zero();

        for height in 0..interval {
            expected = (expected
                + block_subsidy(Height(height), &network)
                    .expect("slow-start subsidy should be valid"))
            .expect("cumulative slow-start issuance should be valid");

            if checkpoints.contains(&height) {
                assert_eq!(
                    SnapshotData::slow_start_issuance_through(height, &network)
                        .expect("closed-form slow-start issuance should be valid"),
                    expected,
                );
            }
        }

        assert_eq!(
            SnapshotData::slow_start_issuance_through(interval, &network)
                .expect("issuance after slow start should stay constant"),
            expected,
        );
    }

    #[test]
    fn total_issuance_uses_current_and_legacy_snapshot_baselines() {
        let network = Network::Mainnet;
        let first_height = Height(1);
        let next_height = Height(2);
        let first_total = ZebraDb::calculate_total_issuance(first_height, &network, None)
            .expect("full issuance calculation should succeed");
        let expected = ZebraDb::calculate_total_issuance(next_height, &network, None)
            .expect("full issuance calculation should succeed");

        let current_snapshot = SnapshotData {
            block_height: first_height.0,
            total_issuance: first_total.zatoshis() as u64,
            ..test_snapshot()
        };
        assert_eq!(
            ZebraDb::calculate_total_issuance(next_height, &network, Some(&current_snapshot),)
                .expect("incremental issuance calculation should succeed"),
            expected,
        );

        // Historical formats omitted all slow-start subsidies. Incremental calculation corrects
        // that bounded omission once, then continues after the legacy snapshot height.
        let legacy_snapshot = SnapshotData {
            disk_format: SnapshotDiskFormat::Legacy,
            block_height: first_height.0,
            total_issuance: 0,
            ..test_snapshot()
        }
        .resolve_legacy_fields(&network);
        assert_eq!(
            ZebraDb::calculate_total_issuance(next_height, &network, Some(&legacy_snapshot))
                .expect("legacy incremental issuance calculation should succeed"),
            expected,
        );
    }

    #[test]
    fn daily_anchor_uses_height_when_timestamps_go_backwards() {
        let network = Network::Mainnet;
        let zebra_db = new_ephemeral_zebra_db(&network);
        let current_timestamp = 1_700_000_000;
        let previous_timestamp = current_timestamp - 24 * 60 * 60;
        let current_date = SnapshotDateKey::from_timestamp(current_timestamp);
        let previous_date = SnapshotDateKey::from_timestamp(previous_timestamp);
        // Deliberately give the lower-height snapshot the later date. Header times are not
        // required to increase, so date-key iteration would choose the wrong anchor here.
        let lower_snapshot = SnapshotData {
            block_timestamp: current_timestamp,
            block_height: 10,
            ..test_snapshot()
        };
        let higher_snapshot = SnapshotData {
            block_timestamp: previous_timestamp,
            block_height: 20,
            ..test_snapshot()
        };

        let mut batch = DiskWriteBatch::new();
        let snapshot_cf = zebra_db.snapshot_data_by_date_cf();
        batch.zs_insert(snapshot_cf, current_date, lower_snapshot);
        batch.zs_insert(snapshot_cf, previous_date, higher_snapshot);
        zebra_db
            .write_batch(batch)
            .expect("daily snapshots should be written");

        assert_eq!(
            zebra_db.latest_daily_snapshot_before_height(Height(21)),
            Some((previous_date, higher_snapshot)),
        );
        assert_eq!(
            zebra_db.latest_daily_snapshot_before_height(Height(20)),
            Some((current_date, lower_snapshot)),
        );
    }

    #[test]
    fn expanded_difficulty_snapshots_are_resolved_on_every_read_path() {
        let network = Network::Mainnet;
        let zebra_db = new_ephemeral_zebra_db(&network);
        let daily_timestamp = 1_700_000_000;
        let realtime_timestamp = daily_timestamp - 24 * 60 * 60;
        let daily_date = SnapshotDateKey::from_timestamp(daily_timestamp);

        let first_block = store_test_block(
            &zebra_db,
            Height(1),
            daily_timestamp,
            block::Hash([0; 32]),
            vec![test_coinbase(Height(1))],
        );
        store_test_block(
            &zebra_db,
            Height(2),
            daily_timestamp + 75,
            first_block.hash(),
            vec![test_coinbase(Height(2))],
        );

        let daily_snapshot = SnapshotData {
            block_timestamp: daily_timestamp,
            block_height: 1,
            ..test_snapshot()
        };
        let daily_bytes = expanded_difficulty_snapshot_bytes(daily_snapshot, &network);
        let expected_daily = expected_expanded_difficulty_snapshot(daily_snapshot, &network);
        assert_eq!(
            SnapshotData::from_bytes(&daily_bytes).resolve_legacy_fields(&network),
            expected_daily,
        );
        assert!(
            expected_daily.inflation_rate_percent() > 0.0,
            "legacy slow-start inflation must be restored from pool supply"
        );
        assert_eq!(
            expected_daily.total_issuance().zatoshis(),
            daily_snapshot.total_issuance().zatoshis()
                + block_subsidy(Height(1), &network)
                    .expect("height-1 slow-start subsidy should be valid")
                    .zatoshis(),
            "legacy reads must restore omitted slow-start issuance",
        );

        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            zebra_db.snapshot_data_by_date_cf(),
            daily_date,
            RawSnapshotBytes(daily_bytes),
        );
        zebra_db
            .write_batch(batch)
            .expect("historical daily snapshot should be written");

        assert_eq!(
            zebra_db.snapshot_data_at_date(daily_date),
            Some(expected_daily)
        );
        assert_eq!(
            zebra_db.snapshot_data_at_height(Height(1)),
            Some(expected_daily)
        );
        // Height 2 has the same UTC date, but no snapshot. Merely converting height to a date must
        // not return the height-1 daily record.
        assert_eq!(zebra_db.snapshot_data_at_height(Height(2)), None);
        assert_eq!(
            zebra_db.recent_daily_snapshot_data(10),
            vec![(daily_date, expected_daily)]
        );
        assert_eq!(
            zebra_db.recent_snapshot_data(10),
            vec![(daily_date, expected_daily)]
        );
        assert_eq!(
            zebra_db.snapshot_data_by_date_range(None, None),
            vec![(daily_date, expected_daily)]
        );
        assert_eq!(
            zebra_db.snapshot_data_by_date_range_limited(None, None, 1),
            vec![(daily_date, expected_daily)]
        );

        let realtime_snapshot = SnapshotData {
            block_timestamp: realtime_timestamp,
            block_height: 2,
            ..test_snapshot()
        };
        let expected_realtime = expected_expanded_difficulty_snapshot(realtime_snapshot, &network);
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            zebra_db.realtime_snapshot_data_cf(),
            REALTIME_SNAPSHOT_KEY,
            RawSnapshotBytes(expanded_difficulty_snapshot_bytes(
                realtime_snapshot,
                &network,
            )),
        );
        zebra_db
            .write_batch(batch)
            .expect("historical realtime snapshot should be written");

        // Realtime uses the highest daily snapshot's key, even though its own timestamp went
        // backwards into a different date.
        assert_eq!(
            zebra_db.get_realtime_snapshot(),
            Some((daily_date, expected_realtime))
        );
        assert_eq!(zebra_db.latest_realtime_snapshot_height(), Some(Height(2)));
        assert_eq!(
            zebra_db.snapshot_data_at_date(daily_date),
            Some(expected_realtime)
        );
        assert_eq!(
            zebra_db.snapshot_data_at_height(Height(1)),
            Some(expected_daily)
        );
        assert_eq!(
            zebra_db.snapshot_data_at_height(Height(2)),
            Some(expected_realtime)
        );
        assert_eq!(
            zebra_db.recent_daily_snapshot_data(10),
            vec![(daily_date, expected_daily)]
        );
        assert_eq!(
            zebra_db.recent_snapshot_data(10),
            vec![(daily_date, expected_realtime)]
        );
        assert_eq!(
            zebra_db.snapshot_data_by_date_range_limited(None, None, 1),
            vec![(daily_date, expected_realtime)]
        );

        // An old realtime entry cannot replace a newer daily snapshot after a crash/restart.
        let newer_date = SnapshotDateKey::from_timestamp(daily_timestamp + 24 * 60 * 60);
        let newer_daily = SnapshotData {
            block_timestamp: daily_timestamp + 24 * 60 * 60,
            block_height: 3,
            ..test_snapshot()
        };
        let expected_newer_daily = expected_expanded_difficulty_snapshot(newer_daily, &network);
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            zebra_db.snapshot_data_by_date_cf(),
            newer_date,
            RawSnapshotBytes(expanded_difficulty_snapshot_bytes(newer_daily, &network)),
        );
        zebra_db
            .write_batch(batch)
            .expect("newer daily snapshot should be written");

        assert_eq!(zebra_db.get_realtime_snapshot(), None);
        assert_eq!(zebra_db.latest_realtime_snapshot_height(), None);
        assert_eq!(
            zebra_db.snapshot_data_at_date(newer_date),
            Some(expected_newer_daily)
        );
        assert_eq!(
            zebra_db.recent_snapshot_data(10),
            vec![
                (daily_date, expected_daily),
                (newer_date, expected_newer_daily)
            ]
        );
    }

    #[test]
    fn metrics_use_historical_spent_outputs_and_complete_time_intervals() {
        let network = Network::Mainnet;
        let zebra_db = new_ephemeral_zebra_db(&network);
        let source_value = amount(10);
        let spend_value = amount(7);
        let source_transaction = Arc::new(Transaction::test_v1(
            vec![Input::Coinbase {
                height: Height(1),
                data: Vec::new(),
                sequence: u32::MAX,
            }],
            vec![Output::new(source_value, Script::new(&[]))],
            LockTime::unlocked(),
        ));
        let source_outpoint = OutPoint {
            hash: source_transaction.hash(),
            index: 0,
        };
        let first_timestamp = 1_700_000_000;
        let first_block = store_test_block(
            &zebra_db,
            Height(1),
            first_timestamp,
            block::Hash([0; 32]),
            vec![source_transaction],
        );

        let coinbase = Arc::new(Transaction::test_v1(
            vec![Input::Coinbase {
                height: Height(2),
                data: Vec::new(),
                sequence: u32::MAX,
            }],
            Vec::new(),
            LockTime::unlocked(),
        ));
        let spending_transaction = Arc::new(Transaction::test_v1(
            vec![Input::PrevOut {
                outpoint: source_outpoint,
                unlock_script: Script::new(&[]),
                sequence: u32::MAX,
            }],
            vec![Output::new(spend_value, Script::new(&[]))],
            LockTime::unlocked(),
        ));
        let second_timestamp = first_timestamp + 75;
        store_test_block(
            &zebra_db,
            Height(2),
            second_timestamp,
            first_block.hash(),
            vec![coinbase, spending_transaction],
        );

        // The test only stores historical transaction data, not the UTXO entry. This reproduces
        // an output that has already been spent and removed from the finalized UTXO set.
        assert!(zebra_db.utxo(&source_outpoint).is_none());

        let (_, flows, (average_block_time, average_block_fee, _)) = zebra_db
            .calculate_all_metrics_combined(Height(2), Height(2), second_timestamp)
            .expect("snapshot metrics should use the historical source transaction");

        assert_eq!(flows.0, u64::from(spend_value));
        assert_eq!(flows.1, u64::from(source_value));
        assert_eq!(average_block_fee, amount(3));
        assert_eq!(average_block_time, 75.0);
    }

    #[test]
    fn realtime_metrics_aggregate_the_replaced_daily_interval_and_daily_retires_realtime() {
        let network = Network::Mainnet;
        let zebra_db = new_ephemeral_zebra_db(&network);
        let first_timestamp = 1_700_000_000;
        let second_timestamp = first_timestamp + 24 * 60 * 60;
        let realtime_timestamp = second_timestamp + 75;

        let first_block = store_test_block(
            &zebra_db,
            Height(0),
            first_timestamp,
            block::Hash([0; 32]),
            Vec::new(),
        );
        let second_block = store_test_block(
            &zebra_db,
            Height(1),
            second_timestamp,
            first_block.hash(),
            vec![test_coinbase(Height(1))],
        );
        store_test_block(
            &zebra_db,
            Height(2),
            realtime_timestamp,
            second_block.hash(),
            vec![test_coinbase(Height(2))],
        );

        let first_date = SnapshotDateKey::from_timestamp(first_timestamp);
        let active_date = SnapshotDateKey::from_timestamp(second_timestamp);
        let first_issuance = ZebraDb::calculate_total_issuance(Height(0), &network, None)
            .expect("issuance should be valid");
        let second_issuance = ZebraDb::calculate_total_issuance(Height(1), &network, None)
            .expect("issuance should be valid");
        let first_daily = SnapshotData {
            block_timestamp: first_timestamp,
            block_height: 0,
            total_issuance: first_issuance.zatoshis() as u64,
            ..test_snapshot()
        };
        let active_daily = SnapshotData {
            block_timestamp: second_timestamp,
            block_height: 1,
            total_issuance: second_issuance.zatoshis() as u64,
            ..test_snapshot()
        };
        let latest_issuance = ZebraDb::calculate_total_issuance(Height(2), &network, None)
            .expect("issuance should be valid");
        let first_totals = SnapshotMetricTotals {
            block_count: 1,
            ..SnapshotMetricTotals::default()
        };
        let active_totals = SnapshotMetricTotals {
            transaction_counts: [0, 1, 0, 0, 0, 0, 0],
            block_count: 2,
            ..SnapshotMetricTotals::default()
        };
        let accumulator = SnapshotAccumulator {
            latest_height: 2,
            latest_timestamp: realtime_timestamp,
            latest_pool_values: test_pool_values(),
            latest_work_difficulty_bits: 1.0f64.to_bits(),
            funded_transparent_address_count: 0,
            total_issuance: latest_issuance.zatoshis() as u64,
            totals: SnapshotMetricTotals {
                transaction_counts: [0, 2, 0, 0, 0, 0, 0],
                block_count: 3,
                ..SnapshotMetricTotals::default()
            },
            daily_header_time_range: SnapshotHeaderTimeRange {
                min_timestamp: second_timestamp,
                max_timestamp: realtime_timestamp,
            },
            realtime_header_time_range: SnapshotHeaderTimeRange {
                min_timestamp: first_timestamp,
                max_timestamp: realtime_timestamp,
            },
            daily_anchor: SnapshotMetricAnchor {
                initialized: true,
                height: 1,
                timestamp: second_timestamp,
                totals: active_totals,
            },
            realtime_anchor: SnapshotMetricAnchor {
                initialized: true,
                height: 0,
                timestamp: first_timestamp,
                totals: first_totals,
            },
        };
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(zebra_db.snapshot_data_by_date_cf(), first_date, first_daily);
        batch.zs_insert(
            zebra_db.snapshot_data_by_date_cf(),
            active_date,
            active_daily,
        );
        batch.zs_insert(
            zebra_db.realtime_snapshot_data_cf(),
            SNAPSHOT_ACCUMULATOR_KEY,
            accumulator,
        );
        zebra_db
            .write_batch(batch)
            .expect("daily snapshots should be written");

        zebra_db
            .store_snapshot_data(Height(2), &network, true)
            .expect("realtime snapshot should be stored");
        let (realtime_key, realtime_data) = zebra_db
            .get_realtime_snapshot()
            .expect("realtime snapshot should be active");
        assert_eq!(realtime_key, active_date);
        assert_eq!(
            zebra_db
                .db
                .zs_get::<_, RealtimeSnapshotAnchorKey, SnapshotData>(
                    zebra_db.realtime_snapshot_data_cf(),
                    &REALTIME_SNAPSHOT_ANCHOR_KEY,
                ),
            Some(active_daily),
            "realtime writes must persist their daily anchor",
        );
        // Realtime replaces the height-1 daily snapshot, so its interval starts after height 0
        // and includes both height-1 and height-2 coinbases.
        assert_eq!(realtime_data.transparent_coinbase_tx_count(), 2);
        assert_eq!(
            zebra_db.recent_snapshot_data(10),
            vec![(first_date, first_daily), (active_date, realtime_data)]
        );

        zebra_db
            .store_snapshot_data(Height(2), &network, false)
            .expect("daily snapshot should be stored");
        let same_date_daily = zebra_db
            .snapshot_data_at_date(active_date)
            .expect("same-date daily replacement should exist");
        assert_eq!(
            same_date_daily.transparent_coinbase_tx_count(),
            2,
            "a restart/due snapshot in the same date must retain the full replaced interval",
        );
        let shifted_accumulator = zebra_db
            .snapshot_accumulator()
            .expect("same-date daily anchors should be persisted");
        assert_eq!(shifted_accumulator.daily_anchor.height, 2);
        assert_eq!(
            shifted_accumulator.realtime_anchor.height, 0,
            "same-date replacement must not advance the preceding realtime anchor",
        );
        assert_eq!(
            zebra_db.db.zs_get::<_, RealtimeSnapshotKey, SnapshotData>(
                zebra_db.realtime_snapshot_data_cf(),
                &REALTIME_SNAPSHOT_KEY,
            ),
            None,
            "daily and realtime changes must be committed atomically",
        );
        assert_eq!(
            zebra_db
                .db
                .zs_get::<_, RealtimeSnapshotAnchorKey, SnapshotData>(
                    zebra_db.realtime_snapshot_data_cf(),
                    &REALTIME_SNAPSHOT_ANCHOR_KEY,
                ),
            None,
            "daily writes must retire realtime anchor metadata atomically",
        );
    }

    #[test]
    fn average_block_time_is_not_negative_when_header_times_go_backwards() {
        let network = Network::Mainnet;
        let zebra_db = new_ephemeral_zebra_db(&network);
        let first_timestamp = 1_700_000_000;
        let first_block = store_test_block(
            &zebra_db,
            Height(1),
            first_timestamp,
            block::Hash([0; 32]),
            vec![test_coinbase(Height(1))],
        );
        let second_timestamp = first_timestamp - 75;
        store_test_block(
            &zebra_db,
            Height(2),
            second_timestamp,
            first_block.hash(),
            vec![test_coinbase(Height(2))],
        );

        let (_, _, (average_block_time, _, _)) = zebra_db
            .calculate_all_metrics_combined(Height(2), Height(2), second_timestamp)
            .expect("snapshot metrics should tolerate non-monotonic header times");

        assert_eq!(average_block_time, 0.0);
    }

    #[test]
    fn transitional_snapshot_defaults_ironwood_metrics_to_zero() {
        let snapshot = test_snapshot();
        // The transitional writer stored the two Sprout directions in reverse order.
        let transitional_layout_snapshot = SnapshotData {
            sprout_inflow: snapshot.sprout_outflow,
            sprout_outflow: snapshot.sprout_inflow,
            ..snapshot
        };
        let bytes = transitional_layout_snapshot.as_bytes();
        let decoded = SnapshotData::from_bytes(&bytes[..TRANSITIONAL_SNAPSHOT_DATA_LEN]);
        let expected = SnapshotData {
            disk_format: SnapshotDiskFormat::Transitional,
            ironwood_tx_count: 0,
            ironwood_inflow: 0,
            ironwood_outflow: 0,
            mining_interval: None,
            ..snapshot
        };

        assert_eq!(decoded, expected);
        assert_eq!(decoded.pool_values.ironwood_amount(), amount(6));
    }

    #[test]
    fn legacy_snapshot_defaults_all_ironwood_data_to_zero() {
        let snapshot = test_snapshot();
        // The legacy writer stored the two Sprout directions in reverse order.
        let legacy_layout_snapshot = SnapshotData {
            sprout_inflow: snapshot.sprout_outflow,
            sprout_outflow: snapshot.sprout_inflow,
            ..snapshot
        };
        let current_bytes = legacy_layout_snapshot.as_bytes();

        // Legacy records have a 40-byte ValueBalance, so remove the trailing Ironwood amount
        // from the 192-byte prefix as well as the 20-byte metrics extension.
        let mut legacy_bytes = Vec::with_capacity(LEGACY_SNAPSHOT_DATA_LEN);
        legacy_bytes.extend_from_slice(&current_bytes[..48]);
        legacy_bytes.extend_from_slice(&current_bytes[56..TRANSITIONAL_SNAPSHOT_DATA_LEN]);
        assert_eq!(legacy_bytes.len(), LEGACY_SNAPSHOT_DATA_LEN);

        let decoded = SnapshotData::from_bytes(legacy_bytes);
        let mut legacy_pool_values = snapshot.pool_values;
        legacy_pool_values
            .set_ironwood_value_balance(ValueBalance::from_ironwood_amount(Amount::zero()));
        let expected = SnapshotData {
            disk_format: SnapshotDiskFormat::Legacy,
            pool_values: legacy_pool_values,
            ironwood_tx_count: 0,
            ironwood_inflow: 0,
            ironwood_outflow: 0,
            mining_interval: None,
            ..snapshot
        };

        assert_eq!(decoded, expected);
    }

    #[test]
    fn snapshot_equality_uses_float_bit_patterns() {
        let snapshot = test_snapshot();

        let first_nan = SnapshotData {
            work_difficulty_bits: 0x7ff8_0000_0000_0001,
            average_block_time_bits: 0x7fc0_0001,
            ..snapshot
        };
        let same_nan = first_nan;
        let different_difficulty_nan = SnapshotData {
            work_difficulty_bits: 0x7ff8_0000_0000_0002,
            ..first_nan
        };
        let different_block_time_nan = SnapshotData {
            average_block_time_bits: 0x7fc0_0002,
            ..first_nan
        };

        assert_eq!(first_nan, first_nan);
        assert_eq!(first_nan, same_nan);
        assert_ne!(first_nan, different_difficulty_nan);
        assert_ne!(first_nan, different_block_time_nan);
        assert_eq!(SnapshotData::from_bytes(first_nan.as_bytes()), first_nan);

        let positive_zero = SnapshotData {
            work_difficulty_bits: 0.0f64.to_bits(),
            average_block_time_bits: 0.0f32.to_bits(),
            ..snapshot
        };
        let negative_difficulty_zero = SnapshotData {
            work_difficulty_bits: (-0.0f64).to_bits(),
            ..positive_zero
        };
        let negative_block_time_zero = SnapshotData {
            average_block_time_bits: (-0.0f32).to_bits(),
            ..positive_zero
        };

        assert_ne!(positive_zero, negative_difficulty_zero);
        assert_ne!(positive_zero, negative_block_time_zero);
    }
}
