//! Incremental, finalized-chain facts for post-deshield transparent outputs.
//!
//! This index deliberately records only observable chain facts. It does not infer ownership,
//! exchange destinations, or multi-hop value flow. A `shielding_observed` output means only that
//! its first spending transaction contains an observable shielded-pool credit.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    ops::RangeInclusive,
    str::FromStr,
};

use zebra_chain::{
    amount::{Amount, NonNegative},
    block::{self, Height},
    transparent,
};

use crate::{
    request::FinalizedBlock,
    service::{
        check::difficulty::POW_MEDIAN_BLOCK_SPAN,
        finalized_state::{
            disk_db::DiskWriteBatch, disk_format::OutputLocation, zebra_db::ZebraDb,
        },
    },
    FromDisk, IntoDisk,
};

use super::{
    super::TypedColumnFamily, snapshot::SnapshotDateKey,
    transaction_facts::FinalizedTransactionFacts,
};

/// The classification contract implemented by this index and exposed over RPC.
pub const TURNSTILE_CLASSIFICATION_VERSION: u32 = 2;

/// Persistent records for eligible transparent outputs, keyed by their chain location.
pub const TURNSTILE_OUTPUTS: &str = "turnstile_outputs";

/// Daily aggregates, keyed by UTC creation date and source pool.
pub const TURNSTILE_COHORTS: &str = "turnstile_cohorts";

const TURNSTILE_OUTPUT_DISK_LEN: usize = 1 + 1 + 8 + 8;
const TURNSTILE_VALUE_DISK_LEN: usize = 16;
const TURNSTILE_COHORT_DISK_LEN: usize = TURNSTILE_VALUE_DISK_LEN * 9;
const TURNSTILE_SOURCE_POOL_COUNT: usize = 5;
const TURNSTILE_ACCUMULATOR_DISK_LEN: usize = 4
    + 4
    + 8
    + 8
    + 8
    + 1
    + POW_MEDIAN_BLOCK_SPAN * 8
    + TURNSTILE_SOURCE_POOL_COUNT * TURNSTILE_VALUE_DISK_LEN * 3;

const SPROUT_CREDIT_BIT: u8 = 1 << 0;
const SAPLING_CREDIT_BIT: u8 = 1 << 1;
const ORCHARD_CREDIT_BIT: u8 = 1 << 2;
const IRONWOOD_CREDIT_BIT: u8 = 1 << 3;

const ONE_DAY_SECONDS: i64 = 24 * 60 * 60;
const COHORT_WINDOWS_DAYS: [i64; 3] = [1, 7, 30];

/// A shielded-pool source classification for an eligible deshield-origin transaction.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TurnstileSourcePool {
    /// Sprout is the only observable net-debited shielded pool.
    Sprout,
    /// Sapling is the only observable net-debited shielded pool.
    Sapling,
    /// Orchard is the only observable net-debited shielded pool.
    Orchard,
    /// Ironwood is the only observable net-debited shielded pool.
    Ironwood,
    /// More than one shielded pool has an observable net debit.
    Mixed,
}

impl TurnstileSourcePool {
    /// All source variants in stable disk and response order.
    pub const ALL: [Self; TURNSTILE_SOURCE_POOL_COUNT] = [
        Self::Sprout,
        Self::Sapling,
        Self::Orchard,
        Self::Ironwood,
        Self::Mixed,
    ];

    /// Returns the stable lowercase RPC representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sprout => "sprout",
            Self::Sapling => "sapling",
            Self::Orchard => "orchard",
            Self::Ironwood => "ironwood",
            Self::Mixed => "mixed",
        }
    }

    const fn disk_byte(self) -> u8 {
        match self {
            Self::Sprout => 0,
            Self::Sapling => 1,
            Self::Orchard => 2,
            Self::Ironwood => 3,
            Self::Mixed => 4,
        }
    }

    fn from_disk_byte(byte: u8) -> Self {
        match byte {
            0 => Self::Sprout,
            1 => Self::Sapling,
            2 => Self::Orchard,
            3 => Self::Ironwood,
            4 => Self::Mixed,
            _ => panic!("invalid turnstile source-pool byte {byte}"),
        }
    }

    fn index(self) -> usize {
        usize::from(self.disk_byte())
    }
}

impl fmt::Display for TurnstileSourcePool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TurnstileSourcePool {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|source| source.as_str() == value)
            .ok_or_else(|| {
                format!(
                    "unknown source_pool {value:?}; expected sprout, sapling, orchard, ironwood, or mixed"
                )
            })
    }
}

/// A count and the corresponding sum of original eligible output values.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TurnstileValue {
    /// Number of eligible outputs.
    pub count: u64,
    /// Sum of the original eligible output values, in zatoshis.
    pub value_zat: u64,
}

impl TurnstileValue {
    fn checked_add_output(&mut self, value_zat: u64) -> Result<(), TurnstileIndexError> {
        self.count = self
            .count
            .checked_add(1)
            .ok_or(TurnstileIndexError::Arithmetic("output count"))?;
        self.value_zat = self
            .value_zat
            .checked_add(value_zat)
            .ok_or(TurnstileIndexError::Arithmetic("output value"))?;
        Ok(())
    }

    fn checked_sub(self, other: Self) -> Result<Self, TurnstileIndexError> {
        Ok(Self {
            count: self
                .count
                .checked_sub(other.count)
                .ok_or(TurnstileIndexError::Arithmetic("unspent output count"))?,
            value_zat: self
                .value_zat
                .checked_sub(other.value_zat)
                .ok_or(TurnstileIndexError::Arithmetic("unspent output value"))?,
        })
    }

    fn append_bytes(self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.count.to_be_bytes());
        bytes.extend_from_slice(&self.value_zat.to_be_bytes());
    }

    fn take_bytes(bytes: &[u8], offset: &mut usize) -> Self {
        Self {
            count: u64::from_be_bytes(take_bytes(bytes, offset)),
            value_zat: u64::from_be_bytes(take_bytes(bytes, offset)),
        }
    }
}

/// Current finalized totals for one source-pool classification.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TurnstileStats {
    /// Source classification represented by these totals.
    pub source_pool: TurnstileSourcePool,
    /// All eligible outputs created through the finalized height.
    pub eligible: TurnstileValue,
    /// Eligible outputs first-spent through the finalized height.
    pub spent: TurnstileValue,
    /// Eligible outputs which remain unspent at the finalized height.
    pub unspent: TurnstileValue,
    /// Spent outputs whose first-spend transaction contains a shielded-pool credit.
    pub shielding_observed: TurnstileValue,
}

/// First-spend facts inside a fixed age window.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TurnstileWindow {
    /// Outputs first-spent within the age window.
    pub spent: TurnstileValue,
    /// Outputs first-spent within the window with shielding observed in that transaction.
    pub shielding_observed: TurnstileValue,
}

/// One daily, per-source cohort record returned to the RPC layer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TurnstileCohort {
    /// UTC creation-date key in the same format as dashboard snapshots.
    pub date_key: SnapshotDateKey,
    /// Source-pool classification.
    pub source_pool: TurnstileSourcePool,
    /// Outputs created in this cohort.
    pub eligible: TurnstileValue,
    /// Outputs in this cohort which have been first-spent.
    pub spent: TurnstileValue,
    /// First-spent outputs whose spending transaction contains a shielded credit.
    pub shielding_observed: TurnstileValue,
    /// One-day facts, present only after the complete UTC cohort has matured.
    pub within_1d: Option<TurnstileWindow>,
    /// Seven-day facts, present only after the complete UTC cohort has matured.
    pub within_7d: Option<TurnstileWindow>,
    /// Thirty-day facts, present only after the complete UTC cohort has matured.
    pub within_30d: Option<TurnstileWindow>,
}

/// An atomically anchored Turnstile query result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TurnstileData {
    /// Classification contract used to produce all records.
    pub classification_version: u32,
    /// Finalized height represented by the aggregates.
    pub as_of_height: Height,
    /// Canonical finalized hash at `as_of_height`.
    pub as_of_hash: block::Hash,
    /// Header timestamp of `as_of_height`.
    pub as_of_timestamp: i64,
    /// Median-time-past for the next block, used exclusively for cohort maturity.
    pub maturity_timestamp: i64,
    /// Genesis chain timestamp defining complete indexed coverage, including zero days.
    pub coverage_start_timestamp: i64,
    /// All-time current totals, independent of the cohort date range.
    pub summaries: Vec<TurnstileStats>,
    /// Bounded cohort page, sorted by date and then source pool.
    pub cohorts: Vec<TurnstileCohort>,
    /// First wholly unreturned UTC date, if more cohort records are available.
    pub next_start_date: Option<SnapshotDateKey>,
}

/// Errors which prevent the Turnstile index from advancing with a finalized block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TurnstileIndexError {
    /// A non-genesis database is missing mandatory Turnstile state.
    #[error(
        "turnstile accumulator is missing while committing non-genesis height {0:?}; resync the v32 database"
    )]
    MissingAtNonGenesis(Height),

    /// The index tip does not directly precede the block being committed.
    #[error(
        "turnstile accumulator is not sequential: latest height {latest:?}, attempted height {attempted:?}"
    )]
    NonSequential {
        /// Current indexed height.
        latest: Height,
        /// Attempted finalized height.
        attempted: Height,
    },

    /// A checked arithmetic operation failed.
    #[error("turnstile arithmetic overflow or underflow calculating {0}")]
    Arithmetic(&'static str),

    /// A chain-time invariant was violated by finalized input data.
    #[error(
        "turnstile median-time-past regressed from creation time {created_timestamp} to spend time {spent_timestamp}"
    )]
    NonMonotonicChainTime {
        /// Median-time-past when the eligible output was created.
        created_timestamp: i64,
        /// Median-time-past when the eligible output was spent.
        spent_timestamp: i64,
    },

    /// A transparent input was missing its already-resolved output location.
    #[error("turnstile index is missing output location for spent outpoint {0:?}")]
    MissingOutputLocation(transparent::OutPoint),

    /// The accumulator anchor is not present in the same finalized RocksDB snapshot.
    #[error("turnstile finalized anchor hash is missing at height {0:?}")]
    MissingAnchorHash(Height),

    /// A consensus amount could not be represented by this non-negative index.
    #[error("turnstile index found invalid {metric}: {reason}")]
    InvalidValue {
        /// Metric being converted.
        metric: &'static str,
        /// Conversion error.
        reason: String,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct TurnstileOutput {
    source_pool: TurnstileSourcePool,
    value_zat: u64,
    created_timestamp: i64,
}

impl TurnstileOutput {
    fn new(source_pool: TurnstileSourcePool, value_zat: u64, created_timestamp: i64) -> Self {
        Self {
            source_pool,
            value_zat,
            created_timestamp,
        }
    }
}

impl IntoDisk for TurnstileOutput {
    type Bytes = [u8; TURNSTILE_OUTPUT_DISK_LEN];

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; TURNSTILE_OUTPUT_DISK_LEN];
        bytes[0] = u8::try_from(TURNSTILE_CLASSIFICATION_VERSION)
            .expect("classification version fits in one output-record byte");
        bytes[1] = self.source_pool.disk_byte();
        bytes[2..10].copy_from_slice(&self.value_zat.to_be_bytes());
        bytes[10..18].copy_from_slice(&self.created_timestamp.to_be_bytes());
        bytes
    }
}

impl FromDisk for TurnstileOutput {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        assert_eq!(bytes.len(), TURNSTILE_OUTPUT_DISK_LEN);
        assert_eq!(u32::from(bytes[0]), TURNSTILE_CLASSIFICATION_VERSION);
        Self {
            source_pool: TurnstileSourcePool::from_disk_byte(bytes[1]),
            value_zat: u64::from_be_bytes(
                bytes[2..10]
                    .try_into()
                    .expect("turnstile output value has eight bytes"),
            ),
            created_timestamp: i64::from_be_bytes(
                bytes[10..18]
                    .try_into()
                    .expect("turnstile creation timestamp has eight bytes"),
            ),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TurnstileCohortKey {
    date_key: SnapshotDateKey,
    source_pool: TurnstileSourcePool,
}

impl TurnstileCohortKey {
    const fn new(date_key: SnapshotDateKey, source_pool: TurnstileSourcePool) -> Self {
        Self {
            date_key,
            source_pool,
        }
    }
}

impl IntoDisk for TurnstileCohortKey {
    type Bytes = [u8; 4];

    fn as_bytes(&self) -> Self::Bytes {
        [
            self.date_key.year,
            self.date_key.month,
            self.date_key.day,
            self.source_pool.disk_byte(),
        ]
    }
}

impl FromDisk for TurnstileCohortKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        assert_eq!(bytes.len(), 4);
        Self {
            date_key: SnapshotDateKey::new(bytes[0], bytes[1], bytes[2]),
            source_pool: TurnstileSourcePool::from_disk_byte(bytes[3]),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct TurnstileCohortAggregate {
    eligible: TurnstileValue,
    spent: TurnstileValue,
    shielding_observed: TurnstileValue,
    spent_within: [TurnstileValue; 3],
    shielding_observed_within: [TurnstileValue; 3],
}

impl TurnstileCohortAggregate {
    fn record_eligible(&mut self, value_zat: u64) -> Result<(), TurnstileIndexError> {
        self.eligible.checked_add_output(value_zat)
    }

    fn record_spend(
        &mut self,
        output: TurnstileOutput,
        spent_timestamp: i64,
        shielding_observed: bool,
    ) -> Result<(), TurnstileIndexError> {
        // Median-time-past is monotonic on a valid chain, so a backwards endpoint is an index
        // invariant violation rather than a zero-age spend.
        let elapsed = spent_timestamp
            .checked_sub(output.created_timestamp)
            .ok_or(TurnstileIndexError::NonMonotonicChainTime {
                created_timestamp: output.created_timestamp,
                spent_timestamp,
            })?;
        if elapsed < 0 {
            return Err(TurnstileIndexError::NonMonotonicChainTime {
                created_timestamp: output.created_timestamp,
                spent_timestamp,
            });
        }

        self.spent.checked_add_output(output.value_zat)?;
        if shielding_observed {
            self.shielding_observed
                .checked_add_output(output.value_zat)?;
        }
        for (window_index, days) in COHORT_WINDOWS_DAYS.into_iter().enumerate() {
            let window_seconds = days
                .checked_mul(ONE_DAY_SECONDS)
                .ok_or(TurnstileIndexError::Arithmetic("cohort window seconds"))?;
            if elapsed <= window_seconds {
                self.spent_within[window_index].checked_add_output(output.value_zat)?;
                if shielding_observed {
                    self.shielding_observed_within[window_index]
                        .checked_add_output(output.value_zat)?;
                }
            }
        }
        Ok(())
    }

    fn into_public(self, key: TurnstileCohortKey, as_of_timestamp: i64) -> TurnstileCohort {
        let window = |index: usize| {
            cohort_is_mature(key.date_key, COHORT_WINDOWS_DAYS[index], as_of_timestamp).then_some(
                TurnstileWindow {
                    spent: self.spent_within[index],
                    shielding_observed: self.shielding_observed_within[index],
                },
            )
        };

        TurnstileCohort {
            date_key: key.date_key,
            source_pool: key.source_pool,
            eligible: self.eligible,
            spent: self.spent,
            shielding_observed: self.shielding_observed,
            within_1d: window(0),
            within_7d: window(1),
            within_30d: window(2),
        }
    }
}

impl IntoDisk for TurnstileCohortAggregate {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = Vec::with_capacity(TURNSTILE_COHORT_DISK_LEN);
        self.eligible.append_bytes(&mut bytes);
        self.spent.append_bytes(&mut bytes);
        self.shielding_observed.append_bytes(&mut bytes);
        for value in self.spent_within {
            value.append_bytes(&mut bytes);
        }
        for value in self.shielding_observed_within {
            value.append_bytes(&mut bytes);
        }
        debug_assert_eq!(bytes.len(), TURNSTILE_COHORT_DISK_LEN);
        bytes
    }
}

impl FromDisk for TurnstileCohortAggregate {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        assert_eq!(bytes.len(), TURNSTILE_COHORT_DISK_LEN);
        let mut offset = 0;
        let eligible = TurnstileValue::take_bytes(bytes, &mut offset);
        let spent = TurnstileValue::take_bytes(bytes, &mut offset);
        let shielding_observed = TurnstileValue::take_bytes(bytes, &mut offset);
        let mut spent_within = [TurnstileValue::default(); 3];
        let mut shielding_observed_within = [TurnstileValue::default(); 3];
        for value in &mut spent_within {
            *value = TurnstileValue::take_bytes(bytes, &mut offset);
        }
        for value in &mut shielding_observed_within {
            *value = TurnstileValue::take_bytes(bytes, &mut offset);
        }
        assert_eq!(offset, TURNSTILE_COHORT_DISK_LEN);
        Self {
            eligible,
            spent,
            shielding_observed,
            spent_within,
            shielding_observed_within,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct TurnstileSummaryAccumulator {
    eligible: TurnstileValue,
    spent: TurnstileValue,
    shielding_observed: TurnstileValue,
}

impl TurnstileSummaryAccumulator {
    fn into_public(
        self,
        source_pool: TurnstileSourcePool,
    ) -> Result<TurnstileStats, TurnstileIndexError> {
        Ok(TurnstileStats {
            source_pool,
            eligible: self.eligible,
            spent: self.spent,
            unspent: self.eligible.checked_sub(self.spent)?,
            shielding_observed: self.shielding_observed,
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct TurnstileAccumulator {
    latest_height: Height,
    latest_timestamp: i64,
    maturity_timestamp: i64,
    coverage_start_timestamp: i64,
    recent_header_timestamp_count: u8,
    recent_header_timestamps: [i64; POW_MEDIAN_BLOCK_SPAN],
    summaries: [TurnstileSummaryAccumulator; TURNSTILE_SOURCE_POOL_COUNT],
}

impl TurnstileAccumulator {
    fn at_genesis(timestamp: i64) -> Self {
        Self {
            latest_height: Height::MIN,
            latest_timestamp: timestamp,
            maturity_timestamp: timestamp,
            coverage_start_timestamp: timestamp,
            recent_header_timestamp_count: 0,
            recent_header_timestamps: [0; POW_MEDIAN_BLOCK_SPAN],
            summaries: [TurnstileSummaryAccumulator::default(); TURNSTILE_SOURCE_POOL_COUNT],
        }
    }

    /// Returns the consensus median-time-past for the next block.
    fn next_block_median_time_past(&self) -> Option<i64> {
        let count = usize::from(self.recent_header_timestamp_count);
        if count == 0 {
            return None;
        }

        let mut timestamps = self.recent_header_timestamps;
        timestamps[..count].sort_unstable();
        Some(timestamps[count / 2])
    }

    fn record_eligible(
        &mut self,
        source_pool: TurnstileSourcePool,
        value_zat: u64,
    ) -> Result<(), TurnstileIndexError> {
        self.summaries[source_pool.index()]
            .eligible
            .checked_add_output(value_zat)
    }

    fn record_spend(
        &mut self,
        output: TurnstileOutput,
        shielding_observed: bool,
    ) -> Result<(), TurnstileIndexError> {
        let summary = &mut self.summaries[output.source_pool.index()];
        summary.spent.checked_add_output(output.value_zat)?;
        if shielding_observed {
            summary
                .shielding_observed
                .checked_add_output(output.value_zat)?;
        }
        Ok(())
    }

    fn advance_tip(&mut self, height: Height, timestamp: i64) -> Result<(), TurnstileIndexError> {
        let count = usize::from(self.recent_header_timestamp_count);
        if count < POW_MEDIAN_BLOCK_SPAN {
            self.recent_header_timestamps[count] = timestamp;
            self.recent_header_timestamp_count = self
                .recent_header_timestamp_count
                .checked_add(1)
                .expect("the median-time-past window has at most eleven entries");
        } else {
            self.recent_header_timestamps.copy_within(1.., 0);
            self.recent_header_timestamps[POW_MEDIAN_BLOCK_SPAN - 1] = timestamp;
        }

        let next_median_time = self
            .next_block_median_time_past()
            .expect("advancing the tip inserts one median-time-past entry");
        if next_median_time < self.maturity_timestamp {
            return Err(TurnstileIndexError::NonMonotonicChainTime {
                created_timestamp: self.maturity_timestamp,
                spent_timestamp: next_median_time,
            });
        }

        self.latest_height = height;
        self.latest_timestamp = timestamp;
        self.maturity_timestamp = next_median_time;
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct TurnstileAccumulatorKey;

impl IntoDisk for TurnstileAccumulatorKey {
    type Bytes = [u8; 1];

    fn as_bytes(&self) -> Self::Bytes {
        [0]
    }
}

impl FromDisk for TurnstileAccumulatorKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        assert_eq!(bytes.as_ref(), [0]);
        Self
    }
}

const TURNSTILE_ACCUMULATOR_KEY: TurnstileAccumulatorKey = TurnstileAccumulatorKey;

impl IntoDisk for TurnstileAccumulator {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = Vec::with_capacity(TURNSTILE_ACCUMULATOR_DISK_LEN);
        bytes.extend_from_slice(&TURNSTILE_CLASSIFICATION_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.latest_height.0.to_be_bytes());
        bytes.extend_from_slice(&self.latest_timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.maturity_timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.coverage_start_timestamp.to_be_bytes());
        bytes.push(self.recent_header_timestamp_count);
        for timestamp in self.recent_header_timestamps {
            bytes.extend_from_slice(&timestamp.to_be_bytes());
        }
        for summary in self.summaries {
            summary.eligible.append_bytes(&mut bytes);
            summary.spent.append_bytes(&mut bytes);
            summary.shielding_observed.append_bytes(&mut bytes);
        }
        debug_assert_eq!(bytes.len(), TURNSTILE_ACCUMULATOR_DISK_LEN);
        bytes
    }
}

impl FromDisk for TurnstileAccumulator {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        assert_eq!(bytes.len(), TURNSTILE_ACCUMULATOR_DISK_LEN);
        let mut offset = 0;
        let version = u32::from_be_bytes(take_bytes(bytes, &mut offset));
        assert_eq!(version, TURNSTILE_CLASSIFICATION_VERSION);
        let latest_height = Height(u32::from_be_bytes(take_bytes(bytes, &mut offset)));
        let latest_timestamp = i64::from_be_bytes(take_bytes(bytes, &mut offset));
        let maturity_timestamp = i64::from_be_bytes(take_bytes(bytes, &mut offset));
        let coverage_start_timestamp = i64::from_be_bytes(take_bytes(bytes, &mut offset));
        let recent_header_timestamp_count = bytes[offset];
        offset += 1;
        assert!(usize::from(recent_header_timestamp_count) <= POW_MEDIAN_BLOCK_SPAN);
        let mut recent_header_timestamps = [0; POW_MEDIAN_BLOCK_SPAN];
        for timestamp in &mut recent_header_timestamps {
            *timestamp = i64::from_be_bytes(take_bytes(bytes, &mut offset));
        }
        let mut summaries = [TurnstileSummaryAccumulator::default(); TURNSTILE_SOURCE_POOL_COUNT];
        for summary in &mut summaries {
            summary.eligible = TurnstileValue::take_bytes(bytes, &mut offset);
            summary.spent = TurnstileValue::take_bytes(bytes, &mut offset);
            summary.shielding_observed = TurnstileValue::take_bytes(bytes, &mut offset);
        }
        assert_eq!(offset, TURNSTILE_ACCUMULATOR_DISK_LEN);
        Self {
            latest_height,
            latest_timestamp,
            maturity_timestamp,
            coverage_start_timestamp,
            recent_header_timestamp_count,
            recent_header_timestamps,
            summaries,
        }
    }
}

type TurnstileOutputCf<'cf> = TypedColumnFamily<'cf, OutputLocation, TurnstileOutput>;
type TurnstileCohortCf<'cf> = TypedColumnFamily<'cf, TurnstileCohortKey, TurnstileCohortAggregate>;
type TurnstileAccumulatorCf<'cf> =
    TypedColumnFamily<'cf, TurnstileAccumulatorKey, TurnstileAccumulator>;

impl ZebraDb {
    fn turnstile_output_cf(&self) -> TurnstileOutputCf<'_> {
        TurnstileOutputCf::new(&self.db, TURNSTILE_OUTPUTS)
            .expect("turnstile output column family must be registered")
    }

    fn turnstile_cohort_cf(&self) -> TurnstileCohortCf<'_> {
        TurnstileCohortCf::new(&self.db, TURNSTILE_COHORTS)
            .expect("turnstile cohort column family must be registered")
    }

    fn turnstile_accumulator_cf(&self) -> TurnstileAccumulatorCf<'_> {
        TurnstileAccumulatorCf::new(&self.db, TURNSTILE_COHORTS)
            .expect("turnstile cohort column family must be registered")
    }

    fn turnstile_accumulator(&self) -> Option<TurnstileAccumulator> {
        self.turnstile_accumulator_cf()
            .zs_get(&TURNSTILE_ACCUMULATOR_KEY)
    }

    /// Returns an atomically finalized and bounded Turnstile cohort page.
    pub(crate) fn turnstile_data(
        &self,
        start_date: Option<SnapshotDateKey>,
        end_date: Option<SnapshotDateKey>,
        limit: usize,
        source_pool: Option<TurnstileSourcePool>,
    ) -> Result<Option<TurnstileData>, TurnstileIndexError> {
        let cohort_cf = self
            .db
            .cf_handle(TURNSTILE_COHORTS)
            .expect("turnstile cohort column family must be registered");
        let hash_by_height = self
            .db
            .cf_handle("hash_by_height")
            .expect("hash-by-height column family must be registered");
        let snapshot = self.db.snapshot();
        let Some(accumulator): Option<TurnstileAccumulator> =
            snapshot.zs_get(&cohort_cf, &TURNSTILE_ACCUMULATOR_KEY)
        else {
            return Ok(None);
        };
        let as_of_hash: block::Hash = snapshot
            .zs_get(&hash_by_height, &accumulator.latest_height)
            .ok_or(TurnstileIndexError::MissingAnchorHash(
                accumulator.latest_height,
            ))?;

        let summaries = TurnstileSourcePool::ALL
            .into_iter()
            .filter(|source| source_pool.is_none_or(|selected| selected == *source))
            .map(|source| accumulator.summaries[source.index()].into_public(source))
            .collect::<Result<Vec<_>, _>>()?;

        let start_date = start_date.unwrap_or_else(|| SnapshotDateKey::new(0, 1, 1));
        let end_date = end_date.unwrap_or_else(|| SnapshotDateKey::new(99, 12, 31));
        let range: RangeInclusive<TurnstileCohortKey> =
            TurnstileCohortKey::new(start_date, TurnstileSourcePool::Sprout)
                ..=TurnstileCohortKey::new(end_date, TurnstileSourcePool::Mixed);

        // Keep each UTC date whole so a date-only continuation cursor cannot duplicate or skip a
        // source row. The response can therefore exceed a small requested limit by at most four.
        let mut cohorts = Vec::new();
        let mut next_start_date = None;
        let mut last_returned_date = None;
        for (key, aggregate) in snapshot
            .zs_forward_range_iter::<_, TurnstileCohortKey, TurnstileCohortAggregate, _>(
                &cohort_cf, range,
            )
        {
            if source_pool.is_some_and(|selected| selected != key.source_pool) {
                continue;
            }
            if cohorts.len() >= limit && last_returned_date != Some(key.date_key) {
                next_start_date = Some(key.date_key);
                break;
            }
            cohorts.push(aggregate.into_public(key, accumulator.maturity_timestamp));
            last_returned_date = Some(key.date_key);
        }

        Ok(Some(TurnstileData {
            classification_version: TURNSTILE_CLASSIFICATION_VERSION,
            as_of_height: accumulator.latest_height,
            as_of_hash,
            as_of_timestamp: accumulator.latest_timestamp,
            maturity_timestamp: accumulator.maturity_timestamp,
            coverage_start_timestamp: accumulator.coverage_start_timestamp,
            summaries,
            cohorts,
            next_start_date,
        }))
    }
}

impl DiskWriteBatch {
    /// Advances all Turnstile facts using one finalized block and adds the writes to this batch.
    ///
    /// Reads and writes are O(the block's transparent inputs and eligible outputs). Every cohort
    /// key is loaded at most once and written once, including same-block create/spend chains.
    pub(super) fn prepare_turnstile_batch(
        &mut self,
        zebra_db: &ZebraDb,
        finalized: &FinalizedBlock,
        transaction_facts: &[FinalizedTransactionFacts],
        out_loc_by_outpoint: &HashMap<transparent::OutPoint, OutputLocation>,
    ) -> Result<(), TurnstileIndexError> {
        let prepare_start = std::time::Instant::now();
        let height = finalized.height;
        let timestamp = finalized.block.header.time.timestamp();
        let mut accumulator = match zebra_db.turnstile_accumulator() {
            Some(accumulator) => {
                if accumulator.latest_height.0.checked_add(1) != Some(height.0) {
                    return Err(TurnstileIndexError::NonSequential {
                        latest: accumulator.latest_height,
                        attempted: height,
                    });
                }
                accumulator
            }
            None if height.is_min() => TurnstileAccumulator::at_genesis(timestamp),
            None => return Err(TurnstileIndexError::MissingAtNonGenesis(height)),
        };
        // Every fact in this block is observed at MedianTime(height): the median of the preceding
        // eleven header times (or all preceding headers near genesis). Genesis has no MTP, so its
        // own header time is the only useful coverage anchor.
        let chain_timestamp = accumulator
            .next_block_median_time_past()
            .unwrap_or(timestamp);

        // A negative point lookup per ordinary transparent input materially slows historical
        // sync, since almost all spent outputs are unrelated to this index. Resolve all marker
        // candidates from earlier blocks in one RocksDB multi-get. Same-block candidates are
        // deliberately excluded: they are found in `output_updates` in transaction order below.
        let mut prior_output_locations = Vec::new();
        for facts in transaction_facts {
            for outpoint in &facts.transparent_input_outpoints {
                let output_location = *out_loc_by_outpoint
                    .get(outpoint)
                    .ok_or(TurnstileIndexError::MissingOutputLocation(*outpoint))?;
                if output_location.height() < height {
                    prior_output_locations.push(output_location);
                }
            }
        }
        prior_output_locations.sort_unstable();
        prior_output_locations.dedup();
        let marker_lookup_count = u64::try_from(prior_output_locations.len())
            .map_err(|_| TurnstileIndexError::Arithmetic("per-block marker lookup count"))?;
        let prefetched_outputs: HashMap<_, _> = prior_output_locations
            .iter()
            .copied()
            .zip(
                zebra_db
                    .turnstile_output_cf()
                    .zs_multi_get(&prior_output_locations, true),
            )
            .filter_map(|(location, output)| output.map(|output| (location, output)))
            .collect();

        // Compute each transaction's Turnstile classification once. Besides avoiding duplicate
        // Sprout aggregation, this lets us know every cohort key the block can update before any
        // transaction-order-dependent output processing begins.
        let mut transaction_classifications = Vec::with_capacity(transaction_facts.len());
        for facts in transaction_facts {
            let (sprout_credit, sprout_debit) = sprout_gross_values(facts)?;
            let credit_bits = shielded_credit_bits_from_facts(
                sprout_credit,
                facts.sapling_value_balance_zat,
                facts.orchard_value_balance_zat,
                facts.ironwood_value_balance_zat,
            );
            let source_pool = eligible_source_from_facts(
                facts.is_coinbase,
                facts.has_transparent_inputs,
                facts.has_transparent_outputs(),
                sprout_credit,
                sprout_debit,
                facts.sapling_value_balance_zat,
                facts.orchard_value_balance_zat,
                facts.ironwood_value_balance_zat,
            );
            transaction_classifications.push((credit_bits, source_pool));
        }

        // A block can touch many old creation cohorts. Fetch them in one sorted RocksDB operation
        // rather than issuing a synchronous point read from the finalized commit loop for every
        // distinct cohort. Same-block creations use the common current MTP date and are included
        // before transaction processing so their later same-block spends share the same aggregate.
        let mut cohort_keys: BTreeSet<_> = prefetched_outputs
            .values()
            .map(|output| {
                TurnstileCohortKey::new(
                    SnapshotDateKey::from_timestamp(output.created_timestamp),
                    output.source_pool,
                )
            })
            .collect();
        let creation_date = SnapshotDateKey::from_timestamp(chain_timestamp);
        for (facts, (_credit_bits, source_pool)) in
            transaction_facts.iter().zip(&transaction_classifications)
        {
            if facts
                .transparent_output_values_zat
                .iter()
                .any(|value| *value > 0)
            {
                if let Some(source_pool) = *source_pool {
                    cohort_keys.insert(TurnstileCohortKey::new(creation_date, source_pool));
                }
            }
        }
        let cohort_keys: Vec<_> = cohort_keys.into_iter().collect();
        let prefetched_cohorts: HashMap<_, _> = cohort_keys
            .iter()
            .copied()
            .zip(
                zebra_db
                    .turnstile_cohort_cf()
                    .zs_multi_get(&cohort_keys, true),
            )
            .map(|(key, aggregate)| (key, aggregate.unwrap_or_default()))
            .collect();

        // Pending RocksDB batch writes are not visible to reads. These maps therefore coalesce all
        // changes to one output or cohort key across the complete block before writing each once.
        let mut output_updates = HashMap::<OutputLocation, TurnstileOutput>::new();
        let mut spent_output_locations = HashSet::<OutputLocation>::new();
        let mut cohort_updates = BTreeMap::<TurnstileCohortKey, TurnstileCohortAggregate>::new();
        let mut eligible_output_count = 0u64;

        for (transaction_index, facts) in transaction_facts.iter().enumerate() {
            let (credit_bits, source_pool) = transaction_classifications[transaction_index];
            for outpoint in &facts.transparent_input_outpoints {
                let output_location = out_loc_by_outpoint
                    .get(outpoint)
                    .ok_or(TurnstileIndexError::MissingOutputLocation(*outpoint))?;
                let output = output_updates
                    .get(output_location)
                    .copied()
                    .or_else(|| prefetched_outputs.get(output_location).copied());
                let Some(output) = output else {
                    continue;
                };
                if !spent_output_locations.insert(*output_location) {
                    // Consensus prevents duplicate spends, but this guard avoids double-counting
                    // if malformed finalized input is ever passed to the index.
                    continue;
                }

                let shielding_observed = credit_bits != 0;
                let cohort_key = TurnstileCohortKey::new(
                    SnapshotDateKey::from_timestamp(output.created_timestamp),
                    output.source_pool,
                );
                let cohort =
                    cohort_for_update(&mut cohort_updates, &prefetched_cohorts, cohort_key);
                cohort.record_spend(output, chain_timestamp, shielding_observed)?;
                accumulator.record_spend(output, shielding_observed)?;
                // Aggregate the first-spend facts once, then remove the marker. Public queries use
                // cohorts and summaries, so retaining spent output-level records only grows disk.
                output_updates.remove(output_location);
            }

            let Some(source_pool) = source_pool else {
                continue;
            };
            let cohort_key = TurnstileCohortKey::new(
                SnapshotDateKey::from_timestamp(chain_timestamp),
                source_pool,
            );
            for (output_index, value_zat) in facts
                .transparent_output_values_zat
                .iter()
                .copied()
                .enumerate()
            {
                let value_zat = u64::try_from(value_zat).map_err(|error| {
                    TurnstileIndexError::InvalidValue {
                        metric: "eligible transparent output",
                        reason: error.to_string(),
                    }
                })?;
                if value_zat == 0 {
                    continue;
                }

                let output_location =
                    OutputLocation::from_usize(height, transaction_index, output_index);
                let indexed_output = TurnstileOutput::new(source_pool, value_zat, chain_timestamp);
                output_updates.insert(output_location, indexed_output);
                cohort_for_update(&mut cohort_updates, &prefetched_cohorts, cohort_key)
                    .record_eligible(value_zat)?;
                accumulator.record_eligible(source_pool, value_zat)?;
                eligible_output_count =
                    eligible_output_count
                        .checked_add(1)
                        .ok_or(TurnstileIndexError::Arithmetic(
                            "per-block eligible output count",
                        ))?;
            }
        }

        accumulator.advance_tip(height, timestamp)?;

        for (location, output) in output_updates {
            let _ = zebra_db
                .turnstile_output_cf()
                .with_batch_for_writing(self)
                .zs_insert(&location, &output);
        }
        for location in spent_output_locations {
            let _ = zebra_db
                .turnstile_output_cf()
                .with_batch_for_writing(self)
                .zs_delete(&location);
        }
        for (key, aggregate) in cohort_updates {
            let _ = zebra_db
                .turnstile_cohort_cf()
                .with_batch_for_writing(self)
                .zs_insert(&key, &aggregate);
        }
        let _ = zebra_db
            .turnstile_accumulator_cf()
            .with_batch_for_writing(self)
            .zs_insert(&TURNSTILE_ACCUMULATOR_KEY, &accumulator);

        metrics::counter!("state.finalized.cumulative.turnstile.marker_lookups")
            .increment(marker_lookup_count);
        metrics::counter!("state.finalized.cumulative.turnstile.eligible_outputs")
            .increment(eligible_output_count);
        metrics::histogram!("zebra.state.turnstile.prepare.duration_seconds")
            .record(prepare_start.elapsed().as_secs_f64());

        Ok(())
    }
}

fn cohort_for_update<'a>(
    updates: &'a mut BTreeMap<TurnstileCohortKey, TurnstileCohortAggregate>,
    prefetched: &HashMap<TurnstileCohortKey, TurnstileCohortAggregate>,
    key: TurnstileCohortKey,
) -> &'a mut TurnstileCohortAggregate {
    updates.entry(key).or_insert_with(|| {
        *prefetched
            .get(&key)
            .expect("every block cohort key is prefetched before transaction processing")
    })
}

#[allow(clippy::too_many_arguments)]
fn eligible_source_from_facts(
    is_coinbase: bool,
    has_transparent_inputs: bool,
    has_positive_transparent_output: bool,
    sprout_credit: u64,
    sprout_debit: u64,
    sapling_value_balance: i64,
    orchard_value_balance: i64,
    ironwood_value_balance: i64,
) -> Option<TurnstileSourcePool> {
    if is_coinbase || has_transparent_inputs || !has_positive_transparent_output {
        return None;
    }

    let sprout_net_debit = i128::from(sprout_debit) - i128::from(sprout_credit);
    let source_flags = [
        sprout_net_debit > 0,
        sapling_value_balance > 0,
        orchard_value_balance > 0,
        ironwood_value_balance > 0,
    ];
    let source_count = source_flags
        .into_iter()
        .filter(|is_source| *is_source)
        .count();
    if source_count == 0 {
        return None;
    }
    if source_count > 1 {
        return Some(TurnstileSourcePool::Mixed);
    }

    Some(if source_flags[0] {
        TurnstileSourcePool::Sprout
    } else if source_flags[1] {
        TurnstileSourcePool::Sapling
    } else if source_flags[2] {
        TurnstileSourcePool::Orchard
    } else {
        TurnstileSourcePool::Ironwood
    })
}

fn shielded_credit_bits_from_facts(
    sprout_credit: u64,
    sapling_value_balance: i64,
    orchard_value_balance: i64,
    ironwood_value_balance: i64,
) -> u8 {
    let mut bits = 0;
    if sprout_credit > 0 {
        bits |= SPROUT_CREDIT_BIT;
    }
    if sapling_value_balance < 0 {
        bits |= SAPLING_CREDIT_BIT;
    }
    if orchard_value_balance < 0 {
        bits |= ORCHARD_CREDIT_BIT;
    }
    if ironwood_value_balance < 0 {
        bits |= IRONWOOD_CREDIT_BIT;
    }
    bits
}

fn sprout_gross_values(
    facts: &FinalizedTransactionFacts,
) -> Result<(u64, u64), TurnstileIndexError> {
    fn total(
        values: impl IntoIterator<Item = i64>,
        metric: &'static str,
    ) -> Result<u64, TurnstileIndexError> {
        values.into_iter().try_fold(0u64, |total, value| {
            let value = Amount::<NonNegative>::try_from(value).map_err(|error| {
                TurnstileIndexError::InvalidValue {
                    metric,
                    reason: error.to_string(),
                }
            })?;
            let value = u64::try_from(value.zatoshis()).map_err(|error| {
                TurnstileIndexError::InvalidValue {
                    metric,
                    reason: error.to_string(),
                }
            })?;
            total
                .checked_add(value)
                .ok_or(TurnstileIndexError::Arithmetic(metric))
        })
    }

    Ok((
        total(
            facts.sprout_inflow_values_zat.iter().copied(),
            "Sprout credit",
        )?,
        total(
            facts.sprout_outflow_values_zat.iter().copied(),
            "Sprout debit",
        )?,
    ))
}

fn cohort_is_mature(date_key: SnapshotDateKey, days: i64, as_of_timestamp: i64) -> bool {
    let Some(date) = chrono::NaiveDate::from_ymd_opt(
        2000 + i32::from(date_key.year),
        u32::from(date_key.month),
        u32::from(date_key.day),
    ) else {
        return false;
    };
    let Some(start_timestamp) = date
        .and_hms_opt(0, 0, 0)
        .map(|datetime| datetime.and_utc().timestamp())
    else {
        return false;
    };
    let maturity_timestamp = days
        .checked_add(1)
        .and_then(|complete_days| complete_days.checked_mul(ONE_DAY_SECONDS))
        .and_then(|seconds| start_timestamp.checked_add(seconds));
    maturity_timestamp.is_some_and(|maturity| as_of_timestamp >= maturity)
}

fn take_bytes<const N: usize>(bytes: &[u8], offset: &mut usize) -> [u8; N] {
    let end = offset
        .checked_add(N)
        .expect("fixed turnstile disk offset must not overflow");
    let value = bytes[*offset..end]
        .try_into()
        .expect("fixed turnstile record has enough bytes");
    *offset = end;
    value
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zebra_chain::{
        amount::DeferredPoolBalanceChange,
        block::{Block, Header},
        parameters::{Network, NetworkUpgrade},
        serialization::ZcashDeserializeInto,
        transaction::{
            arbitrary::shielded::{insert_fake_orchard_shielded_data, with_orchard_value_balance},
            LockTime, Transaction,
        },
        transparent::{new_ordered_outputs_with_height, Input, OutPoint, Output, Script},
    };

    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        request::{SemanticallyVerifiedBlock, Treestate},
        service::finalized_state::STATE_COLUMN_FAMILIES_IN_CODE,
        CheckpointVerifiedBlock, Config,
    };

    use super::*;

    fn amount(zatoshis: u64) -> Amount<NonNegative> {
        Amount::try_from(zatoshis).expect("test amount must be valid")
    }

    fn new_ephemeral_zebra_db() -> ZebraDb {
        ZebraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening an ephemeral database should succeed")
    }

    fn finalized_block(
        height: Height,
        timestamp: i64,
        previous_block_hash: block::Hash,
        transactions: Vec<Arc<Transaction>>,
    ) -> FinalizedBlock {
        let mut header: Header = zebra_test::vectors::DUMMY_HEADER
            .as_slice()
            .zcash_deserialize_into()
            .expect("dummy header should deserialize");
        header.previous_block_hash = previous_block_hash;
        header.time =
            chrono::DateTime::from_timestamp(timestamp, 0).expect("test timestamp should be valid");

        let block = Arc::new(Block {
            header: Arc::new(header),
            transactions,
        });
        let transaction_hashes: Arc<[_]> = block.transactions.iter().map(|tx| tx.hash()).collect();
        let new_outputs = new_ordered_outputs_with_height(&block, height, &transaction_hashes);
        FinalizedBlock::from_checkpoint_verified(
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
        )
    }

    fn orchard_transaction(
        value_balance: i64,
        inputs: Vec<Input>,
        outputs: Vec<Output>,
    ) -> Arc<Transaction> {
        let transaction = Transaction::test_v5(
            NetworkUpgrade::Nu5,
            inputs,
            outputs,
            LockTime::unlocked(),
            Height(10),
        );
        let transaction = insert_fake_orchard_shielded_data(transaction);
        Arc::new(with_orchard_value_balance(transaction, value_balance))
    }

    fn prepare_turnstile_block(
        zebra_db: &ZebraDb,
        finalized: &FinalizedBlock,
        input_locations: &HashMap<OutPoint, OutputLocation>,
    ) -> DiskWriteBatch {
        let mut batch = DiskWriteBatch::new();
        batch.prepare_block_header_and_transaction_data_batch(zebra_db.db(), finalized);
        let transaction_facts = FinalizedTransactionFacts::from_block(finalized);
        batch
            .prepare_turnstile_batch(zebra_db, finalized, &transaction_facts, input_locations)
            .expect("test Turnstile block should prepare");
        batch
    }

    #[test]
    fn fixed_disk_records_round_trip() {
        let output = TurnstileOutput {
            source_pool: TurnstileSourcePool::Orchard,
            value_zat: 123_456,
            created_timestamp: 1_700_000_000,
        };
        assert_eq!(TurnstileOutput::from_bytes(output.as_bytes()), output);

        let mut cohort = TurnstileCohortAggregate::default();
        cohort.record_eligible(123_456).unwrap();
        cohort.record_spend(output, 1_700_086_400, true).unwrap();
        assert_eq!(
            TurnstileCohortAggregate::from_bytes(cohort.as_bytes()),
            cohort
        );

        let mut accumulator = TurnstileAccumulator::at_genesis(1_700_000_000);
        accumulator.advance_tip(Height::MIN, 1_700_000_000).unwrap();
        accumulator.advance_tip(Height(1), 1_700_000_075).unwrap();
        accumulator
            .record_eligible(output.source_pool, 123_456)
            .unwrap();
        accumulator.record_spend(output, true).unwrap();
        assert_eq!(
            TurnstileAccumulator::from_bytes(accumulator.as_bytes()),
            accumulator
        );
    }

    #[test]
    fn finalized_batches_index_first_spends_and_query_complete_dates() {
        let zebra_db = new_ephemeral_zebra_db();
        let first_day = 1_704_067_200;
        let genesis_timestamp = first_day - 60;

        let genesis = finalized_block(
            Height::MIN,
            genesis_timestamp,
            block::Hash([0; 32]),
            Vec::new(),
        );
        zebra_db
            .write_batch(prepare_turnstile_block(
                &zebra_db,
                &genesis,
                &HashMap::new(),
            ))
            .expect("genesis Turnstile batch should commit");

        let first_exit = orchard_transaction(
            80,
            Vec::new(),
            vec![Output::new(amount(80), Script::new(&[]))],
        );
        let first_outpoint = OutPoint {
            hash: first_exit.hash(),
            index: 0,
        };
        let first_block = finalized_block(Height(1), first_day, genesis.hash, vec![first_exit]);
        let first_batch = prepare_turnstile_block(&zebra_db, &first_block, &HashMap::new());

        // Reads remain anchored to the previous finalized batch until all block and index writes
        // are committed together.
        let before_first_commit = zebra_db
            .turnstile_data(None, None, 100, None)
            .expect("genesis Turnstile query should succeed")
            .expect("genesis Turnstile accumulator should exist");
        assert_eq!(before_first_commit.as_of_height, Height::MIN);
        assert_eq!(
            before_first_commit
                .summaries
                .iter()
                .map(|summary| summary.eligible.count)
                .sum::<u64>(),
            0
        );
        zebra_db
            .write_batch(first_batch)
            .expect("first exit batch should commit");
        assert!(zebra_db
            .turnstile_output_cf()
            .zs_get(&OutputLocation::from_usize(Height(1), 0, 0))
            .is_some());

        let first_spend = orchard_transaction(
            -80,
            vec![Input::PrevOut {
                outpoint: first_outpoint,
                unlock_script: Script::new(&[]),
                sequence: u32::MAX,
            }],
            Vec::new(),
        );
        let spend_block = finalized_block(
            Height(2),
            first_day + 3_600,
            first_block.hash,
            vec![first_spend],
        );
        zebra_db
            .write_batch(prepare_turnstile_block(
                &zebra_db,
                &spend_block,
                &HashMap::from([(first_outpoint, OutputLocation::from_usize(Height(1), 0, 0))]),
            ))
            .expect("first-spend batch should commit");
        assert!(zebra_db
            .turnstile_output_cf()
            .zs_get(&OutputLocation::from_usize(Height(1), 0, 0))
            .is_none());

        let second_exit = orchard_transaction(
            40,
            Vec::new(),
            vec![Output::new(amount(40), Script::new(&[]))],
        );
        let second_block = finalized_block(
            Height(3),
            first_day + ONE_DAY_SECONDS,
            spend_block.hash,
            vec![second_exit],
        );
        zebra_db
            .write_batch(prepare_turnstile_block(
                &zebra_db,
                &second_block,
                &HashMap::new(),
            ))
            .expect("second exit batch should commit");

        let mut maturity_block = finalized_block(
            Height(4),
            first_day + 3 * ONE_DAY_SECONDS,
            second_block.hash,
            Vec::new(),
        );
        zebra_db
            .write_batch(prepare_turnstile_block(
                &zebra_db,
                &maturity_block,
                &HashMap::new(),
            ))
            .expect("maturity batch should commit");
        // Median-time-past needs a majority of the recent header window to move forward. Three
        // more valid empty blocks make the first two UTC cohorts mature without relying on a
        // single far-future header timestamp.
        for height in 5..=7 {
            let next_block = finalized_block(
                Height(height),
                first_day + 3 * ONE_DAY_SECONDS + i64::from(height),
                maturity_block.hash,
                Vec::new(),
            );
            zebra_db
                .write_batch(prepare_turnstile_block(
                    &zebra_db,
                    &next_block,
                    &HashMap::new(),
                ))
                .expect("median-time-past maturity batch should commit");
            maturity_block = next_block;
        }

        let data = zebra_db
            .turnstile_data(None, None, 100, Some(TurnstileSourcePool::Orchard))
            .expect("Turnstile query should succeed")
            .expect("Turnstile accumulator should exist");
        assert_eq!(data.as_of_height, Height(7));
        assert_eq!(data.coverage_start_timestamp, genesis_timestamp);
        assert_eq!(data.summaries.len(), 1);
        let summary = data.summaries[0];
        assert_eq!(
            summary.eligible,
            TurnstileValue {
                count: 2,
                value_zat: 120,
            }
        );
        assert_eq!(
            summary.spent,
            TurnstileValue {
                count: 1,
                value_zat: 80,
            }
        );
        assert_eq!(
            summary.unspent,
            TurnstileValue {
                count: 1,
                value_zat: 40,
            }
        );
        assert_eq!(summary.shielding_observed, summary.spent);
        assert!(zebra_db
            .turnstile_output_cf()
            .zs_get(&OutputLocation::from_usize(Height(3), 0, 0))
            .is_some());
        assert_eq!(data.cohorts.len(), 2);
        assert!(data.cohorts.iter().all(|cohort| cohort.within_1d.is_some()));
        assert_eq!(
            data.cohorts[0]
                .within_1d
                .expect("first cohort is mature")
                .shielding_observed,
            summary.spent
        );

        let first_page = zebra_db
            .turnstile_data(None, None, 1, Some(TurnstileSourcePool::Orchard))
            .expect("paginated Turnstile query should succeed")
            .expect("Turnstile accumulator should exist");
        assert_eq!(first_page.cohorts.len(), 1);
        assert_eq!(
            first_page.next_start_date,
            Some(SnapshotDateKey::new(24, 1, 1))
        );
        assert_eq!(first_page.summaries, data.summaries);

        let sapling_only = zebra_db
            .turnstile_data(None, None, 100, Some(TurnstileSourcePool::Sapling))
            .expect("source-filtered Turnstile query should succeed")
            .expect("Turnstile accumulator should exist");
        assert!(sapling_only.cohorts.is_empty());
        assert_eq!(sapling_only.summaries.len(), 1);
        assert_eq!(
            sapling_only.summaries[0].eligible,
            TurnstileValue::default()
        );
    }

    #[test]
    fn source_filtered_pagination_skips_non_matching_rows_without_empty_pages() {
        let zebra_db = new_ephemeral_zebra_db();
        let timestamp = 1_704_067_200;
        let genesis = finalized_block(Height::MIN, timestamp, block::Hash([0; 32]), Vec::new());
        zebra_db
            .write_batch(prepare_turnstile_block(
                &zebra_db,
                &genesis,
                &HashMap::new(),
            ))
            .expect("genesis Turnstile batch should commit");

        // More than `(limit + 1) * source_count` non-matching rows precede the requested source.
        // A scan cutoff based on all source rows used to return an empty page and a continuation
        // cursor even though the requested Ironwood row was available later in the same range.
        let mut batch = DiskWriteBatch::new();
        for day in 1..=3 {
            for source_pool in [
                TurnstileSourcePool::Sprout,
                TurnstileSourcePool::Sapling,
                TurnstileSourcePool::Orchard,
                TurnstileSourcePool::Mixed,
            ] {
                let key = TurnstileCohortKey::new(SnapshotDateKey::new(24, 1, day), source_pool);
                let _ = zebra_db
                    .turnstile_cohort_cf()
                    .with_batch_for_writing(&mut batch)
                    .zs_insert(&key, &TurnstileCohortAggregate::default());
            }
        }
        let ironwood_date = SnapshotDateKey::new(24, 1, 4);
        let ironwood_key = TurnstileCohortKey::new(ironwood_date, TurnstileSourcePool::Ironwood);
        let _ = zebra_db
            .turnstile_cohort_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&ironwood_key, &TurnstileCohortAggregate::default());
        zebra_db
            .write_batch(batch)
            .expect("test cohort rows should commit");

        let page = zebra_db
            .turnstile_data(
                Some(SnapshotDateKey::new(24, 1, 1)),
                Some(ironwood_date),
                1,
                Some(TurnstileSourcePool::Ironwood),
            )
            .expect("source-filtered Turnstile query should succeed")
            .expect("Turnstile accumulator should exist");
        assert_eq!(page.cohorts.len(), 1);
        assert_eq!(page.cohorts[0].date_key, ironwood_date);
        assert_eq!(page.cohorts[0].source_pool, TurnstileSourcePool::Ironwood);
        assert_eq!(page.next_start_date, None);
    }

    #[test]
    fn same_block_batch_coalesces_multiple_output_lifecycles() {
        let zebra_db = new_ephemeral_zebra_db();
        let timestamp = 1_704_067_200;
        let genesis = finalized_block(
            Height::MIN,
            timestamp - 60,
            block::Hash([0; 32]),
            Vec::new(),
        );
        zebra_db
            .write_batch(prepare_turnstile_block(
                &zebra_db,
                &genesis,
                &HashMap::new(),
            ))
            .expect("genesis Turnstile batch should commit");

        let exit = orchard_transaction(
            100,
            Vec::new(),
            vec![
                Output::new(amount(40), Script::new(&[])),
                Output::new(amount(60), Script::new(&[])),
            ],
        );
        let first_outpoint = OutPoint {
            hash: exit.hash(),
            index: 0,
        };
        let second_outpoint = OutPoint {
            hash: exit.hash(),
            index: 1,
        };
        let spend = orchard_transaction(
            -100,
            vec![
                Input::PrevOut {
                    outpoint: first_outpoint,
                    unlock_script: Script::new(&[]),
                    sequence: u32::MAX,
                },
                Input::PrevOut {
                    outpoint: second_outpoint,
                    unlock_script: Script::new(&[]),
                    sequence: u32::MAX,
                },
            ],
            Vec::new(),
        );
        let block = finalized_block(Height(1), timestamp, genesis.hash, vec![exit, spend]);
        let input_locations = HashMap::from([
            (first_outpoint, OutputLocation::from_usize(Height(1), 0, 0)),
            (second_outpoint, OutputLocation::from_usize(Height(1), 0, 1)),
        ]);
        zebra_db
            .write_batch(prepare_turnstile_block(&zebra_db, &block, &input_locations))
            .expect("same-block lifecycle batch should commit");

        let data = zebra_db
            .turnstile_data(None, None, 100, Some(TurnstileSourcePool::Orchard))
            .expect("Turnstile query should succeed")
            .expect("Turnstile accumulator should exist");
        assert_eq!(data.cohorts.len(), 1);
        assert_eq!(
            data.cohorts[0].eligible,
            TurnstileValue {
                count: 2,
                value_zat: 100,
            }
        );
        assert_eq!(data.cohorts[0].spent, data.cohorts[0].eligible);
        assert_eq!(data.cohorts[0].shielding_observed, data.cohorts[0].eligible);
        assert_eq!(data.summaries[0].spent, data.summaries[0].eligible);
        assert_eq!(data.summaries[0].unspent, TurnstileValue::default());
        assert!(zebra_db
            .turnstile_output_cf()
            .zs_get(&OutputLocation::from_usize(Height(1), 0, 0))
            .is_none());
        assert!(zebra_db
            .turnstile_output_cf()
            .zs_get(&OutputLocation::from_usize(Height(1), 0, 1))
            .is_none());
    }

    #[test]
    fn same_block_updates_to_one_cohort_are_coalesced() {
        let date = SnapshotDateKey::new(24, 1, 1);
        let key = TurnstileCohortKey::new(date, TurnstileSourcePool::Sapling);
        let mut updates = BTreeMap::new();
        let cohort = updates
            .entry(key)
            .or_insert_with(TurnstileCohortAggregate::default);

        // One eligible transaction can create multiple outputs, and another transaction in the
        // same finalized block can spend an eligible output created earlier in that block.
        cohort.record_eligible(40).unwrap();
        cohort.record_eligible(60).unwrap();
        let output = TurnstileOutput::new(TurnstileSourcePool::Sapling, 40, 1_704_067_200);
        cohort.record_spend(output, 1_704_067_200, true).unwrap();

        let cohort = updates.get(&key).unwrap();
        assert_eq!(
            cohort.eligible,
            TurnstileValue {
                count: 2,
                value_zat: 100
            }
        );
        assert_eq!(
            cohort.spent,
            TurnstileValue {
                count: 1,
                value_zat: 40
            }
        );
        assert_eq!(
            cohort.shielding_observed,
            TurnstileValue {
                count: 1,
                value_zat: 40
            }
        );
        for window in cohort.spent_within {
            assert_eq!(
                window,
                TurnstileValue {
                    count: 1,
                    value_zat: 40
                }
            );
        }
    }

    #[test]
    fn cohort_windows_only_appear_after_complete_utc_day_matures() {
        let key =
            TurnstileCohortKey::new(SnapshotDateKey::new(24, 1, 1), TurnstileSourcePool::Orchard);
        let aggregate = TurnstileCohortAggregate::default();
        let before_one_day = aggregate.into_public(key, 1_704_239_999);
        assert!(before_one_day.within_1d.is_none());
        let at_one_day = aggregate.into_public(key, 1_704_240_000);
        assert!(at_one_day.within_1d.is_some());
        assert!(at_one_day.within_7d.is_none());
    }

    #[test]
    fn maturity_uses_monotonic_median_time_past_when_header_time_regresses() {
        let mut accumulator = TurnstileAccumulator::at_genesis(1);
        accumulator.advance_tip(Height::MIN, 1).unwrap();
        for timestamp in 2..=11 {
            accumulator
                .advance_tip(Height(timestamp as u32 - 1), timestamp)
                .unwrap();
        }
        assert_eq!(accumulator.maturity_timestamp, 6);

        accumulator.advance_tip(Height(11), 100).unwrap();
        assert_eq!(accumulator.maturity_timestamp, 7);
        accumulator.advance_tip(Height(12), 8).unwrap();
        assert_eq!(accumulator.latest_timestamp, 8);
        assert_eq!(accumulator.maturity_timestamp, 8);
        assert_eq!(accumulator.coverage_start_timestamp, 1);
    }

    #[test]
    fn eligible_source_classification_is_conservative() {
        let classify =
            |is_coinbase, has_inputs, has_output, sprout_in, sprout_out, sapling, orchard| {
                eligible_source_from_facts(
                    is_coinbase,
                    has_inputs,
                    has_output,
                    sprout_in,
                    sprout_out,
                    sapling,
                    orchard,
                    0,
                )
            };

        assert_eq!(classify(true, false, true, 0, 0, 10, 0), None);
        assert_eq!(classify(false, true, true, 0, 0, 10, 0), None);
        assert_eq!(classify(false, false, false, 0, 0, 10, 0), None);
        assert_eq!(classify(false, false, true, 0, 0, 0, 0), None);
        assert_eq!(
            classify(false, false, true, 0, 0, 10, 0),
            Some(TurnstileSourcePool::Sapling)
        );
        assert_eq!(
            classify(false, false, true, 0, 0, 10, 20),
            Some(TurnstileSourcePool::Mixed)
        );
        assert_eq!(
            classify(false, false, true, 5, 10, 0, 0),
            Some(TurnstileSourcePool::Sprout)
        );
        assert_eq!(
            classify(false, false, true, 10, 5, 20, 0),
            Some(TurnstileSourcePool::Sapling)
        );
    }

    #[test]
    fn shielding_observed_is_an_association_not_an_amount() {
        assert_eq!(shielded_credit_bits_from_facts(0, 0, 0, 0), 0);
        assert_eq!(
            shielded_credit_bits_from_facts(1, -2, 0, -3),
            SPROUT_CREDIT_BIT | SAPLING_CREDIT_BIT | IRONWOOD_CREDIT_BIT
        );

        let output = TurnstileOutput::new(TurnstileSourcePool::Orchard, 50, 100);
        let mut without_shielding = TurnstileCohortAggregate::default();
        without_shielding.record_eligible(50).unwrap();
        without_shielding.record_spend(output, 200, false).unwrap();
        assert_eq!(
            without_shielding.shielding_observed,
            TurnstileValue::default()
        );

        let mut with_shielding = TurnstileCohortAggregate::default();
        with_shielding.record_eligible(50).unwrap();
        with_shielding.record_spend(output, 200, true).unwrap();
        assert_eq!(
            with_shielding.shielding_observed,
            TurnstileValue {
                count: 1,
                value_zat: 50,
            }
        );

        let mut invalid_chain_time = TurnstileCohortAggregate::default();
        let error = invalid_chain_time
            .record_spend(output, 99, false)
            .expect_err("median-time-past must not regress");
        assert!(matches!(
            error,
            TurnstileIndexError::NonMonotonicChainTime { .. }
        ));
    }
}
