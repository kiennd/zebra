//! Snapshot data storage and retrieval for block height snapshots.
//!
//! This module provides functionality to store and retrieve snapshot data
//! at the first block of each UTC day, including:
//! - Holder counts
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
    parameters::{subsidy::block_subsidy, Network, NetworkUpgrade},
    value_balance::ValueBalance,
    work::difficulty::{ParameterDifficulty, U256},
};

use crate::{
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        zebra_db::ZebraDb,
    },
    BoxError, FromDisk, IntoDisk,
};

use super::super::TypedColumnFamily;

/// The name of the snapshot data by date column family.
/// Stores holder count, pool values, difficulty, issuance, inflation rate, and timestamp.
/// Key format: YY:MM:DD (year, month, day) as 3 bytes: [year, month, day]
/// This is used for daily snapshots only.
pub const SNAPSHOT_DATA_BY_DATE: &str = "snapshot_data_by_date";

/// The name of the realtime snapshot data column family.
/// Stores the same data as daily snapshots, but for realtime snapshots (taken when fully synced).
/// Only keeps the current day's snapshot, using a constant key since we overwrite it each time.
/// This is used for realtime snapshots only, to prevent them from interfering with daily snapshot calculations.
pub const REALTIME_SNAPSHOT_DATA: &str = "realtime_snapshot_data";

/// Key type for realtime snapshots (only one entry, overwritten each time)
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

/// Constant key for realtime snapshots
const REALTIME_SNAPSHOT_KEY: RealtimeSnapshotKey = RealtimeSnapshotKey;

/// Snapshot records written before the Ironwood value pool was added.
const LEGACY_SNAPSHOT_DATA_LEN: usize = 184;
/// Snapshot records written after the Ironwood pool was added, but before its metrics were stored.
const TRANSITIONAL_SNAPSHOT_DATA_LEN: usize = 192;
/// Current snapshot records, including the Ironwood pool and its transaction/flow metrics.
const CURRENT_SNAPSHOT_DATA_LEN: usize = 212;

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

/// Snapshot data stored in RocksDB containing holder count, pool values, difficulty, issuance, timestamp, and transaction counts.
///
/// Floating-point metrics are stored as their IEEE bit patterns. This makes equality match the
/// on-disk representation and remain reflexive for NaN values; different NaN payloads and signed
/// zeroes remain distinct.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SnapshotData {
    /// Number of holders (addresses with non-zero balances).
    holder_count: u64,
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
}

impl SnapshotData {
    /// Creates a new SnapshotData.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        holder_count: u64,
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
    ) -> Self {
        // Convert inflation rate to basis points (hundredths of a percent)
        let inflation_rate_bps = (inflation_rate_percent * 100.0).round() as u32;

        SnapshotData {
            holder_count,
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
        }
    }

    pub fn holder_count(&self) -> u64 {
        self.holder_count
    }

    pub fn pool_values(&self) -> ValueBalance<NonNegative> {
        self.pool_values
    }

    pub fn work_difficulty(&self) -> f64 {
        f64::from_bits(self.work_difficulty_bits)
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
}

impl IntoDisk for SnapshotData {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        // Keep the original fields as a 192-byte prefix, then append Ironwood metrics.
        // This lets the decoder continue reading legacy 184-byte records and transitional
        // 192-byte records that contain the Ironwood value pool but not its metrics.
        let mut bytes = Vec::with_capacity(CURRENT_SNAPSHOT_DATA_LEN);
        bytes.extend_from_slice(&self.holder_count.to_be_bytes());
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
        debug_assert_eq!(bytes.len(), CURRENT_SNAPSHOT_DATA_LEN);
        bytes
    }
}

impl FromDisk for SnapshotData {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        let pool_values_len = match bytes.len() {
            LEGACY_SNAPSHOT_DATA_LEN => 40,
            TRANSITIONAL_SNAPSHOT_DATA_LEN | CURRENT_SNAPSHOT_DATA_LEN => 48,
            actual_len => panic!(
                "SnapshotData deserialization error: expected {LEGACY_SNAPSHOT_DATA_LEN}, \
                 {TRANSITIONAL_SNAPSHOT_DATA_LEN}, or {CURRENT_SNAPSHOT_DATA_LEN} bytes, \
                 got {actual_len} bytes"
            ),
        };

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

        let mut offset = 0;
        let holder_count = u64::from_be_bytes(take(bytes, &mut offset, "holder count"));

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
        let (sprout_inflow, sprout_outflow) = if bytes.len() != CURRENT_SNAPSHOT_DATA_LEN {
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

        let (ironwood_tx_count, ironwood_inflow, ironwood_outflow) =
            if bytes.len() == CURRENT_SNAPSHOT_DATA_LEN {
                (
                    u32::from_be_bytes(take(bytes, &mut offset, "ironwood tx count")),
                    u64::from_be_bytes(take(bytes, &mut offset, "ironwood inflow")),
                    u64::from_be_bytes(take(bytes, &mut offset, "ironwood outflow")),
                )
            } else {
                (0, 0, 0)
            };

        assert_eq!(
            offset,
            bytes.len(),
            "SnapshotData decoder must consume the entire record"
        );

        SnapshotData {
            holder_count,
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
        }
    }
}

impl ZebraDb {
    /// Returns a handle to the `snapshot_data_by_date` RocksDB column family.
    /// This is used for daily snapshots only.
    pub fn snapshot_data_by_date_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(SNAPSHOT_DATA_BY_DATE).unwrap()
    }

    /// Returns a handle to the `realtime_snapshot_data` RocksDB column family.
    /// This is used for realtime snapshots only (only keeps current day).
    pub fn realtime_snapshot_data_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(REALTIME_SNAPSHOT_DATA).unwrap()
    }

    /// Returns the holder count for a given block height, if it was stored in a snapshot.
    ///
    /// Returns `None` if no snapshot was stored at that height.
    /// This reads from the snapshot data column family.
    pub fn holder_count_at_height(&self, height: Height) -> Option<u64> {
        let snapshot_data = self.snapshot_data_at_height(height)?;
        Some(snapshot_data.holder_count())
    }

    /// Returns the most recent holder count snapshots, limited to the specified count.
    ///
    /// Returns a vector of (height, holder_count) pairs, sorted by height (ascending).
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
    pub fn recent_holder_count_snapshots(&self, limit: usize) -> Vec<(SnapshotDateKey, u64)> {
        self.recent_snapshot_data(limit)
            .into_iter()
            .map(|(date_key, snapshot_data)| (date_key, snapshot_data.holder_count()))
            .collect()
    }

    /// Returns the latest daily snapshot at or before `latest_date` whose height is lower than
    /// `height`.
    #[allow(clippy::unwrap_in_result)]
    fn latest_daily_snapshot_before_height(
        &self,
        latest_date: SnapshotDateKey,
        height: Height,
    ) -> Option<(SnapshotDateKey, SnapshotData)> {
        let typed_cf = TypedColumnFamily::<SnapshotDateKey, SnapshotData>::new(
            &self.db,
            SNAPSHOT_DATA_BY_DATE,
        )
        .expect("column family was created when database was created");

        let previous_snapshot = typed_cf
            .zs_reverse_range_iter(..=latest_date)
            .find(|(_, snapshot)| snapshot.block_height() < height.0);

        previous_snapshot
    }

    /// Calculate total ZEC issuance up to and including a given height.
    fn calculate_total_issuance(
        height: Height,
        network: &Network,
    ) -> Result<Amount<NonNegative>, BoxError> {
        use std::ops::Add;

        let mut total = Amount::zero();

        // `block_subsidy` includes the slow-start schedule, so every height contributes to the
        // issuance total. Earlier fork code skipped the first slow-start interval entirely.
        for h in Height::MIN.0..=height.0 {
            let block_height = Height(h);
            let subsidy = block_subsidy(block_height, network)
                .map_err(|e| format!("failed to calculate block subsidy at height {h}: {e}"))?;
            total = total
                .add(subsidy)
                .map_err(|e| format!("overflow calculating total issuance at height {h}: {e}"))?;
        }

        Ok(total)
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
        // Determine blocks per year based on network upgrade
        let blocks_per_year =
            if let Some(blossom_height) = NetworkUpgrade::Blossom.activation_height(network) {
                if height >= blossom_height {
                    420_480.0 // 75 seconds per block after Blossom
                } else {
                    210_240.0 // 150 seconds per block before Blossom
                }
            } else {
                210_240.0 // Default to pre-Blossom if Blossom is not activated
            };

        let block_subsidy_amount = block_subsidy(height, network)
            .map_err(|e| format!("failed to get block subsidy: {}", e))?;

        // Avoid division by zero
        if total_supply == Amount::<NonNegative>::zero() {
            return Ok(0.0);
        }

        // Calculate annual inflation rate as percentage
        let annual_issuance_zat = (block_subsidy_amount.zatoshis() as f64) * blocks_per_year;
        let total_supply_zat = total_supply.zatoshis() as f64;

        let inflation_rate = (annual_issuance_zat / total_supply_zat) * 100.0;

        Ok(inflation_rate)
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
                        transparent_outflow_zat = transparent_outflow_zat.saturating_add(value_zat);
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
                    transparent_inflow_zat = transparent_inflow_zat.saturating_add(value_zat);
                }

                // Sprout pool: vpub_old (inflow) and vpub_new (outflow)
                for vpub_old_zat in transaction.output_values_to_sprout() {
                    let vpub_old = Amount::<NonNegative>::try_from(vpub_old_zat)
                        .map_err(|e| format!("invalid sprout vpub_old amount: {}", e))?;
                    let value_zat = u64::try_from(vpub_old.zatoshis())
                        .map_err(|e| format!("sprout vpub_old does not fit in u64: {e}"))?;
                    sprout_inflow_zat = sprout_inflow_zat.saturating_add(value_zat);
                }

                for vpub_new_zat in transaction.input_values_from_sprout() {
                    let vpub_new = Amount::<NonNegative>::try_from(vpub_new_zat)
                        .map_err(|e| format!("invalid sprout vpub_new amount: {}", e))?;
                    let value_zat = u64::try_from(vpub_new.zatoshis())
                        .map_err(|e| format!("sprout vpub_new does not fit in u64: {e}"))?;
                    sprout_outflow_zat = sprout_outflow_zat.saturating_add(value_zat);
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
                    sapling_inflow_zat = sapling_inflow_zat.saturating_add(value_zat);
                } else if sapling_zatoshis > 0 {
                    // Net outflow: value leaving sapling pool
                    let value_zat = sapling_zatoshis as u64;
                    sapling_outflow_zat = sapling_outflow_zat.saturating_add(value_zat);
                }

                // Orchard pool: value_balance represents net change
                // Negative value_balance = net inflow, positive = net outflow
                let orchard_vb = transaction.orchard_value_balance();
                let orchard_net = orchard_vb.orchard_amount();
                let orchard_zatoshis = orchard_net.zatoshis();
                if orchard_zatoshis < 0 {
                    // Net inflow: value entering orchard pool
                    let value_zat = (-orchard_zatoshis) as u64;
                    orchard_inflow_zat = orchard_inflow_zat.saturating_add(value_zat);
                } else if orchard_zatoshis > 0 {
                    // Net outflow: value leaving orchard pool
                    let value_zat = orchard_zatoshis as u64;
                    orchard_outflow_zat = orchard_outflow_zat.saturating_add(value_zat);
                }

                // Ironwood pool: value_balance represents net change
                // Negative value_balance = net inflow, positive = net outflow
                let ironwood_vb = transaction.ironwood_value_balance();
                let ironwood_net = ironwood_vb.ironwood_amount();
                let ironwood_zatoshis = ironwood_net.zatoshis();
                if ironwood_zatoshis < 0 {
                    // Net inflow: value entering the Ironwood pool
                    let value_zat = (-ironwood_zatoshis) as u64;
                    ironwood_inflow_zat = ironwood_inflow_zat.saturating_add(value_zat);
                } else if ironwood_zatoshis > 0 {
                    // Net outflow: value leaving the Ironwood pool
                    let value_zat = ironwood_zatoshis as u64;
                    ironwood_outflow_zat = ironwood_outflow_zat.saturating_add(value_zat);
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
            let time_diff = end_timestamp - interval_start_timestamp;
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

    /// Stores snapshot data (holder count, pool values, difficulty, issuance, inflation, timestamp)
    /// to RocksDB at the given block height.
    ///
    /// # Warning
    ///
    /// This operation scans the entire balance column family and may be slow.
    /// It should be run in a blocking thread to avoid hanging the tokio executor.
    ///
    /// # Parameters
    ///
    /// - `height`: The block height at which this snapshot is taken
    /// - `network`: The network (mainnet/testnet/regtest) for subsidy calculations
    pub fn store_snapshot_data(
        &self,
        height: Height,
        network: &Network,
        use_current_date: bool,
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

        // Calculate work difficulty: pow_limit / expanded_difficulty
        // This matches the calculation in RPC getdifficulty method
        let pow_limit: U256 = network.target_difficulty_limit().into();

        // Shift out the lower 128 bits (same as RPC getdifficulty)
        let pow_limit_shifted = pow_limit >> 128;
        let difficulty_shifted = U256::from(expanded_difficulty) >> 128;

        // Convert to u128 then f64
        let pow_limit_f64 = pow_limit_shifted.as_u128() as f64;
        let difficulty_f64 = difficulty_shifted.as_u128() as f64;

        // Calculate work difficulty (avoid division by zero)
        let work_difficulty = if difficulty_f64 == 0.0 {
            0.0
        } else {
            pow_limit_f64 / difficulty_f64
        };

        // 5. Get block timestamp (Unix timestamp in seconds)
        let block_timestamp = header.time.timestamp();

        // 6. Calculate total issuance and inflation rate.
        let total_issuance = Self::calculate_total_issuance(height, network)?;

        // Pool values represent the actual monetary base (all ZEC in circulation).
        let total_supply = (pool_values.transparent_amount()
            + pool_values.sprout_amount()
            + pool_values.sapling_amount()
            + pool_values.orchard_amount()
            + pool_values.deferred_amount()
            + pool_values.ironwood_amount())
        .map_err(|e| format!("overflow calculating total supply from pool values: {e}"))?;
        let inflation_rate = self.calculate_inflation_rate(height, network, total_supply)?;

        // 8. Find the latest earlier daily snapshot height to avoid double counting blocks.
        // A chain can legitimately have no block on a UTC day, so an exact previous-day snapshot
        // is not required.
        let previous_snapshot_height = {
            let current_date_key = SnapshotDateKey::from_timestamp(block_timestamp);

            // Include the current date so realtime snapshots anchor at that day's daily snapshot.
            let previous_snapshot =
                self.latest_daily_snapshot_before_height(current_date_key, height);

            if let Some((previous_date_key, previous_snapshot)) = previous_snapshot {
                let previous_height = Height(previous_snapshot.block_height());
                tracing::debug!(
                    ?height,
                    ?block_timestamp,
                    ?current_date_key,
                    ?previous_date_key,
                    ?previous_height,
                    "found latest earlier daily snapshot"
                );
                Some(previous_height)
            } else {
                tracing::debug!(
                    ?height,
                    ?block_timestamp,
                    ?current_date_key,
                    "no earlier daily snapshot found; calculating metrics from genesis"
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
            // No previous snapshot found, start from height 0 (first snapshot)
            Height(0)
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
        );

        // 14. Store in RocksDB
        // Use separate column families for daily vs realtime snapshots
        let mut batch = DiskWriteBatch::new();
        if use_current_date {
            // Realtime snapshot: use constant key (only keep current day, overwrite each time)
            let realtime_snapshot_cf = self.realtime_snapshot_data_cf();
            batch.zs_insert(realtime_snapshot_cf, REALTIME_SNAPSHOT_KEY, snapshot_data);
        } else {
            // Daily snapshot: use date key (YY:MM:DD format)
            let date_key = SnapshotDateKey::from_timestamp(block_timestamp);
            let snapshot_cf = self.snapshot_data_by_date_cf();
            batch.zs_insert(snapshot_cf, date_key, snapshot_data);
        }
        self.db.write(batch)?;

        // Prepare date_key string for logging
        let date_key_str = if use_current_date {
            "realtime".to_string()
        } else {
            SnapshotDateKey::from_timestamp(block_timestamp).to_string()
        };

        tracing::info!(
            ?height,
            date_key = %date_key_str,
            use_current_date,
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
    /// Checks both daily and realtime snapshots, with realtime taking precedence if date matches.
    ///
    /// Returns `None` if no snapshot was stored for that date.
    pub fn snapshot_data_at_date(&self, date_key: SnapshotDateKey) -> Option<SnapshotData> {
        // First check realtime snapshot (more recent, takes precedence)
        if let Some(realtime_data) = self.get_realtime_snapshot() {
            let realtime_date_key =
                SnapshotDateKey::from_timestamp(realtime_data.block_timestamp());
            if realtime_date_key == date_key {
                return Some(realtime_data);
            }
        }

        // Fall back to daily snapshot
        let snapshot_cf = self.snapshot_data_by_date_cf();
        self.db.zs_get(snapshot_cf, &date_key)
    }

    /// Gets the current realtime snapshot, if it exists.
    fn get_realtime_snapshot(&self) -> Option<SnapshotData> {
        let realtime_cf = self.realtime_snapshot_data_cf();
        self.db.zs_get(realtime_cf, &REALTIME_SNAPSHOT_KEY)
    }

    /// Returns the snapshot data for a given block height, if it was stored.
    /// This is a convenience method that converts height to date.
    ///
    /// Returns `None` if no snapshot was stored at that height.
    pub fn snapshot_data_at_height(&self, height: Height) -> Option<SnapshotData> {
        let block = self.block(height.into())?;
        let timestamp = block.header.time.timestamp();
        let date_key = SnapshotDateKey::from_timestamp(timestamp);
        self.snapshot_data_at_date(date_key)
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
        let mut snapshots: Vec<(SnapshotDateKey, SnapshotData)> =
            typed_cf.zs_reverse_range_iter(..).take(limit).collect();

        // Reverse to get ascending order by date
        snapshots.reverse();

        snapshots
    }

    /// Returns the most recent snapshot data, limited to the specified count.
    /// Merges daily and realtime snapshots, with realtime taking precedence for the same date.
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
        let mut snapshots: Vec<(SnapshotDateKey, SnapshotData)> =
            typed_cf.zs_reverse_range_iter(..).take(limit).collect();

        // Reverse to get ascending order by date
        snapshots.reverse();

        // Merge with realtime snapshot if it exists
        if let Some(realtime_data) = self.get_realtime_snapshot() {
            let realtime_date_key =
                SnapshotDateKey::from_timestamp(realtime_data.block_timestamp());

            // Check if realtime snapshot date already exists in daily snapshots
            if let Some(existing_idx) = snapshots
                .iter()
                .position(|(key, _)| *key == realtime_date_key)
            {
                // Replace existing daily snapshot with realtime (more recent)
                snapshots[existing_idx] = (realtime_date_key, realtime_data);
            } else {
                // Add realtime snapshot and sort by date
                snapshots.push((realtime_date_key, realtime_data));
                snapshots.sort_by_key(|(key, _)| *key);

                // Keep only the most recent ones if we exceeded the limit
                if snapshots.len() > limit {
                    snapshots = snapshots.into_iter().rev().take(limit).collect();
                    snapshots.reverse();
                }
            }
        }

        snapshots
    }

    /// Returns snapshot data within a date range.
    /// Merges daily and realtime snapshots, with realtime taking precedence for the same date.
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
        let typed_cf = TypedColumnFamily::<SnapshotDateKey, SnapshotData>::new(
            &self.db,
            SNAPSHOT_DATA_BY_DATE,
        )
        .expect("column family was created when database was created");

        // Get daily snapshots in the range
        let mut snapshots: Vec<(SnapshotDateKey, SnapshotData)> = match (start_date, end_date) {
            (Some(start), Some(end)) => typed_cf.zs_forward_range_iter(start..=end).collect(),
            (Some(start), None) => typed_cf.zs_forward_range_iter(start..).collect(),
            (None, Some(end)) => typed_cf.zs_forward_range_iter(..=end).collect(),
            (None, None) => typed_cf.zs_forward_range_iter(..).collect(),
        };

        // Merge with realtime snapshot if it's in the range
        if let Some(realtime_data) = self.get_realtime_snapshot() {
            let realtime_date_key =
                SnapshotDateKey::from_timestamp(realtime_data.block_timestamp());

            // Check if realtime snapshot is in the range
            let in_range = match (start_date, end_date) {
                (Some(start), Some(end)) => realtime_date_key >= start && realtime_date_key <= end,
                (Some(start), None) => realtime_date_key >= start,
                (None, Some(end)) => realtime_date_key <= end,
                (None, None) => true,
            };

            if in_range {
                // Check if realtime snapshot date already exists in daily snapshots
                if let Some(existing_idx) = snapshots
                    .iter()
                    .position(|(key, _)| *key == realtime_date_key)
                {
                    // Replace existing daily snapshot with realtime (more recent)
                    snapshots[existing_idx] = (realtime_date_key, realtime_data);
                } else {
                    // Add realtime snapshot and sort by date
                    snapshots.push((realtime_date_key, realtime_data));
                    snapshots.sort_by_key(|(key, _)| *key);
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
        transaction::{LockTime, Transaction},
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

    fn test_snapshot() -> SnapshotData {
        SnapshotData {
            holder_count: 7,
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
    fn total_issuance_includes_slow_start() {
        let network = Network::Mainnet;
        let height = Height(1);
        let expected = block_subsidy(height, &network).expect("slow-start subsidy should be valid");

        assert!(expected > Amount::<NonNegative>::zero());
        assert_eq!(
            ZebraDb::calculate_total_issuance(height, &network)
                .expect("issuance calculation should succeed"),
            expected,
        );
    }

    #[test]
    fn realtime_metrics_anchor_at_the_same_dates_daily_snapshot() {
        let network = Network::Mainnet;
        let zebra_db = new_ephemeral_zebra_db(&network);
        let current_timestamp = 1_700_000_000;
        let previous_timestamp = current_timestamp - 24 * 60 * 60;
        let current_date = SnapshotDateKey::from_timestamp(current_timestamp);
        let previous_date = SnapshotDateKey::from_timestamp(previous_timestamp);
        let previous_snapshot = SnapshotData {
            block_timestamp: previous_timestamp,
            block_height: 10,
            ..test_snapshot()
        };
        let current_snapshot = SnapshotData {
            block_timestamp: current_timestamp,
            block_height: 20,
            ..test_snapshot()
        };

        let mut batch = DiskWriteBatch::new();
        let snapshot_cf = zebra_db.snapshot_data_by_date_cf();
        batch.zs_insert(snapshot_cf, previous_date, previous_snapshot);
        batch.zs_insert(snapshot_cf, current_date, current_snapshot);
        zebra_db
            .write_batch(batch)
            .expect("daily snapshots should be written");

        assert_eq!(
            zebra_db.latest_daily_snapshot_before_height(current_date, Height(21)),
            Some((current_date, current_snapshot)),
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
            ironwood_tx_count: 0,
            ironwood_inflow: 0,
            ironwood_outflow: 0,
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
            pool_values: legacy_pool_values,
            ironwood_tx_count: 0,
            ironwood_inflow: 0,
            ironwood_outflow: 0,
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
