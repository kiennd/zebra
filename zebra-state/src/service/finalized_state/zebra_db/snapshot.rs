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

use std::collections::HashMap;

use rocksdb::ColumnFamily;
use zebra_chain::{
    amount::{Amount, MAX_MONEY, NonNegative},
    block::Height,
    parameters::{
        subsidy::block_subsidy,
        Network, NetworkUpgrade,
    },
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
    
    /// Format as string "YY:MM:DD"
    pub fn to_string(&self) -> String {
        format!("{:02}:{:02}:{:02}", self.year, self.month, self.day)
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
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct SnapshotData {
    /// Number of holders (addresses with non-zero balances).
    holder_count: u64,
    /// Pool values (value balance) at this height.
    pool_values: ValueBalance<NonNegative>,
    /// Mining work difficulty (as a multiple of the minimum difficulty, stored as f64).
    /// This matches the value returned by the `getdifficulty` RPC method.
    work_difficulty: f64,
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
    sprout_tx_count: u32,
    /// Number of sapling transactions (from previous snapshot to this snapshot).
    sapling_tx_count: u32,
    /// Number of orchard transactions (from previous snapshot to this snapshot).
    orchard_tx_count: u32,
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
    /// Average block time in seconds (from previous snapshot to this snapshot).
    /// Stored as f32 (4 bytes) to save space, precision is sufficient for block time.
    average_block_time: f32,
    /// Average transaction fee in zatoshis per block (from previous snapshot to this snapshot).
    average_block_fee_zat: u64,
    /// Average block size in bytes (from previous snapshot to this snapshot).
    average_block_size: u32,
}

impl SnapshotData {
    /// Creates a new SnapshotData.
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
        transparent_inflow: Amount<NonNegative>,
        transparent_outflow: Amount<NonNegative>,
        sprout_inflow: Amount<NonNegative>,
        sprout_outflow: Amount<NonNegative>,
        sapling_inflow: Amount<NonNegative>,
        sapling_outflow: Amount<NonNegative>,
        orchard_inflow: Amount<NonNegative>,
        orchard_outflow: Amount<NonNegative>,
        average_block_time: f32,
        average_block_fee_zat: Amount<NonNegative>,
        average_block_size: u32,
    ) -> Self {
        // Convert inflation rate to basis points (hundredths of a percent)
        let inflation_rate_bps = (inflation_rate_percent * 100.0).round() as u32;
        
        SnapshotData {
            holder_count,
            pool_values,
            work_difficulty,
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
            transparent_inflow: transparent_inflow.zatoshis() as u64,
            transparent_outflow: transparent_outflow.zatoshis() as u64,
            sprout_inflow: sprout_inflow.zatoshis() as u64,
            sprout_outflow: sprout_outflow.zatoshis() as u64,
            sapling_inflow: sapling_inflow.zatoshis() as u64,
            sapling_outflow: sapling_outflow.zatoshis() as u64,
            orchard_inflow: orchard_inflow.zatoshis() as u64,
            orchard_outflow: orchard_outflow.zatoshis() as u64,
            average_block_time,
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
        self.work_difficulty
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
    
    pub fn transparent_inflow(&self) -> Amount<NonNegative> {
        Amount::try_from(self.transparent_inflow).expect("transparent_inflow should be valid")
    }
    
    pub fn transparent_outflow(&self) -> Amount<NonNegative> {
        Amount::try_from(self.transparent_outflow).expect("transparent_outflow should be valid")
    }
    
    pub fn sprout_inflow(&self) -> Amount<NonNegative> {
        Amount::try_from(self.sprout_inflow).expect("sprout_inflow should be valid")
    }
    
    pub fn sprout_outflow(&self) -> Amount<NonNegative> {
        Amount::try_from(self.sprout_outflow).expect("sprout_outflow should be valid")
    }
    
    pub fn sapling_inflow(&self) -> Amount<NonNegative> {
        Amount::try_from(self.sapling_inflow).expect("sapling_inflow should be valid")
    }
    
    pub fn sapling_outflow(&self) -> Amount<NonNegative> {
        Amount::try_from(self.sapling_outflow).expect("sapling_outflow should be valid")
    }
    
    pub fn orchard_inflow(&self) -> Amount<NonNegative> {
        Amount::try_from(self.orchard_inflow).expect("orchard_inflow should be valid")
    }
    
    pub fn orchard_outflow(&self) -> Amount<NonNegative> {
        Amount::try_from(self.orchard_outflow).expect("orchard_outflow should be valid")
    }
    
    pub fn average_block_time(&self) -> f32 {
        self.average_block_time
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
        // 8 + 40 + 8 + 8 + 4 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 8*8 + 4 + 8 + 4 = 184 bytes total
        let mut bytes = Vec::with_capacity(184);
        bytes.extend_from_slice(&self.holder_count.to_be_bytes());
        bytes.extend_from_slice(&self.pool_values.as_bytes());
        bytes.extend_from_slice(&self.work_difficulty.to_be_bytes());
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
        bytes.extend_from_slice(&self.average_block_time.to_be_bytes());
        bytes.extend_from_slice(&self.average_block_fee_zat.to_be_bytes());
        bytes.extend_from_slice(&self.average_block_size.to_be_bytes());
        bytes
    }
}

impl FromDisk for SnapshotData {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        // Current format: 184 bytes
        // 8 + 40 + 8 + 8 + 4 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 8*8 + 4 + 8 + 4 = 184 bytes
        
        // Validate byte length
        if bytes.len() != 184 {
            panic!(
                "SnapshotData deserialization error: expected 184 bytes, got {} bytes. \
                This may indicate old database format or corrupted data. \
                Please clear the database and resync.",
                bytes.len()
            );
        }
        
        let holder_count = u64::from_be_bytes(
            bytes[0..8].try_into().expect("holder count must be 8 bytes")
        );
        let pool_values = ValueBalance::<NonNegative>::from_bytes(&bytes[8..48])
            .expect("pool values must be 40 bytes");
        let work_difficulty = f64::from_be_bytes(
            bytes[48..56].try_into().expect("work difficulty must be 8 bytes")
        );
        let total_issuance = u64::from_be_bytes(
            bytes[56..64].try_into().expect("total issuance must be 8 bytes")
        );
        let inflation_rate_bps = u32::from_be_bytes(
            bytes[64..68].try_into().expect("inflation rate must be 4 bytes")
        );
        let block_timestamp = i64::from_be_bytes(
            bytes[68..76].try_into().expect("block timestamp must be 8 bytes")
        );
        let block_height = u32::from_be_bytes(
            bytes[76..80].try_into().expect("block height must be 4 bytes")
        );
        
        // Transaction counts
        let transparent_tx_count = u32::from_be_bytes(bytes[80..84].try_into().expect("transparent tx count must be 4 bytes"));
        let transparent_coinbase_tx_count = u32::from_be_bytes(bytes[84..88].try_into().expect("transparent coinbase tx count must be 4 bytes"));
        let shielded_coinbase_migration_tx_count = u32::from_be_bytes(bytes[88..92].try_into().expect("shielded coinbase migration tx count must be 4 bytes"));
        let sprout_tx_count = u32::from_be_bytes(bytes[92..96].try_into().expect("sprout tx count must be 4 bytes"));
        let sapling_tx_count = u32::from_be_bytes(bytes[96..100].try_into().expect("sapling tx count must be 4 bytes"));
        let orchard_tx_count = u32::from_be_bytes(bytes[100..104].try_into().expect("orchard tx count must be 4 bytes"));
        
        // Inflow/outflow
        let transparent_inflow = u64::from_be_bytes(bytes[104..112].try_into().expect("transparent inflow must be 8 bytes"));
        let transparent_outflow = u64::from_be_bytes(bytes[112..120].try_into().expect("transparent outflow must be 8 bytes"));
        let sprout_inflow = u64::from_be_bytes(bytes[120..128].try_into().expect("sprout inflow must be 8 bytes"));
        let sprout_outflow = u64::from_be_bytes(bytes[128..136].try_into().expect("sprout outflow must be 8 bytes"));
        let sapling_inflow = u64::from_be_bytes(bytes[136..144].try_into().expect("sapling inflow must be 8 bytes"));
        let sapling_outflow = u64::from_be_bytes(bytes[144..152].try_into().expect("sapling outflow must be 8 bytes"));
        let orchard_inflow = u64::from_be_bytes(bytes[152..160].try_into().expect("orchard inflow must be 8 bytes"));
        let orchard_outflow = u64::from_be_bytes(bytes[160..168].try_into().expect("orchard outflow must be 8 bytes"));
        
        // Average block time
        let average_block_time = f32::from_be_bytes(bytes[168..172].try_into().expect("average block time must be 4 bytes"));
        
        // Average fee and block size
        let average_block_fee_zat = u64::from_be_bytes(bytes[172..180].try_into().expect("average block fee must be 8 bytes"));
        let average_block_size = u32::from_be_bytes(bytes[180..184].try_into().expect("average block size must be 4 bytes"));
        
        SnapshotData {
            holder_count,
            pool_values,
            work_difficulty,
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
            transparent_inflow,
            transparent_outflow,
            sprout_inflow,
            sprout_outflow,
            sapling_inflow,
            sapling_outflow,
            orchard_inflow,
            orchard_outflow,
            average_block_time,
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

    /// Calculate total ZEC issuance up to a given height.
    /// This sums all block subsidies from the end of slow start interval to the given height.
    /// 
    /// Note: Blocks in the slow start interval (height 0 to slow_start_interval) are not included
    /// because block_subsidy() doesn't support those heights. The slow start period represents
    /// a small portion of total issuance and is handled through checkpointing.
    fn calculate_total_issuance(
        &self,
        height: Height,
        network: &Network,
    ) -> Result<Amount<NonNegative>, BoxError> {
        use std::ops::Add;
        
        let mut total = Amount::zero();
        let slow_start_end = network.slow_start_interval();
        
        // Start from the end of slow start interval since block_subsidy doesn't support
        // heights in the slow start period (0 to slow_start_interval)
        let start_height = if height < slow_start_end {
            // If we're still in slow start, return zero (no supported subsidies yet)
            return Ok(Amount::zero());
        } else {
            slow_start_end.0
        };
        
        // Sum block subsidies from end of slow start to the given height
        for h in start_height..=height.0 {
            let block_height = Height(h);
            match block_subsidy(block_height, network) {
                Ok(subsidy) => {
                    total = total
                        .add(subsidy)
                        .map_err(|e| format!("overflow calculating total issuance at height {}: {}", h, e))?;
                }
                Err(e) => {
                    // Skip heights that don't support subsidy calculation
                    // This handles edge cases and future network upgrades
                    tracing::debug!(
                        ?block_height,
                        error = ?e,
                        "skipping block subsidy calculation for unsupported height"
                    );
                }
            }
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
        let blocks_per_year = if let Some(blossom_height) = NetworkUpgrade::Blossom.activation_height(network) {
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
    
    /// Combined calculation of transaction counts, pool flows, and block metrics in a single pass.
    /// This combines count_transactions_by_pool, calculate_pool_flows, and calculate_block_metrics
    /// into one iteration for better performance, while maintaining identical calculation logic.
    fn calculate_all_metrics_combined(
        &self,
        start_height: Height,
        end_height: Height,
        end_timestamp: i64,
    ) -> Result<(
        // Transaction counts: (transparent_count, transparent_coinbase_count, shielded_coinbase_migration_count, sprout_count, sapling_count, orchard_count)
        (u32, u32, u32, u32, u32, u32),
        // Pool flows: (transparent_inflow, transparent_outflow, sprout_inflow, sprout_outflow,
        //              sapling_inflow, sapling_outflow, orchard_inflow, orchard_outflow)
        (Amount<NonNegative>, Amount<NonNegative>, Amount<NonNegative>, Amount<NonNegative>,
         Amount<NonNegative>, Amount<NonNegative>, Amount<NonNegative>, Amount<NonNegative>),
        // Block metrics: (average_block_time_seconds, average_block_fee_zat, average_block_size_bytes)
        (f32, Amount<NonNegative>, u32),
    ), BoxError> {
        use std::ops::Add;
        use zebra_chain::transparent::Utxo;
        use zebra_chain::serialization::ZcashSerialize;
        
        // Transaction counts
        let mut transparent_count = 0u32;
        let mut transparent_coinbase_count = 0u32;
        let mut shielded_coinbase_migration_count = 0u32;
        let mut sprout_count = 0u32;
        let mut sapling_count = 0u32;
        let mut orchard_count = 0u32;
        
        // Pool flows - use u64 to avoid overflow, cap at MAX_MONEY when converting to Amount
        let mut transparent_inflow_zat = 0u64;
        let mut transparent_outflow_zat = 0u64;
        let mut sprout_inflow_zat = 0u64;
        let mut sprout_outflow_zat = 0u64;
        let mut sapling_inflow_zat = 0u64;
        let mut sapling_outflow_zat = 0u64;
        let mut orchard_inflow_zat = 0u64;
        let mut orchard_outflow_zat = 0u64;
        
        // Block metrics
        let mut total_fees = Amount::<NonNegative>::zero();
        let mut total_block_size = 0u64;
        let mut block_count = 0u32;
        
        // Get start timestamp for block time calculation
        let start_timestamp = if start_height.0 < end_height.0 {
            let start_block = self.block(start_height.into())
                .ok_or_else(|| format!("block at height {:?} not found", start_height))?;
            start_block.header.time.timestamp()
        } else {
            end_timestamp
        };
        
        // UTXO cache to reduce database lookups
        let mut utxo_cache: HashMap<zebra_chain::transparent::OutPoint, zebra_chain::transparent::Utxo> = HashMap::new();
        
        // Single pass through all blocks
        for h in start_height.0..=end_height.0 {
            let block_height = Height(h);
            let block = match self.block(block_height.into()) {
                Some(b) => b,
                None => {
                    tracing::debug!(?block_height, "block not found, skipping");
                    continue;
                }
            };
            
            // Calculate block size (same as calculate_block_metrics)
            let block_size = block.zcash_serialize_to_vec()
                .map_err(|e| format!("failed to serialize block at height {:?}: {}", block_height, e))?;
            total_block_size = total_block_size
                .checked_add(block_size.len() as u64)
                .ok_or_else(|| "overflow calculating total block size")?;
            
            // Build UTXO map for this block (for same-block references)
            // This is used in both pool flows and fee calculations
            let mut block_utxos = HashMap::new();
            for (tx_idx, tx) in block.transactions.iter().enumerate() {
                for (out_idx, output) in tx.outputs().iter().enumerate() {
                    let outpoint = zebra_chain::transparent::OutPoint {
                        hash: tx.hash(),
                        index: out_idx as u32,
                    };
                    let utxo = Utxo::from_location(
                        output.clone(),
                        block_height,
                        tx_idx,
                    );
                    block_utxos.insert(outpoint, utxo);
                }
            }
            
            // Process all transactions in this block
            for (tx_idx, transaction) in block.transactions.iter().enumerate() {
                // Check if this is a coinbase transaction (first transaction in block)
                let is_coinbase = tx_idx == 0;
                
                // === TRANSACTION COUNTING (same logic as count_transactions_by_pool) ===
                // Check for transparent activity
                let has_transparent = transaction.has_transparent_inputs() || transaction.has_transparent_outputs();
                let has_transparent_inputs = transaction.has_transparent_inputs();
                
                // Check for shielded outputs (sapling or orchard)
                let has_sapling_outputs = transaction.has_sapling_shielded_data();
                let has_orchard_outputs = transaction.has_orchard_shielded_data();
                let has_shielded_outputs = has_sapling_outputs || has_orchard_outputs;
                
                // Check for sprout joinsplit
                let has_sprout = transaction.has_sprout_joinsplit_data();
                
                // Count each transaction exactly once using priority order
                if is_coinbase && has_transparent {
                    // Priority 1: Transparent coinbase
                    transparent_coinbase_count = transparent_coinbase_count
                        .checked_add(1)
                        .ok_or_else(|| "transparent coinbase transaction count overflow")?;
                } else if !is_coinbase && has_transparent_inputs && has_shielded_outputs {
                    // Priority 2: Shielded coinbase migration (transparent inputs -> shielded outputs)
                    shielded_coinbase_migration_count = shielded_coinbase_migration_count
                        .checked_add(1)
                        .ok_or_else(|| "shielded coinbase migration transaction count overflow")?;
                } else if has_orchard_outputs {
                    // Priority 3: Orchard transactions
                    orchard_count = orchard_count
                        .checked_add(1)
                        .ok_or_else(|| "orchard transaction count overflow")?;
                } else if has_sapling_outputs {
                    // Priority 4: Sapling transactions
                    sapling_count = sapling_count
                        .checked_add(1)
                        .ok_or_else(|| "sapling transaction count overflow")?;
                } else if has_sprout {
                    // Priority 5: Sprout transactions
                    sprout_count = sprout_count
                        .checked_add(1)
                        .ok_or_else(|| "sprout transaction count overflow")?;
                } else if has_transparent {
                    // Priority 6: Regular transparent transactions
                    transparent_count = transparent_count
                        .checked_add(1)
                        .ok_or_else(|| "transparent transaction count overflow")?;
                }
                
                // === POOL FLOWS (same logic as calculate_pool_flows) ===
                // Transparent pool: sum outputs (inflow) and inputs (outflow)
                for output in transaction.outputs() {
                    let value_zat = output.value().zatoshis() as u64;
                    transparent_inflow_zat = transparent_inflow_zat
                        .saturating_add(value_zat);
                }
                
                // Try block UTXOs first (for same-block references), then cache, then database
                for input in transaction.inputs() {
                    if let Some(outpoint) = input.outpoint() {
                        let input_value = if let Some(utxo) = block_utxos.get(&outpoint) {
                            // UTXO from earlier transaction in same block
                            Some(utxo.output.value())
                        } else if let Some(cached_utxo) = utxo_cache.get(&outpoint) {
                            // UTXO from cache (previously looked up)
                            Some(cached_utxo.output.value())
                        } else {
                            // UTXO from previous blocks - look up from database and cache it
                            if let Some(ordered_utxo) = self.utxo(&outpoint) {
                                let value = ordered_utxo.utxo.output.value();
                                utxo_cache.insert(outpoint, ordered_utxo.utxo);
                                Some(value)
                            } else {
                                None
                            }
                        };
                        
                        if let Some(value) = input_value {
                            let value_zat = value.zatoshis() as u64;
                            transparent_outflow_zat = transparent_outflow_zat
                                .saturating_add(value_zat);
                        }
                    }
                }
                
                // Sprout pool: vpub_new (inflow) and vpub_old (outflow)
                for vpub_new in transaction.input_values_from_sprout() {
                    let value_zat = vpub_new.zatoshis() as u64;
                    sprout_inflow_zat = sprout_inflow_zat
                        .saturating_add(value_zat);
                }
                
                for vpub_old in transaction.output_values_to_sprout() {
                    let value_zat = vpub_old.zatoshis() as u64;
                    sprout_outflow_zat = sprout_outflow_zat
                        .saturating_add(value_zat);
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
                    sapling_inflow_zat = sapling_inflow_zat
                        .saturating_add(value_zat);
                } else if sapling_zatoshis > 0 {
                    // Net outflow: value leaving sapling pool
                    let value_zat = sapling_zatoshis as u64;
                    sapling_outflow_zat = sapling_outflow_zat
                        .saturating_add(value_zat);
                }
                
                // Orchard pool: value_balance represents net change
                // Negative value_balance = net inflow, positive = net outflow
                let orchard_vb = transaction.orchard_value_balance();
                let orchard_net = orchard_vb.orchard_amount();
                let orchard_zatoshis = orchard_net.zatoshis();
                if orchard_zatoshis < 0 {
                    // Net inflow: value entering orchard pool
                    let value_zat = (-orchard_zatoshis) as u64;
                    orchard_inflow_zat = orchard_inflow_zat
                        .saturating_add(value_zat);
                } else if orchard_zatoshis > 0 {
                    // Net outflow: value leaving orchard pool
                    let value_zat = orchard_zatoshis as u64;
                    orchard_outflow_zat = orchard_outflow_zat
                        .saturating_add(value_zat);
                }
                
                // === FEE CALCULATION (same logic as calculate_block_metrics) ===
                // Calculate fees for each transaction (excluding coinbase)
                if !is_coinbase {
                    // Calculate transparent inputs value
                    let mut input_value = Amount::<NonNegative>::zero();
                    for input in transaction.inputs() {
                        if let Some(outpoint) = input.outpoint() {
                            let input_val = if let Some(utxo) = block_utxos.get(&outpoint) {
                                Some(utxo.output.value())
                            } else if let Some(cached_utxo) = utxo_cache.get(&outpoint) {
                                Some(cached_utxo.output.value())
                            } else {
                                // Look up from database and cache it
                                if let Some(ordered_utxo) = self.utxo(&outpoint) {
                                    let value = ordered_utxo.utxo.output.value();
                                    utxo_cache.insert(outpoint, ordered_utxo.utxo);
                                    Some(value)
                                } else {
                                    None
                                }
                            };
                            
                            if let Some(value) = input_val {
                                input_value = input_value
                                    .add(value)
                                    .map_err(|e| format!("overflow calculating input value: {}", e))?;
                            }
                        }
                    }
                    
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
                    for vpub_old in transaction.output_values_to_sprout() {
                        sprout_vpub_old = sprout_vpub_old
                            .add(*vpub_old)
                            .map_err(|e| format!("overflow calculating sprout vpub_old: {}", e))?;
                    }
                    
                    let mut sprout_vpub_new = Amount::<NonNegative>::zero();
                    for vpub_new in transaction.input_values_from_sprout() {
                        sprout_vpub_new = sprout_vpub_new
                            .add(*vpub_new)
                            .map_err(|e| format!("overflow calculating sprout vpub_new: {}", e))?;
                    }
                    
                    // Sprout value balance (signed): vpub_new - vpub_old
                    // Positive = value leaving sprout (deshielding), Negative = value entering sprout (shielding)
                    let sprout_value_balance_zatoshis = if sprout_vpub_new > sprout_vpub_old {
                        // Value leaving sprout (positive)
                        (sprout_vpub_new - sprout_vpub_old)
                            .map_err(|e| format!("error calculating sprout value balance: {}", e))?
                            .zatoshis() as i64
                    } else if sprout_vpub_old > sprout_vpub_new {
                        // Value entering sprout (negative)
                        -((sprout_vpub_old - sprout_vpub_new)
                            .map_err(|e| format!("error calculating sprout value balance: {}", e))?
                            .zatoshis() as i64)
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
                    
                    // Calculate transparent value balance (signed)
                    // This is: transparent_inputs - transparent_outputs
                    let transparent_value_balance_zatoshis = if input_value > output_value {
                        // Positive: more inputs than outputs
                        (input_value - output_value)
                            .map_err(|e| format!("error calculating transparent value balance: {}", e))?
                            .zatoshis() as i64
                    } else if output_value > input_value {
                        // Negative: more outputs than inputs (valid when there's shielding)
                        -((output_value - input_value)
                            .map_err(|e| format!("error calculating transparent value balance: {}", e))?
                            .zatoshis() as i64)
                    } else {
                        // Zero: inputs equal outputs
                        0i64
                    };
                    
                    // Calculate fee using the consensus formula from value_balance.rs:
                    // remaining_transaction_value = transparent + sprout + sapling + orchard
                    //
                    // This is the sum of all value balances:
                    // - transparent: transparent_inputs - transparent_outputs
                    // - sprout: vpub_new - vpub_old
                    // - sapling: value_balance (negative = shielding, positive = deshielding)
                    // - orchard: value_balance (negative = shielding, positive = deshielding)
                    //
                    // The remaining value is the transaction fee (for non-coinbase transactions)
                    let fee_zatoshis = transparent_value_balance_zatoshis
                        .saturating_add(sprout_value_balance_zatoshis)
                        .saturating_add(sapling_zatoshis)
                        .saturating_add(orchard_zatoshis);
                    
                    // Only add positive fees
                    if fee_zatoshis > 0 {
                        if let Ok(fee_amount) = Amount::<NonNegative>::try_from(fee_zatoshis as u64) {
                            total_fees = total_fees
                                .add(fee_amount)
                                .map_err(|e| format!("overflow calculating total fees: {}", e))?;
                        }
                    }
                }
            }
            
            block_count = block_count
                .checked_add(1)
                .ok_or_else(|| "block count overflow")?;
        }
        
        // Calculate averages (same logic as calculate_block_metrics)
        let average_block_time = if block_count > 0 && start_height.0 < end_height.0 {
            let time_diff = end_timestamp - start_timestamp;
            (time_diff as f32) / (block_count as f32)
        } else {
            0.0
        };
        
        let average_fee = if block_count > 0 {
            // Divide total fees by number of blocks
            // Since Amount doesn't support division directly, we'll use zatoshis
            let total_fees_zat = total_fees.zatoshis() as u64;
            let avg_fee_zat = total_fees_zat / (block_count as u64);
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
        
        // Convert u64 values to Amount, capping at MAX_MONEY to prevent overflow
        // Cumulative flow values can exceed MAX_MONEY when summing across many blocks,
        // so we cap them at the maximum valid Amount value
        let max_money_u64 = MAX_MONEY as u64;
        let transparent_inflow = Amount::try_from(transparent_inflow_zat.min(max_money_u64))
            .map_err(|e| format!("failed to create transparent_inflow amount: {}", e))?;
        let transparent_outflow = Amount::try_from(transparent_outflow_zat.min(max_money_u64))
            .map_err(|e| format!("failed to create transparent_outflow amount: {}", e))?;
        let sprout_inflow = Amount::try_from(sprout_inflow_zat.min(max_money_u64))
            .map_err(|e| format!("failed to create sprout_inflow amount: {}", e))?;
        let sprout_outflow = Amount::try_from(sprout_outflow_zat.min(max_money_u64))
            .map_err(|e| format!("failed to create sprout_outflow amount: {}", e))?;
        let sapling_inflow = Amount::try_from(sapling_inflow_zat.min(max_money_u64))
            .map_err(|e| format!("failed to create sapling_inflow amount: {}", e))?;
        let sapling_outflow = Amount::try_from(sapling_outflow_zat.min(max_money_u64))
            .map_err(|e| format!("failed to create sapling_outflow amount: {}", e))?;
        let orchard_inflow = Amount::try_from(orchard_inflow_zat.min(max_money_u64))
            .map_err(|e| format!("failed to create orchard_inflow amount: {}", e))?;
        let orchard_outflow = Amount::try_from(orchard_outflow_zat.min(max_money_u64))
            .map_err(|e| format!("failed to create orchard_outflow amount: {}", e))?;
        
        Ok((
            (transparent_count, transparent_coinbase_count, shielded_coinbase_migration_count, sprout_count, sapling_count, orchard_count),
            (transparent_inflow, transparent_outflow, sprout_inflow, sprout_outflow,
             sapling_inflow, sapling_outflow, orchard_inflow, orchard_outflow),
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
        let block = self.block(height.into())
            .ok_or_else(|| format!("block at height {:?} not found", height))?;
        let header = &block.header;
        
        // 4. Calculate work difficulty (matches RPC getdifficulty)
        // Get expanded difficulty from block header
        let expanded_difficulty = header.difficulty_threshold
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
        
        // 6. Calculate total issuance and inflation rate
        // Note: block_subsidy() doesn't support heights in the slow start interval (0-20,000),
        // so we set these to zero for those heights.
        let slow_start_end = network.slow_start_interval();
        let (total_issuance, inflation_rate) = if height < slow_start_end {
            // Heights in slow start interval: set issuance and inflation to zero
            (
                Amount::<NonNegative>::zero(),
                0.0,
            )
        } else {
            // Heights after slow start: calculate normally
            let calculated_issuance = self.calculate_total_issuance(height, network)?;
            
            // Calculate inflation rate using pool values as total supply
            // Pool values represent the actual monetary base (all ZEC in circulation),
            // which is more accurate than calculated issuance (which excludes slow start).
            let total_supply = (pool_values.transparent_amount()
                + pool_values.sprout_amount()
                + pool_values.sapling_amount()
                + pool_values.orchard_amount()
                + pool_values.deferred_amount())
                .map_err(|e| format!("overflow calculating total supply from pool values: {}", e))?;
            let calculated_inflation = self.calculate_inflation_rate(height, network, total_supply)?;
            
            (calculated_issuance, calculated_inflation)
        };
        
        // 8. Find the previous snapshot height to avoid double counting blocks
        // Find the snapshot of the previous day (yesterday), not the highest height
        // Only consider daily snapshots (exclude realtime snapshots)
        // If previous day snapshot is not found in the 10 most recent snapshots, panic
        let previous_snapshot_height = {
            // Calculate the date of the previous day (yesterday)
            let current_date = chrono::DateTime::from_timestamp(block_timestamp, 0)
                .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap())
                .date_naive();
            let previous_date = current_date - chrono::Duration::days(1);
            let previous_date_key = SnapshotDateKey::from_timestamp(
                previous_date.and_hms_opt(0, 0, 0)
                    .expect("00:00:00 should always be valid")
                    .and_utc()
                    .timestamp()
            );
            
            // Get daily snapshots directly from column family (excludes realtime)
            let typed_cf = TypedColumnFamily::<SnapshotDateKey, SnapshotData>::new(
                &self.db,
                SNAPSHOT_DATA_BY_DATE,
            )
            .expect("column family was created when database was created");
            
            // Get the 10 most recent snapshots and find previous day snapshot in one pass
            let recent_snapshots: Vec<(SnapshotDateKey, SnapshotData)> = typed_cf
                .zs_reverse_range_iter(..)
                .take(10)
                .collect();
            
            // If no snapshots exist, this is the first snapshot - return None (will start from height 0)
            if recent_snapshots.is_empty() {
                tracing::debug!(
                    ?height,
                    ?block_timestamp,
                    current_date = %current_date,
                    previous_date = %previous_date,
                    "no previous snapshots found, this is the first snapshot"
                );
                None
            } else {
                // Find previous day snapshot in one pass
                if let Some((_, previous_snapshot)) = recent_snapshots.iter()
                    .find(|(date_key, _)| *date_key == previous_date_key) {
                    let prev_height = previous_snapshot.block_height();
                    if prev_height < height.0 {
                        tracing::debug!(
                            ?height,
                            ?block_timestamp,
                            current_date = %current_date,
                            previous_date = %previous_date,
                            previous_snapshot_height = ?prev_height,
                            "found previous day snapshot"
                        );
                        Some(Height(prev_height))
                    } else {
                        tracing::warn!(
                            ?height,
                            ?block_timestamp,
                            previous_snapshot_height = ?prev_height,
                            "previous day snapshot height >= current height, ignoring"
                        );
                        None
                    }
                } else {
                    // Previous day snapshot not found in 10 most recent snapshots - panic
                    // This should only happen if there are snapshots but previous day is missing
                    panic!(
                        "CRITICAL: Previous day snapshot not found in 10 most recent snapshots. \
                        Current height: {:?}, Current date: {}, Previous date: {}, \
                        Previous date key: {:?}, Recent snapshots checked: {}, \
                        Recent snapshot dates: {:?}",
                        height,
                        current_date,
                        previous_date,
                        previous_date_key,
                        recent_snapshots.len(),
                        recent_snapshots.iter().map(|(k, _)| k).collect::<Vec<_>>()
                    );
                }
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
        let ((transparent_tx_count, transparent_coinbase_tx_count, shielded_coinbase_migration_tx_count, sprout_tx_count, sapling_tx_count, orchard_tx_count),
             (transparent_inflow, transparent_outflow, sprout_inflow, sprout_outflow,
              sapling_inflow, sapling_outflow, orchard_inflow, orchard_outflow),
             (average_block_time, average_block_fee_zat, average_block_size)) =
            self.calculate_all_metrics_combined(start_height, height, block_timestamp)?;
        
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
            transparent_inflow,
            transparent_outflow,
            sprout_inflow,
            sprout_outflow,
            sapling_inflow,
            sapling_outflow,
            orchard_inflow,
            orchard_outflow,
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
            batch.zs_insert(realtime_snapshot_cf, &REALTIME_SNAPSHOT_KEY, &snapshot_data);
        } else {
            // Daily snapshot: use date key (YY:MM:DD format)
            let date_key = SnapshotDateKey::from_timestamp(block_timestamp);
            let snapshot_cf = self.snapshot_data_by_date_cf();
            batch.zs_insert(snapshot_cf, &date_key, &snapshot_data);
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
            transparent_inflow_zat = transparent_inflow.zatoshis(),
            transparent_outflow_zat = transparent_outflow.zatoshis(),
            sprout_inflow_zat = sprout_inflow.zatoshis(),
            sprout_outflow_zat = sprout_outflow.zatoshis(),
            sapling_inflow_zat = sapling_inflow.zatoshis(),
            sapling_outflow_zat = sapling_outflow.zatoshis(),
            orchard_inflow_zat = orchard_inflow.zatoshis(),
            orchard_outflow_zat = orchard_outflow.zatoshis(),
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
            let realtime_date_key = SnapshotDateKey::from_timestamp(realtime_data.block_timestamp());
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
        let mut snapshots: Vec<(SnapshotDateKey, SnapshotData)> = typed_cf
            .zs_reverse_range_iter(..)
            .take(limit)
            .collect();

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
        let mut snapshots: Vec<(SnapshotDateKey, SnapshotData)> = typed_cf
            .zs_reverse_range_iter(..)
            .take(limit)
            .collect();

        // Reverse to get ascending order by date
        snapshots.reverse();
        
        // Merge with realtime snapshot if it exists
        if let Some(realtime_data) = self.get_realtime_snapshot() {
            let realtime_date_key = SnapshotDateKey::from_timestamp(realtime_data.block_timestamp());
            
            // Check if realtime snapshot date already exists in daily snapshots
            if let Some(existing_idx) = snapshots.iter().position(|(key, _)| *key == realtime_date_key) {
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
            (Some(start), Some(end)) => {
                typed_cf
                    .zs_forward_range_iter(start..=end)
                    .collect()
            }
            (Some(start), None) => {
                typed_cf
                    .zs_forward_range_iter(start..)
                    .collect()
            }
            (None, Some(end)) => {
                typed_cf
                    .zs_forward_range_iter(..=end)
                    .collect()
            }
            (None, None) => {
                typed_cf
                    .zs_forward_range_iter(..)
                    .collect()
            }
        };
        
        // Merge with realtime snapshot if it's in the range
        if let Some(realtime_data) = self.get_realtime_snapshot() {
            let realtime_date_key = SnapshotDateKey::from_timestamp(realtime_data.block_timestamp());
            
            // Check if realtime snapshot is in the range
            let in_range = match (start_date, end_date) {
                (Some(start), Some(end)) => realtime_date_key >= start && realtime_date_key <= end,
                (Some(start), None) => realtime_date_key >= start,
                (None, Some(end)) => realtime_date_key <= end,
                (None, None) => true,
            };
            
            if in_range {
                // Check if realtime snapshot date already exists in daily snapshots
                if let Some(existing_idx) = snapshots.iter().position(|(key, _)| *key == realtime_date_key) {
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

