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
    amount::{Amount, NonNegative},
    block::Height,
    parameters::{
        subsidy::block_subsidy,
        Network, NetworkUpgrade,
    },
    serialization::BytesInDisplayOrder,
    value_balance::ValueBalance,
};

use crate::{
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        zebra_db::ZebraDb,
    },
    BoxError, FromDisk, IntoDisk,
};

use super::super::TypedColumnFamily;

/// The name of the snapshot data by block height column family.
/// Stores holder count, pool values, difficulty, issuance, inflation rate, and timestamp.
pub const SNAPSHOT_DATA_BY_HEIGHT: &str = "snapshot_data_by_height";

/// Snapshot data stored in RocksDB containing holder count, pool values, difficulty, issuance, timestamp, and transaction counts.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct SnapshotData {
    /// Number of holders (addresses with non-zero balances).
    holder_count: u64,
    /// Pool values (value balance) at this height.
    pool_values: ValueBalance<NonNegative>,
    /// Mining difficulty (expanded difficulty as U256, stored as 32 bytes in big-endian).
    difficulty: [u8; 32],
    /// Total ZEC issuance up to this height (in zatoshis).
    total_issuance: u64,
    /// Inflation rate per year (in basis points, e.g., 200 = 2.00%).
    /// Stored as u32: 0 = 0%, 10000 = 100.00%.
    inflation_rate_bps: u32,
    /// Block timestamp (Unix timestamp in seconds).
    block_timestamp: i64,
    /// Number of transparent transactions (from previous snapshot to this snapshot).
    transparent_tx_count: u32,
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
    average_fee_zat: u64,
    /// Average block size in bytes (from previous snapshot to this snapshot).
    average_block_size: u32,
}

impl SnapshotData {
    /// Creates a new SnapshotData.
    pub fn new(
        holder_count: u64,
        pool_values: ValueBalance<NonNegative>,
        difficulty_bytes: [u8; 32],
        total_issuance: Amount<NonNegative>,
        inflation_rate_percent: f64,
        block_timestamp: i64,
        transparent_tx_count: u32,
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
        average_fee_zat: Amount<NonNegative>,
        average_block_size: u32,
    ) -> Self {
        // Convert inflation rate to basis points (hundredths of a percent)
        let inflation_rate_bps = (inflation_rate_percent * 100.0).round() as u32;
        
        SnapshotData {
            holder_count,
            pool_values,
            difficulty: difficulty_bytes,
            total_issuance: total_issuance.zatoshis() as u64,
            inflation_rate_bps,
            block_timestamp,
            transparent_tx_count,
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
            average_fee_zat: average_fee_zat.zatoshis() as u64,
            average_block_size,
        }
    }

    pub fn holder_count(&self) -> u64 {
        self.holder_count
    }

    pub fn pool_values(&self) -> ValueBalance<NonNegative> {
        self.pool_values
    }
    
    pub fn difficulty_bytes(&self) -> [u8; 32] {
        self.difficulty
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
    
    pub fn transparent_tx_count(&self) -> u32 {
        self.transparent_tx_count
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
    
    pub fn average_fee_zat(&self) -> Amount<NonNegative> {
        Amount::try_from(self.average_fee_zat).expect("average_fee_zat should be valid")
    }
    
    pub fn average_block_size(&self) -> u32 {
        self.average_block_size
    }
}

impl IntoDisk for SnapshotData {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        // 8 + 40 + 32 + 8 + 4 + 8 + 4 + 4 + 4 + 4 + 8*8 + 4 + 8 + 4 = 196 bytes total
        let mut bytes = Vec::with_capacity(196);
        bytes.extend_from_slice(&self.holder_count.to_be_bytes());
        bytes.extend_from_slice(&self.pool_values.as_bytes());
        bytes.extend_from_slice(&self.difficulty);
        bytes.extend_from_slice(&self.total_issuance.to_be_bytes());
        bytes.extend_from_slice(&self.inflation_rate_bps.to_be_bytes());
        bytes.extend_from_slice(&self.block_timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.transparent_tx_count.to_be_bytes());
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
        bytes.extend_from_slice(&self.average_fee_zat.to_be_bytes());
        bytes.extend_from_slice(&self.average_block_size.to_be_bytes());
        bytes
    }
}

impl FromDisk for SnapshotData {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        // Support old formats: 100 bytes (original), 116 bytes (with tx counts), 180 bytes (with inflow/outflow), 
        // 184 bytes (with avg block time), 196 bytes (with avg fee and block size)
        let has_tx_counts = bytes.len() >= 116;
        let has_inflow_outflow = bytes.len() >= 180;
        let has_avg_block_time = bytes.len() >= 184;
        let has_avg_fee_size = bytes.len() >= 196;
        
        let holder_count = u64::from_be_bytes(
            bytes[0..8].try_into().expect("holder count must be 8 bytes")
        );
        let pool_values = ValueBalance::<NonNegative>::from_bytes(&bytes[8..48])
            .expect("pool values must be 40 bytes");
        let difficulty: [u8; 32] = bytes[48..80].try_into().expect("difficulty must be 32 bytes");
        let total_issuance = u64::from_be_bytes(
            bytes[80..88].try_into().expect("total issuance must be 8 bytes")
        );
        let inflation_rate_bps = u32::from_be_bytes(
            bytes[88..92].try_into().expect("inflation rate must be 4 bytes")
        );
        let block_timestamp = i64::from_be_bytes(
            bytes[92..100].try_into().expect("block timestamp must be 8 bytes")
        );
        
        // Transaction counts (new fields, default to 0 for old format)
        let (transparent_tx_count, sprout_tx_count, sapling_tx_count, orchard_tx_count) = if has_tx_counts {
            (
                u32::from_be_bytes(bytes[100..104].try_into().expect("transparent tx count must be 4 bytes")),
                u32::from_be_bytes(bytes[104..108].try_into().expect("sprout tx count must be 4 bytes")),
                u32::from_be_bytes(bytes[108..112].try_into().expect("sapling tx count must be 4 bytes")),
                u32::from_be_bytes(bytes[112..116].try_into().expect("orchard tx count must be 4 bytes")),
            )
        } else {
            (0, 0, 0, 0)
        };
        
        // Inflow/outflow (new fields, default to 0 for old format)
        let (transparent_inflow, transparent_outflow, sprout_inflow, sprout_outflow,
             sapling_inflow, sapling_outflow, orchard_inflow, orchard_outflow) = if has_inflow_outflow {
            (
                u64::from_be_bytes(bytes[116..124].try_into().expect("transparent inflow must be 8 bytes")),
                u64::from_be_bytes(bytes[124..132].try_into().expect("transparent outflow must be 8 bytes")),
                u64::from_be_bytes(bytes[132..140].try_into().expect("sprout inflow must be 8 bytes")),
                u64::from_be_bytes(bytes[140..148].try_into().expect("sprout outflow must be 8 bytes")),
                u64::from_be_bytes(bytes[148..156].try_into().expect("sapling inflow must be 8 bytes")),
                u64::from_be_bytes(bytes[156..164].try_into().expect("sapling outflow must be 8 bytes")),
                u64::from_be_bytes(bytes[164..172].try_into().expect("orchard inflow must be 8 bytes")),
                u64::from_be_bytes(bytes[172..180].try_into().expect("orchard outflow must be 8 bytes")),
            )
        } else {
            (0, 0, 0, 0, 0, 0, 0, 0)
        };
        
        // Average block time (new field, default to 0.0 for old format)
        let average_block_time = if has_avg_block_time {
            f32::from_be_bytes(bytes[180..184].try_into().expect("average block time must be 4 bytes"))
        } else {
            0.0
        };
        
        // Average fee and block size (new fields, default to 0 for old format)
        let (average_fee_zat, average_block_size) = if has_avg_fee_size {
            (
                u64::from_be_bytes(bytes[184..192].try_into().expect("average fee must be 8 bytes")),
                u32::from_be_bytes(bytes[192..196].try_into().expect("average block size must be 4 bytes")),
            )
        } else {
            (0, 0)
        };
        
        SnapshotData {
            holder_count,
            pool_values,
            difficulty,
            total_issuance,
            inflation_rate_bps,
            block_timestamp,
            transparent_tx_count,
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
            average_fee_zat,
            average_block_size,
        }
    }
}

impl ZebraDb {
    /// Returns a handle to the `snapshot_data_by_height` RocksDB column family.
    pub fn snapshot_data_by_height_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(SNAPSHOT_DATA_BY_HEIGHT).unwrap()
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
    pub fn recent_holder_count_snapshots(&self, limit: usize) -> Vec<(Height, u64)> {
        self.recent_snapshot_data(limit)
            .into_iter()
            .map(|(height, snapshot_data)| (height, snapshot_data.holder_count()))
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
    
    /// Count transactions per pool between two heights (inclusive).
    /// 
    /// Returns (transparent_count, sprout_count, sapling_count, orchard_count).
    /// Counts transactions that have any activity in each pool type.
    fn count_transactions_by_pool(
        &self,
        start_height: Height,
        end_height: Height,
    ) -> Result<(u32, u32, u32, u32), BoxError> {
        let mut transparent_count = 0u32;
        let mut sprout_count = 0u32;
        let mut sapling_count = 0u32;
        let mut orchard_count = 0u32;
        
        // Iterate through all blocks from start_height to end_height (inclusive)
        for h in start_height.0..=end_height.0 {
            let block_height = Height(h);
            let block = match self.block(block_height.into()) {
                Some(b) => b,
                None => {
                    tracing::debug!(?block_height, "block not found, skipping transaction count");
                    continue;
                }
            };
            
            // Count transactions in each pool
            for transaction in &block.transactions {
                // A transaction can belong to multiple pools, so we check each one
                if transaction.has_transparent_inputs() || transaction.has_transparent_outputs() {
                    transparent_count = transparent_count
                        .checked_add(1)
                        .ok_or_else(|| "transparent transaction count overflow")?;
                }
                
                if transaction.has_sprout_joinsplit_data() {
                    sprout_count = sprout_count
                        .checked_add(1)
                        .ok_or_else(|| "sprout transaction count overflow")?;
                }
                
                if transaction.has_sapling_shielded_data() {
                    sapling_count = sapling_count
                        .checked_add(1)
                        .ok_or_else(|| "sapling transaction count overflow")?;
                }
                
                if transaction.has_orchard_shielded_data() {
                    orchard_count = orchard_count
                        .checked_add(1)
                        .ok_or_else(|| "orchard transaction count overflow")?;
                }
            }
        }
        
        Ok((transparent_count, sprout_count, sapling_count, orchard_count))
    }
    
    /// Calculate inflow and outflow for each pool between two heights (inclusive).
    /// 
    /// Returns (transparent_inflow, transparent_outflow, sprout_inflow, sprout_outflow,
    ///          sapling_inflow, sapling_outflow, orchard_inflow, orchard_outflow).
    /// 
    /// Inflow = value entering the pool
    /// Outflow = value leaving the pool
    fn calculate_pool_flows(
        &self,
        start_height: Height,
        end_height: Height,
    ) -> Result<(
        Amount<NonNegative>,
        Amount<NonNegative>,
        Amount<NonNegative>,
        Amount<NonNegative>,
        Amount<NonNegative>,
        Amount<NonNegative>,
        Amount<NonNegative>,
        Amount<NonNegative>,
    ), BoxError> {
        use std::ops::Add;
        use zebra_chain::transparent::Utxo;
        
        let mut transparent_inflow = Amount::<NonNegative>::zero();
        let mut transparent_outflow = Amount::<NonNegative>::zero();
        let mut sprout_inflow = Amount::<NonNegative>::zero();
        let mut sprout_outflow = Amount::<NonNegative>::zero();
        let mut sapling_inflow = Amount::<NonNegative>::zero();
        let mut sapling_outflow = Amount::<NonNegative>::zero();
        let mut orchard_inflow = Amount::<NonNegative>::zero();
        let mut orchard_outflow = Amount::<NonNegative>::zero();
        
        // Iterate through all blocks from start_height to end_height (inclusive)
        for h in start_height.0..=end_height.0 {
            let block_height = Height(h);
            let block = match self.block(block_height.into()) {
                Some(b) => b,
                None => {
                    tracing::debug!(?block_height, "block not found, skipping flow calculation");
                    continue;
                }
            };
            
            // Build UTXO map for transparent value balance calculation
            // We need to track UTXOs created in previous transactions in this block
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
            
            // Calculate flows for each transaction
            for transaction in &block.transactions {
                // Transparent pool: sum outputs (inflow) and inputs (outflow)
                for output in transaction.outputs() {
                    let value = output.value();
                    transparent_inflow = transparent_inflow
                        .add(value)
                        .map_err(|e| format!("overflow calculating transparent inflow: {}", e))?;
                }
                
                // Try block UTXOs first (for same-block references), then database
                for input in transaction.inputs() {
                    if let Some(outpoint) = input.outpoint() {
                        let input_value = if let Some(utxo) = block_utxos.get(&outpoint) {
                            // UTXO from earlier transaction in same block
                            Some(utxo.output.value())
                        } else {
                            // UTXO from previous blocks - look up from database
                            self.utxo(&outpoint)
                                .map(|ordered_utxo| ordered_utxo.utxo.output.value())
                        };
                        
                        if let Some(value) = input_value {
                            transparent_outflow = transparent_outflow
                                .add(value)
                                .map_err(|e| format!("overflow calculating transparent outflow: {}", e))?;
                        }
                    }
                }
                
                // Sprout pool: vpub_new (inflow) and vpub_old (outflow)
                for vpub_new in transaction.input_values_from_sprout() {
                    sprout_inflow = sprout_inflow
                        .add(*vpub_new)
                        .map_err(|e| format!("overflow calculating sprout inflow: {}", e))?;
                }
                
                for vpub_old in transaction.output_values_to_sprout() {
                    sprout_outflow = sprout_outflow
                        .add(*vpub_old)
                        .map_err(|e| format!("overflow calculating sprout outflow: {}", e))?;
                }
                
                // Sapling pool: value_balance represents net change
                // Negative value_balance = net inflow, positive = net outflow
                let sapling_vb = transaction.sapling_value_balance();
                let sapling_net = sapling_vb.sapling_amount();
                let sapling_zatoshis = sapling_net.zatoshis();
                if sapling_zatoshis < 0 {
                    // Net inflow: value entering sapling pool
                    // Convert negative to positive for NonNegative
                    if let Ok(neg_amount) = Amount::<NonNegative>::try_from((-sapling_zatoshis) as u64) {
                        sapling_inflow = sapling_inflow
                            .add(neg_amount)
                            .map_err(|e| format!("overflow calculating sapling inflow: {}", e))?;
                    }
                } else if sapling_zatoshis > 0 {
                    // Net outflow: value leaving sapling pool
                    if let Ok(pos_amount) = Amount::<NonNegative>::try_from(sapling_zatoshis as u64) {
                        sapling_outflow = sapling_outflow
                            .add(pos_amount)
                            .map_err(|e| format!("overflow calculating sapling outflow: {}", e))?;
                    }
                }
                
                // Orchard pool: value_balance represents net change
                // Negative value_balance = net inflow, positive = net outflow
                let orchard_vb = transaction.orchard_value_balance();
                let orchard_net = orchard_vb.orchard_amount();
                let orchard_zatoshis = orchard_net.zatoshis();
                if orchard_zatoshis < 0 {
                    // Net inflow: value entering orchard pool
                    if let Ok(neg_amount) = Amount::<NonNegative>::try_from((-orchard_zatoshis) as u64) {
                        orchard_inflow = orchard_inflow
                            .add(neg_amount)
                            .map_err(|e| format!("overflow calculating orchard inflow: {}", e))?;
                    }
                } else if orchard_zatoshis > 0 {
                    // Net outflow: value leaving orchard pool
                    if let Ok(pos_amount) = Amount::<NonNegative>::try_from(orchard_zatoshis as u64) {
                        orchard_outflow = orchard_outflow
                            .add(pos_amount)
                            .map_err(|e| format!("overflow calculating orchard outflow: {}", e))?;
                    }
                }
            }
        }
        
        Ok((
            transparent_inflow,
            transparent_outflow,
            sprout_inflow,
            sprout_outflow,
            sapling_inflow,
            sapling_outflow,
            orchard_inflow,
            orchard_outflow,
        ))
    }
    
    /// Calculate average block time, average fee, and average block size between two heights (inclusive).
    /// 
    /// Returns (average_block_time_seconds, average_fee_zat, average_block_size_bytes).
    fn calculate_block_metrics(
        &self,
        start_height: Height,
        end_height: Height,
        end_timestamp: i64,
    ) -> Result<(f32, Amount<NonNegative>, u32), BoxError> {
        use std::ops::Add;
        use zebra_chain::transparent::Utxo;
        use zebra_chain::serialization::ZcashSerialize;
        
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
        
        // Iterate through all blocks from start_height to end_height (inclusive)
        for h in start_height.0..=end_height.0 {
            let block_height = Height(h);
            let block = match self.block(block_height.into()) {
                Some(b) => b,
                None => {
                    tracing::debug!(?block_height, "block not found, skipping metrics calculation");
                    continue;
                }
            };
            
            // Calculate block size
            let block_size = block.zcash_serialize_to_vec()
                .map_err(|e| format!("failed to serialize block at height {:?}: {}", block_height, e))?;
            total_block_size = total_block_size
                .checked_add(block_size.len() as u64)
                .ok_or_else(|| "overflow calculating total block size")?;
            
            // Build UTXO map for fee calculation
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
            
            // Calculate fees for each transaction (excluding coinbase)
            for (tx_idx, transaction) in block.transactions.iter().enumerate() {
                // Skip coinbase transaction (first transaction in block)
                if tx_idx == 0 {
                    continue;
                }
                
                // Calculate transparent inputs value
                let mut input_value = Amount::<NonNegative>::zero();
                for input in transaction.inputs() {
                    if let Some(outpoint) = input.outpoint() {
                        let input_val = if let Some(utxo) = block_utxos.get(&outpoint) {
                            Some(utxo.output.value())
                        } else {
                            self.utxo(&outpoint)
                                .map(|ordered_utxo| ordered_utxo.utxo.output.value())
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
                
                // Fee = inputs - outputs (for transparent transactions)
                if input_value > output_value {
                    let fee = (input_value - output_value)
                        .map_err(|e| format!("error calculating fee: {}", e))?;
                    if let Ok(fee_non_neg) = Amount::<NonNegative>::try_from(fee.zatoshis() as u64) {
                        total_fees = total_fees
                            .add(fee_non_neg)
                            .map_err(|e| format!("overflow calculating total fees: {}", e))?;
                    }
                }
            }
            
            block_count = block_count
                .checked_add(1)
                .ok_or_else(|| "block count overflow")?;
        }
        
        // Calculate averages
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
        
        Ok((average_block_time, average_fee, average_block_size))
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
    ) -> Result<(), BoxError> {
        // 1. Count holders
        let holder_count = self.holder_count();
        
        // 2. Get pool values
        let pool_values = self.finalized_value_pool();
        
        // 3. Get block header for difficulty and timestamp
        let block = self.block(height.into())
            .ok_or_else(|| format!("block at height {:?} not found", height))?;
        let header = &block.header;
        
        // 4. Get mining difficulty (expanded difficulty)
        let difficulty = header.difficulty_threshold
            .to_expanded()
            .ok_or_else(|| "invalid difficulty threshold".to_string())?;
        
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
        
        // 8. Convert difficulty to bytes (big-endian)
        let difficulty_bytes = difficulty.bytes_in_serialized_order();
        
        // 9. Count transactions per pool from previous snapshot to current snapshot
        // Previous snapshot is at height - 1000 (or 0 if this is the first snapshot)
        let previous_snapshot_height = if height.0 >= 1000 {
            Height(height.0 - 1000)
        } else {
            Height(0)
        };
        let (transparent_tx_count, sprout_tx_count, sapling_tx_count, orchard_tx_count) =
            self.count_transactions_by_pool(previous_snapshot_height, height)?;
        
        // 10. Calculate inflow/outflow for each pool
        let (transparent_inflow, transparent_outflow, sprout_inflow, sprout_outflow,
             sapling_inflow, sapling_outflow, orchard_inflow, orchard_outflow) =
            self.calculate_pool_flows(previous_snapshot_height, height)?;
        
        // 11. Calculate average block time, average fee, and average block size
        let (average_block_time, average_fee_zat, average_block_size) = 
            self.calculate_block_metrics(previous_snapshot_height, height, block_timestamp)?;
        
        // 12. Create snapshot data
        let snapshot_data = SnapshotData::new(
            holder_count as u64,
            pool_values,
            difficulty_bytes,
            total_issuance,
            inflation_rate,
            block_timestamp,
            transparent_tx_count,
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
            average_fee_zat,
            average_block_size,
        );

        // 14. Store in RocksDB
        let snapshot_cf = self.snapshot_data_by_height_cf();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(snapshot_cf, &height, &snapshot_data);
        self.db.write(batch)?;

        tracing::info!(
            ?height,
            holder_count,
            ?pool_values,
            ?difficulty,
            total_issuance_zat = total_issuance.zatoshis(),
            inflation_rate_percent = inflation_rate,
            block_timestamp,
            transparent_tx_count,
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
            average_fee_zat = average_fee_zat.zatoshis(),
            average_block_size_bytes = average_block_size,
            "stored snapshot data to RocksDB"
        );

        Ok(())
    }

    /// Returns the snapshot data for a given block height, if it was stored.
    ///
    /// Returns `None` if no snapshot was stored at that height.
    pub fn snapshot_data_at_height(&self, height: Height) -> Option<SnapshotData> {
        let snapshot_cf = self.snapshot_data_by_height_cf();
        self.db.zs_get(snapshot_cf, &height)
    }

    /// Returns the most recent snapshot data, limited to the specified count.
    ///
    /// Returns a vector of (height, snapshot_data) pairs, sorted by height (ascending).
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
    pub fn recent_snapshot_data(&self, limit: usize) -> Vec<(Height, SnapshotData)> {
        let typed_cf = TypedColumnFamily::<Height, SnapshotData>::new(
            &self.db,
            SNAPSHOT_DATA_BY_HEIGHT,
        )
        .expect("column family was created when database was created");

        // Use reverse iteration to get the most recent snapshots first
        let mut snapshots: Vec<(Height, SnapshotData)> = typed_cf
            .zs_reverse_range_iter(..)
            .take(limit)
            .collect();

        // Reverse to get ascending order by height
        snapshots.reverse();
        snapshots
    }
}

