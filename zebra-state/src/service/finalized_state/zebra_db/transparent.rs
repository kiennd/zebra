//! Provides high-level access to database:
//! - unspent [`transparent::Output`]s (UTXOs),
//! - spent [`transparent::Output`]s, and
//! - transparent address indexes.
//!
//! This module makes sure that:
//! - all disk writes happen inside a RocksDB transaction, and
//! - format-specific invariants are maintained.
//!
//! # Correctness
//!
//! [`crate::constants::state_database_format_version_in_code()`] must be incremented
//! each time the database format (column, serialization, etc) changes.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::RangeInclusive,
};

use rocksdb::ColumnFamily;
use zebra_chain::{
    amount::{self, Amount, Constraint, NonNegative},
    block::Height,
    parameters::{
        subsidy::block_subsidy,
        Network, NetworkUpgrade,
    },
    serialization::BytesInDisplayOrder,
    transaction::{self, Transaction},
    transparent::{self, Input},
    value_balance::ValueBalance,
};

use crate::{
    request::FinalizedBlock,
    service::finalized_state::{
        disk_db::{DiskDb, DiskWriteBatch, ReadDisk, WriteDisk},
        disk_format::{
            transparent::{
                AddressBalanceLocation, AddressBalanceLocationChange, AddressBalanceLocationInner,
                AddressBalanceLocationUpdates, AddressLocation, AddressTransaction,
                AddressUnspentOutput, OutputLocation,
            },
            TransactionLocation,
        },
        zebra_db::ZebraDb,
    },
    BoxError, FromDisk, IntoDisk,
};

use super::super::TypedColumnFamily;

/// The name of the transaction hash by spent outpoints column family.
pub const TX_LOC_BY_SPENT_OUT_LOC: &str = "tx_loc_by_spent_out_loc";

/// The name of the [balance](AddressBalanceLocation) by transparent address column family.
pub const BALANCE_BY_TRANSPARENT_ADDR: &str = "balance_by_transparent_addr";

/// The name of the [`BALANCE_BY_TRANSPARENT_ADDR`] column family's merge operator
pub const BALANCE_BY_TRANSPARENT_ADDR_MERGE_OP: &str = "fetch_add_balance_and_received";

/// The name of the snapshot data by block height column family.
/// Stores holder count, pool values, difficulty, issuance, inflation rate, and timestamp.
pub const SNAPSHOT_DATA_BY_HEIGHT: &str = "snapshot_data_by_height";

/// Snapshot data stored in RocksDB containing holder count, pool values, difficulty, issuance, timestamp, and transaction counts.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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
}

impl IntoDisk for SnapshotData {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        // 8 + 40 + 32 + 8 + 4 + 8 + 4 + 4 + 4 + 4 + 8*8 = 180 bytes total
        let mut bytes = Vec::with_capacity(180);
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
        bytes
    }
}

impl FromDisk for SnapshotData {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        // Support old formats: 100 bytes (original), 116 bytes (with tx counts), 180 bytes (with inflow/outflow)
        let has_tx_counts = bytes.len() >= 116;
        let has_inflow_outflow = bytes.len() >= 180;
        
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
        }
    }
}

/// A RocksDB merge operator for the [`BALANCE_BY_TRANSPARENT_ADDR`] column family.
pub fn fetch_add_balance_and_received(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    // # Correctness
    //
    // Merge operands are ordered, but may be combined without an existing value in partial merges, so
    // we may need to return a negative balance here.
    existing_val
        .into_iter()
        .chain(operands)
        .map(AddressBalanceLocationChange::from_bytes)
        .reduce(|a, b| (a + b).expect("address balance/received should not overflow"))
        .map(|address_balance_location| address_balance_location.as_bytes().to_vec())
}

/// The type for reading value pools from the database.
///
/// This constant should be used so the compiler can detect incorrectly typed accesses to the
/// column family.
pub type TransactionLocationBySpentOutputLocationCf<'cf> =
    TypedColumnFamily<'cf, OutputLocation, TransactionLocation>;

impl ZebraDb {
    // Column family convenience methods

    /// Returns a typed handle to the transaction location by spent output location column family.
    pub(crate) fn tx_loc_by_spent_output_loc_cf(
        &self,
    ) -> TransactionLocationBySpentOutputLocationCf<'_> {
        TransactionLocationBySpentOutputLocationCf::new(&self.db, TX_LOC_BY_SPENT_OUT_LOC)
            .expect("column family was created when database was created")
    }

    // Read transparent methods

    /// Returns the [`TransactionLocation`] for a transaction that spent the output
    /// at the provided [`OutputLocation`], if it is in the finalized state.
    pub fn tx_location_by_spent_output_location(
        &self,
        output_location: &OutputLocation,
    ) -> Option<TransactionLocation> {
        self.tx_loc_by_spent_output_loc_cf().zs_get(output_location)
    }

    /// Returns a handle to the `balance_by_transparent_addr` RocksDB column family.
    pub fn address_balance_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(BALANCE_BY_TRANSPARENT_ADDR).unwrap()
    }

    /// Returns a handle to the `snapshot_data_by_height` RocksDB column family.
    pub fn snapshot_data_by_height_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(SNAPSHOT_DATA_BY_HEIGHT).unwrap()
    }

    /// Returns the [`AddressBalanceLocation`] for a [`transparent::Address`],
    /// if it is in the finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn address_balance_location(
        &self,
        address: &transparent::Address,
    ) -> Option<AddressBalanceLocation> {
        let balance_by_transparent_addr = self.address_balance_cf();

        self.db.zs_get(&balance_by_transparent_addr, address)
    }

    /// Returns the balance and received balance for a [`transparent::Address`],
    /// if it is in the finalized state.
    pub fn address_balance(
        &self,
        address: &transparent::Address,
    ) -> Option<(Amount<NonNegative>, u64)> {
        self.address_balance_location(address)
            .map(|abl| (abl.balance(), abl.received()))
    }

    /// Returns the first output that sent funds to a [`transparent::Address`],
    /// if it is in the finalized state.
    ///
    /// This location is used as an efficient index key for addresses.
    pub fn address_location(&self, address: &transparent::Address) -> Option<AddressLocation> {
        self.address_balance_location(address)
            .map(|abl| abl.address_location())
    }

    /// Returns the [`OutputLocation`] for a [`transparent::OutPoint`].
    ///
    /// This method returns the locations of spent and unspent outpoints.
    /// Returns `None` if the output was never in the finalized state.
    pub fn output_location(&self, outpoint: &transparent::OutPoint) -> Option<OutputLocation> {
        self.transaction_location(outpoint.hash)
            .map(|transaction_location| {
                OutputLocation::from_outpoint(transaction_location, outpoint)
            })
    }

    /// Returns the transparent output for a [`transparent::OutPoint`],
    /// if it is unspent in the finalized state.
    pub fn utxo(&self, outpoint: &transparent::OutPoint) -> Option<transparent::OrderedUtxo> {
        let output_location = self.output_location(outpoint)?;

        self.utxo_by_location(output_location)
    }

    /// Returns the [`TransactionLocation`] of the transaction that spent the given
    /// [`transparent::OutPoint`], if it is unspent in the finalized state and its
    /// spending transaction hash has been indexed.
    pub fn spending_tx_loc(&self, outpoint: &transparent::OutPoint) -> Option<TransactionLocation> {
        let output_location = self.output_location(outpoint)?;
        self.tx_location_by_spent_output_location(&output_location)
    }

    /// Returns the transparent output for an [`OutputLocation`],
    /// if it is unspent in the finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn utxo_by_location(
        &self,
        output_location: OutputLocation,
    ) -> Option<transparent::OrderedUtxo> {
        let utxo_by_out_loc = self.db.cf_handle("utxo_by_out_loc").unwrap();

        let output = self.db.zs_get(&utxo_by_out_loc, &output_location)?;

        let utxo = transparent::OrderedUtxo::new(
            output,
            output_location.height(),
            output_location.transaction_index().as_usize(),
        );

        Some(utxo)
    }

    /// Returns the unspent transparent outputs for a [`transparent::Address`],
    /// if they are in the finalized state.
    pub fn address_utxos(
        &self,
        address: &transparent::Address,
    ) -> BTreeMap<OutputLocation, transparent::Output> {
        let address_location = match self.address_location(address) {
            Some(address_location) => address_location,
            None => return BTreeMap::new(),
        };

        let output_locations = self.address_utxo_locations(address_location);

        // Ignore any outputs spent by blocks committed during this query
        output_locations
            .iter()
            .filter_map(|&addr_out_loc| {
                Some((
                    addr_out_loc.unspent_output_location(),
                    self.utxo_by_location(addr_out_loc.unspent_output_location())?
                        .utxo
                        .output,
                ))
            })
            .collect()
    }

    /// Returns the unspent transparent output locations for a [`transparent::Address`],
    /// if they are in the finalized state.
    pub fn address_utxo_locations(
        &self,
        address_location: AddressLocation,
    ) -> BTreeSet<AddressUnspentOutput> {
        let utxo_loc_by_transparent_addr_loc = self
            .db
            .cf_handle("utxo_loc_by_transparent_addr_loc")
            .unwrap();

        // Manually fetch the entire addresses' UTXO locations
        let mut addr_unspent_outputs = BTreeSet::new();

        // An invalid key representing the minimum possible output
        let mut unspent_output = AddressUnspentOutput::address_iterator_start(address_location);

        loop {
            // Seek to a valid entry for this address, or the first entry for the next address
            unspent_output = match self
                .db
                .zs_next_key_value_from(&utxo_loc_by_transparent_addr_loc, &unspent_output)
            {
                Some((unspent_output, ())) => unspent_output,
                // We're finished with the final address in the column family
                None => break,
            };

            // We found the next address, so we're finished with this address
            if unspent_output.address_location() != address_location {
                break;
            }

            addr_unspent_outputs.insert(unspent_output);

            // A potentially invalid key representing the next possible output
            unspent_output.address_iterator_next();
        }

        addr_unspent_outputs
    }

    /// Returns the transaction hash for an [`TransactionLocation`].
    #[allow(clippy::unwrap_in_result)]
    pub fn tx_id_by_location(&self, tx_location: TransactionLocation) -> Option<transaction::Hash> {
        let hash_by_tx_loc = self.db.cf_handle("hash_by_tx_loc").unwrap();

        self.db.zs_get(&hash_by_tx_loc, &tx_location)
    }

    /// Returns the transaction IDs that sent or received funds to `address`,
    /// in the finalized chain `query_height_range`.
    ///
    /// If address has no finalized sends or receives,
    /// or the `query_height_range` is totally outside the finalized block range,
    /// returns an empty list.
    pub fn address_tx_ids(
        &self,
        address: &transparent::Address,
        query_height_range: RangeInclusive<Height>,
    ) -> BTreeMap<TransactionLocation, transaction::Hash> {
        let address_location = match self.address_location(address) {
            Some(address_location) => address_location,
            None => return BTreeMap::new(),
        };

        // Skip this address if it was first used after the end height.
        //
        // The address location is the output location of the first UTXO sent to the address,
        // and addresses can not spend funds until they receive their first UTXO.
        if address_location.height() > *query_height_range.end() {
            return BTreeMap::new();
        }

        let transaction_locations =
            self.address_transaction_locations(address_location, query_height_range);

        transaction_locations
            .iter()
            .map(|&tx_loc| {
                (
                    tx_loc.transaction_location(),
                    self.tx_id_by_location(tx_loc.transaction_location())
                        .expect("transactions whose locations are stored must exist"),
                )
            })
            .collect()
    }

    /// Returns the locations of any transactions that sent or received from a [`transparent::Address`],
    /// if they are in the finalized state.
    pub fn address_transaction_locations(
        &self,
        address_location: AddressLocation,
        query_height_range: RangeInclusive<Height>,
    ) -> BTreeSet<AddressTransaction> {
        let tx_loc_by_transparent_addr_loc =
            self.db.cf_handle("tx_loc_by_transparent_addr_loc").unwrap();

        // A potentially invalid key representing the first UTXO send to the address,
        // or the query start height.
        let transaction_location_range =
            AddressTransaction::address_iterator_range(address_location, query_height_range);

        self.db
            .zs_forward_range_iter(&tx_loc_by_transparent_addr_loc, transaction_location_range)
            .map(|(tx_loc, ())| tx_loc)
            .collect()
    }

    // Address index queries

    /// Returns the total transparent balance and received balance for `addresses` in the finalized chain.
    ///
    /// If none of the addresses have a balance, returns zeroes.
    ///
    /// # Correctness
    ///
    /// Callers should apply the non-finalized balance change for `addresses` to the returned balances.
    ///
    /// The total balances will only be correct if the non-finalized chain matches the finalized state.
    /// Specifically, the root of the partial non-finalized chain must be a child block of the finalized tip.
    pub fn partial_finalized_transparent_balance(
        &self,
        addresses: &HashSet<transparent::Address>,
    ) -> (Amount<NonNegative>, u64) {
        let balance: amount::Result<(Amount<NonNegative>, u64)> = addresses
            .iter()
            .filter_map(|address| self.address_balance(address))
            .try_fold(
                (Amount::zero(), 0),
                |(a_balance, a_received): (Amount<NonNegative>, u64), (b_balance, b_received)| {
                    let received = a_received.saturating_add(b_received);
                    Ok(((a_balance + b_balance)?, received))
                },
            );

        balance.expect(
            "unexpected amount overflow: value balances are valid, so partial sum should be valid",
        )
    }

    /// Returns the UTXOs for `addresses` in the finalized chain.
    ///
    /// If none of the addresses has finalized UTXOs, returns an empty list.
    ///
    /// # Correctness
    ///
    /// Callers should apply the non-finalized UTXO changes for `addresses` to the returned UTXOs.
    ///
    /// The UTXOs will only be correct if the non-finalized chain matches or overlaps with
    /// the finalized state.
    ///
    /// Specifically, a block in the partial chain must be a child block of the finalized tip.
    /// (But the child block does not have to be the partial chain root.)
    pub fn partial_finalized_address_utxos(
        &self,
        addresses: &HashSet<transparent::Address>,
    ) -> BTreeMap<OutputLocation, transparent::Output> {
        addresses
            .iter()
            .flat_map(|address| self.address_utxos(address))
            .collect()
    }

    /// Returns the total number of addresses with balances in the finalized state.
    ///
    /// # Warning
    ///
    /// This operation scans the entire balance column family and may be slow.
    /// It should be run in a blocking thread to avoid hanging the tokio executor.
    pub fn address_count(&self) -> usize {
        let balance_by_transparent_addr = self.address_balance_cf();
        self.db
            .zs_forward_range_iter::<_, transparent::Address, AddressBalanceLocation, _>(&balance_by_transparent_addr, ..)
            .count()
    }

    /// Returns the number of holders (addresses with non-zero balances) in the finalized state.
    ///
    /// # Warning
    ///
    /// This operation scans the entire balance column family and may be slow.
    /// It should be run in a blocking thread to avoid hanging the tokio executor.
    pub fn holder_count(&self) -> usize {
        let balance_by_transparent_addr = self.address_balance_cf();
        self.db
            .zs_forward_range_iter::<_, transparent::Address, AddressBalanceLocation, _>(&balance_by_transparent_addr, ..)
            .filter(|(_address, balance_location): &(transparent::Address, AddressBalanceLocation)| {
                balance_location.balance() > Amount::<NonNegative>::zero()
            })
            .count()
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
        use std::collections::HashMap;
        
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
        
        // 6. Calculate total issuance up to this height
        // Note: This only includes blocks from slow_start_interval onwards,
        // as block_subsidy() doesn't support heights in the slow start period.
        let total_issuance = self.calculate_total_issuance(height, network)?;
        
        // 7. Calculate inflation rate using pool values as total supply
        // Pool values represent the actual monetary base (all ZEC in circulation),
        // which is more accurate than calculated issuance (which excludes slow start).
        let total_supply = (pool_values.transparent_amount()
            + pool_values.sprout_amount()
            + pool_values.sapling_amount()
            + pool_values.orchard_amount()
            + pool_values.deferred_amount())
            .map_err(|e| format!("overflow calculating total supply from pool values: {}", e))?;
        let inflation_rate = self.calculate_inflation_rate(height, network, total_supply)?;
        
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
        
        // 11. Create snapshot data
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
        );

        // 12. Store in RocksDB
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

    /// Returns the top N addresses by balance in the finalized state.
    ///
    /// # Warning
    ///
    /// This operation scans the entire balance column family and may be slow.
    /// It should be run in a blocking thread to avoid hanging the tokio executor.
    ///
    /// # Parameters
    ///
    /// - `limit`: Maximum number of addresses to return
    pub fn top_addresses_by_balance(
        &self,
        limit: usize,
    ) -> Vec<(transparent::Address, Amount<NonNegative>)> {
        let balance_by_transparent_addr = self.address_balance_cf();

        let mut addresses_with_balances: Vec<(transparent::Address, Amount<NonNegative>)> = self
            .db
            .zs_forward_range_iter::<_, transparent::Address, AddressBalanceLocation, _>(&balance_by_transparent_addr, ..)
            .filter_map(|(address, balance_location): (transparent::Address, AddressBalanceLocation)| {
                let balance = balance_location.balance();
                if balance > Amount::<NonNegative>::zero() {
                    Some((address, balance))
                } else {
                    None
                }
            })
            .collect();

        // Sort by balance descending
        addresses_with_balances.sort_by(|a, b| b.1.cmp(&a.1));

        // Take top N
        addresses_with_balances.truncate(limit);

        addresses_with_balances
    }

    /// Returns the transaction IDs that sent or received funds to `addresses`,
    /// in the finalized chain `query_height_range`.
    ///
    /// If none of the addresses has finalized sends or receives,
    /// or the `query_height_range` is totally outside the finalized block range,
    /// returns an empty list.
    ///
    /// # Correctness
    ///
    /// Callers should combine the non-finalized transactions for `addresses`
    /// with the returned transactions.
    ///
    /// The transaction IDs will only be correct if the non-finalized chain matches or overlaps with
    /// the finalized state.
    ///
    /// Specifically, a block in the partial chain must be a child block of the finalized tip.
    /// (But the child block does not have to be the partial chain root.)
    ///
    /// This condition does not apply if there is only one address.
    /// Since address transactions are only appended by blocks, and this query reads them in order,
    /// it is impossible to get inconsistent transactions for a single address.
    pub fn partial_finalized_transparent_tx_ids(
        &self,
        addresses: &HashSet<transparent::Address>,
        query_height_range: RangeInclusive<Height>,
    ) -> BTreeMap<TransactionLocation, transaction::Hash> {
        addresses
            .iter()
            .flat_map(|address| self.address_tx_ids(address, query_height_range.clone()))
            .collect()
    }
}

impl DiskWriteBatch {
    /// Prepare a database batch containing `finalized.block`'s transparent transaction indexes,
    /// and return it (without actually writing anything).
    ///
    /// If this method returns an error, it will be propagated,
    /// and the batch should not be written to the database.
    ///
    /// # Errors
    ///
    /// - Propagates any errors from updating note commitment trees
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_transparent_transaction_batch(
        &mut self,
        zebra_db: &ZebraDb,
        network: &Network,
        finalized: &FinalizedBlock,
        new_outputs_by_out_loc: &BTreeMap<OutputLocation, transparent::Utxo>,
        spent_utxos_by_outpoint: &HashMap<transparent::OutPoint, transparent::Utxo>,
        spent_utxos_by_out_loc: &BTreeMap<OutputLocation, transparent::Utxo>,
        #[cfg(feature = "indexer")] out_loc_by_outpoint: &HashMap<
            transparent::OutPoint,
            OutputLocation,
        >,
        mut address_balances: AddressBalanceLocationUpdates,
    ) -> Result<(), BoxError> {
        let db = &zebra_db.db;
        let FinalizedBlock { block, height, .. } = finalized;

        // Update created and spent transparent outputs
        self.prepare_new_transparent_outputs_batch(
            db,
            network,
            new_outputs_by_out_loc,
            &mut address_balances,
        )?;
        self.prepare_spent_transparent_outputs_batch(
            db,
            network,
            spent_utxos_by_out_loc,
            &mut address_balances,
        )?;

        // Index the transparent addresses that spent in each transaction
        for (tx_index, transaction) in block.transactions.iter().enumerate() {
            let spending_tx_location = TransactionLocation::from_usize(*height, tx_index);

            self.prepare_spending_transparent_tx_ids_batch(
                zebra_db,
                network,
                spending_tx_location,
                transaction,
                spent_utxos_by_outpoint,
                #[cfg(feature = "indexer")]
                out_loc_by_outpoint,
                &address_balances,
            )?;
        }

        self.prepare_transparent_balances_batch(db, address_balances)
    }

    /// Prepare a database batch for the new UTXOs in `new_outputs_by_out_loc`.
    ///
    /// Adds the following changes to this batch:
    /// - insert created UTXOs,
    /// - insert transparent address UTXO index entries, and
    /// - insert transparent address transaction entries,
    ///
    /// without actually writing anything.
    ///
    /// Also modifies the `address_balances` for these new UTXOs.
    ///
    /// # Errors
    ///
    /// - This method doesn't currently return any errors, but it might in future
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_new_transparent_outputs_batch(
        &mut self,
        db: &DiskDb,
        network: &Network,
        new_outputs_by_out_loc: &BTreeMap<OutputLocation, transparent::Utxo>,
        address_balances: &mut AddressBalanceLocationUpdates,
    ) -> Result<(), BoxError> {
        let utxo_by_out_loc = db.cf_handle("utxo_by_out_loc").unwrap();
        let utxo_loc_by_transparent_addr_loc =
            db.cf_handle("utxo_loc_by_transparent_addr_loc").unwrap();
        let tx_loc_by_transparent_addr_loc =
            db.cf_handle("tx_loc_by_transparent_addr_loc").unwrap();

        // Index all new transparent outputs
        for (new_output_location, utxo) in new_outputs_by_out_loc {
            let unspent_output = &utxo.output;
            let receiving_address = unspent_output.address(network);

            // Update the address balance by adding this UTXO's value
            if let Some(receiving_address) = receiving_address {
                // TODO: fix up tests that use missing outputs,
                //       then replace entry() with get_mut().expect()

                // In memory:
                // - create the balance for the address, if needed.
                // - create or fetch the link from the address to the AddressLocation
                //   (the first location of the address in the chain).

                fn update_addr_loc<
                    C: Constraint + Copy + std::fmt::Debug,
                    T: std::ops::DerefMut<Target = AddressBalanceLocationInner<C>>
                        + From<AddressBalanceLocationInner<C>>,
                >(
                    addr_locs: &mut HashMap<transparent::Address, T>,
                    receiving_address: transparent::Address,
                    new_output_location: &OutputLocation,
                    unspent_output: &transparent::Output,
                ) -> AddressLocation {
                    let addr_loc = addr_locs.entry(receiving_address).or_insert_with(|| {
                        AddressBalanceLocationInner::new(*new_output_location).into()
                    });

                    // Update the balance for the address in memory.
                    addr_loc
                        .receive_output(unspent_output)
                        .expect("balance overflow already checked");

                    addr_loc.address_location()
                }

                // Update the balance for the address in memory.
                let receiving_address_location = match address_balances {
                    AddressBalanceLocationUpdates::Merge(balance_changes) => update_addr_loc(
                        balance_changes,
                        receiving_address,
                        new_output_location,
                        unspent_output,
                    ),
                    AddressBalanceLocationUpdates::Insert(balances) => update_addr_loc(
                        balances,
                        receiving_address,
                        new_output_location,
                        unspent_output,
                    ),
                };

                // Create a link from the AddressLocation to the new OutputLocation in the database.
                let address_unspent_output =
                    AddressUnspentOutput::new(receiving_address_location, *new_output_location);
                self.zs_insert(
                    &utxo_loc_by_transparent_addr_loc,
                    address_unspent_output,
                    (),
                );

                // Create a link from the AddressLocation to the new TransactionLocation in the database.
                // Unlike the OutputLocation link, this will never be deleted.
                let address_transaction = AddressTransaction::new(
                    receiving_address_location,
                    new_output_location.transaction_location(),
                );
                self.zs_insert(&tx_loc_by_transparent_addr_loc, address_transaction, ());
            }

            // Use the OutputLocation to store a copy of the new Output in the database.
            // (For performance reasons, we don't want to deserialize the whole transaction
            // to get an output.)
            self.zs_insert(&utxo_by_out_loc, new_output_location, unspent_output);
        }

        Ok(())
    }

    /// Prepare a database batch for the spent outputs in `spent_utxos_by_out_loc`.
    ///
    /// Adds the following changes to this batch:
    /// - delete spent UTXOs, and
    /// - delete transparent address UTXO index entries,
    ///
    /// without actually writing anything.
    ///
    /// Also modifies the `address_balances` for these new UTXOs.
    ///
    /// # Errors
    ///
    /// - This method doesn't currently return any errors, but it might in future
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_spent_transparent_outputs_batch(
        &mut self,
        db: &DiskDb,
        network: &Network,
        spent_utxos_by_out_loc: &BTreeMap<OutputLocation, transparent::Utxo>,
        address_balances: &mut AddressBalanceLocationUpdates,
    ) -> Result<(), BoxError> {
        let utxo_by_out_loc = db.cf_handle("utxo_by_out_loc").unwrap();
        let utxo_loc_by_transparent_addr_loc =
            db.cf_handle("utxo_loc_by_transparent_addr_loc").unwrap();

        // Mark all transparent inputs as spent.
        //
        // Coinbase inputs represent new coins, so there are no UTXOs to mark as spent.
        for (spent_output_location, utxo) in spent_utxos_by_out_loc {
            let spent_output = &utxo.output;
            let sending_address = spent_output.address(network);

            // Fetch the balance, and the link from the address to the AddressLocation, from memory.
            if let Some(sending_address) = sending_address {
                fn update_addr_loc<
                    C: Constraint + Copy + std::fmt::Debug,
                    T: std::ops::DerefMut<Target = AddressBalanceLocationInner<C>>
                        + From<AddressBalanceLocationInner<C>>,
                >(
                    addr_locs: &mut HashMap<transparent::Address, T>,
                    sending_address: transparent::Address,
                    spent_output: &transparent::Output,
                ) -> AddressLocation {
                    let addr_loc = addr_locs
                        .get_mut(&sending_address)
                        .expect("spent outputs must already have an address balance");

                    // Update the address balance by subtracting this UTXO's value, in memory.
                    addr_loc
                        .spend_output(spent_output)
                        .expect("balance underflow already checked");

                    addr_loc.address_location()
                }

                let address_location = match address_balances {
                    AddressBalanceLocationUpdates::Merge(balance_changes) => {
                        update_addr_loc(balance_changes, sending_address, spent_output)
                    }
                    AddressBalanceLocationUpdates::Insert(balances) => {
                        update_addr_loc(balances, sending_address, spent_output)
                    }
                };

                // Delete the link from the AddressLocation to the spent OutputLocation in the database.
                let address_spent_output =
                    AddressUnspentOutput::new(address_location, *spent_output_location);

                self.zs_delete(&utxo_loc_by_transparent_addr_loc, address_spent_output);
            }

            // Delete the OutputLocation, and the copy of the spent Output in the database.
            self.zs_delete(&utxo_by_out_loc, spent_output_location);
        }

        Ok(())
    }

    /// Prepare a database batch indexing the transparent addresses that spent in this transaction.
    ///
    /// Adds the following changes to this batch:
    /// - index spending transactions for each spent transparent output
    ///   (this is different from the transaction that created the output),
    ///
    /// without actually writing anything.
    ///
    /// # Errors
    ///
    /// - This method doesn't currently return any errors, but it might in future
    #[allow(clippy::unwrap_in_result, clippy::too_many_arguments)]
    pub fn prepare_spending_transparent_tx_ids_batch(
        &mut self,
        zebra_db: &ZebraDb,
        network: &Network,
        spending_tx_location: TransactionLocation,
        transaction: &Transaction,
        spent_utxos_by_outpoint: &HashMap<transparent::OutPoint, transparent::Utxo>,
        #[cfg(feature = "indexer")] out_loc_by_outpoint: &HashMap<
            transparent::OutPoint,
            OutputLocation,
        >,
        address_balances: &AddressBalanceLocationUpdates,
    ) -> Result<(), BoxError> {
        let db = &zebra_db.db;
        let tx_loc_by_transparent_addr_loc =
            db.cf_handle("tx_loc_by_transparent_addr_loc").unwrap();

        // Index the transparent addresses that spent in this transaction.
        //
        // Coinbase inputs represent new coins, so there are no UTXOs to mark as spent.
        for spent_outpoint in transaction.inputs().iter().filter_map(Input::outpoint) {
            let spent_utxo = spent_utxos_by_outpoint
                .get(&spent_outpoint)
                .expect("unexpected missing spent output");
            let sending_address = spent_utxo.output.address(network);

            // Fetch the balance, and the link from the address to the AddressLocation, from memory.
            if let Some(sending_address) = sending_address {
                let sending_address_location = match address_balances {
                    AddressBalanceLocationUpdates::Merge(balance_changes) => balance_changes
                        .get(&sending_address)
                        .expect("spent outputs must already have an address balance")
                        .address_location(),
                    AddressBalanceLocationUpdates::Insert(balances) => balances
                        .get(&sending_address)
                        .expect("spent outputs must already have an address balance")
                        .address_location(),
                };

                // Create a link from the AddressLocation to the spent TransactionLocation in the database.
                // Unlike the OutputLocation link, this will never be deleted.
                //
                // The value is the location of this transaction,
                // not the transaction the spent output is from.
                let address_transaction =
                    AddressTransaction::new(sending_address_location, spending_tx_location);
                self.zs_insert(&tx_loc_by_transparent_addr_loc, address_transaction, ());
            }

            #[cfg(feature = "indexer")]
            {
                let spent_output_location = out_loc_by_outpoint
                    .get(&spent_outpoint)
                    .expect("spent outpoints must already have output locations");

                let _ = zebra_db
                    .tx_loc_by_spent_output_loc_cf()
                    .with_batch_for_writing(self)
                    .zs_insert(spent_output_location, &spending_tx_location);
            }
        }

        Ok(())
    }

    /// Prepare a database batch containing `finalized.block`'s:
    /// - transparent address balance changes,
    ///
    /// and return it (without actually writing anything).
    ///
    /// # Errors
    ///
    /// - This method doesn't currently return any errors, but it might in future
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_transparent_balances_batch(
        &mut self,
        db: &DiskDb,
        address_balances: AddressBalanceLocationUpdates,
    ) -> Result<(), BoxError> {
        let balance_by_transparent_addr = db.cf_handle(BALANCE_BY_TRANSPARENT_ADDR).unwrap();

        // Update all the changed address balances in the database.
        // Some of these balances are new, and some are updates
        match address_balances {
            AddressBalanceLocationUpdates::Merge(balance_changes) => {
                for (address, address_balance_location_change) in balance_changes.into_iter() {
                    self.zs_merge(
                        &balance_by_transparent_addr,
                        address,
                        address_balance_location_change,
                    );
                }
            }

            AddressBalanceLocationUpdates::Insert(balances) => {
                for (address, address_balance_location_change) in balances.into_iter() {
                    self.zs_insert(
                        &balance_by_transparent_addr,
                        address,
                        address_balance_location_change,
                    );
                }
            }
        };

        Ok(())
    }
}
