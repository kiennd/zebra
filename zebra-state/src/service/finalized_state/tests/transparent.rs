//! Fixed-vector tests for the finalized transparent address balance writer.
//!
//! Regression coverage for the credit-before-debit overflow panic: applying a
//! same-address transparent self-spend chain in one block used to push the
//! intermediate address balance above `MAX_MONEY` and panic the writer, even
//! though the final consensus balance was valid.
//!
//! This test drives `DiskWriteBatch::prepare_transparent_transaction_batch`
//! (the writer's public entry point, whose signature is unchanged by the fix)
//! so the same source compiles against both the buggy and the fixed revision:
//! it panics on the buggy revision and passes on the fixed one.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use zebra_chain::{
    amount::{Amount, DeferredPoolBalanceChange, NonNegative, MAX_MONEY},
    block::{self, Block, Height},
    parameters::{Network, NetworkKind},
    serialization::ZcashDeserializeInto,
    transaction::{self, LockTime, Transaction},
    transparent::{
        self, new_ordered_outputs_with_height, Address, Input, OutPoint, Output, Script,
    },
};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    request::{FinalizedBlock, SemanticallyVerifiedBlock, Treestate},
    service::finalized_state::{
        disk_db::DiskWriteBatch,
        disk_format::transparent::{
            AddressBalanceLocation, AddressBalanceLocationUpdates, AddressTransaction,
            OutputLocation,
        },
        ZebraDb, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    CheckpointVerifiedBlock, Config,
};

fn new_ephemeral_zebra_db(network: &Network) -> ZebraDb {
    ZebraDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        // The raw database accesses in this test create invalid database formats.
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("opening an ephemeral database should succeed")
}

/// Cross-version regression test for the credit-before-debit overflow panic.
///
/// Builds a synthetic block whose two transactions form a same-address self-spend
/// chain that re-creates an existing `MAX_MONEY / 2` UTXO twice, and drives it
/// through [`DiskWriteBatch::prepare_transparent_transaction_batch`].
///
/// - On the **buggy** revision the writer credits both new outputs before debiting
///   the matching spends, so the intermediate balance reaches `1.5 * MAX_MONEY`
///   and panics with `"balance overflow already checked"`.
/// - On the **fixed** revision the writer processes each transaction in block order
///   (debit-then-credit), so the per-address running balance stays in
///   `[0, MAX_MONEY]` and the final on-disk balance equals the original
///   `MAX_MONEY / 2`.
#[test]
fn intra_block_self_spend_chain_in_finalized_state() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let height = Height(1);
    let address = Address::from_script_hash(NetworkKind::Mainnet, [0x42; 20]);
    let value = Amount::<NonNegative>::try_from(MAX_MONEY / 2)
        .expect("MAX_MONEY / 2 fits in Amount<NonNegative>");

    // T0 spends a pre-existing on-chain UTXO of value V to address A and re-creates V to A.
    let existing_outpoint = OutPoint {
        hash: transaction::Hash([0x00; 32]),
        index: 0,
    };
    let t0 = Arc::new(Transaction::test_v1(
        vec![Input::PrevOut {
            outpoint: existing_outpoint,
            unlock_script: Script::new(&[]),
            sequence: 0xffff_ffff,
        }],
        vec![Output::new(value, address.script())],
        LockTime::unlocked(),
    ));
    let t0_hash = t0.hash();

    // T1 spends T0's output and creates V back to A.
    let t0_output_outpoint = OutPoint {
        hash: t0_hash,
        index: 0,
    };
    let t1 = Arc::new(Transaction::test_v1(
        vec![Input::PrevOut {
            outpoint: t0_output_outpoint,
            unlock_script: Script::new(&[]),
            sequence: 0xffff_ffff,
        }],
        vec![Output::new(value, address.script())],
        LockTime::unlocked(),
    ));

    // Synthetic block. The header is a dummy from zebra-test (round-tripped); the writer
    // doesn't validate the header — only the transactions and the block height matter.
    let header: block::Header = zebra_test::vectors::DUMMY_HEADER
        .as_slice()
        .zcash_deserialize_into()
        .expect("DUMMY_HEADER deserializes");
    let block = Arc::new(Block {
        header: Arc::new(header),
        transactions: vec![t0.clone(), t1.clone()],
    });
    let transaction_hashes: Arc<[_]> = block.transactions.iter().map(|tx| tx.hash()).collect();
    let new_outputs = new_ordered_outputs_with_height(&block, height, &transaction_hashes);

    let semantically_verified = SemanticallyVerifiedBlock {
        block: block.clone(),
        hash: block::Hash([0x00; 32]),
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

    // Inputs to `prepare_transparent_transaction_batch`, prepared the way `write_block` does.
    let existing_output_location = OutputLocation::from_usize(Height(0), 0, 0);
    let t0_output_location = OutputLocation::from_usize(height, 0, 0);
    let t1_output_location = OutputLocation::from_usize(height, 1, 0);

    let make_utxo =
        |h: Height| transparent::Utxo::new(Output::new(value, address.script()), h, false);
    let existing_utxo = make_utxo(Height(0));
    let t0_output_utxo = make_utxo(height);
    let t1_output_utxo = make_utxo(height);

    let new_outputs_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo> = BTreeMap::from([
        (t0_output_location, t0_output_utxo.clone()),
        (t1_output_location, t1_output_utxo),
    ]);
    let spent_utxos_by_outpoint: HashMap<OutPoint, transparent::Utxo> = HashMap::from([
        (existing_outpoint, existing_utxo.clone()),
        (t0_output_outpoint, t0_output_utxo.clone()),
    ]);
    let spent_utxos_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo> = BTreeMap::from([
        (existing_output_location, existing_utxo),
        (t0_output_location, t0_output_utxo),
    ]);

    // Pre-populate `address_balances` with A's pre-block on-chain balance, the way
    // `block.rs` does via `read_addr_locs`.
    let mut existing_abl = AddressBalanceLocation::new(existing_output_location);
    *existing_abl.balance_mut() = value;
    *existing_abl.received_mut() = u64::from(value);
    let address_balances =
        AddressBalanceLocationUpdates::Insert(HashMap::from([(address, existing_abl)]));

    let zebra_db = new_ephemeral_zebra_db(&network);
    let mut batch = DiskWriteBatch::new();

    // On the buggy revision this call panics with "balance overflow already checked" during
    // the credit-first batch (intermediate balance reaches 1.5 * MAX_MONEY). On the fixed
    // revision it completes cleanly.
    batch.prepare_transparent_transaction_batch(
        &zebra_db,
        &network,
        &finalized,
        &new_outputs_by_out_loc,
        &spent_utxos_by_outpoint,
        &spent_utxos_by_out_loc,
        #[cfg(feature = "indexer")]
        &HashMap::from([
            (existing_outpoint, existing_output_location),
            (t0_output_outpoint, t0_output_location),
        ]),
        address_balances,
        vec![(address, value, value)],
    );

    // Write the batch and confirm the final on-disk balance matches the consensus value
    // (existing V − 2*V debits + 2*V credits = V).
    zebra_db
        .write_batch(batch)
        .expect("ephemeral db accepts the batch");

    let (balance, received) = zebra_db
        .address_balance(&address)
        .expect("address balance is present after writing the batch");
    assert_eq!(balance, value, "final balance equals the existing balance");
    assert_eq!(
        received,
        u64::from(value).saturating_mul(3),
        "received counts the existing V plus two intra-block credits of V",
    );

    let address_location = zebra_db
        .address_location(&address)
        .expect("address location is indexed");
    for tx_index in 0..=1 {
        let key = AddressTransaction::new(
            address_location,
            crate::TransactionLocation::from_usize(height, tx_index),
        );
        let activity = zebra_db
            .transparent_tx_balance_by_addr_loc_cf()
            .zs_get(&key)
            .expect("self-spend transaction activity is indexed");
        assert_eq!(activity.received_zat(), u64::from(value));
        assert_eq!(activity.spent_zat(), u64::from(value));
    }

    // A missing value in the optional CF represents a legacy row that still needs backfill; the
    // original unit-valued transaction index remains readable and authoritative.
    let legacy_key = AddressTransaction::new(
        address_location,
        crate::TransactionLocation::from_usize(height, 0),
    );
    zebra_db
        .transparent_tx_balance_by_addr_loc_cf()
        .new_batch_for_writing()
        .zs_delete(&legacy_key)
        .write_batch()
        .expect("test can remove the optional activity value");
    let indexed = zebra_db.address_transaction_balances_reverse(&address, height, None, 2);
    assert!(indexed.iter().any(|(location, activity)| {
        *location == crate::TransactionLocation::from_usize(height, 0) && activity.is_none()
    }));
}

/// Per-address transaction activity aggregates every matching input and output exactly once.
#[test]
fn transparent_transaction_balances_aggregate_inputs_outputs_and_coinbase() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let height = Height(1);
    let p2pkh = Address::from_pub_key_hash(NetworkKind::Mainnet, [0x11; 20]);
    let p2sh = Address::from_script_hash(NetworkKind::Mainnet, [0x22; 20]);
    let amount = |value| Amount::<NonNegative>::try_from(value).expect("test amount is valid");
    let prevout = |tag, index| OutPoint {
        hash: transaction::Hash([tag; 32]),
        index,
    };
    let input = |outpoint| Input::PrevOut {
        outpoint,
        unlock_script: Script::new(&[]),
        sequence: u32::MAX,
    };
    let first_outpoint = prevout(1, 0);
    let second_outpoint = prevout(2, 1);
    let transfer = Transaction::test_v1(
        vec![input(first_outpoint), input(second_outpoint)],
        vec![
            Output::new(amount(5), p2pkh.script()),
            Output::new(amount(3), p2pkh.script()),
            Output::new(amount(4), p2sh.script()),
            Output::new(amount(6), p2sh.script()),
        ],
        LockTime::unlocked(),
    );
    let max_first = MAX_MONEY / 2;
    let max_second = MAX_MONEY - max_first;
    let coinbase = Transaction::test_v1(
        vec![Input::Coinbase {
            height,
            data: vec![0, 0],
            sequence: u32::MAX,
        }],
        vec![
            Output::new(amount(max_first), p2sh.script()),
            Output::new(amount(max_second), p2sh.script()),
        ],
        LockTime::unlocked(),
    );
    let spent_utxos = HashMap::from([
        (
            first_outpoint,
            transparent::Utxo::new(Output::new(amount(7), p2pkh.script()), Height(0), false),
        ),
        (
            second_outpoint,
            transparent::Utxo::new(Output::new(amount(11), p2pkh.script()), Height(0), false),
        ),
    ]);
    let p2pkh_location = OutputLocation::from_usize(Height(0), 0, 0);
    let p2sh_location = OutputLocation::from_usize(height, 0, 2);
    let address_balances = AddressBalanceLocationUpdates::Insert(HashMap::from([
        (p2pkh, AddressBalanceLocation::new(p2pkh_location)),
        (p2sh, AddressBalanceLocation::new(p2sh_location)),
    ]));
    let zebra_db = new_ephemeral_zebra_db(&network);
    let mut batch = DiskWriteBatch::new();

    batch.prepare_transparent_transaction_balances_batch(
        &zebra_db,
        &network,
        crate::TransactionLocation::from_usize(height, 0),
        &transfer,
        &spent_utxos,
        &address_balances,
    );
    batch.prepare_transparent_transaction_balances_batch(
        &zebra_db,
        &network,
        crate::TransactionLocation::from_usize(height, 1),
        &coinbase,
        &HashMap::new(),
        &address_balances,
    );
    zebra_db.write_batch(batch).expect("activity batch writes");

    let activity = |address_location, tx_index| {
        zebra_db
            .transparent_tx_balance_by_addr_loc_cf()
            .zs_get(&AddressTransaction::new(
                address_location,
                crate::TransactionLocation::from_usize(height, tx_index),
            ))
            .expect("expected transaction activity")
    };
    let p2pkh_transfer = activity(p2pkh_location, 0);
    assert_eq!(p2pkh_transfer.received_zat(), 8);
    assert_eq!(p2pkh_transfer.spent_zat(), 18);
    let p2sh_transfer = activity(p2sh_location, 0);
    assert_eq!(p2sh_transfer.received_zat(), 10);
    assert_eq!(p2sh_transfer.spent_zat(), 0);
    let p2sh_coinbase = activity(p2sh_location, 1);
    assert_eq!(
        p2sh_coinbase.received_zat(),
        u64::try_from(MAX_MONEY).expect("MAX_MONEY is non-negative"),
    );
    assert_eq!(p2sh_coinbase.spent_zat(), 0);
}

/// Address and holder counts represent addresses with spendable balances, rather than every
/// historical address retained by the balance index.
#[test]
fn address_counts_exclude_zero_balances() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let zero_address = Address::from_script_hash(NetworkKind::Mainnet, [0x00; 20]);
    let funded_address = Address::from_script_hash(NetworkKind::Mainnet, [0x01; 20]);
    let richest_address = Address::from_script_hash(NetworkKind::Mainnet, [0x02; 20]);
    let first_tied_address = Address::from_script_hash(NetworkKind::Mainnet, [0x03; 20]);
    let second_tied_address = Address::from_script_hash(NetworkKind::Mainnet, [0x04; 20]);
    let funded_balance = Amount::<NonNegative>::try_from(1u64).expect("1 zatoshi is valid");
    let tied_balance = Amount::<NonNegative>::try_from(2u64).expect("2 zatoshis is valid");
    let richest_balance = Amount::<NonNegative>::try_from(3u64).expect("3 zatoshis is valid");

    let zero_balance = AddressBalanceLocation::new(OutputLocation::from_usize(Height(1), 0, 0));
    let mut positive_balance =
        AddressBalanceLocation::new(OutputLocation::from_usize(Height(1), 1, 0));
    *positive_balance.balance_mut() = funded_balance;
    let mut richest_balance_location =
        AddressBalanceLocation::new(OutputLocation::from_usize(Height(1), 2, 0));
    *richest_balance_location.balance_mut() = richest_balance;
    let mut first_tied_balance_location =
        AddressBalanceLocation::new(OutputLocation::from_usize(Height(1), 3, 0));
    *first_tied_balance_location.balance_mut() = tied_balance;
    let mut second_tied_balance_location =
        AddressBalanceLocation::new(OutputLocation::from_usize(Height(1), 4, 0));
    *second_tied_balance_location.balance_mut() = tied_balance;

    let zebra_db = new_ephemeral_zebra_db(&network);
    let mut batch = DiskWriteBatch::new();
    batch.prepare_transparent_balances_batch(
        zebra_db.db(),
        AddressBalanceLocationUpdates::Insert(HashMap::from([
            (zero_address, zero_balance),
            (funded_address, positive_balance),
            (richest_address, richest_balance_location),
            (first_tied_address, first_tied_balance_location),
            (second_tied_address, second_tied_balance_location),
        ])),
    );
    batch.prepare_transparent_balance_index_batch(
        zebra_db.db(),
        vec![
            (funded_address, Amount::zero(), funded_balance),
            (richest_address, Amount::zero(), richest_balance),
            (first_tied_address, Amount::zero(), tied_balance),
            (second_tied_address, Amount::zero(), tied_balance),
        ],
    );
    zebra_db
        .write_batch(batch)
        .expect("ephemeral db accepts address balances");

    assert_eq!(zebra_db.holder_count(), 4);
    assert_eq!(zebra_db.address_count(), 4);
    assert!(zebra_db.top_addresses_by_balance(0).1.is_empty());
    assert_eq!(
        zebra_db.top_addresses_by_balance(2),
        (
            None,
            vec![
                (richest_address, richest_balance),
                (first_tied_address, tied_balance),
            ],
        ),
    );

    // Changing a balance must remove the old ordered key, rather than leaving a duplicate that
    // would make a future top-K query return stale data.
    let mut batch = DiskWriteBatch::new();
    batch.prepare_transparent_balance_index_batch(
        zebra_db.db(),
        vec![(richest_address, richest_balance, funded_balance)],
    );
    zebra_db
        .write_batch(batch)
        .expect("ephemeral db accepts an ordered balance update");
    assert_eq!(
        zebra_db.top_addresses_by_balance(2).1,
        vec![
            (first_tied_address, tied_balance),
            (second_tied_address, tied_balance),
        ],
    );
}
