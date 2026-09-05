//! ZIP-317 tests.

use super::{conventional_actions, conventional_fee, mempool_checks, Amount, Error};
use crate::{
    amount::NonNegative,
    block::Block,
    parameters::NetworkUpgrade,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{self, LockTime, Transaction},
    transparent::{self, Script},
};

fn assert_conventional_fee(transaction: &Transaction, expected_actions: u32) {
    assert_eq!(conventional_actions(transaction), expected_actions);
    assert_eq!(
        u64::from(conventional_fee(transaction)),
        u64::from(expected_actions) * 5_000,
    );
}

#[test]
fn zip317_transparent_sizes_use_ceiling_and_the_larger_side() {
    let no_actions = Transaction::test_v1(Vec::new(), Vec::new(), LockTime::unlocked());
    assert_conventional_fee(&no_actions, 2);

    let input_with_size = |script_size| transparent::Input::PrevOut {
        outpoint: transparent::OutPoint {
            hash: transaction::Hash::from([0; 32]),
            index: 0,
        },
        unlock_script: Script::new(&vec![0; script_size]),
        sequence: 0,
    };
    let output_with_size = |script_size| {
        transparent::Output::new(
            Amount::<NonNegative>::try_from(0).expect("zero is a valid amount"),
            Script::new(&vec![0; script_size]),
        )
    };

    // 36-byte outpoint + 3-byte CompactSize + script + 4-byte sequence.
    let input_at_boundary = input_with_size(257);
    let input_over_boundary = input_with_size(258);
    assert_eq!(input_at_boundary.zcash_serialized_size(), 300);
    assert_eq!(input_over_boundary.zcash_serialized_size(), 301);

    let at_input_boundary = Transaction::test_v1(
        vec![input_at_boundary.clone()],
        Vec::new(),
        LockTime::unlocked(),
    );
    let over_input_boundary =
        Transaction::test_v1(vec![input_over_boundary], Vec::new(), LockTime::unlocked());
    assert_conventional_fee(&at_input_boundary, 2);
    assert_conventional_fee(&over_input_boundary, 3);

    // 8-byte value + 1-byte CompactSize + script.
    let output_at_boundary = output_with_size(59);
    let output_over_boundary = output_with_size(60);
    assert_eq!(output_at_boundary.zcash_serialized_size(), 68);
    assert_eq!(output_over_boundary.zcash_serialized_size(), 69);

    // The output side is larger (3 actions), so it wins the transparent max over the input side
    // (2 actions).
    let output_side_wins = Transaction::test_v1(
        vec![input_at_boundary],
        vec![output_over_boundary],
        LockTime::unlocked(),
    );
    assert_conventional_fee(&output_side_wins, 3);
}

#[test]
fn zip317_sprout_contribution_has_two_actions_per_joinsplit() {
    let block: Block = zebra_test::vectors::BLOCK_MAINNET_419201_BYTES
        .zcash_deserialize_into()
        .expect("fixed Sprout block must deserialize");
    let transaction = block
        .transactions
        .iter()
        .find(|transaction| transaction.joinsplit_count() > 0)
        .expect("fixed block must contain a Sprout transaction");

    // This fixed transaction has a 2-action transparent output side and one JoinSplit. It has no
    // other shielded contributions, so its expected total is 2 + (2 * 1) = 4 actions.
    assert_eq!(transaction.inputs().len(), 0);
    assert_eq!(
        transaction
            .outputs()
            .iter()
            .map(ZcashSerialize::zcash_serialized_size)
            .sum::<usize>(),
        68,
    );
    assert_eq!(transaction.joinsplit_count(), 1);
    assert_eq!(transaction.sapling_spends_count(), 0);
    assert_eq!(transaction.sapling_outputs().count(), 0);
    assert_eq!(transaction.orchard_actions().count(), 0);
    assert_eq!(transaction.ironwood_actions().count(), 0);
    assert_conventional_fee(transaction, 4);
}

#[test]
fn zip317_sapling_contribution_uses_max_of_spends_and_outputs() {
    let equal_sides: Transaction = zebra_test::vectors::ZIP243_1
        .as_slice()
        .zcash_deserialize_into()
        .expect("fixed Sapling transaction must deserialize");
    assert_eq!(equal_sides.sapling_spends_count(), 3);
    assert_eq!(equal_sides.sapling_outputs().count(), 3);
    assert_eq!(
        equal_sides
            .outputs()
            .iter()
            .map(ZcashSerialize::zcash_serialized_size)
            .sum::<usize>(),
        29,
    );
    // One transparent action + max(3, 3), rather than adding both Sapling sides.
    assert_conventional_fee(&equal_sides, 4);

    let outputs_only: Transaction = zebra_test::vectors::ZIP243_2
        .as_slice()
        .zcash_deserialize_into()
        .expect("fixed Sapling transaction must deserialize");
    assert_eq!(outputs_only.sapling_spends_count(), 0);
    assert_eq!(outputs_only.sapling_outputs().count(), 3);
    assert_eq!(
        outputs_only
            .inputs()
            .iter()
            .map(ZcashSerialize::zcash_serialized_size)
            .sum::<usize>(),
        95,
    );
    assert_eq!(
        outputs_only
            .outputs()
            .iter()
            .map(ZcashSerialize::zcash_serialized_size)
            .sum::<usize>(),
        26,
    );
    assert_conventional_fee(&outputs_only, 4);
}

#[test]
fn zip317_revision_one_adds_orchard_and_ironwood_actions() {
    use crate::transaction::arbitrary::{fake_bundle_for_branch, fake_v6_transaction};

    assert_eq!(super::ZIP317_REVISION, 1);

    let orchard = fake_bundle_for_branch(
        zcash_protocol::consensus::BranchId::Nu6_3,
        ::orchard::ValuePool::Orchard,
        2,
        1,
    )
    .expect("the Orchard pool is defined at NU6.3");
    let ironwood = fake_bundle_for_branch(
        zcash_protocol::consensus::BranchId::Nu6_3,
        ::orchard::ValuePool::Ironwood,
        3,
        2,
    )
    .expect("the Ironwood pool is defined at NU6.3");
    let transaction = fake_v6_transaction(NetworkUpgrade::Nu6_3, Some(orchard), Some(ironwood));

    assert!(transaction.inputs().is_empty());
    assert!(transaction.outputs().is_empty());
    assert_eq!(transaction.sapling_spends_count(), 0);
    assert_eq!(transaction.sapling_outputs().count(), 0);
    assert_eq!(transaction.joinsplit_count(), 0);
    assert_eq!(transaction.orchard_actions().count(), 2);
    assert_eq!(transaction.ironwood_actions().count(), 3);
    assert_conventional_fee(&transaction, 5);
}

#[test]
fn zip317_unpaid_actions_err() {
    let check = mempool_checks(1, Amount::try_from(1).unwrap(), 1);

    assert!(check.is_err());
    assert_eq!(check.err(), Some(Error::UnpaidActions));
}

#[test]
fn zip317_minimum_rate_fee_err() {
    let check = mempool_checks(0, Amount::try_from(1).unwrap(), 1000);

    assert!(check.is_err());
    assert_eq!(check.err(), Some(Error::FeeBelowMinimumRate));
}

#[test]
fn zip317_mempool_checks_ok() {
    assert!(mempool_checks(0, Amount::try_from(100).unwrap(), 1000).is_ok())
}
