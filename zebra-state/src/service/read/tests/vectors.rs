//! Fixed test vectors for the ReadStateService.

use std::{collections::HashSet, sync::Arc};

use tower::ServiceExt;
use zebra_chain::{
    block::{Block, Hash, Height, MAX_BLOCK_LOCATOR_LENGTH},
    orchard,
    parameters::Network::*,
    serialization::{ZcashDeserializeInto, ZcashSerialize as _},
    subtree::{NoteCommitmentSubtree, NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    transaction,
};

use zebra_test::{
    prelude::Result,
    transcript::{ExpectedTranscriptError, Transcript},
};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    init_test_services, populated_state,
    response::MinedTx,
    service::{
        finalized_state::{DiskWriteBatch, ZebraDb, STATE_COLUMN_FAMILIES_IN_CODE},
        non_finalized_state::Chain,
        read::{orchard_subtrees, sapling_subtrees},
    },
    Config, ReadRequest, ReadResponse,
};

/// Test that ReadStateService responds correctly when empty.
#[tokio::test]
async fn empty_read_state_still_responds_to_requests() -> Result<()> {
    let _init_guard = zebra_test::init();

    let transcript = Transcript::from(empty_state_test_cases());

    let network = Mainnet;
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        init_test_services(&network).await;

    let block_summary_response = read_state
        .clone()
        .oneshot(ReadRequest::BlockSummary(Height::MIN))
        .await
        .expect("empty block summary request should succeed");
    assert_eq!(block_summary_response, ReadResponse::BlockSummary(None));

    let recent_response = read_state
        .clone()
        .oneshot(ReadRequest::RecentBlockSummaries {
            limit: 10,
            before_height: None,
            session_anchor: None,
            cursor_anchor: None,
        })
        .await
        .expect("empty recent block summaries request should succeed");
    assert_eq!(
        recent_response,
        ReadResponse::RecentBlockSummaries {
            best_tip: None,
            finalized_tip: None,
            blocks: Vec::new(),
            cursor_valid: true,
        }
    );

    let transaction_response = read_state
        .clone()
        .oneshot(ReadRequest::TransactionSummaryPage {
            limit: 10,
            before: None,
            session_anchor: None,
            cursor_anchor: None,
        })
        .await
        .expect("empty transaction summary request should succeed");
    assert_eq!(
        transaction_response,
        ReadResponse::TransactionSummaryPage {
            best_tip: None,
            finalized_tip: None,
            transactions: Vec::new(),
            cursor_valid: true,
        }
    );

    let chain_tips_response = read_state
        .clone()
        .oneshot(ReadRequest::ExplorerChainTips)
        .await
        .expect("empty chain tips request should succeed");
    assert_eq!(
        chain_tips_response,
        ReadResponse::ExplorerChainTips {
            best_tip: None,
            finalized_tip: None,
            tips: Vec::new(),
        }
    );

    transcript.check(read_state).await?;

    Ok(())
}

/// Test that the ReadStateService rejects a `FindForkPoint` locator longer than
/// `MAX_BLOCK_LOCATOR_LENGTH`, rather than performing an unbounded number of lookups.
#[tokio::test]
async fn find_fork_point_rejects_over_long_locator() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = Mainnet;
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        init_test_services(&network).await;

    // One hash over the cap. The contents are irrelevant: the length is checked
    // before any block is looked up.
    let over_long = vec![Hash([0; 32]); MAX_BLOCK_LOCATOR_LENGTH as usize + 1];

    let transcript = Transcript::from(vec![(
        ReadRequest::FindForkPoint {
            known_blocks: over_long,
        },
        Err(ExpectedTranscriptError::Any),
    )]);

    transcript.check(read_state).await?;

    Ok(())
}

/// Test that ReadStateService responds correctly when the state contains blocks.
#[tokio::test(flavor = "multi_thread")]
async fn populated_read_state_responds_correctly() -> Result<()> {
    let _init_guard = zebra_test::init();

    // Create a continuous chain of mainnet blocks from genesis
    let blocks: Vec<Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();

    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks.clone(), &Mainnet).await;

    let tip_height = Height(blocks.len() as u32 - 1);
    let tip_hash = blocks
        .last()
        .expect("populated state has at least one block")
        .hash();

    let empty_cases = Transcript::from(empty_state_test_cases());
    empty_cases.check(read_state.clone()).await?;

    for block in blocks {
        let block_cases = vec![
            (
                ReadRequest::Block(block.hash().into()),
                Ok(ReadResponse::Block(Some(block.clone()))),
            ),
            (
                ReadRequest::Block(block.coinbase_height().unwrap().into()),
                Ok(ReadResponse::Block(Some(block.clone()))),
            ),
        ];

        let block_cases = Transcript::from(block_cases);
        block_cases.check(read_state.clone()).await?;

        let fork_point_cases = vec![(
            ReadRequest::FindForkPoint {
                known_blocks: vec![block.hash()],
            },
            Ok(ReadResponse::ForkPoint(Some((
                block.coinbase_height().unwrap(),
                block.hash(),
            )))),
        )];
        let fork_point_cases = Transcript::from(fork_point_cases);
        fork_point_cases.check(read_state.clone()).await?;

        // Spec: transactions in the genesis block are ignored.
        if block.coinbase_height().unwrap().0 == 0 {
            continue;
        }

        for transaction in &block.transactions {
            let transaction_cases = vec![(
                ReadRequest::Transaction(transaction.hash()),
                Ok(ReadResponse::Transaction(Some(MinedTx {
                    tx: transaction.clone(),
                    height: block.coinbase_height().unwrap(),
                    confirmations: 1 + tip_height.0 - block.coinbase_height().unwrap().0,
                    block_time: block.header.time,
                    best_chain_tip_hash: tip_hash,
                }))),
            )];

            let transaction_cases = Transcript::from(transaction_cases);
            transaction_cases.check(read_state.clone()).await?;
        }
    }

    Ok(())
}

/// Recent block summaries are a contiguous, newest-first view backed by lightweight indexes.
#[tokio::test(flavor = "multi_thread")]
async fn recent_block_summaries_are_ordered_and_indexed() -> Result<()> {
    let _init_guard = zebra_test::init();

    let blocks: Vec<Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks.clone(), &Mainnet).await;

    let requested_count = 3.min(blocks.len());
    let response = read_state
        .clone()
        .oneshot(ReadRequest::RecentBlockSummaries {
            limit: requested_count,
            before_height: None,
            session_anchor: None,
            cursor_anchor: None,
        })
        .await
        .expect("recent block summaries request should succeed");
    let ReadResponse::RecentBlockSummaries {
        best_tip,
        finalized_tip,
        blocks: summaries,
        cursor_valid,
    } = response
    else {
        panic!("unexpected response to recent block summaries request")
    };
    assert!(cursor_valid);

    let expected_tip = blocks.last().expect("test chain is not empty");
    let expected_tip = (expected_tip.coinbase_height().unwrap(), expected_tip.hash());
    assert_eq!(best_tip, Some(expected_tip));
    assert_eq!(finalized_tip, Some(expected_tip));
    assert_eq!(summaries.len(), requested_count);

    let top_response = read_state
        .clone()
        .oneshot(ReadRequest::TopAddressesByBalance { limit: 1 })
        .await
        .expect("top-address request should succeed");
    let ReadResponse::TopAddressesByBalance { finalized_tip, .. } = top_response else {
        panic!("unexpected response to top-address request")
    };
    assert_eq!(finalized_tip, Some(expected_tip));

    for (summary, expected_block) in summaries.iter().zip(blocks.iter().rev()) {
        assert_eq!(summary.height, expected_block.coinbase_height().unwrap());
        assert_eq!(summary.hash, expected_block.hash());
        assert_eq!(summary.time, expected_block.header.time);
        assert_eq!(
            summary.info.size(),
            expected_block.zcash_serialized_size() as u32
        );
        assert_eq!(
            summary.info.transaction_count(),
            Some(expected_block.transactions.len() as u32)
        );
        assert!(summary.info.total_fee().is_some());
        assert!(summary.finalized);
    }

    let cursor_height = summaries
        .last()
        .expect("the first page is not empty")
        .height;
    let cursor_hash = summaries.last().expect("the first page is not empty").hash;
    let response = read_state
        .clone()
        .oneshot(ReadRequest::RecentBlockSummaries {
            limit: requested_count,
            before_height: Some(cursor_height),
            session_anchor: Some(expected_tip),
            cursor_anchor: Some((cursor_height, cursor_hash)),
        })
        .await
        .expect("cursor block summaries request should succeed");
    let ReadResponse::RecentBlockSummaries {
        blocks: cursor_summaries,
        ..
    } = response
    else {
        panic!("unexpected response to cursor block summaries request")
    };
    assert!(cursor_summaries
        .iter()
        .all(|summary| summary.height < cursor_height));

    let expected_cursor_blocks = blocks
        .iter()
        .rev()
        .skip(requested_count)
        .take(requested_count);
    for (summary, expected_block) in cursor_summaries.iter().zip(expected_cursor_blocks) {
        assert_eq!(summary.height, expected_block.coinbase_height().unwrap());
        assert_eq!(summary.hash, expected_block.hash());
    }

    let stale_response = read_state
        .oneshot(ReadRequest::RecentBlockSummaries {
            limit: requested_count,
            before_height: Some(cursor_height),
            session_anchor: Some((expected_tip.0, Hash([0x7f; 32]))),
            cursor_anchor: Some((cursor_height, cursor_hash)),
        })
        .await
        .expect("stale cursor request should return an explicit state response");
    let ReadResponse::RecentBlockSummaries {
        blocks,
        cursor_valid,
        ..
    } = stale_response
    else {
        panic!("unexpected response to stale cursor block summaries request")
    };
    assert!(!cursor_valid);
    assert!(blocks.is_empty());

    Ok(())
}

/// Transaction summary pages are newest-first and use an exclusive chain-location cursor.
#[tokio::test(flavor = "multi_thread")]
async fn transaction_summary_pages_are_ordered_and_cursor_paginated() -> Result<()> {
    let _init_guard = zebra_test::init();

    let blocks: Vec<Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks.clone(), &Mainnet).await;

    let expected_transactions: Vec<_> = blocks
        .iter()
        .rev()
        .flat_map(|block| {
            let height = block.coinbase_height().unwrap();
            block
                .transactions
                .iter()
                .enumerate()
                .rev()
                .map(move |(index, tx)| (height, block, index, tx))
        })
        .collect();
    let requested_count = 3.min(expected_transactions.len());

    let response = read_state
        .clone()
        .oneshot(ReadRequest::TransactionSummaryPage {
            limit: requested_count,
            before: None,
            session_anchor: None,
            cursor_anchor: None,
        })
        .await
        .expect("transaction summary page request should succeed");
    let ReadResponse::TransactionSummaryPage {
        best_tip,
        finalized_tip,
        transactions,
        cursor_valid,
    } = response
    else {
        panic!("unexpected response to transaction summary page request")
    };
    assert!(cursor_valid);

    let expected_tip = blocks.last().expect("test chain is not empty");
    let expected_tip = (expected_tip.coinbase_height().unwrap(), expected_tip.hash());
    assert_eq!(best_tip, Some(expected_tip));
    assert_eq!(finalized_tip, Some(expected_tip));
    assert_eq!(transactions.len(), requested_count);

    for (summary, (height, block, index, tx)) in
        transactions.iter().zip(expected_transactions.iter())
    {
        assert_eq!(summary.location.height, *height);
        assert_eq!(summary.location.index.as_usize(), *index);
        assert_eq!(summary.hash, tx.hash());
        assert_eq!(summary.block_hash, block.hash());
        assert_eq!(summary.block_time, block.header.time);
        assert_eq!(summary.size, tx.zcash_serialized_size() as u32);
        assert_eq!(summary.version, tx.version());
        assert_eq!(summary.coinbase, tx.is_coinbase());
        assert_eq!(
            summary.positive_transparent_output_count,
            u32::try_from(
                tx.outputs()
                    .iter()
                    .filter(|output| output.value().zatoshis() > 0)
                    .count()
            )
            .expect("transparent output count is bounded by transaction bytes")
        );
        let expected_credit_zat = tx
            .output_values_to_sprout()
            .into_iter()
            .map(|value| u64::try_from(value).unwrap())
            .sum::<u64>()
            + [
                tx.sapling_value_balance().sapling_amount().zatoshis(),
                tx.orchard_value_balance().orchard_amount().zatoshis(),
                tx.ironwood_value_balance().ironwood_amount().zatoshis(),
            ]
            .into_iter()
            .filter(|value| *value < 0)
            .map(i64::unsigned_abs)
            .sum::<u64>();
        let expected_debit_zat = tx
            .input_values_from_sprout()
            .into_iter()
            .map(|value| u64::try_from(value).unwrap())
            .sum::<u64>()
            + [
                tx.sapling_value_balance().sapling_amount().zatoshis(),
                tx.orchard_value_balance().orchard_amount().zatoshis(),
                tx.ironwood_value_balance().ironwood_amount().zatoshis(),
            ]
            .into_iter()
            .filter(|value| *value > 0)
            .map(i64::unsigned_abs)
            .sum::<u64>();
        assert_eq!(summary.shielded_credit_zat, expected_credit_zat);
        assert_eq!(summary.shielded_debit_zat, expected_debit_zat);
        assert!(summary.finalized);
    }

    let cursor = transactions
        .last()
        .expect("the first transaction page is not empty")
        .location;
    let cursor_hash = transactions
        .last()
        .expect("the first transaction page is not empty")
        .block_hash;
    let response = read_state
        .oneshot(ReadRequest::TransactionSummaryPage {
            limit: requested_count,
            before: Some(cursor),
            session_anchor: Some(expected_tip),
            cursor_anchor: Some((cursor.height, cursor_hash)),
        })
        .await
        .expect("cursor transaction summary page request should succeed");
    let ReadResponse::TransactionSummaryPage {
        transactions: cursor_transactions,
        ..
    } = response
    else {
        panic!("unexpected response to cursor transaction summary page request")
    };

    for (summary, (height, _block, index, tx)) in cursor_transactions
        .iter()
        .zip(expected_transactions.iter().skip(requested_count))
    {
        assert_eq!(summary.location.height, *height);
        assert_eq!(summary.location.index.as_usize(), *index);
        assert_eq!(summary.hash, tx.hash());
        assert!(summary.location < cursor);
    }

    Ok(())
}

/// Address transaction pages use the address index and an exclusive location cursor.
#[tokio::test(flavor = "multi_thread")]
async fn address_transaction_summary_pages_are_bounded_and_cursor_paginated() -> Result<()> {
    let _init_guard = zebra_test::init();

    let blocks: Vec<Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (address, expected_hash) = blocks
        .iter()
        .rev()
        .flat_map(|block| block.transactions.iter().rev())
        .find_map(|tx| {
            tx.outputs()
                .iter()
                .find_map(|output| output.address(&Mainnet))
                .map(|address| (address, tx.hash()))
        })
        .expect("continuous blocks must contain a transparent output");
    let session_block = blocks.last().expect("test chain is not empty");
    let session_anchor = (
        session_block.coinbase_height().unwrap(),
        session_block.hash(),
    );
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks, &Mainnet).await;

    let response = read_state
        .clone()
        .oneshot(ReadRequest::AddressTransactionSummaryPage {
            address,
            limit: 100,
            before: None,
            session_anchor: None,
            cursor_anchor: None,
        })
        .await
        .expect("address transaction summary page request should succeed");
    let ReadResponse::AddressTransactionSummaryPage {
        transactions: all_transactions,
        ..
    } = response
    else {
        panic!("unexpected response to address transaction summary page request")
    };
    assert!(all_transactions
        .iter()
        .any(|summary| summary.transaction.hash == expected_hash));
    assert!(all_transactions
        .windows(2)
        .all(|pair| pair[0].transaction.location > pair[1].transaction.location));
    assert!(all_transactions
        .iter()
        .all(|summary| { summary.received_zat.is_some() && summary.spent_zat.is_some() }));

    let response = read_state
        .clone()
        .oneshot(ReadRequest::AddressTransactionSummaryPage {
            address,
            limit: 1,
            before: None,
            session_anchor: None,
            cursor_anchor: None,
        })
        .await
        .expect("first address transaction page request should succeed");
    let ReadResponse::AddressTransactionSummaryPage {
        transactions: first_page,
        ..
    } = response
    else {
        panic!("unexpected response to first address transaction page request")
    };
    assert_eq!(first_page, all_transactions[..1]);

    let cursor = first_page[0].transaction.location;
    let cursor_hash = first_page[0].transaction.block_hash;
    let response = read_state
        .oneshot(ReadRequest::AddressTransactionSummaryPage {
            address,
            limit: 1,
            before: Some(cursor),
            session_anchor: Some(session_anchor),
            cursor_anchor: Some((cursor.height, cursor_hash)),
        })
        .await
        .expect("cursor address transaction page request should succeed");
    let ReadResponse::AddressTransactionSummaryPage {
        transactions: second_page,
        ..
    } = response
    else {
        panic!("unexpected response to cursor address transaction page request")
    };
    assert_eq!(second_page, all_transactions.get(1..2).unwrap_or_default());
    assert!(second_page
        .iter()
        .all(|summary| summary.transaction.location < cursor));

    Ok(())
}

/// Address UTXO pages are newest-first and bound to the exact best tip.
#[tokio::test(flavor = "multi_thread")]
async fn address_utxo_summary_page_is_bounded_and_exact_tip_anchored() -> Result<()> {
    let _init_guard = zebra_test::init();

    let blocks: Vec<Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let candidate_addresses: HashSet<_> = blocks
        .iter()
        .flat_map(|block| block.transactions.iter())
        .flat_map(|transaction| transaction.outputs())
        .filter_map(|output| output.address(&Mainnet))
        .collect();
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks.clone(), &Mainnet).await;

    let mut first_page = None;
    for address in candidate_addresses {
        let response = read_state
            .clone()
            .oneshot(ReadRequest::AddressUtxoSummaryPage {
                address,
                limit: 1,
                before: None,
                cursor_anchor: None,
            })
            .await
            .expect("address UTXO summary request should succeed");
        let ReadResponse::AddressUtxoSummaryPage {
            best_tip,
            finalized_tip,
            cursor_valid,
            utxos,
        } = response
        else {
            panic!("unexpected response to address UTXO summary request")
        };
        if let Some(utxo) = utxos.into_iter().next() {
            first_page = Some((address, best_tip.unwrap(), finalized_tip, utxo));
            assert!(cursor_valid);
            break;
        }
    }

    let (address, anchor, finalized_tip, first_utxo) =
        first_page.expect("continuous vectors contain an indexed unspent address output");
    let expected_tip = blocks.last().expect("test chain is not empty");
    assert_eq!(
        anchor,
        (expected_tip.coinbase_height().unwrap(), expected_tip.hash())
    );
    assert_eq!(finalized_tip, Some(anchor));
    assert_eq!(first_utxo.output.address(&Mainnet), Some(address));

    let response = read_state
        .clone()
        .oneshot(ReadRequest::AddressUtxoSummaryPage {
            address,
            limit: 1,
            before: Some(first_utxo.location),
            cursor_anchor: Some(anchor),
        })
        .await
        .expect("anchored address UTXO page request should succeed");
    let ReadResponse::AddressUtxoSummaryPage {
        cursor_valid,
        utxos,
        ..
    } = response
    else {
        panic!("unexpected response to anchored address UTXO summary request")
    };
    assert!(cursor_valid);
    assert!(utxos
        .iter()
        .all(|summary| summary.location < first_utxo.location));

    let response = read_state
        .oneshot(ReadRequest::AddressUtxoSummaryPage {
            address,
            limit: 1,
            before: Some(first_utxo.location),
            cursor_anchor: Some((anchor.0, Hash([0x7f; 32]))),
        })
        .await
        .expect("stale address UTXO cursor should return an explicit state response");
    let ReadResponse::AddressUtxoSummaryPage {
        cursor_valid,
        utxos,
        ..
    } = response
    else {
        panic!("unexpected response to stale address UTXO summary request")
    };
    assert!(!cursor_valid);
    assert!(utxos.is_empty());

    Ok(())
}

/// Arbitrary-height block summaries use the same lightweight indexes as recent summaries.
#[tokio::test(flavor = "multi_thread")]
async fn block_summaries_are_available_by_height() -> Result<()> {
    let _init_guard = zebra_test::init();

    let blocks: Vec<Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks.clone(), &Mainnet).await;

    for expected_block in &blocks {
        let height = expected_block.coinbase_height().unwrap();
        let response = read_state
            .clone()
            .oneshot(ReadRequest::BlockSummary(height))
            .await
            .expect("block summary request should succeed");
        let ReadResponse::BlockSummary(Some(summary)) = response else {
            panic!("expected a block summary at height {height:?}")
        };

        assert_eq!(summary.height, height);
        assert_eq!(summary.hash, expected_block.hash());
        assert_eq!(summary.time, expected_block.header.time);
        assert_eq!(
            summary.info.size(),
            expected_block.zcash_serialized_size() as u32
        );
        assert_eq!(
            summary.info.transaction_count(),
            Some(expected_block.transactions.len() as u32)
        );
        assert!(summary.info.total_fee().is_some());
        assert!(summary.finalized);
    }

    let missing_height = Height(
        blocks
            .last()
            .expect("test chain is not empty")
            .coinbase_height()
            .unwrap()
            .0
            + 1,
    );
    let missing_response = read_state
        .oneshot(ReadRequest::BlockSummary(missing_height))
        .await
        .expect("missing block summary request should succeed");
    assert_eq!(missing_response, ReadResponse::BlockSummary(None));

    Ok(())
}

/// Tests if Zebra combines the note commitment subtrees from the finalized and
/// non-finalized states correctly.
#[tokio::test]
async fn test_read_subtrees() -> Result<()> {
    use std::ops::Bound::*;

    let dummy_subtree = |(index, height)| {
        NoteCommitmentSubtree::new(
            u16::try_from(index).expect("should fit in u16"),
            Height(height),
            sapling_crypto::Node::from_bytes([0; 32]).unwrap(),
        )
    };

    let num_db_subtrees = 10;
    let num_chain_subtrees = 2;
    let index_offset = usize::try_from(num_db_subtrees).expect("constant should fit in usize");
    let db_height_range = 0..num_db_subtrees;
    let chain_height_range = num_db_subtrees..(num_db_subtrees + num_chain_subtrees);

    // Prepare the finalized state.
    let db = {
        let db = new_ephemeral_db();

        let db_subtrees = db_height_range.enumerate().map(dummy_subtree);
        for db_subtree in db_subtrees {
            let mut db_batch = DiskWriteBatch::new();
            db_batch.insert_sapling_subtree(&db, &db_subtree);
            db.write(db_batch)
                .expect("Writing a batch with a Sapling subtree should succeed.");
        }
        db
    };

    // Prepare the non-finalized state.
    let chain = {
        let mut chain = Chain::default();
        let chain_subtrees = chain_height_range
            .enumerate()
            .map(|(index, height)| dummy_subtree((index_offset + index, height)));

        for chain_subtree in chain_subtrees {
            chain.insert_sapling_subtree(chain_subtree);
        }

        Arc::new(chain)
    };

    let modify_chain = |chain: &Arc<Chain>, index: usize, height| {
        let mut chain = chain.as_ref().clone();
        chain.insert_sapling_subtree(dummy_subtree((index, height)));
        Some(Arc::new(chain))
    };

    // There should be 10 entries in db and 2 in chain with no overlap

    // Unbounded range should start at 0
    let all_subtrees = sapling_subtrees(Some(chain.clone()), &db, ..);
    assert_eq!(all_subtrees.len(), 12, "should have 12 subtrees in state");

    // Add a subtree to `chain` that overlaps and is not consistent with the db subtrees
    let first_chain_index = index_offset - 1;
    let end_height = Height(400_000);
    let modified_chain = modify_chain(&chain, first_chain_index, end_height.0);

    // The inconsistent entry and any later entries should be omitted
    let all_subtrees = sapling_subtrees(modified_chain.clone(), &db, ..);
    assert_eq!(all_subtrees.len(), 10, "should have 10 subtrees in state");

    let first_chain_index =
        NoteCommitmentSubtreeIndex(u16::try_from(first_chain_index).expect("should fit in u16"));

    // Entries should be returned without reading from disk if the chain contains the first subtree index in the range
    let mut chain_subtrees = sapling_subtrees(modified_chain, &db, first_chain_index..);
    assert_eq!(chain_subtrees.len(), 3, "should have 3 subtrees in chain");

    let (index, subtree) = chain_subtrees
        .pop_first()
        .expect("chain_subtrees should not be empty");
    assert_eq!(first_chain_index, index, "subtree indexes should match");
    assert_eq!(
        end_height, subtree.end_height,
        "subtree end heights should match"
    );

    // Check that Zebra retrieves subtrees correctly when using a range with an Excluded start bound

    let start = 0.into();
    let range = (Excluded(start), Unbounded);
    let subtrees = sapling_subtrees(Some(chain), &db, range);
    assert_eq!(subtrees.len(), 11);
    assert!(
        !subtrees.contains_key(&start),
        "should not contain excluded start bound"
    );

    Ok(())
}

/// Tests if Zebra combines the Sapling note commitment subtrees from the finalized and
/// non-finalized states correctly.
#[tokio::test]
async fn test_sapling_subtrees() -> Result<()> {
    let dummy_subtree_root = sapling_crypto::Node::from_bytes([0; 32]).unwrap();

    // Prepare the finalized state.
    let db_subtree = NoteCommitmentSubtree::new(0, Height(1), dummy_subtree_root);

    let db = new_ephemeral_db();
    let mut db_batch = DiskWriteBatch::new();
    db_batch.insert_sapling_subtree(&db, &db_subtree);
    db.write(db_batch)
        .expect("Writing a batch with a Sapling subtree should succeed.");

    // Prepare the non-finalized state.
    let chain_subtree = NoteCommitmentSubtree::new(1, Height(3), dummy_subtree_root);
    let mut chain = Chain::default();
    chain.insert_sapling_subtree(chain_subtree);
    let chain = Some(Arc::new(chain));

    // At this point, we have one Sapling subtree in the finalized state and one Sapling subtree in
    // the non-finalized state.

    // Retrieve only the first subtree and check its properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..1.into());
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));

    // Retrieve both subtrees using a limit and check their properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..2.into());
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve both subtrees without using a limit and check their properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..);
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree and check its properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..2.into());
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, using a limit that would allow for more trees if they were
    // present, and check its properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..3.into());
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, without using any limit, and check its properties.
    let subtrees = sapling_subtrees(chain, &db, NoteCommitmentSubtreeIndex(1)..);
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    Ok(())
}

/// Tests if Zebra combines the Orchard note commitment subtrees from the finalized and
/// non-finalized states correctly.
#[tokio::test]
async fn test_orchard_subtrees() -> Result<()> {
    let dummy_subtree_root = orchard::tree::Node::default();

    // Prepare the finalized state.
    let db_subtree = NoteCommitmentSubtree::new(0, Height(1), dummy_subtree_root);

    let db = new_ephemeral_db();
    let mut db_batch = DiskWriteBatch::new();
    db_batch.insert_orchard_subtree(&db, &db_subtree);
    db.write(db_batch)
        .expect("Writing a batch with an Orchard subtree should succeed.");

    // Prepare the non-finalized state.
    let chain_subtree = NoteCommitmentSubtree::new(1, Height(3), dummy_subtree_root);
    let mut chain = Chain::default();
    chain.insert_orchard_subtree(chain_subtree);
    let chain = Some(Arc::new(chain));

    // At this point, we have one Orchard subtree in the finalized state and one Orchard subtree in
    // the non-finalized state.

    // Retrieve only the first subtree and check its properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..1.into());
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));

    // Retrieve both subtrees using a limit and check their properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..2.into());
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve both subtrees without using a limit and check their properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..);
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree and check its properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..2.into());
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, using a limit that would allow for more trees if they were
    // present, and check its properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..3.into());
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, without using any limit, and check its properties.
    let subtrees = orchard_subtrees(chain, &db, NoteCommitmentSubtreeIndex(1)..);
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    Ok(())
}

/// Returns test cases for the empty state and missing blocks.
fn empty_state_test_cases() -> Vec<(ReadRequest, Result<ReadResponse, ExpectedTranscriptError>)> {
    let block: Arc<Block> = zebra_test::vectors::BLOCK_MAINNET_419200_BYTES
        .zcash_deserialize_into()
        .unwrap();

    vec![
        (
            ReadRequest::Transaction(transaction::Hash([0; 32])),
            Ok(ReadResponse::Transaction(None)),
        ),
        (
            ReadRequest::Block(block.hash().into()),
            Ok(ReadResponse::Block(None)),
        ),
        (
            ReadRequest::Block(block.coinbase_height().unwrap().into()),
            Ok(ReadResponse::Block(None)),
        ),
        (
            ReadRequest::FindForkPoint {
                known_blocks: vec![block.hash()],
            },
            Ok(ReadResponse::ForkPoint(None)),
        ),
    ]
}

/// Returns `true` if `index` and `subtree_data` match the contents of `subtree`. Otherwise, returns
/// `false`.
fn subtrees_eq<N>(
    (index, subtree_data): (&NoteCommitmentSubtreeIndex, &NoteCommitmentSubtreeData<N>),
    subtree: &NoteCommitmentSubtree<N>,
) -> bool
where
    N: PartialEq + Copy,
{
    index == &subtree.index && subtree_data == &subtree.into_data()
}

/// Returns a new ephemeral database with no consistency checks.
fn new_ephemeral_db() -> ZebraDb {
    ZebraDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        &Mainnet,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("opening an ephemeral database should succeed")
}

/// Test that AnyChainBlock can find blocks by hash and height.
#[tokio::test(flavor = "multi_thread")]
async fn any_chain_block_test() -> Result<()> {
    let _init_guard = zebra_test::init();

    // Create a continuous chain of mainnet blocks from genesis
    let blocks: Vec<Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();

    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks.clone(), &Mainnet).await;

    // Test: AnyChainBlock should find blocks by hash (same as Block)
    for block in &blocks {
        let request = ReadRequest::AnyChainBlock(block.hash().into());
        let response = read_state
            .clone()
            .oneshot(request)
            .await
            .expect("request should succeed");
        assert!(
            matches!(
                response,
                ReadResponse::Block(Some(found_block)) if found_block.hash() == block.hash()
            ),
            "AnyChainBlock should find block by hash"
        );
    }

    // Test: AnyChainBlock should find blocks by height (same as Block)
    for block in &blocks {
        let height = block.coinbase_height().unwrap();
        let request = ReadRequest::AnyChainBlock(height.into());
        let response = read_state
            .clone()
            .oneshot(request)
            .await
            .expect("request should succeed");
        assert!(
            matches!(
                response,
                ReadResponse::Block(Some(found_block)) if found_block.hash() == block.hash()
            ),
            "AnyChainBlock should find block by height"
        );
    }

    // Test: Non-existent block should return None
    let fake_hash = zebra_chain::block::Hash([0xff; 32]);
    let request = ReadRequest::AnyChainBlock(fake_hash.into());
    let response = read_state
        .clone()
        .oneshot(request)
        .await
        .expect("request should succeed");
    assert!(
        matches!(response, ReadResponse::Block(None)),
        "AnyChainBlock should return None for non-existent block"
    );

    Ok(())
}

/// Test that AnyChainBlock finds blocks in side chains, while Block does not.
#[tokio::test(flavor = "multi_thread")]
async fn any_chain_block_finds_side_chain_blocks() -> Result<()> {
    use crate::{
        arbitrary::Prepare,
        service::{finalized_state::FinalizedState, non_finalized_state::NonFinalizedState},
        tests::FakeChainHelper,
    };
    use zebra_chain::{amount::NonNegative, value_balance::ValueBalance};

    let _init_guard = zebra_test::init();

    let network = Mainnet;

    // Use pre-Heartwood blocks to avoid history tree complications
    let genesis: Arc<Block> = Arc::new(network.test_block(653599, 583999).unwrap());

    // Create two different blocks from genesis
    // They have the same parent but different work, making them compete
    let best_chain_block = genesis.make_fake_child().set_work(100);
    let side_chain_block = genesis.make_fake_child().set_work(50);

    // Even though they have the same structure, changing work changes the header hash
    // because difficulty_threshold is part of the header
    let best_hash = best_chain_block.hash();
    let side_hash = side_chain_block.hash();

    // If hashes are the same, we can't test side chains properly
    // This would mean our fake block generation isn't working as expected
    if best_hash == side_hash {
        tracing::warn!("unable to create different block hashes, skipping side chain test");
        return Ok(());
    }

    // Create state with a finalized and non-finalized component
    let mut non_finalized_state = NonFinalizedState::new(&network);
    let finalized_state = FinalizedState::new(
        &Config::ephemeral(),
        &network,
        #[cfg(feature = "elasticsearch")]
        false,
    )
    .expect("opening an ephemeral database should succeed");

    let fake_value_pool = ValueBalance::<NonNegative>::fake_populated_pool();
    finalized_state.set_finalized_value_pool(fake_value_pool);

    // Commit genesis as the first chain
    non_finalized_state.commit_new_chain(genesis.clone().prepare(), &finalized_state)?;

    // Commit best chain block (higher work) - extends the genesis chain
    non_finalized_state.commit_block(best_chain_block.clone().prepare(), &finalized_state)?;

    // Commit side chain block (lower work) - also tries to extend genesis, creating a fork
    non_finalized_state.commit_block(side_chain_block.clone().prepare(), &finalized_state)?;

    // Verify we have 2 chains (genesis extended by best_chain_block, and genesis extended by side_chain_block)
    assert_eq!(
        non_finalized_state.chain_count(),
        2,
        "Should have 2 competing chains"
    );

    // Now test with the read interface
    // We'll use the low-level block lookup functions directly
    use crate::service::read::block::{any_block, block, explorer_chain_tips};

    // Test 1: any_block with all chains should find the side chain block by hash
    let found = any_block(
        non_finalized_state.chain_iter(),
        &finalized_state.db,
        side_hash.into(),
    );
    assert!(
        found.is_some(),
        "any_block should find side chain block by hash"
    );
    assert_eq!(found.unwrap().hash(), side_hash);

    // Test 2: block with only best chain should NOT find the side chain block by hash
    let found = block(
        non_finalized_state.best_chain(),
        &finalized_state.db,
        side_hash.into(),
    );
    assert!(
        found.is_none(),
        "block should NOT find side chain block by hash"
    );

    // Test 3: any_block should find the best chain block by hash
    let found = any_block(
        non_finalized_state.chain_iter(),
        &finalized_state.db,
        best_hash.into(),
    );
    assert!(
        found.is_some(),
        "any_block should find best chain block by hash"
    );
    assert_eq!(found.unwrap().hash(), best_hash);

    let (best_tip, finalized_tip, tips) =
        explorer_chain_tips(&non_finalized_state, &finalized_state.db);
    assert_eq!(
        best_tip,
        Some((best_chain_block.coinbase_height().unwrap(), best_hash))
    );
    assert_eq!(finalized_tip, None);
    assert_eq!(tips.len(), 2);
    assert!(tips[0].active);
    assert_eq!(tips[0].hash, best_hash);
    assert_eq!(tips[0].branch_length, 0);
    assert!(!tips[1].active);
    assert_eq!(tips[1].hash, side_hash);
    assert_eq!(tips[1].branch_length, 1);
    assert_eq!(tips[1].fork_height, genesis.coinbase_height());
    assert_eq!(tips[1].fork_hash, Some(genesis.hash()));

    // Test 4: block should also find the best chain block by hash
    let found = block(
        non_finalized_state.best_chain(),
        &finalized_state.db,
        best_hash.into(),
    );
    assert!(
        found.is_some(),
        "block should find best chain block by hash"
    );
    assert_eq!(found.unwrap().hash(), best_hash);

    Ok(())
}
