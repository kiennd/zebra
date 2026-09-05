//! Shared block, header, and transaction reading code.
//!
//! In the functions in this module:
//!
//! The block write task commits blocks to the finalized state before updating
//! `chain` or `non_finalized_state` with a cached copy of the non-finalized chains
//! in `NonFinalizedState.chain_set`. Then the block commit task can
//! commit additional blocks to the finalized state after we've cloned the
//! `chain` or `non_finalized_state`.
//!
//! This means that some blocks can be in both:
//! - the cached [`Chain`] or [`NonFinalizedState`], and
//! - the shared finalized [`ZebraDb`] reference.

use std::{
    ops::Bound::{Excluded, Included, Unbounded},
    sync::Arc,
};

use chrono::{DateTime, Utc};

use zebra_chain::{
    block::{self, Block, Height},
    block_info::BlockInfo,
    serialization::ZcashSerialize as _,
    transaction::{self, Transaction},
    transparent::{self, Utxo},
};

use crate::{
    response::{AnyTx, ExplorerChainTip, ExplorerTransactionSummary, MinedTx, RecentBlockSummary},
    service::{
        finalized_state::ZebraDb,
        non_finalized_state::{Chain, NonFinalizedState},
        read::tip,
    },
    HashOrHeight,
};

#[cfg(feature = "indexer")]
use crate::request::Spend;

/// Returns the [`Block`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chains` or finalized `db`.
pub fn any_block<'a, C: AsRef<Chain> + 'a>(
    mut chains: impl Iterator<Item = &'a C>,
    db: &ZebraDb,
    hash_or_height: HashOrHeight,
) -> Option<Arc<Block>> {
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chains
        .find_map(|c| c.as_ref().block(hash_or_height))
        .map(|contextual| contextual.block.clone())
        .or_else(|| db.block(hash_or_height))
}

/// Returns the [`Block`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chain` or finalized `db`.
pub fn block<C>(chain: Option<C>, db: &ZebraDb, hash_or_height: HashOrHeight) -> Option<Arc<Block>>
where
    C: AsRef<Chain>,
{
    any_block(chain.iter(), db, hash_or_height)
}

/// Returns the [`Block`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chain` or finalized `db`.
pub fn block_and_size<C>(
    chain: Option<C>,
    db: &ZebraDb,
    hash_or_height: HashOrHeight,
) -> Option<(Arc<Block>, usize)>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chain
        .as_ref()
        .and_then(|chain| chain.as_ref().block(hash_or_height))
        .map(|contextual| {
            let size = contextual.block.zcash_serialize_to_vec().unwrap().len();
            (contextual.block.clone(), size)
        })
        .or_else(|| db.block_and_size(hash_or_height))
}

/// Returns the [`block::Header`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chain` or finalized `db`.
pub fn block_header<C>(
    chain: Option<C>,
    db: &ZebraDb,
    hash_or_height: HashOrHeight,
) -> Option<Arc<block::Header>>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chain
        .as_ref()
        .and_then(|chain| chain.as_ref().block(hash_or_height))
        .map(|contextual| contextual.block.header.clone())
        .or_else(|| db.block_header(hash_or_height))
}

/// Returns the [`Transaction`] with [`transaction::Hash`], if it exists in the
/// non-finalized `chain` or finalized `db`.
fn transaction<C>(
    chain: Option<C>,
    db: &ZebraDb,
    hash: transaction::Hash,
) -> Option<(Arc<Transaction>, Height, DateTime<Utc>)>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since transactions are the same in the finalized and non-finalized state,
    // we check the most efficient alternative first. (`chain` is always in
    // memory, but `db` stores transactions on disk, with a memory cache.)
    chain
        .and_then(|chain| {
            chain
                .as_ref()
                .transaction(hash)
                .map(|(tx, height, time)| (tx.clone(), height, time))
        })
        .or_else(|| db.transaction(hash))
}

/// Returns a [`MinedTx`] for a [`Transaction`] with [`transaction::Hash`],
/// if one exists in the non-finalized `chain` or finalized `db`.
pub fn mined_transaction<C>(
    chain: Option<C>,
    db: &ZebraDb,
    hash: transaction::Hash,
) -> Option<MinedTx>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // It is ok to do this lookup in two different calls. Finalized state updates
    // can only add overlapping blocks, and hashes are unique.
    let chain = chain.as_ref();

    let (tx, height, time) = transaction(chain, db, hash)?;
    let (tip_height, tip_hash) = tip(chain, db)?;
    let confirmations = 1 + tip_height.0 - height.0;

    Some(MinedTx::new(tx, height, confirmations, time, tip_hash))
}

/// Returns a [`AnyTx`] for a [`Transaction`] with [`transaction::Hash`],
/// if one exists in any chain in `chains` or finalized `db`.
/// The first chain in `chains` must be the best chain.
pub fn any_transaction<'a>(
    chains: impl Iterator<Item = &'a Arc<Chain>>,
    db: &ZebraDb,
    hash: transaction::Hash,
) -> Option<AnyTx> {
    // # Correctness
    //
    // It is ok to do this lookup in multiple different calls. Finalized state updates
    // can only add overlapping blocks, and hashes are unique.
    //
    // Capture the best chain tip before searching, not inside the search closure.
    // The closure only runs when the tx is found in a non-finalized chain; if the tx
    // is only in the finalized DB, the closure never fires and best_chain would stay
    // None, causing tip_height to undercount confirmations by ~MAX_BLOCK_REORG_HEIGHT.
    // See <https://github.com/ZcashFoundation/zebra/issues/10470>.
    // peekable() reads the first element without consuming it, so the iterator can
    // still be used in find_map below.
    let mut chains = chains.peekable();
    let best_chain = chains.peek().copied();
    let (tx, height, time, in_best_chain, containing_chain) = chains
        .enumerate()
        .find_map(|(i, chain)| {
            chain
                .as_ref()
                .transaction(hash)
                .map(|(tx, height, time)| (tx.clone(), height, time, i == 0, Some(chain)))
        })
        .or_else(|| {
            db.transaction(hash)
                .map(|(tx, height, time)| (tx.clone(), height, time, true, None))
        })?;

    if in_best_chain {
        let (tip_height, tip_hash) = tip(best_chain, db)?;
        let confirmations = 1 + tip_height.0 - height.0;
        Some(AnyTx::Mined(MinedTx::new(
            tx,
            height,
            confirmations,
            time,
            tip_hash,
        )))
    } else {
        let block_hash = containing_chain?.block(height.into())?.hash;
        Some(AnyTx::Side((tx, block_hash)))
    }
}

/// Returns the [`transaction::Hash`]es for the block with `hash_or_height`,
/// if it exists in the non-finalized `chain` or finalized `db`.
///
/// The returned hashes are in block order.
///
/// Returns `None` if the block is not found.
pub fn transaction_hashes_for_block<C>(
    chain: Option<C>,
    db: &ZebraDb,
    hash_or_height: HashOrHeight,
) -> Option<Arc<[transaction::Hash]>>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chain
        .as_ref()
        .and_then(|chain| chain.as_ref().transaction_hashes_for_block(hash_or_height))
        .or_else(|| db.transaction_hashes_for_block(hash_or_height))
}

/// Returns the [`transaction::Hash`]es for the block with `hash_or_height`,
/// if it exists in any chain in `chains` or finalized `db`.
/// The first chain in `chains` must be the best chain.
///
/// The returned hashes are in block order.
///
/// Returns `None` if the block is not found.
pub fn transaction_hashes_for_any_block<'a>(
    chains: impl Iterator<Item = &'a Arc<Chain>>,
    db: &ZebraDb,
    hash_or_height: HashOrHeight,
) -> Option<(Arc<[transaction::Hash]>, bool)> {
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chains
        .enumerate()
        .find_map(|(i, chain)| {
            chain
                .as_ref()
                .transaction_hashes_for_block(hash_or_height)
                .map(|hashes| (hashes.clone(), i == 0))
        })
        .or_else(|| {
            db.transaction_hashes_for_block(hash_or_height)
                .map(|hashes| (hashes, true))
        })
}

/// Returns the [`Utxo`] for [`transparent::OutPoint`], if it exists in the
/// non-finalized `chain` or finalized `db`.
///
/// Non-finalized UTXOs are returned regardless of whether they have been spent.
///
/// Finalized UTXOs are only returned if they are unspent in the finalized chain.
/// They may have been spent in the non-finalized chain,
/// but this function returns them without checking for non-finalized spends,
/// because we don't know which non-finalized chain will be committed to the finalized state.
pub fn utxo<C>(chain: Option<C>, db: &ZebraDb, outpoint: transparent::OutPoint) -> Option<Utxo>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since UTXOs are the same in the finalized and non-finalized state,
    // we check the most efficient alternative first. (`chain` is always in
    // memory, but `db` stores transactions on disk, with a memory cache.)
    chain
        .and_then(|chain| chain.as_ref().created_utxo(&outpoint))
        .or_else(|| db.utxo(&outpoint).map(|utxo| utxo.utxo))
}

/// Returns the [`Utxo`] for [`transparent::OutPoint`], if it exists and is unspent in the
/// non-finalized `chain` or finalized `db`.
pub fn unspent_utxo<C>(
    chain: Option<C>,
    db: &ZebraDb,
    outpoint: transparent::OutPoint,
) -> Option<Utxo>
where
    C: AsRef<Chain>,
{
    match chain {
        Some(chain) if chain.as_ref().spent_utxos.contains_key(&outpoint) => None,
        chain => utxo(chain, db, outpoint),
    }
}

/// Returns the [`Hash`](transaction::Hash) of the transaction that spent an output at
/// the provided [`transparent::OutPoint`] or revealed the provided nullifier, if it exists
/// and is spent or revealed in the non-finalized `chain` or finalized `db` and its
/// spending transaction hash has been indexed.
#[cfg(feature = "indexer")]
pub fn spending_transaction_hash<C>(
    chain: Option<C>,
    db: &ZebraDb,
    spend: Spend,
) -> Option<transaction::Hash>
where
    C: AsRef<Chain>,
{
    chain
        .and_then(|chain| chain.as_ref().spending_transaction_hash(&spend))
        .or_else(|| db.spending_transaction_hash(&spend))
}

/// Returns the [`Utxo`] for [`transparent::OutPoint`], if it exists in any chain
/// in the `non_finalized_state`, or in the finalized `db`.
///
/// Non-finalized UTXOs are returned regardless of whether they have been spent.
///
/// Finalized UTXOs are only returned if they are unspent in the finalized chain.
/// They may have been spent in one or more non-finalized chains,
/// but this function returns them without checking for non-finalized spends,
/// because we don't know which non-finalized chain the request belongs to.
///
/// UTXO spends are checked once the block reaches the non-finalized state,
/// by [`check::utxo::transparent_spend()`](crate::service::check::utxo::transparent_spend).
pub fn any_utxo(
    non_finalized_state: NonFinalizedState,
    db: &ZebraDb,
    outpoint: transparent::OutPoint,
) -> Option<Utxo> {
    // # Correctness
    //
    // Since UTXOs are the same in the finalized and non-finalized state,
    // we check the most efficient alternative first. (`non_finalized_state` is always in
    // memory, but `db` stores transactions on disk, with a memory cache.)
    non_finalized_state
        .any_utxo(&outpoint)
        .or_else(|| db.utxo(&outpoint).map(|utxo| utxo.utxo))
}

/// Returns the [`BlockInfo`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chain` or finalized `db`.
pub fn block_info<C>(
    chain: Option<C>,
    db: &ZebraDb,
    hash_or_height: HashOrHeight,
) -> Option<BlockInfo>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chain
        .as_ref()
        .and_then(|chain| chain.as_ref().block_info(hash_or_height))
        .or_else(|| db.block_info(hash_or_height))
}

/// Returns a lightweight summary for `height` in the best chain.
///
/// This query only reads the height/hash index, block header, and [`BlockInfo`]; it does not
/// deserialize block transactions.
pub fn block_summary<C>(
    chain: Option<C>,
    db: &ZebraDb,
    height: Height,
) -> Option<RecentBlockSummary>
where
    C: AsRef<Chain> + Clone,
{
    let finalized_tip = db.tip();
    let is_at_or_below_finalized_tip =
        finalized_tip.is_some_and(|(finalized_height, _)| height <= finalized_height);

    let (hash, header, info, finalized) = if is_at_or_below_finalized_tip {
        // A cached non-finalized chain can briefly overlap a newly finalized fork. Read the
        // sampled finalized prefix exclusively from the database to avoid returning a losing fork.
        let hash = db.hash(height)?;
        let header = db.block_header(height.into())?;
        let info = db.block_info(height.into())?;
        (hash, header, info, true)
    } else {
        let hash = crate::service::read::find::hash_by_height(chain.clone(), db, height)?;
        let header = block_header(chain.clone(), db, hash.into())?;
        let info = block_info(chain, db, hash.into())?;
        (hash, header, info, false)
    };

    Some(RecentBlockSummary {
        height,
        hash,
        time: header.time,
        info,
        finalized,
    })
}

/// Returns `true` when an optional explorer pagination boundary is still canonical.
///
/// Boundary-anchored cursors remain valid while new tip blocks are appended. They become stale
/// only when a reorganization replaces the block that separated two pages.
pub fn canonical_boundary_matches<C>(
    chain: Option<C>,
    db: &ZebraDb,
    cursor_anchor: Option<(Height, block::Hash)>,
) -> bool
where
    C: AsRef<Chain>,
{
    let Some((height, expected_hash)) = cursor_anchor else {
        return true;
    };

    let finalized_hash = db
        .tip()
        .filter(|(finalized_height, _)| height <= *finalized_height)
        .and_then(|_| db.hash(height));
    let canonical_hash = finalized_hash.or_else(|| {
        chain
            .as_ref()
            .and_then(|chain| chain.as_ref().hash_by_height(height))
            .or_else(|| db.hash(height))
    });

    canonical_hash == Some(expected_hash)
}

/// Returns lightweight summaries for up to `limit` recent blocks in the best chain.
///
/// The summaries are ordered from newest to oldest. This query only reads block headers,
/// height/hash indexes, and [`BlockInfo`]; it does not deserialize block transactions.
pub fn recent_block_summaries<C>(
    chain: Option<C>,
    db: &ZebraDb,
    limit: usize,
    before_height: Option<Height>,
) -> (
    Option<(Height, block::Hash)>,
    Option<(Height, block::Hash)>,
    Vec<RecentBlockSummary>,
)
where
    C: AsRef<Chain> + Clone,
{
    let finalized_tip = db.tip();
    let non_finalized_tip = chain
        .as_ref()
        .map(|chain| chain.as_ref().non_finalized_tip());
    let best_tip = match (non_finalized_tip, finalized_tip) {
        (Some(non_finalized_tip), Some(finalized_tip))
            if finalized_tip.0 >= non_finalized_tip.0 =>
        {
            Some(finalized_tip)
        }
        (Some(non_finalized_tip), _) => Some(non_finalized_tip),
        (None, finalized_tip) => finalized_tip,
    };

    let Some((tip_height, _tip_hash)) = best_tip else {
        return (best_tip, finalized_tip, Vec::new());
    };

    let first_height = before_height
        .map(|before_height| before_height.previous().ok())
        .unwrap_or(Some(tip_height))
        .map(|height| height.min(tip_height));

    let blocks = first_height
        .into_iter()
        .flat_map(|first_height| (0..limit).map(move |offset| (first_height, offset)))
        .map_while(|offset| {
            let (first_height, offset) = offset;
            let offset = u32::try_from(offset).ok()?;
            first_height.0.checked_sub(offset).map(Height)
        })
        .map_while(|height| {
            let is_at_or_below_finalized_tip =
                finalized_tip.is_some_and(|(finalized_height, _)| height <= finalized_height);
            let (hash, header, info, finalized) = if is_at_or_below_finalized_tip {
                // Read the sampled finalized prefix exclusively from the database. During a state
                // update, a cached non-finalized chain can briefly overlap a newly finalized fork;
                // mixing those sources here could return a losing-fork block as the best chain.
                let hash = db.hash(height)?;
                let header = db.block_header(hash.into())?;
                let info = db.block_info(hash.into())?;
                (hash, header, info, true)
            } else {
                let hash = crate::service::read::find::hash_by_height(chain.clone(), db, height)?;
                let header = block_header(chain.clone(), db, hash.into())?;
                let info = block_info(chain.clone(), db, hash.into())?;
                (hash, header, info, false)
            };

            Some(RecentBlockSummary {
                height,
                hash,
                time: header.time,
                info,
                finalized,
            })
        })
        .collect();

    (best_tip, finalized_tip, blocks)
}

/// Returns lightweight summaries for up to `limit` transactions in the current best chain.
///
/// Results are ordered newest-first by chain location. `before`, when supplied, is an exclusive
/// cursor. Finalized transactions are read with one reverse range scan over the transaction column
/// family; non-finalized transactions are read directly from the in-memory best chain.
pub fn transaction_summary_page<C>(
    chain: Option<C>,
    db: &ZebraDb,
    limit: usize,
    before: Option<crate::TransactionLocation>,
) -> (
    Option<(Height, block::Hash)>,
    Option<(Height, block::Hash)>,
    Vec<ExplorerTransactionSummary>,
)
where
    C: AsRef<Chain> + Clone,
{
    let finalized_tip = db.tip();
    let non_finalized_tip = chain
        .as_ref()
        .map(|chain| chain.as_ref().non_finalized_tip());
    let best_tip = match (non_finalized_tip, finalized_tip) {
        (Some(non_finalized_tip), Some(finalized_tip))
            if finalized_tip.0 >= non_finalized_tip.0 =>
        {
            Some(finalized_tip)
        }
        (Some(non_finalized_tip), _) => Some(non_finalized_tip),
        (None, finalized_tip) => finalized_tip,
    };

    let Some((best_height, _best_hash)) = best_tip else {
        return (best_tip, finalized_tip, Vec::new());
    };
    if limit == 0 {
        return (best_tip, finalized_tip, Vec::new());
    }

    let mut summaries = Vec::with_capacity(limit);
    let finalized_height = finalized_tip.map(|(height, _hash)| height);

    // The cached non-finalized chain can briefly overlap a newly advanced finalized tip. Only use
    // its strict suffix so a transaction is never returned twice.
    if let Some(chain) = chain.as_ref().map(AsRef::as_ref) {
        let chain_tip_height = chain.non_finalized_tip_height();
        if finalized_height.is_none_or(|height| chain_tip_height > height) {
            let mut height = before
                .map(|location| location.height.min(chain_tip_height))
                .unwrap_or(chain_tip_height)
                .min(best_height);

            loop {
                if finalized_height.is_some_and(|finalized| height <= finalized) {
                    break;
                }

                if let Some(contextual) = chain.block(height.into()) {
                    for (index, tx) in contextual.block.transactions.iter().enumerate().rev() {
                        let location = crate::TransactionLocation::from_usize(height, index);
                        if before.is_some_and(|before| location >= before) {
                            continue;
                        }

                        summaries.push(explorer_transaction_summary(
                            location,
                            tx,
                            contextual.hash,
                            contextual.block.header.time,
                            false,
                        ));
                        if summaries.len() == limit {
                            return (best_tip, finalized_tip, summaries);
                        }
                    }
                }

                let Ok(previous_height) = height.previous() else {
                    break;
                };
                height = previous_height;
            }
        }
    }

    let Some((finalized_height, _finalized_hash)) = finalized_tip else {
        return (best_tip, finalized_tip, summaries);
    };
    if summaries.len() == limit {
        return (best_tip, finalized_tip, summaries);
    }

    let finalized_max = crate::TransactionLocation::max_for_height(finalized_height);
    let upper_bound = match before {
        Some(before) if before <= finalized_max => Excluded(before),
        _ => Included(finalized_max),
    };
    let mut cached_block = None;

    for (location, tx) in db.transactions_by_location_range_reverse((Unbounded, upper_bound)) {
        let (block_hash, block_time) = match cached_block {
            Some((height, hash, time)) if height == location.height => (hash, time),
            _ => {
                let Some(hash) = db.hash(location.height) else {
                    continue;
                };
                let Some(header) = db.block_header(hash.into()) else {
                    continue;
                };
                let time = header.time;
                cached_block = Some((location.height, hash, time));
                (hash, time)
            }
        };

        summaries.push(explorer_transaction_summary(
            location, &tx, block_hash, block_time, true,
        ));
        if summaries.len() == limit {
            break;
        }
    }

    (best_tip, finalized_tip, summaries)
}

/// Returns newest-first summaries for transactions involving `address` in the best chain.
///
/// The finalized prefix is read using a bounded reverse scan of the transparent address index.
/// Only the bounded non-finalized window is materialized, so this query never scans the address's
/// history from genesis. `before`, when supplied, is an exclusive chain-location cursor.
pub fn address_transaction_summary_page<C>(
    chain: Option<C>,
    db: &ZebraDb,
    address: transparent::Address,
    limit: usize,
    before: Option<crate::TransactionLocation>,
) -> (
    Option<(Height, block::Hash)>,
    Option<(Height, block::Hash)>,
    Vec<ExplorerTransactionSummary>,
)
where
    C: AsRef<Chain> + Clone,
{
    let finalized_tip = db.tip();
    let non_finalized_tip = chain
        .as_ref()
        .map(|chain| chain.as_ref().non_finalized_tip());
    let best_tip = match (non_finalized_tip, finalized_tip) {
        (Some(non_finalized_tip), Some(finalized_tip))
            if finalized_tip.0 >= non_finalized_tip.0 =>
        {
            Some(finalized_tip)
        }
        (Some(non_finalized_tip), _) => Some(non_finalized_tip),
        (None, finalized_tip) => finalized_tip,
    };

    let Some((best_height, _best_hash)) = best_tip else {
        return (best_tip, finalized_tip, Vec::new());
    };
    if limit == 0 {
        return (best_tip, finalized_tip, Vec::new());
    }

    let mut summaries = Vec::with_capacity(limit);
    let finalized_height = finalized_tip.map(|(height, _hash)| height);

    // Only query the strict non-finalized suffix. A cached chain can briefly overlap newly
    // finalized blocks, and including that overlap would duplicate address transactions.
    if let Some(chain) = chain.as_ref().map(AsRef::as_ref) {
        let chain_tip_height = chain.non_finalized_tip_height().min(best_height);
        if finalized_height.is_none_or(|height| chain_tip_height > height) {
            let start_height = finalized_height
                .and_then(|height| height.next().ok())
                .unwrap_or_else(|| chain.non_finalized_root_height());
            let end_height = before
                .map(|location| location.height.min(chain_tip_height))
                .unwrap_or(chain_tip_height);

            if start_height <= end_height {
                let addresses = std::iter::once(address).collect();
                let tx_ids =
                    chain.partial_transparent_tx_ids(&addresses, start_height..=end_height);

                for (location, expected_hash) in tx_ids.into_iter().rev() {
                    if before.is_some_and(|before| location >= before) {
                        continue;
                    }

                    let Some(contextual) = chain.block(location.height.into()) else {
                        continue;
                    };
                    let Some(tx) = contextual.block.transactions.get(location.index.as_usize())
                    else {
                        continue;
                    };
                    debug_assert_eq!(tx.hash(), expected_hash);

                    summaries.push(explorer_transaction_summary(
                        location,
                        tx,
                        contextual.hash,
                        contextual.block.header.time,
                        false,
                    ));
                    if summaries.len() == limit {
                        return (best_tip, finalized_tip, summaries);
                    }
                }
            }
        }
    }

    let Some((finalized_height, _finalized_hash)) = finalized_tip else {
        return (best_tip, finalized_tip, summaries);
    };
    let remaining = limit - summaries.len();
    let locations =
        db.address_transaction_locations_reverse(&address, finalized_height, before, remaining);
    let mut cached_block = None;

    for location in locations {
        let Some(tx) = db.transaction_by_location(location) else {
            continue;
        };
        let (block_hash, block_time) = match cached_block {
            Some((height, hash, time)) if height == location.height => (hash, time),
            _ => {
                let Some(hash) = db.hash(location.height) else {
                    continue;
                };
                let Some(header) = db.block_header(hash.into()) else {
                    continue;
                };
                let time = header.time;
                cached_block = Some((location.height, hash, time));
                (hash, time)
            }
        };

        summaries.push(explorer_transaction_summary(
            location, &tx, block_hash, block_time, true,
        ));
    }

    (best_tip, finalized_tip, summaries)
}

/// Returns the active chain and every currently tracked, contextually valid side-chain tip.
///
/// This is a snapshot of Zebra's bounded non-finalized state, not persistent reorganization or
/// orphan history. Zebra retains at most `MAX_NON_FINALIZED_CHAIN_FORKS` chains within its rollback
/// window.
pub fn explorer_chain_tips(
    non_finalized_state: &NonFinalizedState,
    db: &ZebraDb,
) -> (
    Option<(Height, block::Hash)>,
    Option<(Height, block::Hash)>,
    Vec<ExplorerChainTip>,
) {
    let finalized_tip = db.tip();
    let Some(best_chain) = non_finalized_state.best_chain() else {
        let tips = finalized_tip
            .map(|(height, hash)| ExplorerChainTip {
                height,
                hash,
                branch_length: 0,
                fork_height: None,
                fork_hash: None,
                active: true,
            })
            .into_iter()
            .collect();
        return (finalized_tip, finalized_tip, tips);
    };

    let non_finalized_best_tip = best_chain.non_finalized_tip();
    if finalized_tip.is_some_and(|(height, _hash)| height >= non_finalized_best_tip.0) {
        let tips = finalized_tip
            .map(|(height, hash)| ExplorerChainTip {
                height,
                hash,
                branch_length: 0,
                fork_height: None,
                fork_hash: None,
                active: true,
            })
            .into_iter()
            .collect();
        return (finalized_tip, finalized_tip, tips);
    }

    let mut tips = Vec::with_capacity(non_finalized_state.chain_count());
    for (index, chain) in non_finalized_state.chain_iter().enumerate() {
        let (height, hash) = chain.non_finalized_tip();
        if finalized_tip.is_some_and(|(finalized_height, _hash)| height <= finalized_height) {
            continue;
        }

        if index == 0 {
            tips.push(ExplorerChainTip {
                height,
                hash,
                branch_length: 0,
                fork_height: None,
                fork_hash: None,
                active: true,
            });
            continue;
        }

        let common_ancestor = common_non_finalized_ancestor(best_chain, chain).or(finalized_tip);
        let (fork_height, fork_hash) = common_ancestor.unzip();
        let branch_length = fork_height
            .and_then(|fork_height| height.0.checked_sub(fork_height.0))
            .unwrap_or(0);

        tips.push(ExplorerChainTip {
            height,
            hash,
            branch_length,
            fork_height,
            fork_hash,
            active: false,
        });
    }

    (Some(non_finalized_best_tip), finalized_tip, tips)
}

fn common_non_finalized_ancestor(active: &Chain, side: &Chain) -> Option<(Height, block::Hash)> {
    common_ancestor_in_non_finalized_overlap(
        active
            .non_finalized_tip_height()
            .min(side.non_finalized_tip_height()),
        active.non_finalized_root_height(),
        side.non_finalized_root_height(),
        |height| (active.hash_by_height(height), side.hash_by_height(height)),
    )
}

/// Finds a common ancestor without searching below either non-finalized chain root.
///
/// Blocks below that overlap are finalized, so callers must use the finalized tip as the
/// fallback. Bounding this search is important because two chains can have distinct roots that
/// both descend from a finalized tip at a very large absolute height.
fn common_ancestor_in_non_finalized_overlap(
    mut height: Height,
    active_root: Height,
    side_root: Height,
    mut hashes_at: impl FnMut(Height) -> (Option<block::Hash>, Option<block::Hash>),
) -> Option<(Height, block::Hash)> {
    let overlap_root = active_root.max(side_root);

    if height < overlap_root {
        return None;
    }

    loop {
        if let (Some(active_hash), Some(side_hash)) = hashes_at(height) {
            if active_hash == side_hash {
                return Some((height, active_hash));
            }
        }

        if height == overlap_root {
            return None;
        }

        height = height
            .previous()
            .expect("the height above a non-finalized root always has a predecessor");
    }
}

fn explorer_transaction_summary(
    location: crate::TransactionLocation,
    tx: &Transaction,
    block_hash: block::Hash,
    block_time: DateTime<Utc>,
    finalized: bool,
) -> ExplorerTransactionSummary {
    let count = |value: usize, field: &'static str| {
        u32::try_from(value).unwrap_or_else(|_| {
            panic!("{field} fits in u32 because it is bounded by transaction bytes")
        })
    };

    ExplorerTransactionSummary {
        location,
        hash: tx.hash(),
        block_hash,
        block_time,
        size: count(tx.zcash_serialized_size(), "serialized transaction size"),
        version: tx.version(),
        coinbase: tx.is_coinbase(),
        transparent_input_count: count(tx.inputs().len(), "transparent input count"),
        transparent_output_count: count(tx.outputs().len(), "transparent output count"),
        sprout_joinsplit_count: count(tx.joinsplit_count(), "Sprout JoinSplit count"),
        sapling_spend_count: count(tx.sapling_spends_count(), "Sapling spend count"),
        sapling_output_count: count(tx.sapling_outputs().count(), "Sapling output count"),
        orchard_action_count: count(tx.orchard_actions().count(), "Orchard action count"),
        ironwood_action_count: count(tx.ironwood_actions().count(), "Ironwood action count"),
        finalized,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::common_ancestor_in_non_finalized_overlap;
    use zebra_chain::block::{Hash, Height};

    #[test]
    fn disjoint_non_finalized_roots_do_not_scan_below_the_rollback_window() {
        // The absolute chain height is deliberately large, while the tracked non-finalized
        // overlap is small. A search that accidentally continues toward genesis would make this
        // regression test perform about two million probes.
        let active_root = Height(1_999_900);
        let side_root = Height(1_999_925);
        let common_tip_height = Height(2_000_000);
        let probes = Cell::new(0usize);

        let ancestor = common_ancestor_in_non_finalized_overlap(
            common_tip_height,
            active_root,
            side_root,
            |_height| {
                probes.set(probes.get() + 1);
                (Some(Hash([1; 32])), Some(Hash([2; 32])))
            },
        );

        assert_eq!(
            ancestor, None,
            "disjoint roots have no non-finalized ancestor"
        );
        assert_eq!(
            probes.get(),
            (common_tip_height.0 - side_root.0 + 1) as usize,
            "the search must stop at the highest non-finalized root"
        );
    }
}
