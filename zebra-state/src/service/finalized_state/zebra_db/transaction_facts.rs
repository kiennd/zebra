//! Transaction data shared by finalized-state indexes while preparing one block.

use zebra_chain::transparent;

use crate::request::FinalizedBlock;

/// Values extracted once from a finalized transaction and reused by block indexes.
#[derive(Clone, Debug)]
pub(super) struct FinalizedTransactionFacts {
    pub(super) transparent_input_outpoints: Vec<transparent::OutPoint>,
    pub(super) transparent_output_values_zat: Vec<i64>,
    pub(super) sprout_inflow_values_zat: Vec<i64>,
    pub(super) sprout_outflow_values_zat: Vec<i64>,
    pub(super) is_coinbase: bool,
    pub(super) has_transparent_inputs: bool,
    pub(super) has_sprout: bool,
    pub(super) has_sapling: bool,
    pub(super) has_orchard: bool,
    pub(super) has_ironwood: bool,
    pub(super) has_shielded_outputs: bool,
    pub(super) is_v6: bool,
    pub(super) orchard_action_count: u32,
    pub(super) ironwood_action_count: u32,
    pub(super) orchard_spends_enabled: bool,
    pub(super) orchard_outputs_enabled: bool,
    pub(super) ironwood_spends_enabled: bool,
    pub(super) ironwood_outputs_enabled: bool,
    pub(super) raw_lock_time: u32,
    pub(super) expiry_height: Option<u32>,
    pub(super) conventional_fee_zat: u64,
    pub(super) sapling_value_balance_zat: i64,
    pub(super) orchard_value_balance_zat: i64,
    pub(super) ironwood_value_balance_zat: i64,
}

impl FinalizedTransactionFacts {
    /// Extracts facts for every transaction in block order.
    pub(super) fn from_block(finalized: &FinalizedBlock) -> Vec<Self> {
        finalized
            .block
            .transactions
            .iter()
            .map(|transaction| {
                let transparent_inputs = transaction.inputs();
                let has_transparent_inputs = !transparent_inputs.is_empty();
                let orchard_flags = transaction.orchard_flags();
                let ironwood_flags = transaction.ironwood_flags();

                Self {
                    transparent_input_outpoints: transparent_inputs
                        .into_iter()
                        .filter_map(|input| input.outpoint())
                        .collect(),
                    transparent_output_values_zat: transaction
                        .outputs()
                        .into_iter()
                        .map(|output| output.value().zatoshis())
                        .collect(),
                    sprout_inflow_values_zat: transaction.output_values_to_sprout(),
                    sprout_outflow_values_zat: transaction.input_values_from_sprout(),
                    is_coinbase: transaction.is_coinbase(),
                    has_transparent_inputs,
                    has_sprout: transaction.has_sprout_joinsplit_data(),
                    has_sapling: transaction.has_sapling_shielded_data(),
                    has_orchard: transaction.has_orchard_shielded_data(),
                    has_ironwood: transaction.has_ironwood_shielded_data(),
                    has_shielded_outputs: transaction.has_shielded_outputs(),
                    is_v6: transaction.version() == 6,
                    orchard_action_count: transaction
                        .orchard_actions()
                        .count()
                        .try_into()
                        .expect("consensus-limited Orchard action count fits in u32"),
                    ironwood_action_count: transaction
                        .ironwood_actions()
                        .count()
                        .try_into()
                        .expect("consensus-limited Ironwood action count fits in u32"),
                    orchard_spends_enabled: orchard_flags
                        .is_some_and(|flags| flags.spends_enabled()),
                    orchard_outputs_enabled: orchard_flags
                        .is_some_and(|flags| flags.outputs_enabled()),
                    ironwood_spends_enabled: ironwood_flags
                        .is_some_and(|flags| flags.spends_enabled()),
                    ironwood_outputs_enabled: ironwood_flags
                        .is_some_and(|flags| flags.outputs_enabled()),
                    raw_lock_time: transaction.raw_lock_time(),
                    expiry_height: transaction.expiry_height().map(|height| height.0),
                    conventional_fee_zat: u64::from(
                        zebra_chain::transaction::zip317::conventional_fee(transaction),
                    ),
                    sapling_value_balance_zat: transaction
                        .sapling_value_balance()
                        .sapling_amount()
                        .zatoshis(),
                    orchard_value_balance_zat: transaction
                        .orchard_value_balance()
                        .orchard_amount()
                        .zatoshis(),
                    ironwood_value_balance_zat: transaction
                        .ironwood_value_balance()
                        .ironwood_amount()
                        .zatoshis(),
                }
            })
            .collect()
    }

    pub(super) fn has_transparent_outputs(&self) -> bool {
        !self.transparent_output_values_zat.is_empty()
    }
}
