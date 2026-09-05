//! Types for the `getzip317feeparameters` RPC.

use derive_getters::Getters;
use derive_new::new;

/// The authoritative parameters used by Zebra's ZIP-317 fee calculator.
#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct GetZip317FeeParametersResponse {
    /// The implemented ZIP-317 specification revision.
    #[getter(copy)]
    pub(crate) zip317_revision: u32,

    /// The marginal fee in zatoshis per logical action.
    #[getter(copy)]
    pub(crate) marginal_fee_zat: u64,

    /// The minimum number of chargeable logical actions.
    #[getter(copy)]
    pub(crate) grace_actions: u32,

    /// The standard transparent input size used to calculate logical actions, in bytes.
    #[getter(copy)]
    pub(crate) standard_transparent_input_size_bytes: usize,

    /// The standard transparent output size used to calculate logical actions, in bytes.
    #[getter(copy)]
    pub(crate) standard_transparent_output_size_bytes: usize,
}
