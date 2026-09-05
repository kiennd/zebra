//! Fixed Zebra RPC serialization test vectors.

use zebra_chain::transaction;

use crate::client::{
    AnalyzeRawTransactionResponse, GetBlockResponse, GetRawTransactionResponse,
    GetZip317FeeParametersResponse, TransactionObject, Zip317FeeAnalysis,
};

#[test]
pub fn test_transaction_serialization() {
    let tx = GetRawTransactionResponse::Raw(vec![0x42].into());

    assert_eq!(serde_json::to_string(&tx).unwrap(), r#""42""#);

    let tx = GetRawTransactionResponse::Object(Box::new(TransactionObject {
        hex: vec![0x42].into(),
        height: Some(1),
        confirmations: Some(0),
        inputs: Vec::new(),
        outputs: Vec::new(),
        shielded_spends: Vec::new(),
        shielded_outputs: Vec::new(),
        joinsplits: Vec::new(),
        value_balance: None,
        value_balance_zat: None,
        orchard: None,
        ironwood: None,
        binding_sig: None,
        joinsplit_pub_key: None,
        joinsplit_sig: None,
        size: None,
        time: None,
        txid: transaction::Hash::from([0u8; 32]),
        in_active_chain: None,
        auth_digest: None,
        overwintered: false,
        version: 2,
        version_group_id: None,
        lock_time: 0,
        // Pre-Overwinter V2 transaction: expiryheight should be omitted (matches zcashd)
        expiry_height: None,
        block_hash: None,
        block_time: None,
    }));

    assert_eq!(
        serde_json::to_string(&tx).unwrap(),
        r#"{"hex":"42","height":1,"confirmations":0,"vin":[],"vout":[],"vShieldedSpend":[],"vShieldedOutput":[],"vjoinsplit":[],"txid":"0000000000000000000000000000000000000000000000000000000000000000","overwintered":false,"version":2,"locktime":0}"#
    );

    let tx = GetRawTransactionResponse::Object(Box::new(TransactionObject {
        hex: vec![0x42].into(),
        height: None,
        confirmations: None,
        inputs: Vec::new(),
        outputs: Vec::new(),
        shielded_spends: Vec::new(),
        shielded_outputs: Vec::new(),
        joinsplits: Vec::new(),
        value_balance: None,
        value_balance_zat: None,
        orchard: None,
        ironwood: None,
        binding_sig: None,
        joinsplit_pub_key: None,
        joinsplit_sig: None,
        size: None,
        time: None,
        txid: transaction::Hash::from([0u8; 32]),
        in_active_chain: None,
        auth_digest: None,
        overwintered: false,
        version: 4,
        version_group_id: None,
        lock_time: 0,
        // Pre-Overwinter V4 transaction: expiryheight should be omitted (matches zcashd)
        expiry_height: None,
        block_hash: None,
        block_time: None,
    }));

    assert_eq!(
        serde_json::to_string(&tx).unwrap(),
        r#"{"hex":"42","vin":[],"vout":[],"vShieldedSpend":[],"vShieldedOutput":[],"vjoinsplit":[],"txid":"0000000000000000000000000000000000000000000000000000000000000000","overwintered":false,"version":4,"locktime":0}"#
    );
}

#[test]
pub fn test_analyze_raw_transaction_serialization() {
    let response = AnalyzeRawTransactionResponse::new(
        Box::default(),
        Zip317FeeAnalysis::new(1, 2, 10_000, 5_000, 2),
    );

    let json = serde_json::to_value(response).expect("response must serialize");

    assert_eq!(
        json["zip317"],
        serde_json::json!({
            "zip317_revision": 1,
            "conventional_actions": 2,
            "conventional_fee_zat": 10_000,
            "marginal_fee_zat": 5_000,
            "grace_actions": 2,
        })
    );
    assert_eq!(json["transaction"]["size"], serde_json::Value::Null);
    assert_eq!(
        json["transaction"]["txid"],
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    assert!(json["transaction"].get("height").is_none());
    assert!(json["transaction"].get("confirmations").is_none());
    assert!(json["transaction"].get("blockhash").is_none());
}

#[test]
pub fn test_zip317_fee_parameters_serialization() {
    let response = GetZip317FeeParametersResponse::new(1, 5_000, 2, 150, 34);

    assert_eq!(
        serde_json::to_value(response).expect("response must serialize"),
        serde_json::json!({
            "zip317_revision": 1,
            "marginal_fee_zat": 5_000,
            "grace_actions": 2,
            "standard_transparent_input_size_bytes": 150,
            "standard_transparent_output_size_bytes": 34,
        })
    );
}

#[test]
pub fn test_block_serialization() {
    let expected_tx = GetBlockResponse::Raw(vec![0x42].into());
    let expected_json = r#""42""#;
    let j = serde_json::to_string(&expected_tx).unwrap();

    assert_eq!(j, expected_json);
}
