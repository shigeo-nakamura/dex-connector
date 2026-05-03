//! Signing helpers for the Lighter connector.
//!
//! Two flavors live here:
//!
//! - **Go SDK calls** (`call_go_sign_create_order`,
//!   `call_go_sign_cancel_order`): hand off to the Lighter Go shared
//!   library through the `super::ffi` bindings and return the resulting
//!   `tx_info` string.
//! - **EVM signing helpers** (`sign_message_with_lighter_go_evm`,
//!   `recover_address_from_signature`, `extract_own_pubkey_from_error`,
//!   `get_go_pubkey_from_check`): produce / verify the EIP-191 EVM
//!   signatures used during ChangePubKey and during Go-SDK pubkey
//!   reconciliation.
//!
//! The methods are all `pub(super)` so the parent module can call them.
//! The `#[cfg(not(feature = "lighter-sdk"))]` stub variants are kept
//! verbatim from the pre-split source for parallelism, even though the
//! parent module is itself `#![cfg(feature = "lighter-sdk")]` and they
//! never compile in practice.

use super::ffi::{
    parse_signed_tx_response, CheckClient, SignCancelOrder, SignCreateOrder,
};
use super::LighterConnector;
use crate::dex_request::DexError;
use libc::{c_int, c_longlong};
use secp256k1::{Message, Secp256k1};
use sha3::{Digest, Keccak256};
use std::ffi::CStr;

impl LighterConnector {
    /// Call Go shared library to generate signature for CreateOrder transaction
    #[cfg(feature = "lighter-sdk")]
    pub(super) async fn call_go_sign_create_order(
        &self,
        market_index: i32,
        client_order_index: i64,
        base_amount: i64,
        price: i32,
        is_ask: i32,
        order_type: i32,
        time_in_force: i32,
        reduce_only: i32,
        trigger_price: i32,
        order_expiry: i64,
        nonce: i64,
    ) -> Result<String, DexError> {
        // First create the client
        self.create_go_client().await?;

        unsafe {
            let result = SignCreateOrder(
                market_index,
                client_order_index,
                base_amount,
                price,
                is_ask,
                order_type,
                time_in_force,
                reduce_only,
                trigger_price,
                order_expiry,
                nonce,
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            let (tx_info, _) = parse_signed_tx_response(result)?;
            Ok(tx_info)
        }
    }

    /// Call Go shared library to generate signature (disabled when lighter-sdk feature is not enabled)
    #[cfg(not(feature = "lighter-sdk"))]
    pub(super) async fn call_go_sign_create_order(
        &self,
        _market_index: i32,
        _client_order_index: i64,
        _base_amount: i64,
        _price: i32,
        _is_ask: i32,
        _order_type: i32,
        _time_in_force: i32,
        _reduce_only: i32,
        _trigger_price: i32,
        _order_expiry: i64,
        _nonce: i64,
    ) -> Result<String, DexError> {
        Err(DexError::Other(
            "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
                .to_string(),
        ))
    }

    /// Call Go shared library to sign a cancel order transaction
    #[cfg(feature = "lighter-sdk")]
    pub(super) async fn call_go_sign_cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        nonce: i64,
    ) -> Result<String, DexError> {
        self.create_go_client().await?;

        unsafe {
            let result = SignCancelOrder(
                market_index,
                order_index,
                nonce,
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            let (tx_info, _) = parse_signed_tx_response(result)?;
            Ok(tx_info)
        }
    }

    #[cfg(not(feature = "lighter-sdk"))]
    pub(super) async fn call_go_sign_cancel_order(
        &self,
        _market_index: i32,
        _order_index: i64,
        _nonce: i64,
    ) -> Result<String, DexError> {
        Err(DexError::Other(
            "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
                .to_string(),
        ))
    }

    #[cfg(feature = "lighter-sdk")]
    pub(super) async fn sign_message_with_lighter_go_evm(
        &self,
        evm_private_key: &str,
        message: &str,
    ) -> Result<String, String> {
        use ethers::signers::{LocalWallet, Signer};
        use std::str::FromStr;

        log::debug!("Using local EVM signer (ethers) for ChangePubKey signature");

        let cleaned_key = evm_private_key
            .strip_prefix("0x")
            .unwrap_or(evm_private_key);
        let wallet = LocalWallet::from_str(cleaned_key)
            .map_err(|e| format!("Invalid private key: {}", e))?;

        let signature = wallet
            .sign_message(message.as_bytes())
            .await
            .map_err(|e| format!("Signing failed: {}", e))?;

        // Ensure 0x prefix is present (Lighter expects 0x-prefixed signatures)
        let signature_with_prefix = format!("0x{}", signature);

        // Check if v value needs to be adjusted from {0,1} to {27,28}
        if signature_with_prefix.len() == 132 {
            // "0x" + 130 hex chars = 132
            let mut sig_bytes = hex::decode(&signature_with_prefix[2..])
                .map_err(|e| format!("Failed to decode signature hex: {}", e))?;

            if sig_bytes.len() == 65 {
                // Check if v is 0 or 1, and convert to 27 or 28
                if sig_bytes[64] == 0 {
                    log::debug!("Converting v from 0 to 27");
                    sig_bytes[64] = 27;
                } else if sig_bytes[64] == 1 {
                    log::debug!("Converting v from 1 to 28");
                    sig_bytes[64] = 28;
                }

                let corrected_signature = format!("0x{}", hex::encode(sig_bytes));
                log::debug!("EVM signature v-corrected: {}", corrected_signature);

                // Log recovered address for debugging
                if let Ok(recovered_addr) =
                    self.recover_address_from_signature(message, &corrected_signature)
                {
                    log::debug!("EVM signature recovery check - Address: {}", recovered_addr);
                } else {
                    log::warn!("Failed to recover address from EVM signature for verification");
                }

                return Ok(corrected_signature);
            }
        }

        Ok(signature_with_prefix)
    }

    #[cfg(not(feature = "lighter-sdk"))]
    pub(super) async fn sign_message_with_lighter_go_evm(
        &self,
        _evm_private_key: &str,
        _message: &str,
    ) -> Result<String, String> {
        Err("EVM signing with lighter-go requires lighter-sdk feature".to_string())
    }

    /// Recover the EVM address from a signature for debugging purposes
    pub(super) fn recover_address_from_signature(
        &self,
        message: &str,
        signature: &str,
    ) -> Result<String, String> {
        // Remove 0x prefix if present
        let signature_hex = signature.strip_prefix("0x").unwrap_or(signature);

        // Decode the signature
        let signature_bytes = hex::decode(signature_hex)
            .map_err(|e| format!("Failed to decode signature hex: {}", e))?;

        if signature_bytes.len() != 65 {
            return Err(format!(
                "Invalid signature length: {} (expected 65)",
                signature_bytes.len()
            ));
        }

        // Split signature into r, s, v components
        let _r = &signature_bytes[0..32];
        let _s = &signature_bytes[32..64];
        let v = signature_bytes[64];

        // Convert v to recovery id (0 or 1)
        let recovery_id = match v {
            27 => 0,
            28 => 1,
            0 | 1 => v, // Already in correct format
            _ => return Err(format!("Invalid v value: {}", v)),
        };

        // Create the message hash (EIP-191 personal_sign format)
        let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
        let mut hasher = Keccak256::new();
        hasher.update(prefix.as_bytes());
        hasher.update(message.as_bytes());
        let message_hash = hasher.finalize();

        // Create secp256k1 objects
        let secp = Secp256k1::new();
        let message_obj = Message::from_digest_slice(&message_hash)
            .map_err(|e| format!("Invalid message hash: {}", e))?;

        // Create recoverable signature
        let recoverable_sig = secp256k1::ecdsa::RecoverableSignature::from_compact(
            &signature_bytes[0..64],
            secp256k1::ecdsa::RecoveryId::from_i32(recovery_id as i32)
                .map_err(|e| format!("Invalid recovery id: {}", e))?,
        )
        .map_err(|e| format!("Failed to create recoverable signature: {}", e))?;

        // Recover the public key
        let public_key = secp
            .recover_ecdsa(&message_obj, &recoverable_sig)
            .map_err(|e| format!("Failed to recover public key: {}", e))?;

        // Convert public key to address
        let public_key_bytes = public_key.serialize_uncompressed();
        let mut hasher = Keccak256::new();
        hasher.update(&public_key_bytes[1..]); // Skip the 0x04 prefix
        let hash = hasher.finalize();

        // Take the last 20 bytes and format as address
        let address = format!("0x{}", hex::encode(&hash[12..]));

        Ok(address)
    }

    #[cfg(feature = "lighter-sdk")]
    pub(super) fn extract_own_pubkey_from_error(error_msg: &str) -> Option<String> {
        let marker = "ownPubKey:";
        let start = error_msg.find(marker)?;
        let after = error_msg[start + marker.len()..].trim_start();
        let end = after.find(' ').unwrap_or(after.len());
        if end == 0 {
            None
        } else {
            Some(after[..end].to_string())
        }
    }

    #[cfg(feature = "lighter-sdk")]
    pub(super) fn get_go_pubkey_from_check(&self) -> Result<String, DexError> {
        unsafe {
            let result = CheckClient(
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            if result.is_null() {
                return Err(DexError::Other(
                    "CheckClient succeeded; no pubkey mismatch detected".to_string(),
                ));
            }

            let error_cstr = CStr::from_ptr(result);
            let error_msg = error_cstr.to_string_lossy().to_string();
            libc::free(result as *mut libc::c_void);

            Self::extract_own_pubkey_from_error(&error_msg).ok_or_else(|| {
                DexError::Other(format!(
                    "Failed to extract public key from CheckClient error: {}",
                    error_msg
                ))
            })
        }
    }
}
