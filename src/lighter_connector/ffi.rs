//! FFI bindings to the Lighter Go signer shared library (`libsigner.so`).
//!
//! The Go SDK is linked dynamically (see `build.rs`) and gates everything
//! through the `lighter-sdk` Cargo feature. The parent `lighter_connector`
//! module is itself `#![cfg(feature = "lighter-sdk")]`, so anything inside
//! this submodule is transitively gated and the per-item `cfg` attributes
//! that existed in the pre-split source are no longer needed.

use crate::dex_request::DexError;
use libc::{c_char, c_int, c_longlong};
use std::ffi::CStr;

#[repr(C)]
pub struct StrOrErr {
    pub str: *mut c_char,
    pub err: *mut c_char,
}

#[repr(C)]
pub struct SignedTxResponse {
    pub tx_type: u8,
    pub tx_info: *mut c_char,
    pub tx_hash: *mut c_char,
    pub message_to_sign: *mut c_char,
    pub err: *mut c_char,
}

extern "C" {
    pub fn CreateClient(
        url: *const c_char,
        private_key: *const c_char,
        chain_id: c_int,
        api_key_index: c_int,
        account_index: c_longlong,
    ) -> *mut c_char;

    pub fn CheckClient(api_key_index: c_int, account_index: c_longlong) -> *mut c_char;

    pub fn SignCreateOrder(
        market_index: c_int,
        client_order_index: c_longlong,
        base_amount: c_longlong,
        price: c_int,
        is_ask: c_int,
        order_type: c_int,
        time_in_force: c_int,
        reduce_only: c_int,
        trigger_price: c_int,
        order_expiry: c_longlong,
        nonce: c_longlong,
        api_key_index: c_int,
        account_index: c_longlong,
    ) -> SignedTxResponse;

    pub fn SignCancelOrder(
        market_index: c_int,
        order_index: c_longlong,
        nonce: c_longlong,
        api_key_index: c_int,
        account_index: c_longlong,
    ) -> SignedTxResponse;

    pub fn SignChangePubKey(
        new_pubkey: *const c_char,
        nonce: c_longlong,
        api_key_index: c_int,
        account_index: c_longlong,
    ) -> SignedTxResponse;
}

/// Take ownership of a `*mut c_char` returned from the Go side, copying it
/// into a Rust `String` and freeing the underlying buffer. Returns `None`
/// for null pointers.
pub unsafe fn take_c_string(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let s = CStr::from_ptr(ptr).to_string_lossy().to_string();
    libc::free(ptr as *mut libc::c_void);
    Some(s)
}

/// Convert a `SignedTxResponse` from the Go signer into either
/// `(tx_info, message_to_sign)` or a `DexError::Other` carrying the Go-side
/// error message. Always frees every C string the Go side allocated.
pub unsafe fn parse_signed_tx_response(
    resp: SignedTxResponse,
) -> Result<(String, Option<String>), DexError> {
    if !resp.err.is_null() {
        let err_msg = take_c_string(resp.err).unwrap_or_else(|| "unknown error".to_string());
        let _ = take_c_string(resp.tx_info);
        let _ = take_c_string(resp.tx_hash);
        let _ = take_c_string(resp.message_to_sign);
        return Err(DexError::Other(format!("Go SDK error: {}", err_msg)));
    }

    let tx_info = take_c_string(resp.tx_info)
        .ok_or_else(|| DexError::Other("Go SDK returned null tx_info".to_string()))?;
    let message_to_sign = take_c_string(resp.message_to_sign);
    let _ = take_c_string(resp.tx_hash);

    Ok((tx_info, message_to_sign))
}
