use crate::DexError;
use ed25519_dalek::{Signer, SigningKey};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

const ED25519_KEY_LEN: usize = 32;

pub(super) struct ArcusAuth {
    address: String,
    account_index: u8,
    api_key: Option<String>,
    signing_key: Option<SigningKey>,
}

impl ArcusAuth {
    pub(super) fn new(
        address: String,
        account_index: u8,
        api_key: Option<String>,
        api_private_key_hex: Option<String>,
    ) -> Result<Self, DexError> {
        let address = normalize_address(&address)?;
        if account_index > 9 {
            return Err(DexError::InvalidInput {
                field: "account_index".to_string(),
                value: account_index.to_string(),
            });
        }

        let api_key = api_key
            .map(|value| decode_fixed_hex("api_key", &value))
            .transpose()?;
        let signing_key = api_private_key_hex
            .map(|value| decode_fixed_hex("api_private_key_hex", &value))
            .transpose()?
            .map(|bytes| SigningKey::from_bytes(&bytes));

        let derived_api_key = signing_key
            .as_ref()
            .map(|key| key.verifying_key().to_bytes());
        if let (Some(configured), Some(derived)) = (&api_key, derived_api_key) {
            if configured != &derived {
                return Err(DexError::InvalidInput {
                    field: "api_key".to_string(),
                    value: "does not match api_private_key_hex".to_string(),
                });
            }
        }

        Ok(Self {
            address,
            account_index,
            api_key: api_key.or(derived_api_key).map(hex::encode),
            signing_key,
        })
    }

    pub(super) fn address(&self) -> &str {
        &self.address
    }

    pub(super) fn account_index(&self) -> u8 {
        self.account_index
    }

    pub(super) fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }

    pub(super) fn can_sign(&self) -> bool {
        self.api_key.is_some() && self.signing_key.is_some()
    }

    pub(super) fn sign_legacy<T: Serialize>(
        &self,
        timestamp_ns: u64,
        action: &str,
        body: &T,
    ) -> Result<String, DexError> {
        let signing_key = self.signing_key.as_ref().ok_or_else(|| {
            DexError::Permanent(
                "Arcus authenticated mutation requires api_private_key_hex (bot-strategy#749)"
                    .to_string(),
            )
        })?;
        let value = serde_json::to_value(body)?;
        let message = format!("{timestamp_ns}{action}{}", canonical_json(&value)?);
        Ok(hex::encode(signing_key.sign(message.as_bytes()).to_bytes()))
    }

    #[cfg(test)]
    fn sign_message(&self, message: &[u8]) -> Result<String, DexError> {
        let signing_key = self.signing_key.as_ref().ok_or_else(|| {
            DexError::Permanent("Arcus signing key is not configured".to_string())
        })?;
        Ok(hex::encode(signing_key.sign(message).to_bytes()))
    }
}

fn normalize_address(raw: &str) -> Result<String, DexError> {
    let trimmed = raw.trim();
    let hex_part = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if hex_part.len() != 40 || !hex_part.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DexError::InvalidInput {
            field: "address".to_string(),
            value: raw.to_string(),
        });
    }
    Ok(format!("0x{}", hex_part.to_ascii_lowercase()))
}

fn decode_fixed_hex(field: &str, raw: &str) -> Result<[u8; ED25519_KEY_LEN], DexError> {
    let trimmed = raw.trim();
    let hex_part = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    let bytes = hex::decode(hex_part).map_err(|_| DexError::InvalidInput {
        field: field.to_string(),
        value: "expected 32-byte hex".to_string(),
    })?;
    bytes.try_into().map_err(|_| DexError::InvalidInput {
        field: field.to_string(),
        value: "expected 32-byte hex".to_string(),
    })
}

fn canonical_json(value: &Value) -> Result<String, DexError> {
    Ok(serde_json::to_string(&canonicalize_value(value)?)?)
}

fn canonicalize_value(value: &Value) -> Result<Value, DexError> {
    match value {
        Value::Object(map) => {
            let sorted = map
                .iter()
                .map(|(key, value)| Ok((key.clone(), canonicalize_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, DexError>>()?;
            Ok(serde_json::to_value(sorted)?)
        }
        Value::Array(values) => Ok(Value::Array(
            values
                .iter()
                .map(canonicalize_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ADDRESS: &str = "0x1234567890abcdef1234567890abcdef12345678";
    const RFC8032_SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
    const RFC8032_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";

    #[test]
    fn derives_public_key_and_matches_rfc8032_signature_vector() {
        let auth = ArcusAuth::new(ADDRESS.to_string(), 0, None, Some(RFC8032_SEED.into()))
            .expect("credentials");
        assert_eq!(auth.api_key(), Some(RFC8032_PUBLIC));
        assert_eq!(
            auth.sign_message(b"").expect("signature"),
            concat!(
                "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155",
                "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
            )
        );
    }

    #[test]
    fn canonicalizes_legacy_body_recursively_before_signing() {
        let value = json!({"z": 1, "nested": {"b": 2, "a": 1}, "a": "first"});
        assert_eq!(
            canonical_json(&value).expect("canonical JSON"),
            r#"{"a":"first","nested":{"a":1,"b":2},"z":1}"#
        );
    }

    #[test]
    fn rejects_mismatched_public_and_private_keys() {
        let err = ArcusAuth::new(
            ADDRESS.to_string(),
            0,
            Some("00".repeat(32)),
            Some(RFC8032_SEED.into()),
        )
        .err()
        .expect("mismatch must fail");
        assert!(matches!(err, DexError::InvalidInput { field, .. } if field == "api_key"));
    }
}
