# Lighter DEX Connector - Go Library Integration

## 概要

Lighter DEX ConnectorはLighter Protocol（zk-rollup DEX）との統合にGo SDKの共有ライブラリを使用しています。これにより、Poseidon2ハッシュ関数とSchnorr署名（Goldilocks quintic extension field）を正しく実装できます。

## 前提条件

### 必要なツール
- Rust (cargo)
- Go 1.21+
- Git

### Go SDKのセットアップ

1. **Go SDKのクローンとビルド**
```bash
# Go SDKをクローン
git clone https://github.com/elliottech/lighter-go.git /home/guest/oss/lighter-go
cd /home/guest/oss/lighter-go

# 共有ライブラリをビルド
go build -buildmode=c-shared -o libsigner.so ./sharedlib
```

2. **環境変数の設定**
```bash
# ライブラリパスを設定（実行時に必要）
export LD_LIBRARY_PATH=/home/guest/oss/lighter-go:$LD_LIBRARY_PATH
```

## ビルドと実行

### ビルド
```bash
cd /path/to/dex-connector
cargo build
```

### テスト実行
```bash
# ライブラリパスを設定してテスト実行
export LD_LIBRARY_PATH=/home/guest/oss/lighter-go:$LD_LIBRARY_PATH
cargo test
```

### 本番実行
```bash
# ライブラリパスを設定してアプリケーション実行
export LD_LIBRARY_PATH=/home/guest/oss/lighter-go:$LD_LIBRARY_PATH
cargo run
```

## アーキテクチャ

### 署名生成フロー

1. **Rust側**: Lighter APIからnonceを取得
2. **FFI呼び出し**: Go共有ライブラリの`CreateClient`と`SignCreateOrder`を呼び出し
3. **Go SDK**: Poseidon2 + Schnorr署名を生成
4. **Rust側**: 署名付きトランザクションJSONをLighter APIに送信

### 主要なコンポーネント

```rust
// FFI bindings
extern "C" {
    fn CreateClient(...) -> *mut c_char;
    fn SignCreateOrder(...) -> StrOrErr;
}

// 動的署名生成
impl LighterConnector {
    fn create_go_client(&self) -> Result<(), DexError> { ... }
    fn call_go_sign_create_order(&self, ...) -> Result<String, DexError> { ... }
}
```

## 設定

### プライベートキー
- Lighter Protocol用に40バイト（80桁hex）のプライベートキーが必要
- 32バイトキーは自動的に40バイトにパディングされます

### API設定
- `api_key_public`: Lighter UIから取得したAPIキー
- `api_key_index`: 通常は0
- `account_index`: アカウントインデックス（例: 65）
- `base_url`: `https://mainnet.zklighter.elliot.ai`

## トラブルシューティング

### "cannot open shared object file: libsigner.so"
```bash
# LD_LIBRARY_PATHが正しく設定されているか確認
echo $LD_LIBRARY_PATH

# libsigner.soが存在するか確認
ls -la /home/guest/oss/lighter-go/libsigner.so
```

### "invalid private key length"
- プライベートキーが64桁（32バイト）または80桁（40バイト）のhex文字列であることを確認
- 0xプレフィックスは自動的に除去されます

### "invalid signature"
- Go SDKが正しくビルドされているか確認
- プライベートキーとAPIキーが同じアカウントに対応しているか確認

## 実装詳細

### 暗号化仕様
- **ハッシュ関数**: Poseidon2 over Goldilocks field (2^64 - 2^32 + 1)
- **署名アルゴリズム**: Schnorr signatures over quintic extension
- **キー長**: 40バイト（320ビット）プライベートキー
- **署名エンコーディング**: Base64

### トランザクション構造
```json
{
  "AccountIndex": 65,
  "ApiKeyIndex": 0,
  "MarketIndex": 1,
  "ClientOrderIndex": 12345,
  "BaseAmount": 50,
  "Price": 4750000,
  "IsAsk": 0,
  "Type": 0,
  "TimeInForce": 1,
  "ReduceOnly": 0,
  "TriggerPrice": 0,
  "OrderExpiry": 1762543091693,
  "ExpiredAt": 1760126802385,
  "Nonce": 560,
  "Sig": "vMJUzeKuVf0sRizt7p38tGL6t2wx1ee8a2yxco0Ym+J+rJtSDOlRHnYwy7XFM9dpkv9P1u4LrMPdAh6dl3dkp/luEsGgUyPAmzHMZJ1ptB4="
}
```

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。