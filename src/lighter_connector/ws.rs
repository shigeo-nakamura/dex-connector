//! Lighter WebSocket plumbing.
//!
//! Owns:
//!
//! - The single connection lifecycle in `start_websocket`: handshake,
//!   subscription set-up, ping/pong heartbeat, reconnect-with-backoff,
//!   and the host-shared WAF cooldown gate.
//! - The dispatch helpers `handle_websocket_message`,
//!   `handle_market_stats_update`, and `handle_account_update` that
//!   classify each incoming frame and update the relevant shared cache
//!   (price / volume / order book / fills / cancels / positions /
//!   balance / funding).
//! - The `OutboundMessage` priority enum used to gate Pong/Close ahead
//!   of any other write traffic on the WS sink.
//!
//! Only `start_websocket` is reachable from `mod.rs` (`pub(super)`); the
//! `handle_*` helpers are called via `Self::*` inside this file.

use super::market_cache::MarketCache;
use super::parsing::{parse_canceled_order, parse_filled_order, value_to_decimal};
use super::{LighterConnector, LighterOrderBook, LighterOrderBookCacheEntry, LighterPosition};
use crate::dex_connector::string_to_decimal;
use crate::dex_request::DexError;
use crate::{
    BalanceResponse, CanceledOrder, FilledOrder, OpenOrder, OrderSide, PositionSnapshot,
};
use rust_decimal::{prelude::FromStr, Decimal};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

// Priority message system for WebSocket sending.
#[derive(Debug)]
pub(super) enum OutboundMessage {
    Control(tokio_tungstenite::tungstenite::Message), // High priority: Pong, Close
}

impl OutboundMessage {
    pub(super) fn into_message(self) -> tokio_tungstenite::tungstenite::Message {
        match self {
            OutboundMessage::Control(msg) => msg,
        }
    }

    pub(super) fn is_pong(&self) -> bool {
        match self {
            OutboundMessage::Control(tokio_tungstenite::tungstenite::Message::Pong(_)) => true,
            _ => false,
        }
    }
}

impl LighterConnector {
    pub(super) async fn start_websocket(&self) -> Result<(), DexError> {
        let ws_url = self
            .websocket_url
            .replace("https://", "wss://")
            .replace("http://", "ws://");

        log::info!("Connecting to WebSocket: {}", ws_url);

        let primary_market_id = if let Some(symbol) = self.tracked_symbols.first() {
            match self.resolve_market_info(symbol).await {
                Ok(info) => info.market_id,
                Err(e) => {
                    log::warn!(
                        "Failed to resolve primary symbol '{}' for WS order book subscription: {}. Falling back to market_id=1",
                        symbol,
                        e
                    );
                    1
                }
            }
        } else {
            1
        };

        // Clone necessary data for the WebSocket task
        let current_price = self.current_price.clone();
        let current_volume = self.current_volume.clone();
        let order_book = self.order_book.clone();
        let filled_orders = self.filled_orders.clone();
        let canceled_orders = self.canceled_orders.clone();
        let cached_positions = self.cached_positions.clone();
        let cached_open_orders = self.cached_open_orders.clone();
        let positions_ready = self.positions_ready.clone();
        let balance_cache = self.balance_cache.clone();
        let cached_collateral = self.cached_collateral.clone();
        let funding_rate_cache = self.funding_rate_cache.clone();
        let is_running = self.is_running.clone();
        let connection_epoch = self.connection_epoch.clone();
        let account_index = self.account_index;
        let price_update_tx = self.price_update_tx.clone();
        let market_cache = Arc::clone(&self.market_cache);
        let mut orderbook_market_ids: Vec<u32> = Vec::new();
        for sym in &self.tracked_symbols {
            match self.resolve_market_info(sym).await {
                Ok(info) => {
                    if !orderbook_market_ids.contains(&info.market_id) {
                        orderbook_market_ids.push(info.market_id);
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Failed to resolve symbol '{}' for WS order book subscription: {}",
                        sym,
                        e
                    );
                }
            }
        }
        if orderbook_market_ids.is_empty() {
            orderbook_market_ids.push(primary_market_id);
        }
        let default_symbol = self
            .tracked_symbols
            .first()
            .cloned()
            .unwrap_or_else(|| "BTC".to_string());

        // Spawn WebSocket handler task with reconnection logic
        let ws_url_clone = ws_url.clone();
        tokio::spawn(async move {
            use rand::Rng;

            const BACKOFF_MAX_SECS: u64 = 60;
            const BACKOFF_BASE: f64 = 1.5;

            let mut reconnect_attempt = 0u32;
            let mut last_reconnect_time = std::time::SystemTime::now();

            async fn reconnect_backoff(attempt: u32) {
                let pow = BACKOFF_BASE.powi(attempt.min(12) as i32);
                let base_secs = (pow as f64).min(BACKOFF_MAX_SECS as f64);
                let jitter_ms: i64 = rand::thread_rng().gen_range(0..=250);
                let dur = std::time::Duration::from_secs_f64(base_secs)
                    + std::time::Duration::from_millis(jitter_ms as u64);

                log::debug!(
                    "Reconnect backoff: attempt={}, delay={:.1}s",
                    attempt,
                    dur.as_secs_f64()
                );
                tokio::time::sleep(dur).await;
            }

            loop {
                if !is_running.load(Ordering::SeqCst) {
                    log::info!("WebSocket task stopping due to is_running flag");
                    break;
                }

                // Reset attempt counter if enough time has passed since last reconnect
                let now = std::time::SystemTime::now();
                if let Ok(elapsed) = now.duration_since(last_reconnect_time) {
                    if elapsed.as_secs() > 300 {
                        // 5 minutes
                        reconnect_attempt = 0;
                        log::debug!("Reset reconnect attempt counter after successful period");
                    }
                }

                // Respect host-shared WAF cooldown before reconnecting.
                if let Some(remaining) = crate::lighter_waf_cooldown::cooldown_remaining() {
                    log::info!(
                        "WAF cooldown active ({:.0}s remaining), delaying WS reconnect",
                        remaining.as_secs_f64()
                    );
                    tokio::time::sleep(remaining).await;
                }

                if reconnect_attempt > 0 {
                    reconnect_backoff(reconnect_attempt).await;
                } else {
                    // Stagger initial reconnects across bot instances. Default
                    // widened from 15s to 30s (bot-strategy#121): during the
                    // 2026-04-20 cascade (#78) the 15s window was still tight
                    // enough that 4 bots reconnected within ~5s and bursted the
                    // REST refetch past Lighter's per-IP ceiling. 30s spreads
                    // the same traffic over a window twice as wide, halving
                    // peak weight/s while staying well inside Lighter's 60s
                    // rolling window.
                    let reconnect_jitter_secs: u64 =
                        std::env::var("LIGHTER_RECONNECT_JITTER_SECS")
                            .ok()
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(30);
                    if reconnect_jitter_secs > 0 {
                        let jitter =
                            rand::thread_rng().gen_range(0..=reconnect_jitter_secs);
                        log::info!(
                            "Reconnect jitter: sleeping {}s (max {}s)",
                            jitter,
                            reconnect_jitter_secs
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(jitter)).await;
                    }
                }

                log::info!(
                    "Attempting WebSocket connection to: {} (attempt: {})",
                    ws_url_clone,
                    reconnect_attempt + 1
                );
                last_reconnect_time = now;

                // Try to establish connection with optimized configuration
                use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
                let mut config = WebSocketConfig::default();

                // Optimize WebSocket configuration for low latency
                config.max_message_size = Some(64 * 1024 * 1024); // 64MB
                config.max_frame_size = Some(16 * 1024 * 1024); // 16MB
                config.write_buffer_size = 128 * 1024; // 128KB write buffer
                config.max_write_buffer_size = 1024 * 1024; // 1MB max write buffer

                let connection_result = tokio_tungstenite::connect_async_with_config(
                    &ws_url_clone,
                    Some(config),
                    false,
                )
                .await;

                match connection_result {
                    Ok((mut ws_stream, response)) => {
                        let headers = response.headers();
                        let header_value = |name: &str| {
                            headers
                                .get(name)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or("-")
                        };
                        log::info!(
                            "WebSocket handshake ok: status={}, server={}, upgrade={}, connection={}, extensions={}",
                            response.status(),
                            header_value("server"),
                            header_value("upgrade"),
                            header_value("connection"),
                            header_value("sec-websocket-extensions"),
                        );
                        log::debug!("WebSocket handshake headers: {:?}", headers);

                        // Increment connection epoch for race detection
                        let current_epoch = connection_epoch.fetch_add(1, Ordering::SeqCst) + 1;

                        // Extract connection information for logging
                        let (local_addr, peer_addr, stream_kind) = match ws_stream.get_ref() {
                            tokio_tungstenite::MaybeTlsStream::Rustls(tls_stream) => {
                                let tcp_stream = tls_stream.get_ref().0;

                                // Apply TCP optimizations for TLS connection
                                if let Err(e) = tcp_stream.set_nodelay(true) {
                                    log::debug!("Failed to set TCP_NODELAY on TLS: {}", e);
                                }

                                match (tcp_stream.local_addr(), tcp_stream.peer_addr()) {
                                    (Ok(local), Ok(peer)) => (local, peer, "rustls"),
                                    _ => {
                                        log::debug!(
                                            "Failed to get TLS socket addresses for epoch {}",
                                            current_epoch
                                        );
                                        (
                                            "0.0.0.0:0".parse().unwrap(),
                                            "0.0.0.0:0".parse().unwrap(),
                                            "rustls",
                                        )
                                    }
                                }
                            }
                            tokio_tungstenite::MaybeTlsStream::Plain(tcp_stream) => {
                                if let Err(e) = tcp_stream.set_nodelay(true) {
                                    log::debug!("Failed to set TCP_NODELAY on plain WS: {}", e);
                                }
                                match (tcp_stream.local_addr(), tcp_stream.peer_addr()) {
                                    (Ok(local), Ok(peer)) => (local, peer, "plain"),
                                    _ => {
                                        log::debug!(
                                            "Failed to get plain socket addresses for epoch {}",
                                            current_epoch
                                        );
                                        (
                                            "0.0.0.0:0".parse().unwrap(),
                                            "0.0.0.0:0".parse().unwrap(),
                                            "plain",
                                        )
                                    }
                                }
                            }
                            other => {
                                log::debug!(
                                    "Unsupported WebSocket stream type {:?} for epoch {}",
                                    other,
                                    current_epoch
                                );
                                (
                                    "0.0.0.0:0".parse().unwrap(),
                                    "0.0.0.0:0".parse().unwrap(),
                                    "unknown",
                                )
                            }
                        };

                        let epoch_prefix = format!("[{:03}]", current_epoch);
                        let conn_label = format!(
                            "{} {} -> {} ({})",
                            epoch_prefix, local_addr, peer_addr, stream_kind
                        );

                        log::info!(
                            "{} WebSocket connected successfully: {} -> {} ({})",
                            epoch_prefix,
                            local_addr,
                            peer_addr,
                            stream_kind
                        );

                        // Send subscription messages
                        let subscribe_account = serde_json::json!({
                            "type": "subscribe",
                            "channel": format!("account_all/{}", account_index)
                        });

                        for market_id in &orderbook_market_ids {
                            let subscribe_orderbook = serde_json::json!({
                                "type": "subscribe",
                                "channel": format!("order_book/{}", market_id)
                            });
                            log::info!(
                                "🔗 [WS_DEBUG] Sending orderbook subscription: {}",
                                subscribe_orderbook
                            );

                            if let Err(e) = ws_stream
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    subscribe_orderbook.to_string(),
                                ))
                                .await
                            {
                                log::error!(
                                    "Failed to send orderbook subscription for {}: {}",
                                    market_id,
                                    e
                                );
                                continue;
                            }
                        }

                        // Subscribe to market_stats for every tracked market so the
                        // funding_rate field arrives over WS and get_ticker no longer
                        // needs the `/funding-rates` REST poll. See bot-strategy#162.
                        for market_id in &orderbook_market_ids {
                            let subscribe_market_stats = serde_json::json!({
                                "type": "subscribe",
                                "channel": format!("market_stats/{}", market_id)
                            });
                            if let Err(e) = ws_stream
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    subscribe_market_stats.to_string(),
                                ))
                                .await
                            {
                                log::error!(
                                    "Failed to send market_stats subscription for {}: {}",
                                    market_id,
                                    e
                                );
                                continue;
                            }
                        }

                        if let Err(e) = ws_stream
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                subscribe_account.to_string(),
                            ))
                            .await
                        {
                            log::error!("Failed to send account subscription: {}", e);
                            continue;
                        }

                        log::info!("WebSocket subscriptions sent successfully");

                        // Single-task pump architecture: no split(), unified read/write with select!
                        use futures::sink::SinkExt;
                        use futures::stream::StreamExt;

                        // Split stream for shared access but use channels for coordination
                        let (write, mut read) = ws_stream.split();

                        // Shared writer protected by Arc<Mutex>
                        let ws_writer_arc = Arc::new(tokio::sync::Mutex::new(write));
                        let ws_writer_for_reader = ws_writer_arc.clone();

                        // Control channel for high-priority messages (ping/pong)
                        let (tx_ctrl, mut rx_ctrl) =
                            tokio::sync::mpsc::channel::<OutboundMessage>(32);
                        let tx_ctrl_for_reader = tx_ctrl.clone();
                        let connection_alive = Arc::new(AtomicBool::new(true));

                        // Create unified writer task with priority handling
                        let writer_is_running = is_running.clone();
                        let writer_connection_alive = connection_alive.clone();
                        let ws_writer_for_task = ws_writer_arc.clone();
                        let writer_conn_label = conn_label.clone();
                        let writer_task = tokio::spawn(async move {
                            loop {
                                if !writer_is_running.load(Ordering::SeqCst)
                                    || !writer_connection_alive.load(Ordering::SeqCst)
                                {
                                    break;
                                }

                                // Handle control messages
                                let (msg, _) = tokio::select! {
                                    // High priority channel (control messages like Pong)
                                    Some(msg) = rx_ctrl.recv() => (msg, true),
                                    else => break,
                                };

                                let send_start = std::time::Instant::now();
                                let is_pong = msg.is_pong();

                                // Use shared writer with short-lived lock
                                let mut ws_write = ws_writer_for_task.lock().await;
                                if let Err(e) = ws_write.send(msg.into_message()).await {
                                    log::error!(
                                        "WebSocket send failed: {:?} (conn={})",
                                        e,
                                        writer_conn_label
                                    );
                                    break;
                                }

                                let send_duration = send_start.elapsed();

                                if is_pong {
                                    let latency_ms = send_duration.as_millis();
                                    if latency_ms > 100 {
                                        log::debug!("High pong send latency: {}ms", latency_ms);
                                    }
                                }
                            }

                            log::debug!("WebSocket writer task terminated");
                        });

                        // Heartbeat strategy: WebSocket control frame ping-pong only
                        // Server requires at least one frame every 2 minutes to keep connection alive.
                        // We send a control Ping every 20s which is well within that limit.
                        const IDLE_PING_SECS: u64 = 20; // Client ping interval
                        // Pong timeout: tolerate transient RTT spikes (cross-region WS to AWS
                        // can briefly stall under congestion). Combined with the 5s check
                        // granularity and the 2-strike rule below, a real dead connection is
                        // still detected within ~40s, while one-shot packet loss no longer
                        // forces a reconnect (bot-strategy#47).
                        const PONG_TIMEOUT_SECS: u64 = 15;
                        const PONG_MAX_MISSES: u32 = 2; // Require N consecutive misses before reconnect
                        const HEARTBEAT_CHECK_SECS: u64 = 5; // Check interval for heartbeat logic

                        fn get_pong_payload(ping_payload: &[u8]) -> Vec<u8> {
                            // Always echo for strict server compliance
                            ping_payload.to_vec()
                        }

                        use parking_lot::Mutex;
                        use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
                        use std::time::{SystemTime, UNIX_EPOCH};

                        fn now_secs() -> u64 {
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                        }

                        let last_rx = std::sync::Arc::new(AtomicU64::new(now_secs()));
                        let last_tx = std::sync::Arc::new(AtomicU64::new(now_secs()));
                        let pending_client_ping = std::sync::Arc::new(AtomicBool::new(false));
                        // Track ping send time independently of last_tx so concurrent
                        // app-level outbound traffic can't reset the pong timeout window.
                        let ping_sent_at = std::sync::Arc::new(AtomicU64::new(0));
                        let pong_miss_count = std::sync::Arc::new(AtomicU32::new(0));
                        let last_client_ping_payload =
                            std::sync::Arc::new(Mutex::new(Vec::<u8>::new()));

                        // Create ping/heartbeat task with priority channel access
                        let ping_is_running = is_running.clone();
                        let ping_connection_alive = connection_alive.clone();
                        let ping_last_tx = last_tx.clone();
                        let ping_pending_client_ping = pending_client_ping.clone();
                        let ping_sent_at_clone = ping_sent_at.clone();
                        let pong_miss_count_clone = pong_miss_count.clone();
                        let ping_last_client_ping_payload = last_client_ping_payload.clone();
                        let ping_tx_ctrl = tx_ctrl.clone();
                        let ping_conn_label = conn_label.clone();

                        let ping_task = tokio::spawn(async move {
                            let mut heartbeat_interval = tokio::time::interval(
                                std::time::Duration::from_secs(HEARTBEAT_CHECK_SECS),
                            );
                            heartbeat_interval
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                            loop {
                                tokio::select! {
                                    // Handle heartbeat check interval
                                    _ = heartbeat_interval.tick() => {
                                        if !ping_is_running.load(Ordering::SeqCst)
                                            || !ping_connection_alive.load(Ordering::SeqCst)
                                        {
                                            break;
                                        }

                                        let now = now_secs();
                                        let idle_tx = now.saturating_sub(ping_last_tx.load(Ordering::SeqCst));

                                        // Send WebSocket control Ping regularly (every 20s)
                                        // Server requires at least one frame every 2 minutes
                                        if !ping_pending_client_ping.load(Ordering::SeqCst)
                                            && idle_tx >= IDLE_PING_SECS
                                        {
                                            let payload: [u8; 8] = (now as u64).to_be_bytes();
                                            *ping_last_client_ping_payload.lock() = payload.to_vec();

                                            let ping_msg = OutboundMessage::Control(
                                                tokio_tungstenite::tungstenite::Message::Ping(payload.to_vec())
                                            );
                                            if let Err(e) = ping_tx_ctrl.send(ping_msg).await {
                                                log::warn!(
                                                    "Failed to send client ping: {:?} (conn={})",
                                                    e,
                                                    ping_conn_label
                                                );
                                                break;
                                            }

                                            ping_pending_client_ping.store(true, Ordering::SeqCst);
                                            ping_sent_at_clone.store(now, Ordering::SeqCst);
                                            ping_last_tx.store(now, Ordering::SeqCst);
                                            log::debug!(
                                                "Sent client ping (conn={}, payload={:?})",
                                                ping_conn_label,
                                                payload
                                            );
                                        }

                                        // Check for control frame pong timeout
                                        if ping_pending_client_ping.load(Ordering::SeqCst) {
                                            let sent_at = ping_sent_at_clone.load(Ordering::SeqCst);
                                            let waited = now.saturating_sub(sent_at);
                                            if waited >= PONG_TIMEOUT_SECS {
                                                let misses = pong_miss_count_clone
                                                    .fetch_add(1, Ordering::SeqCst)
                                                    + 1;
                                                if misses < PONG_MAX_MISSES {
                                                    log::info!(
                                                        "Pong missed ({}s, {}/{} strikes), retrying (conn={})",
                                                        waited,
                                                        misses,
                                                        PONG_MAX_MISSES,
                                                        ping_conn_label,
                                                    );
                                                    // Allow a fresh ping next tick instead of reconnecting
                                                    ping_pending_client_ping
                                                        .store(false, Ordering::SeqCst);
                                                } else {
                                                    log::warn!(
                                                        "Pong timeout ({}s, {} strikes), reconnecting (conn={})",
                                                        waited,
                                                        misses,
                                                        ping_conn_label,
                                                    );
                                                    let close_msg = OutboundMessage::Control(
                                                        tokio_tungstenite::tungstenite::Message::Close(None)
                                                    );
                                                    let _ = ping_tx_ctrl.send(close_msg).await;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            log::debug!("Heartbeat task ended");
                        });

                        // Handle messages in this connection with performance tracking
                        log::debug!("Starting WebSocket message handling loop");

                        while let Some(message) = read.next().await {
                            if !is_running.load(Ordering::SeqCst) {
                                log::info!("WebSocket stopping due to is_running flag");
                                break;
                            }

                            match message {
                                Ok(message) => match message {
                                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                                        let msg_start = std::time::Instant::now();
                                        // Update last received timestamp for any text message
                                        let now = now_secs();
                                        last_rx.store(now, Ordering::SeqCst);

                                        if let Ok(parsed) = serde_json::from_str::<Value>(&text) {
                                            let msg_type = parsed
                                                .get("type")
                                                .and_then(|t| t.as_str())
                                                .unwrap_or("");
                                            let channel = parsed
                                                .get("channel")
                                                .and_then(|c| c.as_str())
                                                .unwrap_or("");
                                            let offset = parsed
                                                .get("offset")
                                                .and_then(|o| o.as_i64())
                                                .unwrap_or_default();
                                            log::trace!(
                                                "WebSocket msg type={} channel={} offset={}",
                                                msg_type,
                                                channel,
                                                offset
                                            );
                                            // Skip application-layer ping/pong messages (server no longer sends them)
                                            if let Some(msg_type) =
                                                parsed.get("type").and_then(|t| t.as_str())
                                            {
                                                if msg_type == "ping" || msg_type == "pong" {
                                                    log::debug!(
                                                        "Ignoring application-layer {} (conn={})",
                                                        msg_type,
                                                        conn_label
                                                    );
                                                    continue;
                                                }
                                            }

                                            Self::handle_websocket_message(
                                                parsed,
                                                &current_price,
                                                &current_volume,
                                                &order_book,
                                                &filled_orders,
                                                &canceled_orders,
                                                &cached_open_orders,
                                                &cached_positions,
                                                &positions_ready,
                                                &balance_cache,
                                                &cached_collateral,
                                                &funding_rate_cache,
                                                account_index,
                                                &market_cache,
                                                default_symbol.as_str(),
                                                &price_update_tx,
                                            )
                                            .await;

                                            let total_duration = msg_start.elapsed();

                                            // Log slow messages only
                                            if total_duration.as_millis() > 10 {
                                                log::debug!(
                                                    "Slow message processing: {}ms (len={})",
                                                    total_duration.as_millis(),
                                                    text.len()
                                                );
                                            }
                                        } else {
                                            log::debug!(
                                                "Failed to parse WebSocket message as JSON (len={}): {}",
                                                text.len(),
                                                text
                                            );
                                        }
                                    }
                                    tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                                        // Server ping -> immediate manual pong response
                                        log::debug!(
                                            "Received control ping: {} bytes (conn={})",
                                            payload.len(),
                                            conn_label
                                        );
                                        let now = now_secs();
                                        last_rx.store(now, Ordering::SeqCst);

                                        // Echo pong payload for server compliance
                                        let pong_payload = get_pong_payload(&payload);

                                        // Get current epoch for race detection logging
                                        let current_epoch = connection_epoch.load(Ordering::SeqCst);

                                        // Direct pong send - bypass writer task for immediate response
                                        if let Ok(mut ws_write) = ws_writer_for_reader.try_lock() {
                                            if let Err(e) = ws_write
                                                .send(
                                                    tokio_tungstenite::tungstenite::Message::Pong(
                                                        pong_payload.clone(),
                                                    ),
                                                )
                                                .await
                                            {
                                                log::error!(
                                                    "🚨 [CRITICAL] Failed to send pong directly: {:?} (conn={})",
                                                    e,
                                                    conn_label
                                                );
                                                break;
                                            }
                                            if let Err(e) =
                                                futures::SinkExt::flush(&mut *ws_write).await
                                            {
                                                log::error!(
                                                    "🚨 [CRITICAL] Failed to flush after pong: {:?} (conn={})",
                                                    e,
                                                    conn_label
                                                );
                                                break;
                                            }
                                            last_tx.store(now, Ordering::SeqCst);

                                            // Verify same connection epoch for race detection
                                            let pong_epoch =
                                                connection_epoch.load(Ordering::SeqCst);

                                            if pong_epoch != current_epoch {
                                                log::error!(
                                                    "Pong epoch mismatch: ping={} pong={}",
                                                    current_epoch,
                                                    pong_epoch
                                                );
                                            }
                                        } else {
                                            // Fallback to try_lock with retry for up to 200ms
                                            let mut pong_sent = false;
                                            for retry in 0..4 {
                                                tokio::time::sleep(
                                                    std::time::Duration::from_millis(50),
                                                )
                                                .await;
                                                if let Ok(mut ws_write) =
                                                    ws_writer_for_reader.try_lock()
                                                {
                                                    if let Err(e) = ws_write.send(tokio_tungstenite::tungstenite::Message::Pong(pong_payload.clone())).await {
                                                        log::error!(
                                                            "Failed to send pong on retry {}: {:?} (conn={})",
                                                            retry,
                                                            e,
                                                            conn_label
                                                        );
                                                        break;
                                                    }
                                                    if let Err(e) =
                                                        futures::SinkExt::flush(&mut *ws_write)
                                                            .await
                                                    {
                                                        log::error!(
                                                            "Failed to flush after pong retry {}: {:?} (conn={})",
                                                            retry,
                                                            e,
                                                            conn_label
                                                        );
                                                        break;
                                                    }
                                                    last_tx.store(now, Ordering::SeqCst);

                                                    pong_sent = true;
                                                    break;
                                                }
                                            }
                                            if !pong_sent {
                                                log::warn!(
                                                    "Pong timeout, closing connection (conn={})",
                                                    conn_label
                                                );
                                                // Proactively close to trigger reconnect before server timeout
                                                let mut ws_write =
                                                    ws_writer_for_reader.lock().await;
                                                let _ = ws_write.close().await;
                                                break;
                                            }
                                        }
                                    }
                                    tokio_tungstenite::tungstenite::Message::Pong(payload) => {
                                        // Pong response - check if it matches our client ping
                                        let now = now_secs();
                                        last_rx.store(now, Ordering::SeqCst);

                                        let expected_payload =
                                            last_client_ping_payload.lock().clone();
                                        if !expected_payload.is_empty()
                                            && payload == expected_payload
                                        {
                                            pending_client_ping.store(false, Ordering::SeqCst);
                                            pong_miss_count.store(0, Ordering::SeqCst);
                                            log::debug!(
                                                "Received control pong (conn={}, payload={:?})",
                                                conn_label,
                                                payload
                                            );
                                        } else if expected_payload.is_empty() {
                                            log::debug!(
                                                "Received unsolicited control pong (conn={}, payload={:?})",
                                                conn_label,
                                                payload
                                            );
                                        } else {
                                            log::warn!(
                                                "Control pong payload mismatch (conn={}, expected={:?}, got={:?})",
                                                conn_label,
                                                expected_payload,
                                                payload
                                            );
                                        }
                                    }
                                    tokio_tungstenite::tungstenite::Message::Close(frame) => {
                                        let now = now_secs();
                                        let idle_rx =
                                            now.saturating_sub(last_rx.load(Ordering::SeqCst));
                                        let idle_tx =
                                            now.saturating_sub(last_tx.load(Ordering::SeqCst));
                                        log::warn!(
                                            "WebSocket close frame received: {:?} (conn={}, idle_rx={}s, idle_tx={}s, pending_client_ping={})",
                                            frame,
                                            conn_label,
                                            idle_rx,
                                            idle_tx,
                                            pending_client_ping.load(Ordering::SeqCst),
                                        );
                                        break;
                                    }
                                    tokio_tungstenite::tungstenite::Message::Binary(data) => {
                                        log::debug!(
                                            "Received binary WebSocket message: {} bytes",
                                            data.len()
                                        );
                                    }
                                    tokio_tungstenite::tungstenite::Message::Frame(_) => {
                                        log::trace!("Received raw WebSocket frame");
                                    }
                                },
                                Err(e) => {
                                    let now = now_secs();
                                    let last_rx_at = last_rx.load(Ordering::SeqCst);
                                    let last_tx_at = last_tx.load(Ordering::SeqCst);
                                    // Protocol errors (e.g. ResetWithoutClosingHandshake from the
                                    // upstream Lighter/CloudFront edge) are bursty, correlated
                                    // across services, and auto-recover via reconnection. Demote
                                    // to INFO so they do not pollute error_summary and trigger
                                    // false alerts in the error-watch workflow
                                    // (shigeo-nakamura/bot-strategy#49). IO/TLS errors indicate
                                    // genuinely unusual conditions and stay at ERROR.
                                    let is_protocol = matches!(
                                        &e,
                                        tokio_tungstenite::tungstenite::Error::Protocol(_)
                                    );
                                    let is_benign_io = matches!(
                                        &e,
                                        tokio_tungstenite::tungstenite::Error::Io(io_err)
                                            if matches!(
                                                io_err.kind(),
                                                std::io::ErrorKind::ConnectionReset
                                                    | std::io::ErrorKind::UnexpectedEof
                                                    | std::io::ErrorKind::BrokenPipe
                                            )
                                    );
                                    if is_protocol {
                                        log::info!(
                                            "WebSocket error: {} (type: {:?}) (conn={}, idle_rx={}s, idle_tx={}s, pending_client_ping={}). Will attempt reconnection.",
                                            e,
                                            std::any::type_name_of_val(&e),
                                            conn_label,
                                            now.saturating_sub(last_rx_at),
                                            now.saturating_sub(last_tx_at),
                                            pending_client_ping.load(Ordering::SeqCst),
                                        );
                                    } else if is_benign_io {
                                        log::warn!(
                                            "WebSocket error: {} (type: {:?}) (conn={}, idle_rx={}s, idle_tx={}s, pending_client_ping={}). Will attempt reconnection.",
                                            e,
                                            std::any::type_name_of_val(&e),
                                            conn_label,
                                            now.saturating_sub(last_rx_at),
                                            now.saturating_sub(last_tx_at),
                                            pending_client_ping.load(Ordering::SeqCst),
                                        );
                                    } else {
                                        log::error!(
                                            "WebSocket error: {} (type: {:?}) (conn={}, idle_rx={}s, idle_tx={}s, pending_client_ping={}). Will attempt reconnection.",
                                            e,
                                            std::any::type_name_of_val(&e),
                                            conn_label,
                                            now.saturating_sub(last_rx_at),
                                            now.saturating_sub(last_tx_at),
                                            pending_client_ping.load(Ordering::SeqCst),
                                        );
                                    }
                                    match &e {
                                        tokio_tungstenite::tungstenite::Error::Protocol(
                                            protocol_err,
                                        ) => {
                                            log::info!(
                                                "WebSocket protocol error detail: {:?} (conn={})",
                                                protocol_err,
                                                conn_label
                                            );
                                        }
                                        tokio_tungstenite::tungstenite::Error::Io(io_err) => {
                                            if is_benign_io {
                                                log::warn!(
                                                    "WebSocket IO error detail: kind={:?}, error={} (conn={})",
                                                    io_err.kind(),
                                                    io_err,
                                                    conn_label
                                                );
                                            } else {
                                                log::error!(
                                                    "WebSocket IO error detail: kind={:?}, error={} (conn={})",
                                                    io_err.kind(),
                                                    io_err,
                                                    conn_label
                                                );
                                            }
                                        }
                                        tokio_tungstenite::tungstenite::Error::Tls(tls_err) => {
                                            log::error!(
                                                "WebSocket TLS error detail: {:?} (conn={})",
                                                tls_err,
                                                conn_label
                                            );
                                        }
                                        _ => {}
                                    }
                                    break; // Break inner loop to attempt reconnection
                                }
                            }
                        }

                        connection_alive.store(false, Ordering::SeqCst);
                        drop(tx_ctrl_for_reader);
                        drop(tx_ctrl);
                        writer_task.abort();
                        ping_task.abort();

                        // NOTE: Do NOT clear order book cache here.
                        // Clearing causes "order book snapshot unavailable" errors
                        // during the gap between reconnection and the first OB update.
                        // The staleness check (ob_stale_after) will naturally expire
                        // stale entries, and the reconnected WS will replace them
                        // with fresh data once the first update arrives.
                        // See: dex-connector#2
                        log::info!(
                            "WebSocket disconnected (conn={}), keeping OB cache (will expire via staleness check)",
                            conn_label
                        );

                        reconnect_attempt += 1;
                    }
                    Err(e) => {
                        reconnect_attempt += 1;

                        // Check for 429 Too Many Requests
                        let error_str = e.to_string();
                        if error_str.contains("429") || error_str.contains("Too Many Requests") {
                            log::error!(
                                "WebSocket connection failed with rate limit (429): {}. Attempt: {}. Using exponential backoff.",
                                e, reconnect_attempt
                            );
                        } else {
                            log::error!(
                                "Failed to connect to WebSocket: {}. Attempt: {}. Will retry with backoff.",
                                e, reconnect_attempt
                            );
                        }
                    }
                }
            }

            log::info!("WebSocket task ended");
        });

        Ok(())
    }

    pub(super) async fn handle_websocket_message(
        message: Value,
        current_price: &Arc<RwLock<HashMap<String, (Decimal, u64)>>>,
        current_volume: &Arc<RwLock<Option<Decimal>>>,
        order_book: &Arc<RwLock<HashMap<u32, LighterOrderBookCacheEntry>>>,
        filled_orders: &Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
        canceled_orders: &Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
        cached_open_orders: &Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
        cached_positions: &Arc<RwLock<Vec<PositionSnapshot>>>,
        positions_ready: &Arc<AtomicBool>,
        balance_cache: &Arc<RwLock<Option<(BalanceResponse, Instant)>>>,
        cached_collateral: &Arc<RwLock<Option<Decimal>>>,
        funding_rate_cache: &Arc<RwLock<HashMap<u32, Decimal>>>,
        account_index: u64,
        market_cache: &Arc<RwLock<MarketCache>>,
        default_symbol: &str,
        price_update_tx: &tokio::sync::broadcast::Sender<crate::PriceUpdate>,
    ) {
        let msg_type = message.get("type").and_then(|t| t.as_str()).unwrap_or("");

        match msg_type {
            "subscribed/order_book" | "update/order_book" => {
                if let Some(order_book_data) = message.get("order_book") {
                    if let Ok(ob) =
                        serde_json::from_value::<LighterOrderBook>(order_book_data.clone())
                    {
                        // Resolve symbol for this order book update using the channel hint (order_book/<market_id>)
                        let channel = message
                            .get("channel")
                            .and_then(|c| c.as_str())
                            .unwrap_or_default();
                        // Server sometimes returns channel as "order_book:1" instead of "order_book/1"
                        let market_id = channel
                            .rsplit(|c| c == '/' || c == ':')
                            .next()
                            .and_then(|id| id.parse::<u32>().ok());
                        let symbol = match market_id {
                            Some(market_id) => {
                                let cache = market_cache.read().await;
                                cache
                                    .by_id
                                    .get(&market_id)
                                    .map(|info| info.canonical_symbol.clone())
                            }
                            None => None,
                        };
                        let symbol = match symbol {
                            Some(symbol) => symbol,
                            None => {
                                if let Some(market_id) = market_id {
                                    log::warn!(
                                        "[WS] order_book update missing symbol for market_id={} (channel='{}'); skipping",
                                        market_id,
                                        channel
                                    );
                                } else {
                                    log::warn!(
                                        "[WS] order_book update missing market_id (channel='{}'); skipping",
                                        channel
                                    );
                                }
                                return;
                            }
                        };
                        log::debug!(
                            "[WS_OB] channel='{}' market_id={:?} resolved_symbol={}",
                            channel,
                            market_id,
                            symbol
                        );

                        // Update current price from best bid/ask
                        if let (Some(best_bid), Some(best_ask)) = (ob.bids.first(), ob.asks.first())
                        {
                            if let (Ok(bid_price), Ok(ask_price)) = (
                                string_to_decimal(Some(best_bid.price.clone())),
                                string_to_decimal(Some(best_ask.price.clone())),
                            ) {
                                let mid_price = (bid_price + ask_price) / Decimal::from(2);

                                // Cross-symbol contamination guard: if we already have a
                                // known price for this symbol, reject OB updates where
                                // bid/ask deviates more than 50% from it.
                                let is_contaminated = {
                                    let prices = current_price.read().await;
                                    if let Some(&(known_price, _)) = prices.get(&symbol) {
                                        if known_price > Decimal::ZERO {
                                            let ratio = if mid_price > known_price {
                                                mid_price / known_price
                                            } else {
                                                known_price / mid_price
                                            };
                                            ratio > Decimal::new(15, 1) // 1.5x = 50% deviation
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    }
                                };

                                if is_contaminated {
                                    log::info!(
                                        "[WS_OB] REJECTED contaminated OB for {} (market_id={:?}): \
                                         bid={} ask={} mid={} deviates >50% from known price",
                                        symbol,
                                        market_id,
                                        bid_price,
                                        ask_price,
                                        mid_price
                                    );
                                    return;
                                }

                                // Prefer the exchange-side `last_updated_at` (microseconds since
                                // epoch) so that all bots observing the same WS feed share an
                                // identical timestamp for the same update. Required for the
                                // multi-bot bar-alignment fix (pairtrade#4). Stored in ms so
                                // within-second orderings are preserved for bar bucketing
                                // (bot-strategy#274 / #276).
                                let exchange_ts_ms = message
                                    .get("last_updated_at")
                                    .and_then(|v| v.as_u64())
                                    .map(|us| us / 1_000);
                                let timestamp = exchange_ts_ms.unwrap_or_else(|| {
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis() as u64
                                });
                                log::debug!(
                                    "[WS_OB] {} best_bid={} best_ask={} mid={} ts_ms={}{}",
                                    symbol,
                                    bid_price,
                                    ask_price,
                                    mid_price,
                                    timestamp,
                                    if exchange_ts_ms.is_some() { " (exchange)" } else { " (local)" }
                                );
                                current_price
                                    .write()
                                    .await
                                    .insert(symbol.clone(), (mid_price, timestamp));
                                // Broadcast real-time price update to subscribers
                                let _ = price_update_tx.send(crate::PriceUpdate {
                                    symbol: symbol.clone(),
                                    mid_price,
                                    best_bid: bid_price,
                                    best_ask: ask_price,
                                    timestamp,
                                });
                                log::trace!(
                                    "Updated price from WebSocket: {} at {} ({})",
                                    mid_price,
                                    timestamp,
                                    symbol
                                );
                            }
                        }

                        // Calculate volume from order book
                        let total_volume: Decimal = ob
                            .bids
                            .iter()
                            .chain(ob.asks.iter())
                            .filter_map(|entry| string_to_decimal(Some(entry.size.clone())).ok())
                            .sum();
                        *current_volume.write().await = Some(total_volume);

                        if let Some(market_id) = market_id {
                            {
                                let mut ob_guard = order_book.write().await;
                                // For delta updates, merge with existing cache:
                                // keep the existing side if the new update has no entries for it
                                let merged_ob = if let Some(existing) = ob_guard.get(&market_id) {
                                    let merged_bids = if ob.bids.is_empty() {
                                        existing.order_book.bids.clone()
                                    } else {
                                        ob.bids
                                    };
                                    let merged_asks = if ob.asks.is_empty() {
                                        existing.order_book.asks.clone()
                                    } else {
                                        ob.asks
                                    };
                                    LighterOrderBook {
                                        bids: merged_bids,
                                        asks: merged_asks,
                                    }
                                } else {
                                    ob
                                };
                                ob_guard.insert(
                                    market_id,
                                    LighterOrderBookCacheEntry {
                                        order_book: merged_ob,
                                        updated_at: Instant::now(),
                                    },
                                );
                            }
                            log::debug!(
                                "[WS_OB] cached order book for {} (market_id={}, channel='{}')",
                                symbol,
                                market_id,
                                channel
                            );
                        }
                    }
                }
            }
            "subscribed/market_stats" | "update/market_stats" => {
                Self::handle_market_stats_update(&message, funding_rate_cache).await;
            }
            "subscribed/account_all" | "update/account_all" => {
                log::trace!(
                    "Received account message: type={}, message={:?}",
                    msg_type,
                    message
                );
                // Handle account updates (filled/canceled orders)
                // For Lighter DEX, the data is directly in the message, not in a 'data' field
                Self::handle_account_update(
                    &message,
                    msg_type,
                    filled_orders,
                    canceled_orders,
                    cached_open_orders,
                    cached_positions,
                    positions_ready,
                    balance_cache,
                    cached_collateral,
                    account_index as u64,
                    market_cache,
                    default_symbol,
                )
                .await;
            }
            _ => {
                log::trace!("Unhandled WebSocket message type: {}", msg_type);
            }
        }
    }

    /// Extract the per-market funding rate from a `market_stats/{id}` push and
    /// store it in the shared cache. Lighter's payload carries both
    /// `funding_rate` (realized at the most recent funding_timestamp) and
    /// `current_funding_rate` (running estimate for the next payment); we keep
    /// the realized value to match what `/funding-rates` REST exposed and what
    /// the strategy is calibrated against. See bot-strategy#162.
    pub(super) async fn handle_market_stats_update(
        message: &Value,
        funding_rate_cache: &Arc<RwLock<HashMap<u32, Decimal>>>,
    ) {
        let channel = message
            .get("channel")
            .and_then(|c| c.as_str())
            .unwrap_or_default();
        // The server accepts `market_stats/<id>` on subscribe but delivers
        // `market_stats:<id>` on the push, matching the order_book pattern.
        let market_id = channel
            .rsplit(|c| c == '/' || c == ':')
            .next()
            .and_then(|id| id.parse::<u32>().ok());
        let Some(market_id) = market_id else {
            log::warn!(
                "[WS] market_stats update missing market_id (channel='{}'); skipping",
                channel
            );
            return;
        };
        let rate_str = message
            .get("market_stats")
            .and_then(|s| s.get("funding_rate"))
            .and_then(|v| v.as_str());
        let Some(rate_str) = rate_str else {
            log::trace!(
                "[WS] market_stats for market_id={} has no funding_rate field (channel='{}')",
                market_id,
                channel
            );
            return;
        };
        match string_to_decimal(Some(rate_str.to_string())) {
            Ok(rate) => {
                funding_rate_cache
                    .write()
                    .await
                    .insert(market_id, rate);
                log::trace!(
                    "[WS_FUNDING] market_id={} funding_rate={}",
                    market_id,
                    rate
                );
            }
            Err(e) => {
                log::warn!(
                    "[WS] failed to parse funding_rate='{}' for market_id={}: {}",
                    rate_str,
                    market_id,
                    e
                );
            }
        }
    }

    pub(super) async fn handle_account_update(
        data: &Value,
        msg_type: &str,
        filled_orders: &Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
        canceled_orders: &Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
        cached_open_orders: &Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
        cached_positions: &Arc<RwLock<Vec<PositionSnapshot>>>,
        positions_ready: &Arc<AtomicBool>,
        balance_cache: &Arc<RwLock<Option<(BalanceResponse, Instant)>>>,
        cached_collateral: &Arc<RwLock<Option<Decimal>>>,
        account_id: u64,
        market_cache: &Arc<RwLock<MarketCache>>,
        default_symbol: &str,
    ) {
        positions_ready.store(true, Ordering::SeqCst);
        log::trace!("handle_account_update called with data: {:?}", data);
        if std::env::var("LIGHTER_WS_ACCOUNT_DUMP").ok().as_deref() == Some("1") {
            log::info!("[WS_ACCOUNT_DUMP] {}", data.to_string());
        }

        // Handle positions update (can be array or object). Track unrealized_pnl
        // alongside the snapshot fields so we can re-derive perp equity from
        // WS without hitting REST (bot-strategy#239).
        let positions_vals: Option<Vec<Value>> =
            if let Some(arr) = data.get("positions").and_then(|p| p.as_array()) {
                Some(arr.clone())
            } else if let Some(map) = data.get("positions").and_then(|p| p.as_object()) {
                Some(map.values().cloned().collect())
            } else {
                None
            };
        // None when no positions field was carried in this message;
        // Some(sum) — possibly zero — when the message did carry positions.
        // The distinction matters: a missing positions field must NOT cause
        // us to treat unrealized_pnl as 0 (which would corrupt the cached
        // equity); a present-but-all-flat positions field correctly means 0.
        let mut sum_unrealized_pnl: Option<Decimal> = None;
        if let Some(vals) = positions_vals {
            let allow_empty = msg_type == "subscribed/account_all";
            if vals.is_empty() && !allow_empty {
                log::debug!(
                    "[WS_ACCOUNT_DUMP] Skipping empty positions update for {}",
                    msg_type
                );
            } else {
                let mut updates: Vec<(String, Option<PositionSnapshot>)> = Vec::new();
                let mut acc = Decimal::ZERO;
                for pos_val in vals {
                    if let Ok(position) = serde_json::from_value::<LighterPosition>(pos_val) {
                        if let Ok(pnl) = Decimal::from_str(&position.unrealized_pnl) {
                            acc += pnl;
                        }
                        let size = match Decimal::from_str(&position.position) {
                            Ok(s) => s.abs(),
                            Err(_) => Decimal::ZERO,
                        };
                        if size.is_zero() {
                            updates.push((position.symbol.clone(), None));
                            continue;
                        }
                        let entry_price = Decimal::from_str(&position.avg_entry_price).ok();
                        updates.push((
                            position.symbol.clone(),
                            Some(PositionSnapshot {
                                symbol: position.symbol,
                                size,
                                sign: position.sign as i32,
                                entry_price,
                            }),
                        ));
                    }
                }
                sum_unrealized_pnl = Some(acc);
                if msg_type == "subscribed/account_all" {
                    let mut cache = cached_positions.write().await;
                    *cache = updates.into_iter().filter_map(|(_, snap)| snap).collect();
                    log::info!("Updated cached positions: {} positions", cache.len());
                } else {
                    let mut cache = cached_positions.write().await;
                    let mut map: HashMap<String, PositionSnapshot> = cache
                        .iter()
                        .cloned()
                        .map(|p| (p.symbol.clone(), p))
                        .collect();
                    for (symbol, maybe_snap) in updates {
                        match maybe_snap {
                            Some(snap) => {
                                map.insert(symbol, snap);
                            }
                            None => {
                                map.remove(&symbol);
                            }
                        }
                    }
                    *cache = map.into_values().collect();
                    log::info!("Merged cached positions: {} positions", cache.len());
                }
            }
        }

        // Equity derivation for perp sub-accounts (bot-strategy#239).
        //
        // Lighter does not publish `total_asset_value` / `available_balance`
        // for perp sub-accounts on either snapshot or update messages
        // (verified empirically; the original direct-field code below was
        // dead for these accounts). The aggregate is reconstructible as
        //   equity = assets[USDC].margin_balance + sum(positions.unrealized_pnl)
        // matching REST `total_asset_value` to within rounding (~$0.002 on
        // $1000 in measured fixtures).
        //
        // `subscribed/account_all` carries assets, so margin_balance gets
        // refreshed on (re)subscribe and after fills (which trigger a REST
        // refresh that reseeds cached_collateral). `update/account_all` ships
        // `assets:null` but does carry positions, so we recompute equity
        // from the latest unrealized_pnl combined with the previously cached
        // collateral. Fall back to the legacy direct-totals path if Lighter
        // ever populates them on the parent account or returns to the older
        // schema.
        let direct_total = data
            .get("total_asset_value")
            .and_then(value_to_decimal);
        let direct_available = data
            .get("available_balance")
            .and_then(value_to_decimal);

        let usdc_margin_balance = data
            .get("assets")
            .and_then(|a| a.as_object())
            .and_then(|map| {
                map.values().find_map(|asset| {
                    let symbol = asset.get("symbol").and_then(|s| s.as_str())?;
                    if symbol != "USDC" {
                        return None;
                    }
                    asset
                        .get("margin_balance")
                        .and_then(value_to_decimal)
                })
            });
        if let Some(mb) = usdc_margin_balance {
            let mut collateral = cached_collateral.write().await;
            *collateral = Some(mb);
        }

        let derived_equity = if let Some(pnl_sum) = sum_unrealized_pnl {
            let collateral = cached_collateral.read().await;
            collateral.map(|c| (c, c + pnl_sum))
        } else {
            None
        };

        if direct_total.is_some() || direct_available.is_some() {
            let equity = direct_total
                .or(direct_available)
                .unwrap_or(Decimal::ZERO);
            let balance = direct_available.or(direct_total).unwrap_or(equity);
            let mut cache = balance_cache.write().await;
            *cache = Some((
                BalanceResponse {
                    equity,
                    balance,
                    position_entry_price: None,
                    position_sign: None,
                },
                Instant::now(),
            ));
            log::debug!(
                "Updated cached balance from WS direct totals: equity={}, balance={}",
                equity,
                balance
            );
        } else if let Some((collateral, equity)) = derived_equity {
            let mut cache = balance_cache.write().await;
            *cache = Some((
                BalanceResponse {
                    equity,
                    balance: collateral,
                    position_entry_price: None,
                    position_sign: None,
                },
                Instant::now(),
            ));
            log::debug!(
                "Updated cached balance from WS derivation: collateral={} +unrealized_pnl_sum={} =equity={}",
                collateral,
                equity - collateral,
                equity
            );
        } else {
            log::debug!(
                "Skipping WS balance update: no direct totals and no derivation inputs (collateral_cached={}, positions_in_msg={})",
                cached_collateral.read().await.is_some(),
                sum_unrealized_pnl.is_some(),
            );
        }

        // Handle filled orders - try both 'fills' and 'trades' fields
        if let Some(fills) = data.get("fills").and_then(|f| f.as_array()) {
            log::info!(
                "✅ [FILL_DETECTION] Found {} fills in account update",
                fills.len()
            );
            let default_symbol = default_symbol.to_string();
            let mut filled_map = filled_orders.write().await;
            for fill in fills {
                log::debug!("🔍 [FILL_DETECTION] Processing fill: {:?}", fill);
                if let Ok(filled_order) = parse_filled_order(fill, account_id) {
                    let order_id = filled_order.order_id.clone();
                    log::info!("✅ [FILL_DETECTION] Added filled order: order_id={}, size={:?}, value={:?}",
                              filled_order.order_id, filled_order.filled_size, filled_order.filled_value);
                    filled_map
                        .entry(default_symbol.clone())
                        .or_insert_with(Vec::new)
                        .push(filled_order);
                    Self::remove_tracked_order(cached_open_orders, &default_symbol, &order_id)
                        .await;
                } else {
                    log::debug!("Failed to parse filled order: {:?}", fill);
                }
            }
        }

        // Process 'trades' field according to Lighter API specification
        if let Some(trades) = data.get("trades").and_then(|t| t.as_object()) {
            log::info!("✅ [FILL_DETECTION] Found trades object in account update");

            let mut pending_inserts: Vec<(String, FilledOrder)> = Vec::new();

            for (market_id, trade_array) in trades {
                let market_id_num = match market_id.parse::<u32>() {
                    Ok(id) => id,
                    Err(_) => {
                        log::warn!(
                            "[FILL_DETECTION] Unable to parse market_id '{}' as u32",
                            market_id
                        );
                        continue;
                    }
                };

                let market_symbol = {
                    let cache = market_cache.read().await;
                    cache
                        .by_id
                        .get(&market_id_num)
                        .map(|info| info.canonical_symbol.clone())
                };

                let market_symbol = match market_symbol {
                    Some(symbol) => symbol,
                    None => {
                        log::warn!(
                            "[FILL_DETECTION] Market cache missing entry for market_id {}",
                            market_id_num
                        );
                        continue;
                    }
                };

                if let Some(trades_array) = trade_array.as_array() {
                    for trade in trades_array {
                        if let (
                            Some(ask_id),
                            Some(bid_id),
                            Some(ask_account_id),
                            Some(_bid_account_id),
                            Some(size_str),
                            Some(price_str),
                        ) = (
                            trade.get("ask_id").and_then(|v| v.as_u64()),
                            trade.get("bid_id").and_then(|v| v.as_u64()),
                            trade.get("ask_account_id").and_then(|v| v.as_u64()),
                            trade.get("bid_account_id").and_then(|v| v.as_u64()),
                            trade.get("size").and_then(|v| v.as_str()),
                            trade.get("price").and_then(|v| v.as_str()),
                        ) {
                            let is_ask = account_id == ask_account_id;
                            let client_id = if is_ask {
                                trade.get("ask_client_id").and_then(|v| v.as_u64())
                            } else {
                                trade.get("bid_client_id").and_then(|v| v.as_u64())
                            };
                            let order_id =
                                client_id.unwrap_or_else(|| if is_ask { ask_id } else { bid_id });

                            log::info!(
                                "✅ [FILL_DETECTION] Trade detected: order_id={}, size={}, price={}, market_id={}",
                                order_id, size_str, price_str, market_id_num
                            );

                            if let (Ok(size), Ok(price)) = (
                                size_str.parse::<rust_decimal::Decimal>(),
                                price_str.parse::<rust_decimal::Decimal>(),
                            ) {
                                let filled_order = FilledOrder {
                                    order_id: order_id.to_string(),
                                    is_rejected: false,
                                    trade_id: trade
                                        .get("trade_id")
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(0)
                                        .to_string(),
                                    filled_side: if is_ask {
                                        Some(OrderSide::Short)
                                    } else {
                                        Some(OrderSide::Long)
                                    },
                                    filled_size: Some(size),
                                    filled_value: Some(size * price),
                                    filled_fee: None,
                                    filled_ts_ms: None,
                                };

                                pending_inserts.push((market_symbol.clone(), filled_order));
                                log::info!(
                                    "✅ [FILL_DETECTION] Added filled order from trade: order_id={}",
                                    order_id
                                );
                                Self::remove_tracked_order(
                                    cached_open_orders,
                                    &market_symbol,
                                    &order_id.to_string(),
                                )
                                .await;
                            }
                        }
                    }
                }
            }

            let had_fills = !pending_inserts.is_empty();
            if had_fills {
                let mut filled_map = filled_orders.write().await;
                for (symbol_key, filled_order) in pending_inserts {
                    filled_map
                        .entry(symbol_key)
                        .or_insert_with(Vec::new)
                        .push(filled_order);
                }
            }

            // Event-sourced equity: a fill changes realized PnL + fees in a
            // way that's not surfaced by the `account_all` WS snapshot for
            // perp sub-accounts (Lighter only publishes per-market state
            // there, not aggregate collateral). Invalidate the cache so the
            // next get_balance(None) falls through to a fresh REST fetch
            // rather than returning pre-fill equity for up to a full TTL
            // window. See bot-strategy#155.
            if had_fills {
                let mut cache = balance_cache.write().await;
                *cache = None;
                log::debug!("Invalidated balance_cache after WS fill");
            }
        } else {
            log::trace!("No 'fills' array or 'trades' object found in account data");
        }

        // Handle canceled orders
        if let Some(cancels) = data.get("cancels").and_then(|c| c.as_array()) {
            let default_symbol = default_symbol.to_string();
            let mut canceled_map = canceled_orders.write().await;
            for cancel in cancels {
                if let Ok(canceled_order) = parse_canceled_order(cancel) {
                    canceled_map
                        .entry(default_symbol.clone())
                        .or_insert_with(Vec::new)
                        .push(canceled_order);
                }
            }
        }
    }
}
