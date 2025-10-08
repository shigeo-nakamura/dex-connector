use dex_connector::{create_lighter_connector, DexConnector, OrderSide};
use rust_decimal::Decimal;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Get API credentials from environment variables
    let api_key = env::var("LIGHTER_API_KEY").expect("LIGHTER_API_KEY must be set");
    let private_key = env::var("LIGHTER_PRIVATE_KEY").expect("LIGHTER_PRIVATE_KEY must be set");
    let is_testnet = env::var("LIGHTER_TESTNET").unwrap_or_default() == "true";

    // Create Lighter connector
    let connector = create_lighter_connector(api_key, private_key, is_testnet)?;

    // Start the connector
    connector.start().await?;

    // Example: Get ticker for ETH
    let ticker = connector.get_ticker("ETH", None).await?;
    println!("ETH Price: {}", ticker.price);

    // Example: Get account balance
    let balance = connector.get_balance(None).await?;
    println!(
        "Account Equity: {}, Available Balance: {}",
        balance.equity, balance.balance
    );

    // Example: Create a limit order (uncomment to test)
    /*
    let order = connector.create_order(
        "ETH",
        Decimal::new(1, 1), // 0.1 ETH
        OrderSide::Long,
        Some(Decimal::new(2000, 0)), // $2000 limit price
        None
    ).await?;
    println!("Created order: {}", order.order_id);
    */

    // Example: Get filled orders
    let filled_orders = connector.get_filled_orders("ETH").await?;
    println!("Number of filled orders: {}", filled_orders.orders.len());

    // Stop the connector
    connector.stop().await?;

    Ok(())
}
