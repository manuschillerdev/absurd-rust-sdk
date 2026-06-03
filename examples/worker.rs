use absurd_rust_sdk::{AwaitEventOptions, Client, Result, Task, WorkerOptions};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize)]
struct OrderParams {
    order_id: String,
    email: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Shipment {
    tracking_number: String,
}

#[derive(Debug, Serialize)]
struct OrderResult {
    order_id: String,
    tracking_number: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let client = Client::from_env_queue("orders").await?;
    client.create_queue(None).await?;

    let order = Task::<OrderParams, OrderResult>::new("order-fulfillment").queue("orders");

    client.register(order, |params, mut ctx| async move {
        let task_id = ctx.task_id();
        let order_id = params.order_id;
        let email = params.email;

        ctx.step("charge-payment", || async move {
            // Call Stripe, Adyen, etc. here. Use ctx.task_id() for idempotency keys.
            Ok(serde_json::json!({ "charged": true, "task_id": task_id }))
        })
        .await?;

        let event_name = format!("shipment.packed:{order_id}");
        let shipment: Shipment = ctx
            .await_event_with_options(
                &event_name,
                AwaitEventOptions::new().timeout(Duration::from_secs(60 * 60 * 24)),
            )
            .await?;

        ctx.step("send-email", || async move {
            Ok(serde_json::json!({ "sent_to": email }))
        })
        .await?;

        Ok(OrderResult {
            order_id,
            tracking_number: shipment.tracking_number,
        })
    })?;

    let worker = client.start_worker(
        WorkerOptions::new()
            .concurrency(8)
            .claim_timeout(Duration::from_secs(120)),
    );

    tokio::signal::ctrl_c()
        .await
        .map_err(|err| absurd_rust_sdk::Error::message(err.to_string()))?;
    worker.close().await?;

    Ok(())
}
