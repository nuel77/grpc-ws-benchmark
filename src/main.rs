use anyhow::Context;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_response::SlotUpdate;
use std::ops::Div;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SlotStatus, SubscribeRequest, SubscribeRequestFilterSlots, SubscribeUpdateSlot,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

#[derive(Debug, PartialEq)]
pub enum Protocol {
    GRPC,
    WS,
}
pub struct Stats {
    pub who_first: Protocol,
    pub first_seen: Instant,
    pub next_seen_latency: u128,
}

impl Stats {
    pub fn record_first_seen(who_first: Protocol) -> Self {
        Self {
            who_first,
            first_seen: Instant::now(),
            next_seen_latency: 0,
        }
    }

    pub fn record_second_seen(&mut self) {
        let now = Instant::now();
        self.next_seen_latency = now.duration_since(self.first_seen).as_millis();
    }
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let ws_url = std::env::var("WS_URL").expect("WS_URL must be set");
    let grpc_url = std::env::var("GRPC_URL").expect("GRPC_URL must be set");
    let run_duration = std::env::var("RUN_DURATION")
        .unwrap_or("10".to_string())
        .parse::<u64>()?;
    let cancel = CancellationToken::new();
    let slot_update_recorder = Arc::new(DashMap::<u64, Stats>::new());

    let pubsub_client = Arc::new(
        PubsubClient::new(&ws_url)
            .await
            .context("cannot connect to ws")?,
    );

    info!("connected to ws");

    let mut client = yellowstone_grpc_client::GeyserGrpcClient::build_from_shared(grpc_url)?
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(1024 * 1024 * 4)
        .tcp_keepalive(Some(Duration::from_secs(15)))
        .connect()
        .await
        .map_err(anyhow::Error::from)?;

    info!("connected to grpc");

    let slot_sub_request = vec![(
        "slot".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(true),
        },
    )]
    .into_iter()
    .collect();

    let subscribe_request = SubscribeRequest {
        slots: slot_sub_request,
        ..Default::default()
    };

    let (mut sink, mut stream) = client
        .subscribe()
        .await
        .context("cannot subscribe to stream")?;

    sink.send(subscribe_request)
        .await
        .map_err(anyhow::Error::from)?;

    info!("spawning tasks...");
    let pubsub_jh = tokio::spawn({
        let cancel = cancel.clone();
        let slot_update_recorder = slot_update_recorder.clone();
        async move {
            let (mut stream, unsub) = match pubsub_client.slot_updates_subscribe().await {
                Ok(val) => val,
                Err(e) => {
                    error!("cannot subscribe to slot updates {:?}", e);
                    return;
                }
            };
            loop {
                let update = tokio::select! {
                    _ = cancel.cancelled() => break,
                    res = stream.next() => match res{
                        Some(res) => res,
                        None => break,
                    }
                };

                let current_slot = match update {
                    SlotUpdate::FirstShredReceived { slot, .. } => slot,
                    SlotUpdate::Completed { slot, .. } => slot + 1,
                    _ => continue,
                };
                slot_update_recorder
                    .entry(current_slot)
                    .and_modify(|item| item.record_second_seen())
                    .or_insert(Stats::record_first_seen(Protocol::WS));
                debug!("Received slot update pubsub: {:?}", current_slot);
            }
            unsub().await
        }
    });

    let grpc_jh = tokio::spawn({
        let slot_update_recorder = slot_update_recorder.clone();
        let cancel = cancel.clone();
        async move {
            loop {
                let update = tokio::select! {
                    _ = cancel.cancelled() => break,
                    update = stream.next() => match update {
                        Some(update) => update,
                        None => {
                            error!("grpc stream ended unexpectedly");
                            break;
                        },
                    },
                };
                match update {
                    Ok(update) => {
                        match update.update_oneof {
                            Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                                slot, status, ..
                            })) => {
                                let current_slot = if status == SlotStatus::SlotCompleted as i32 {
                                    slot + 1
                                } else if status == SlotStatus::SlotFirstShredReceived as i32 {
                                    slot
                                } else {
                                    continue;
                                };
                                debug!("Received slot update grpc: {:?}", current_slot);
                                slot_update_recorder
                                    .entry(current_slot)
                                    .and_modify(|item| item.record_second_seen())
                                    .or_insert(Stats::record_first_seen(Protocol::GRPC));
                            }
                            _ => continue,
                        };
                    }
                    Err(e) => {
                        error!("error receiving response: {:?}", e);
                        break;
                    }
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(run_duration)).await;
    cancel.cancel();
    futures_util::future::try_join(pubsub_jh, grpc_jh).await?;

    let grpc_won_metrics = slot_update_recorder
        .iter()
        .filter(|item| item.value().who_first == Protocol::GRPC)
        .collect::<Vec<_>>();

    let grpc_won_count = grpc_won_metrics.len();
    let grpc_won_average = grpc_won_metrics
        .iter()
        .map(|item| item.value().next_seen_latency)
        .sum::<u128>()
        .checked_div(grpc_won_count as u128);

    let ws_won_metrics = slot_update_recorder
        .iter()
        .filter(|item| item.value().who_first == Protocol::WS)
        .collect::<Vec<_>>();

    let ws_won_count = ws_won_metrics.len();
    let ws_won_average = ws_won_metrics
        .iter()
        .map(|item| item.value().next_seen_latency)
        .sum::<u128>()
        .checked_div(ws_won_count as u128);

    assert_eq!(ws_won_count + grpc_won_count, slot_update_recorder.len());
    info!(
        "ws won count: {}, grpc won count: {}, total slots {}, ws_wins_avg_ms: {:?}, grpc_wins_avg_ms: {:?}",
        ws_won_count,
        grpc_won_count,
        slot_update_recorder.len(),
        ws_won_average,
        grpc_won_average,
    );
    Ok(())
}
