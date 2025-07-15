use anyhow::Context;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_response::SlotUpdate;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SlotStatus, SubscribeRequest, SubscribeRequestFilterSlots, SubscribeUpdateSlot,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cancel = CancellationToken::new();
    let grpc_url = "http://astralane-fra-rpc:10000";
    let ws_url = "ws://astralane-fra-rpc:8900";
    let slot_update_recorder = Arc::new(DashMap::new());

    let pubsub_client = Arc::new(
        PubsubClient::new(ws_url)
            .await
            .context("cannot connect to ws")?,
    );

    let mut client = yellowstone_grpc_client::GeyserGrpcClient::build_from_static(grpc_url)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(1024 * 1024 * 4)
        .tcp_keepalive(Some(Duration::from_secs(15)))
        .connect()
        .await
        .map_err(anyhow::Error::from)?;

    let slot_sub_request = vec![(
        "slot_sub".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
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

    let pubsub_jh = tokio::spawn({
        let cancel = cancel.clone();
        let slot_update_recorder = slot_update_recorder.clone();
        async move {
            let (mut stream, unsub) = match pubsub_client.slot_updates_subscribe().await {
                Ok(val) => val,
                Err(e) => {
                    error!("cannot subscribe to slot updates");
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
                    .or_insert("ws".to_string());
                // info!("Received slot update pubsub: {:?}", current_slot);
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
                // info!("Received update: {:?}", update);
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
                                //info!("Received slot update grpc: {:?}", current_slot);
                                slot_update_recorder
                                    .entry(current_slot)
                                    .or_insert("grpc".to_string());
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
    tokio::time::sleep(Duration::from_secs(20)).await;
    cancel.cancel();
    futures_util::future::try_join(pubsub_jh, grpc_jh).await?;

    let grpc_won_count = slot_update_recorder
        .iter()
        .map(|item| item.value().eq("grpc"))
        .filter(|item| *item)
        .count();

    let ws_won = slot_update_recorder
        .iter()
        .map(|item| item.value().eq("ws"))
        .filter(|item| *item)
        .count();
    info!("ws won: {}, grpc won: {}", ws_won, grpc_won_count);
    Ok(())
}
