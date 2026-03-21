use jito_protos::shredstream::{
    shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest,
};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let mut client = ShredstreamProxyClient::connect("http://127.0.0.1:9999")
        .await
        .unwrap();
    let mut stream = client
        .subscribe_entries(SubscribeEntriesRequest {})
        .await
        .unwrap()
        .into_inner();

    while let Some(slot_entry) = stream.message().await.unwrap() {
        let entries =
            match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(&slot_entry.entries) {
                Ok(e) => e,
                Err(e) => {
                    println!("Deserialization failed with err: {e}");
                    continue;
                }
            };

        if slot_entry.producer_timestamp_nanos > 0 {
            let now_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let total_us =
                now_nanos.saturating_sub(slot_entry.producer_timestamp_nanos) / 1_000;
            let producer_us = slot_entry.stage_channel_transit_us
                + slot_entry.stage_ingest_us
                + slot_entry.stage_fec_recovery_us
                + slot_entry.stage_deshred_us;
            let grpc_us = total_us.saturating_sub(producer_us);

            println!(
                "slot={} entries={} txns={} | total={}us transit={}us ingest={}us fec={}us deshred={}us grpc={}us",
                slot_entry.slot,
                entries.len(),
                entries.iter().map(|e| e.transactions.len()).sum::<usize>(),
                total_us,
                slot_entry.stage_channel_transit_us,
                slot_entry.stage_ingest_us,
                slot_entry.stage_fec_recovery_us,
                slot_entry.stage_deshred_us,
                grpc_us,
            );
        } else {
            println!(
                "slot={} entries={} txns={}",
                slot_entry.slot,
                entries.len(),
                entries.iter().map(|e| e.transactions.len()).sum::<usize>(),
            );
        }
    }
    Ok(())
}
