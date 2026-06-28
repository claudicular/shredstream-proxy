//! Slot -> leader-validator resolution via RPC `getSlotLeaders`.
//!
//! A dedicated background thread polls the RPC for a rolling window of slot
//! leaders and publishes it into an `ArcSwap`, so the (single, hot) aggregator
//! thread can resolve `slot -> leader pubkey` with a lock-free load and NEVER
//! blocks on RPC. We keep the full `Vec<Pubkey>` (unlike arb_bot's
//! `leader_slot_cache`, which collapses it to a relevance bitset and discards
//! the identity).

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{Builder, JoinHandle},
    time::Duration,
};

use arc_swap::ArcSwap;
use log::{info, warn};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{clock::Slot, pubkey::Pubkey};

/// Max number of slots fetched per `getSlotLeaders` call (its documented cap).
const WINDOW_LEN: u64 = 5_000;
/// How far before the current slot the window starts, to cover slightly-lagging
/// observations. Must exceed the aggregator's eviction `window_slots` so evicted
/// slots can still be resolved to a leader.
const WINDOW_BACKFILL: u64 = 1_024;
/// How often the poller refreshes the window. 5000 slots ~= 33 min, so a 30s
/// refresh keeps the window comfortably ahead of live observations.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// Backoff after an RPC error before retrying.
const ERROR_BACKOFF: Duration = Duration::from_secs(5);
/// RPC timeout, bounded so a stuck RPC can't stall process shutdown.
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct LeaderWindow {
    base_slot: Slot,
    leaders: Vec<Pubkey>,
}

#[derive(Clone)]
pub struct LeaderScheduleHandle {
    window: Arc<ArcSwap<LeaderWindow>>,
}

impl LeaderScheduleHandle {
    /// Resolve the leader for a slot, or `None` if the slot is outside the
    /// currently-published window (not yet fetched, or already rolled past).
    pub fn leader_for_slot(&self, slot: Slot) -> Option<Pubkey> {
        let w = self.window.load();
        if w.leaders.is_empty() || slot < w.base_slot {
            return None;
        }
        let offset = (slot - w.base_slot) as usize;
        w.leaders.get(offset).copied()
    }

    /// The currently-published window as `(base_slot, end_slot_exclusive)`, or
    /// `None` if nothing has been fetched yet.
    pub fn bounds(&self) -> Option<(Slot, Slot)> {
        let w = self.window.load();
        if w.leaders.is_empty() {
            None
        } else {
            Some((w.base_slot, w.base_slot + w.leaders.len() as u64))
        }
    }
}

/// Spawn the background poller. Returns the handle and the join handle.
pub fn spawn(rpc_url: String, exit: Arc<AtomicBool>) -> (LeaderScheduleHandle, JoinHandle<()>) {
    let window = Arc::new(ArcSwap::from_pointee(LeaderWindow::default()));
    let handle = LeaderScheduleHandle {
        window: window.clone(),
    };

    let join = Builder::new()
        .name("ssBenchLeaders".to_string())
        .spawn(move || {
            let rpc = RpcClient::new_with_timeout(rpc_url, RPC_TIMEOUT);
            while !exit.load(Ordering::Relaxed) {
                match refresh(&rpc) {
                    Ok(new_window) => {
                        info!(
                            "benchmark leader schedule refreshed: base_slot={}, leaders={}",
                            new_window.base_slot,
                            new_window.leaders.len()
                        );
                        window.store(Arc::new(new_window));
                        sleep_interruptible(&exit, REFRESH_INTERVAL);
                    }
                    Err(e) => {
                        warn!("benchmark leader schedule refresh failed: {e}");
                        sleep_interruptible(&exit, ERROR_BACKOFF);
                    }
                }
            }
        })
        .unwrap();

    (handle, join)
}

fn refresh(rpc: &RpcClient) -> Result<LeaderWindow, solana_client::client_error::ClientError> {
    // One call gives current slot AND epoch geometry, so we can clamp the
    // request to the epoch end (getSlotLeaders errors if asked beyond the
    // currently-known leader schedule, e.g. near an epoch boundary).
    let epoch = rpc.get_epoch_info()?;
    let current = epoch.absolute_slot;
    let base = current.saturating_sub(WINDOW_BACKFILL);
    // Slots from `current` to the epoch end, inclusive of `current`.
    let slots_left_in_epoch = epoch.slots_in_epoch.saturating_sub(epoch.slot_index);
    // Most slots we can request from `base` without crossing the epoch end.
    let max_from_base = (current - base) + slots_left_in_epoch;
    let limit = WINDOW_LEN.min(max_from_base).max(1);
    let leaders = rpc.get_slot_leaders(base, limit)?;
    Ok(LeaderWindow {
        base_slot: base,
        leaders,
    })
}

/// Sleep in small increments so shutdown is responsive.
fn sleep_interruptible(exit: &AtomicBool, dur: Duration) {
    let step = Duration::from_millis(250);
    let mut remaining = dur;
    while remaining > Duration::ZERO && !exit.load(Ordering::Relaxed) {
        let s = step.min(remaining);
        std::thread::sleep(s);
        remaining = remaining.saturating_sub(s);
    }
}
