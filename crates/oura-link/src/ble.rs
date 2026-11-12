//! `btleplug`-backed [`Transport`] for real rings (feature `ble`).

use std::time::{Duration, Instant};

use btleplug::api::{
    Central, CharPropFlags, Characteristic, Manager as _, Peripheral as _, ScanFilter, WriteType,
};
use btleplug::platform::{Manager, Peripheral};
use futures::StreamExt;
use tokio::sync::broadcast;

use crate::error::{Error, Result};
use crate::transport::Transport;
use oura_protocol::protocol;

/// Upper bound on the BLE connect + service-discovery + subscribe phase. CoreBluetooth
/// imposes no deadline itself, so without this a ring that won't complete the GATT
/// handshake hangs the caller forever.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Time to wait for GATT service discovery to resolve after connection.
///
/// On Linux/BlueZ, `connect()` completes before GATT service discovery finishes
/// over the air (`Device1.ServicesResolved` remains false for ~0.5–2s).
pub const SERVICES_RESOLVED_TIMEOUT: Duration = Duration::from_millis(2500);

/// Polling interval for retrying service discovery while services resolve.
pub const SERVICES_RESOLVED_POLL_INTERVAL: Duration = Duration::from_millis(150);

/// Upper bound on a single GATT write-with-response. One write is a single ATT
/// round-trip, normally well under a second. If the link drops silently, the
/// response never arrives and the platform never fails the call
/// (https://github.com/Th0rgal/open_oura/issues/40).
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on a disconnect request, for the same reason as [`WRITE_TIMEOUT`].
pub const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Selection rank given to a ring that is already connected to this host.
///
/// A connected peripheral stops advertising, so BlueZ reports no RSSI for it.
/// It is the ring we are most certain to reach, so it outranks any advertiser
/// (real RSSI values are negative dBm).
pub const CONNECTED_RANK: i16 = i16::MAX;

/// Rank an Oura peripheral for selection, or `None` if it is not reachable.
///
/// An advertiser ranks by RSSI. A peripheral that is already connected to this
/// host (for example, a link left open by an earlier process that was killed
/// mid-sync) does not advertise and has no RSSI. Skipping it made the ring
/// unreachable until the link was dropped by hand. It gets [`CONNECTED_RANK`].
/// A device that is only in the platform cache (bonded, but neither advertising
/// nor connected) is skipped.
pub(crate) fn candidate_rank(rssi: Option<i16>, connected: bool) -> Option<i16> {
    match (rssi, connected) {
        (Some(rssi), _) => Some(rssi),
        (None, true) => Some(CONNECTED_RANK),
        (None, false) => None,
    }
}

/// Run a BLE call with a deadline, mapping expiry to `on_timeout`.
async fn bounded<T>(
    limit: Duration,
    on_timeout: Error,
    call: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(limit, call)
        .await
        .unwrap_or(Err(on_timeout))
}

/// RSSI plus connection state for an Oura peripheral, resolved to a selection rank.
///
/// `is_connected` is only queried when there is no RSSI, so the common
/// advertising case costs no extra platform call.
async fn peripheral_rank(p: &Peripheral, rssi: Option<i16>) -> Option<i16> {
    let connected = rssi.is_none() && p.is_connected().await.unwrap_or(false);
    candidate_rank(rssi, connected)
}

/// Resolve the Oura service write characteristic and notification characteristics,
/// retrying service discovery until GATT resolution finishes or `timeout` expires.
///
/// On Linux/BlueZ, `connect()` returns as soon as the ACL link forms, but BlueZ discovers
/// GATT services asynchronously in the background (`ServicesResolved: false`). Calling
/// `discover_services()` immediately often returns an empty characteristic list.
/// This helper retries service discovery for up to `timeout` before reporting
/// `CharacteristicNotFound`.
pub(crate) async fn resolve_oura_characteristics<F, Fut, G, E>(
    mut discover: F,
    mut get_chars: G,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Characteristic, Vec<Characteristic>)>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), E>>,
    G: FnMut() -> std::collections::BTreeSet<Characteristic>,
    E: std::fmt::Display,
{
    let deadline = Instant::now() + timeout;
    let mut last_error = None;

    loop {
        match discover().await {
            Ok(()) => {}
            Err(e) => {
                tracing::debug!("service discovery attempt returned error: {e}");
                last_error = Some(e.to_string());
            }
        }

        let chars = get_chars();
        if let Some(write_char) = chars.iter().find(|c| c.uuid == protocol::OURA_WRITE).cloned() {
            let notify_chars = chars
                .into_iter()
                .filter(|c| {
                    c.service_uuid == protocol::OURA_SERVICE
                        && c.properties
                            .intersects(CharPropFlags::NOTIFY | CharPropFlags::INDICATE)
                })
                .collect();
            return Ok((write_char, notify_chars));
        }

        if Instant::now() >= deadline {
            if let Some(e) = last_error {
                tracing::warn!("service discovery deadline reached, last error: {e}");
            }
            return Err(Error::CharacteristicNotFound(protocol::OURA_WRITE.to_string()));
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// A ring discovered while scanning.
#[derive(Clone, Debug)]
pub struct Discovered {
    pub id: String,
    pub name: String,
    /// Advertised signal strength in dBm. Meaningless when `connected` is true:
    /// a connected ring does not advertise, and this holds [`CONNECTED_RANK`].
    pub rssi: i16,
    /// Already connected to this host, so not advertising.
    pub connected: bool,
}

/// A connected BLE link to a ring. Notifications from every notify/indicate
/// characteristic in the Oura service are merged into one broadcast stream, which
/// keeps the client working across ring generations that expose extra
/// characteristics (Ring 5 adds `…0004/0005/0006`).
pub struct BleTransport {
    peripheral: Peripheral,
    write_char: Characteristic,
    tx: broadcast::Sender<Vec<u8>>,
    _pump: tokio::task::JoinHandle<()>,
}

/// Start an unfiltered scan; callers check `OURA_SERVICE` on each peripheral.
///
/// With a BlueZ discovery filter on the service UUID (`ScanFilter { services }`),
/// a factory-reset Ring 3 that advertised the UUID was never reported; an
/// unfiltered scan on the same adapter reported it.
async fn start_unfiltered_scan(adapter: &btleplug::platform::Adapter) -> Result<()> {
    adapter.start_scan(ScanFilter::default()).await?;
    Ok(())
}

async fn first_adapter() -> Result<btleplug::platform::Adapter> {
    let manager = Manager::new().await?;
    manager
        .adapters()
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::Ble("no Bluetooth adapter found".into()))
}

/// Whether an advertised local name matches `--name`.
///
/// Scan already requires the Oura service UUID, so the name is only a secondary
/// filter. A bonded ring often stops advertising a local name; treating that as
/// a miss (`"".contains("oura") == false`) is https://github.com/Th0rgal/open_oura/issues/13.
/// Empty names therefore pass the default `"Oura"` needle. A more specific needle
/// still requires a name, so `--name "Ring 5"` does not pick every unnamed device.
pub fn name_matches(advertised: &str, needle: &str) -> bool {
    let needle = needle.trim().to_lowercase();
    if needle.is_empty() {
        return true;
    }
    if advertised.is_empty() {
        return needle == "oura";
    }
    advertised.to_lowercase().contains(&needle)
}

/// Scan for Oura rings advertising the service, filtered by case-insensitive name
/// substring. Returns candidates sorted by signal strength (strongest first).
pub async fn scan(name_contains: &str, timeout: Duration) -> Result<Vec<Discovered>> {
    let adapter = first_adapter().await?;
    start_unfiltered_scan(&adapter).await?;

    let deadline = Instant::now() + timeout;
    let mut found: Vec<Discovered> = Vec::new();
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(400)).await;
        for p in adapter.peripherals().await? {
            let Some(props) = p.properties().await? else {
                continue;
            };
            if !props.services.contains(&protocol::OURA_SERVICE) {
                continue;
            }
            let name = props.local_name.unwrap_or_default();
            if !name_matches(&name, name_contains) {
                continue;
            }
            let Some(rank) = peripheral_rank(&p, props.rssi).await else {
                continue;
            };
            let id = p.id().to_string();
            let entry = Discovered {
                id: id.clone(),
                name: if name.is_empty() {
                    "(unnamed)".into()
                } else {
                    name
                },
                rssi: rank,
                connected: props.rssi.is_none(),
            };
            match found.iter_mut().find(|d| d.id == id) {
                Some(existing) => *existing = entry,
                None => found.push(entry),
            }
        }
    }
    let _ = adapter.stop_scan().await;
    found.sort_by_key(|d| std::cmp::Reverse(d.rssi));
    Ok(found)
}

impl BleTransport {
    /// Scan for and connect to a ring, selecting the strongest match for
    /// `name_contains`. If `address` is given, only a device whose id matches is
    /// considered.
    pub async fn connect(
        name_contains: &str,
        address: Option<&str>,
        scan_timeout: Duration,
    ) -> Result<Self> {
        let adapter = first_adapter().await?;
        start_unfiltered_scan(&adapter).await?;

        let deadline = Instant::now() + scan_timeout;
        let mut chosen: Option<(Peripheral, i16)> = None;
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(400)).await;
            for p in adapter.peripherals().await? {
                let Some(props) = p.properties().await? else {
                    continue;
                };
                if !props.services.contains(&protocol::OURA_SERVICE) {
                    continue;
                }
                let name = props.local_name.unwrap_or_default();
                if !name_matches(&name, name_contains) {
                    continue;
                }
                if let Some(addr) = address {
                    let pid = p.id().to_string();
                    let bda = props.address.to_string();
                    if !pid.eq_ignore_ascii_case(addr) && !bda.eq_ignore_ascii_case(addr) {
                        continue;
                    }
                }
                let Some(rank) = peripheral_rank(&p, props.rssi).await else {
                    continue;
                };
                if rank == CONNECTED_RANK {
                    tracing::info!(
                        "ring {} is already connected to this host; reusing the link",
                        p.id()
                    );
                }
                if chosen.as_ref().map(|(_, r)| rank > *r).unwrap_or(true) {
                    chosen = Some((p, rank));
                }
            }
            if chosen.is_some() {
                // brief settle to prefer the strongest advertiser
                tokio::time::sleep(Duration::from_millis(300)).await;
                break;
            }
        }

        let Some((peripheral, _)) = chosen else {
            let _ = adapter.stop_scan().await;
            return Err(Error::DeviceNotFound);
        };

        // CoreBluetooth's connect (and, on some stacks, service discovery) has no
        // deadline of its own: a ring that advertises but won't complete the GATT
        // handshake — e.g. because a phone still holds the single peripheral link —
        // makes these calls hang forever. Bound the whole setup phase so we fail with
        // an actionable error instead of blocking indefinitely.
        let write_char = tokio::time::timeout(CONNECT_TIMEOUT, async {
            if !peripheral.is_connected().await? {
                if let Err(e) = peripheral.connect().await {
                    tracing::warn!("first connect failed: {e}; retrying in 1s...");
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    peripheral.connect().await?;
                }
            }

            let (write_char, notify_chars) = resolve_oura_characteristics(
                || peripheral.discover_services(),
                || peripheral.characteristics(),
                SERVICES_RESOLVED_TIMEOUT,
                SERVICES_RESOLVED_POLL_INTERVAL,
            )
            .await?;

            for c in &notify_chars {
                if let Err(e) = peripheral.subscribe(c).await {
                    tracing::warn!("subscribing to {} returned {e} (ignoring)", c.uuid);
                }
            }
            Ok::<_, Error>(write_char)
        })
        .await;
        // Keep discovery running until the link is up. On Linux, connecting to a
        // bonded ring (which advertises a resolvable private address) with
        // discovery stopped hands RPA resolution to the controller; an Intel
        // controller was observed never to match the ring, so the connect timed
        // out. With active discovery running, the kernel resolves the RPA itself.
        let _ = adapter.stop_scan().await;
        let write_char = write_char.map_err(|_| Error::ConnectTimeout)??;

        let (tx, _) = broadcast::channel(256);
        let pump_tx = tx.clone();
        let pump_peripheral = peripheral.clone();
        let pump = tokio::spawn(async move {
            if let Ok(mut stream) = pump_peripheral.notifications().await {
                while let Some(n) = stream.next().await {
                    // Best-effort fan-out; ignore if there are no live receivers.
                    let _ = pump_tx.send(n.value);
                }
            }
        });

        Ok(Self {
            peripheral,
            write_char,
            tx,
            _pump: pump,
        })
    }

    /// Disconnect from the ring.
    pub async fn disconnect(&self) -> Result<()> {
        bounded(DISCONNECT_TIMEOUT, Error::DisconnectTimeout, async {
            self.peripheral.disconnect().await?;
            Ok(())
        })
        .await
    }
}

#[async_trait::async_trait]
impl Transport for BleTransport {
    async fn write(&self, data: &[u8]) -> Result<()> {
        bounded(WRITE_TIMEOUT, Error::WriteTimeout, async {
            self.peripheral
                .write(&self.write_char, data, WriteType::WithResponse)
                .await?;
            Ok(())
        })
        .await
    }

    fn subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    #[test]
    fn default_needle_accepts_unnamed_bonded_ring() {
        assert!(name_matches("", "Oura"));
        assert!(name_matches("", "oura"));
        assert!(name_matches("Oura Ring 4", "Oura"));
    }

    #[test]
    fn specific_needle_still_requires_a_name() {
        assert!(!name_matches("", "Ring 5"));
        assert!(name_matches("Oura Ring 5", "Ring 5"));
        assert!(!name_matches("Oura Ring 4", "Ring 5"));
    }

    #[test]
    fn empty_needle_matches_everything() {
        assert!(name_matches("", ""));
        assert!(name_matches("anything", "  "));
    }

    #[test]
    fn advertising_ring_ranks_by_rssi() {
        assert_eq!(candidate_rank(Some(-60), false), Some(-60));
        // A fresh RSSI wins even if the platform also reports it connected.
        assert_eq!(candidate_rank(Some(-60), true), Some(-60));
    }

    #[test]
    fn connected_ring_without_rssi_is_still_a_candidate() {
        // A ring already linked to this host stops advertising, so BlueZ has no
        // RSSI for it. It must still be selectable, ahead of any advertiser.
        assert_eq!(candidate_rank(None, true), Some(CONNECTED_RANK));
        assert!(CONNECTED_RANK > 0);
    }

    #[test]
    fn stale_cache_entry_is_not_a_candidate() {
        // Known to BlueZ (bonded) but neither advertising nor connected.
        assert_eq!(candidate_rank(None, false), None);
    }

    #[tokio::test]
    async fn bounded_passes_through_a_result_that_arrives_in_time() {
        let ok = bounded(Duration::from_millis(100), Error::WriteTimeout, async {
            Ok::<_, Error>(7)
        })
        .await
        .unwrap();
        assert_eq!(ok, 7);

        let err = bounded(Duration::from_millis(100), Error::WriteTimeout, async {
            Err::<(), _>(Error::Ble("gatt error".into()))
        })
        .await
        .unwrap_err();
        assert!(matches!(err, Error::Ble(_)));
    }

    #[tokio::test]
    async fn bounded_turns_a_hung_call_into_the_given_error() {
        // A silently dropped link never answers a GATT write (issue #40).
        let err = bounded(
            Duration::from_millis(20),
            Error::WriteTimeout,
            std::future::pending::<Result<()>>(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::WriteTimeout));
    }

    fn mock_characteristic(
        uuid: uuid::Uuid,
        service_uuid: uuid::Uuid,
        flags: CharPropFlags,
    ) -> Characteristic {
        Characteristic {
            uuid,
            service_uuid,
            properties: flags,
            descriptors: BTreeSet::new(),
        }
    }

    #[tokio::test]
    async fn resolve_services_not_yet_resolved_retries_and_succeeds() {
        let attempts = Arc::new(Mutex::new(0usize));
        let attempts_disc = attempts.clone();
        let attempts_chars = attempts.clone();

        let (write_char, notify_chars) = resolve_oura_characteristics(
            move || {
                let attempts = attempts_disc.clone();
                async move {
                    let mut lock = attempts.lock().unwrap();
                    *lock += 1;
                    Ok::<(), &str>(())
                }
            },
            move || {
                let lock = attempts_chars.lock().unwrap();
                // Simulate BlueZ: return empty characteristics until attempt 3
                if *lock >= 3 {
                    let mut set = BTreeSet::new();
                    set.insert(mock_characteristic(
                        protocol::OURA_WRITE,
                        protocol::OURA_SERVICE,
                        CharPropFlags::WRITE,
                    ));
                    set.insert(mock_characteristic(
                        uuid::Uuid::from_u128(0x1234_5678),
                        protocol::OURA_SERVICE,
                        CharPropFlags::NOTIFY,
                    ));
                    set
                } else {
                    BTreeSet::new()
                }
            },
            Duration::from_millis(500),
            Duration::from_millis(10),
        )
        .await
        .expect("should succeed once services resolve");

        assert_eq!(write_char.uuid, protocol::OURA_WRITE);
        assert_eq!(notify_chars.len(), 1);
        assert!(*attempts.lock().unwrap() >= 3);
    }

    #[tokio::test]
    async fn resolve_services_times_out_with_characteristic_not_found() {
        let attempts = Arc::new(Mutex::new(0usize));
        let attempts_disc = attempts.clone();

        let err = resolve_oura_characteristics(
            move || {
                let attempts = attempts_disc.clone();
                async move {
                    let mut lock = attempts.lock().unwrap();
                    *lock += 1;
                    Ok::<(), &str>(())
                }
            },
            BTreeSet::new,
            Duration::from_millis(40),
            Duration::from_millis(10),
        )
        .await
        .expect_err("should fail when services never resolve");

        match err {
            Error::CharacteristicNotFound(uuid_str) => {
                assert_eq!(uuid_str, protocol::OURA_WRITE.to_string());
            }
            other => panic!("expected CharacteristicNotFound, got {other:?}"),
        }

        assert!(*attempts.lock().unwrap() > 1);
    }

    #[tokio::test]
    async fn resolve_services_immediate_success() {
        let attempts = Arc::new(Mutex::new(0usize));
        let attempts_disc = attempts.clone();

        let (write_char, _) = resolve_oura_characteristics(
            move || {
                let attempts = attempts_disc.clone();
                async move {
                    let mut lock = attempts.lock().unwrap();
                    *lock += 1;
                    Ok::<(), &str>(())
                }
            },
            || {
                let mut set = BTreeSet::new();
                set.insert(mock_characteristic(
                    protocol::OURA_WRITE,
                    protocol::OURA_SERVICE,
                    CharPropFlags::WRITE,
                ));
                set
            },
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
        .await
        .expect("immediate resolution should succeed");

        assert_eq!(write_char.uuid, protocol::OURA_WRITE);
        assert_eq!(*attempts.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn resolve_services_recovers_from_transient_discovery_error() {
        let attempts = Arc::new(Mutex::new(0usize));
        let attempts_disc = attempts.clone();
        let attempts_chars = attempts.clone();

        let (write_char, _) = resolve_oura_characteristics(
            move || {
                let attempts = attempts_disc.clone();
                async move {
                    let mut lock = attempts.lock().unwrap();
                    *lock += 1;
                    if *lock == 1 {
                        Err("transient BlueZ D-Bus error")
                    } else {
                        Ok(())
                    }
                }
            },
            move || {
                let lock = attempts_chars.lock().unwrap();
                if *lock >= 2 {
                    let mut set = BTreeSet::new();
                    set.insert(mock_characteristic(
                        protocol::OURA_WRITE,
                        protocol::OURA_SERVICE,
                        CharPropFlags::WRITE,
                    ));
                    set
                } else {
                    BTreeSet::new()
                }
            },
            Duration::from_millis(200),
            Duration::from_millis(10),
        )
        .await
        .expect("should recover from transient error");

        assert_eq!(write_char.uuid, protocol::OURA_WRITE);
    }
}
