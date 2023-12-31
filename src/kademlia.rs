use std::{
    collections::{HashMap, HashSet},
    iter::repeat,
    sync::Arc,
    time::Duration,
};

use borsh::{BorshDeserialize, BorshSerialize};
use ethnum::U256;
use tokio::{task::JoinSet, time::timeout};

use crate::{
    channel::{EventSender, EventSource, PromiseSender, SubmitHandle, SubscribeSource},
    crypto::{digest, Packet, Signer, Verifier},
    event_channel, promise_channel,
    task::BackgroundSpawner,
    transport::Addr,
    Transport,
};

pub type PeerId = [u8; 32];
pub type Location = [u8; 32];

fn peer_id(verifier: &Verifier) -> crate::Result<PeerId> {
    digest(verifier)
}

fn distance(id: &PeerId, target: &Location) -> U256 {
    U256::from_le_bytes(*id) ^ U256::from_le_bytes(*target)
}

fn distance_from(id: &PeerId, distance: U256) -> Location {
    (U256::from_le_bytes(*id) ^ distance).to_le_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum Message {
    FindPeer(Packet),
    FindPeerOk(Packet),
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct FindPeer {
    target: Location,
    count: usize,
    verifier: Verifier,
    instant: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct FindPeerOk {
    target: Location,
    closest: Vec<PeerRecord>,
    verifier: Verifier,
    instant: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SystemTime(std::time::SystemTime);

impl BorshSerialize for SystemTime {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.0
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .serialize(writer)
    }
}

impl BorshDeserialize for SystemTime {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        Ok(Self(
            std::time::SystemTime::UNIX_EPOCH
                + Duration::from_secs(<u64 as BorshDeserialize>::deserialize_reader(reader)?),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct PeerRecord {
    pub id: PeerId, // redundant for a well-formed record
    pub verifier: Verifier,
    pub addr: Addr,
}

impl PeerRecord {
    pub fn new(signer: &Signer, addr: Addr) -> crate::Result<Self> {
        let verifier = Verifier::from(signer);
        Ok(Self {
            id: peer_id(&verifier)?,
            verifier,
            addr,
        })
    }
}

impl TryFrom<kademlia_control_messages::Peer> for PeerRecord {
    type Error = crate::Error;

    fn try_from(value: kademlia_control_messages::Peer) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            verifier: Verifier::from(secp256k1::PublicKey::from_slice(&value.key)?),
            addr: Addr::Socket(value.addr),
        })
    }
}

impl From<PeerRecord> for kademlia_control_messages::Peer {
    fn from(value: PeerRecord) -> Self {
        let Addr::Socket(addr) = value.addr else {
            unimplemented!()
        };
        Self {
            id: value.id,
            key: value.verifier.0.serialize().into(),
            addr,
        }
    }
}

const MAX_CONCURRENCY: usize = 3;
const BUCKET_SIZE: usize = 20;

#[derive(Debug)]
pub struct Peer {
    pub verifier: Verifier,
    pub signer: Signer,
    pub spawner: BackgroundSpawner,
}

async fn find_session(
    peer: Arc<Peer>,
    target: Location,
    count: usize,
    local_closest: Vec<PeerRecord>,
    handle: SubmitHandle<(Location, PeerId), FindPeerOk>,
    event: EventSender<Vec<PeerRecord>>,
    transport: impl Transport<Message>,
) -> crate::Result<()> {
    let peer_id = peer_id(&peer.verifier)?;
    let mut contacted = HashSet::new();
    contacted.insert(peer_id);
    let mut contacting = HashSet::new();
    let mut closest = local_closest;
    let mut submit_sessions = JoinSet::new();

    let find_peer = FindPeer {
        target,
        count,
        verifier: peer.verifier,
        instant: SystemTime(std::time::SystemTime::now()),
    };
    let find_peer = Message::FindPeer(peer.signer.serialize_sign(find_peer)?.into());

    let mut destinations = Vec::new();
    for ((id, addr), handle) in closest
        .iter()
        .filter(|record| !contacted.contains(&record.id))
        .take(MAX_CONCURRENCY)
        .map(|record| (record.id, record.addr.clone()))
        .zip(repeat(handle.clone()))
    {
        submit_sessions.spawn(async move {
            let result = handle.submit((target, id)).await;
            (id, result.ok())
        });
        contacting.insert(id);
        destinations.push(addr)
    }
    assert!(!destinations.is_empty());
    // due to the nature of asynchronous spawning above, there's chance that message has been
    // broadcast here, replies has been received, verified and dropped by main session (see below),
    // and `handle.submit` finally submitted
    // not going to solve this since it should not stuck the protocol (instead a false positive
    // timeout is raised) and it's highly unlikely to happen
    transport
        .send_to_all(destinations.into_iter(), find_peer.clone())
        .await?;

    loop {
        let (peer_id, message) = tokio::select! {
            result = submit_sessions.join_next() => result.unwrap()?,
            () = event.0.closed() => break,
        };
        assert!(contacting.remove(&peer_id));
        contacted.insert(peer_id);
        if let Some(message) = message {
            for record in message.closest {
                if contacted.contains(&record.id) || contacting.contains(&record.id) {
                    continue;
                }
                match closest.binary_search_by_key(&distance(&record.id, &target), |record| {
                    distance(&record.id, &target)
                }) {
                    Ok(index) => assert_eq!(closest[index].verifier, record.verifier),
                    Err(index) => closest.insert(index, record),
                }
            }
        } else {
            let index = closest
                .iter()
                .position(|record| record.id == peer_id)
                .unwrap();
            closest.remove(index);
        }
        // the two iterations can be merged into one pass
        // but i feel it's more clear to be separated written
        if event
            .send(
                // the (unconditional) first `count` peers that has been contacted
                closest
                    .iter()
                    .filter(|record| contacted.contains(&record.id))
                    .take(count)
                    .cloned()
                    .collect(),
            )
            .is_err()
        {
            break;
        }
        if closest
            .iter()
            .take(count)
            .all(|record| contacted.contains(&record.id))
        {
            break;
        }
        while contacting.len() < MAX_CONCURRENCY {
            let Some(record) = closest
                .iter()
                .find(|record| !contacted.contains(&record.id) && !contacting.contains(&record.id))
            else {
                break;
            };
            let id = record.id;
            let handle = handle.clone();
            submit_sessions.spawn(async move {
                let result = handle.submit((target, id)).await;
                (id, result.ok())
            });
            contacting.insert(id);
            transport
                .send_to(record.addr.clone(), find_peer.clone())
                .await?
        }
        assert!(!contacting.is_empty())
    }
    drop(event);
    while let Some(result) = submit_sessions.join_next().await {
        result?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Buckets {
    center: PeerRecord,
    distances: Vec<Bucket>,
}

#[derive(Debug, Clone, Default)]
struct Bucket {
    records: Vec<PeerRecord>,
    cached_records: Vec<PeerRecord>,
}

impl Buckets {
    pub fn new(center: PeerRecord) -> Self {
        Self {
            center,
            distances: vec![Default::default(); U256::BITS as _],
        }
    }

    fn index(&self, location: &Location) -> usize {
        distance(&self.center.id, location).leading_zeros() as _
    }

    pub fn insert(&mut self, record: PeerRecord) {
        let index = self.index(&record.id);
        let bucket = &mut self.distances[index];
        // if record exists in the bucket, move it to the end
        // otherwise insert it only if the bucket is not full yet
        if let Some(bucket_index) = bucket
            .records
            .iter()
            .position(|bucket_record| bucket_record.id == record.id)
        {
            bucket.records.remove(bucket_index);
        }
        if bucket.records.len() < BUCKET_SIZE {
            bucket.records.push(record);
            return;
        }

        // repeat on cached entries, only shifting on a full cache
        // this is surprisingly duplicated code to the above
        if let Some(bucket_index) = bucket
            .cached_records
            .iter()
            .position(|bucket_record| bucket_record.id == record.id)
        {
            bucket.cached_records.remove(bucket_index);
        }
        if bucket.cached_records.len() == BUCKET_SIZE {
            bucket.cached_records.remove(0);
        }
        bucket.cached_records.push(record)
    }

    pub fn remove(&mut self, id: &PeerId) -> Option<PeerRecord> {
        let index = self.index(id);
        let bucket = &mut self.distances[index];
        let Some(bucket_index) = bucket.records.iter().position(|record| &record.id == id) else {
            return None;
        };
        let record = bucket.records.remove(bucket_index);
        if let Some(cache_record) = bucket.cached_records.pop() {
            bucket.records.push(cache_record)
        } else {
            // "mark" the record as stalled by prepending it back to the list
            // should stalled record appear in find result? paper not mentioned
            bucket.records.insert(0, record.clone())
        }
        Some(record)
    }

    fn find_closest(&self, target: &Location, count: usize) -> Vec<PeerRecord> {
        let mut records = Vec::new();
        let index = self.index(target);
        let center_distance = distance(&self.center.id, target);
        // look up order derived from libp2p::kad, personally i don't understand why this works
        // anyway the result is asserted before returning
        // notice that bucket index here is reversed to libp2p's, i.e. libp2p_to_this(i) = 255 - i
        for index in (index..U256::BITS as _)
            .filter(|i| center_distance >> (U256::BITS - 1 - *i as u32) & 1 == 1)
            .chain(
                (0..U256::BITS as _)
                    .rev()
                    .filter(|i| center_distance >> (U256::BITS - 1 - *i as u32) & 1 == 0),
            )
        {
            let mut index_records = self.distances[index].records.clone();
            // ensure center peer is included if it is indeed close enough
            // can it be more elegant?
            if index == U256::BITS as usize - 1 {
                index_records.push(self.center.clone())
            }
            index_records.sort_unstable_by_key(|record| distance(&record.id, target));
            records.extend(index_records.into_iter().take(count - records.len()));
            assert!(records.len() <= count);
            if records.len() == count {
                break;
            }
        }
        assert!(records
            .windows(2)
            .all(|records| distance(&records[0].id, target) < distance(&records[1].id, target)));
        records
    }
}

pub async fn session(
    peer: Arc<Peer>,
    buckets: &mut Buckets,
    mut subscribe_source: SubscribeSource<(Location, usize), Vec<PeerRecord>>,
    message_source: &mut EventSource<(Addr, Message)>,
    transport: impl Transport<Message>,
) -> crate::Result<()> {
    let mut verify_find_peer_sessions = JoinSet::new();
    let mut verify_find_peer_ok_sessions = JoinSet::new();
    let (submit_handle, mut submit_source) = event_channel();
    let mut find_peer_ok_events = HashMap::new();
    let mut find_sessions = JoinSet::new();
    let (timeout_event, mut timeout_source) = event_channel();

    loop {
        enum Select {
            Subscribe(((Location, usize), EventSender<Vec<PeerRecord>>)),
            Message((Addr, Message)),
            VerifiedFindPeer(crate::Result<(Addr, FindPeer)>),
            VerifiedFindPeerOk(crate::Result<(Addr, FindPeerOk)>),
            Submit(((Location, PeerId), PromiseSender<FindPeerOk>)),
            JoinFindSession(()),
            Timeout((Location, PeerId)),
            Stop,
        }
        let select = async {
            Ok::<_, crate::Error>(tokio::select! {
                Some(subscribe) = subscribe_source.option_next() => Select::Subscribe(subscribe),
                Some(result) = find_sessions.join_next() => Select::JoinFindSession(result??),
                else => Select::Stop,
            })
        };
        match tokio::select! {
            select = select => select?,
            message = message_source.next() => Select::Message(message?),
            Some(verified) = verify_find_peer_sessions.join_next()
                => Select::VerifiedFindPeer(verified?),
            Some(verified) = verify_find_peer_ok_sessions.join_next()
                => Select::VerifiedFindPeerOk(verified?),
            submit = submit_source.next() => Select::Submit(submit?),
            timeout = timeout_source.next() => Select::Timeout(timeout?),
        } {
            Select::Stop => break,
            Select::JoinFindSession(()) => {}
            Select::Subscribe(((target, count), event)) => {
                find_sessions.spawn(find_session(
                    peer.clone(),
                    target,
                    count,
                    // minimum count is 2 to prevent returning single record which is this peer
                    buckets.find_closest(&target, count.max(MAX_CONCURRENCY + 1)),
                    submit_handle.clone(),
                    event,
                    transport.clone(),
                ));
            }
            Select::Submit(((target, id), result)) => {
                let (message, promise_message) = promise_channel();
                let evicted = find_peer_ok_events.insert((target, id), message);
                assert!(
                    evicted.is_none(),
                    "concurrent contactinng peer {} for target {}",
                    hex_string(&id),
                    hex_string(&target)
                );
                let timeout_event = timeout_event.clone();
                peer.spawner.spawn(async move {
                    match timeout(Duration::from_secs(1), promise_message).await {
                        Ok(message) => result.resolve(message?),
                        Err(_) => timeout_event.send((target, id)),
                    }
                });
            }
            Select::Message((remote, Message::FindPeer(message))) => {
                verify_find_peer_sessions.spawn(async move {
                    let mut message = message.deserialize::<FindPeer>()?;
                    // TODO tweak timeout
                    if message.instant.0.elapsed().unwrap_or_default() > Duration::from_secs(10) {
                        crate::bail!("expired")
                    }
                    message.verifier.clone().verify(&mut message)?;
                    Ok::<_, crate::Error>((remote, message.inner))
                });
            }
            Select::Message((remote, Message::FindPeerOk(message))) => {
                verify_find_peer_ok_sessions.spawn(async move {
                    let mut message = message.deserialize::<FindPeerOk>()?;
                    // TODO tweak timeout
                    if message.instant.0.elapsed().unwrap_or_default() > Duration::from_secs(1) {
                        crate::bail!("expired")
                    }
                    message.verifier.clone().verify(&mut message)?;
                    Ok::<_, crate::Error>((remote, message.inner))
                });
            }
            Select::VerifiedFindPeer(Err(_)) => {}
            Select::VerifiedFindPeer(Ok((remote, message))) => {
                let record = PeerRecord {
                    id: peer_id(&message.verifier)?,
                    verifier: message.verifier,
                    addr: remote.clone(),
                };
                buckets.insert(record);
                let find_peer_ok = FindPeerOk {
                    target: message.target,
                    closest: buckets.find_closest(&message.target, message.count),
                    verifier: peer.verifier,
                    instant: SystemTime(std::time::SystemTime::now()),
                };
                let signer = peer.signer;
                let transport = transport.clone();
                peer.spawner.spawn(async move {
                    transport
                        .send_to(
                            remote,
                            Message::FindPeerOk(signer.serialize_sign(find_peer_ok)?.into()),
                        )
                        .await?;
                    Ok(())
                });
            }
            Select::VerifiedFindPeerOk(Err(_)) => {}
            Select::VerifiedFindPeerOk(Ok((remote, message))) => {
                let record = PeerRecord {
                    id: peer_id(&message.verifier)?,
                    verifier: message.verifier,
                    addr: remote.clone(),
                };
                buckets.insert(record);
                if let Some(find_peer_ok) =
                    find_peer_ok_events.remove(&(message.target, peer_id(&message.verifier)?))
                {
                    find_peer_ok.resolve(message)?
                }
            }
            Select::Timeout((target, id)) => {
                // println!("timeout target {target:02x?} id {id:02x?}");
                find_peer_ok_events.remove(&(target, id)).unwrap();
                buckets.remove(&id);
            }
        }
    }
    assert!(find_peer_ok_events.is_empty());
    Ok(())
}

pub async fn bootstrap_session(
    peer: Arc<Peer>,
    buckets: &mut Buckets,
    message_source: &mut EventSource<(Addr, Message)>,
    transport: impl Transport<Message>,
) -> crate::Result<()> {
    let peer_id = peer_id(&peer.verifier)?;
    let (handle, source) = event_channel();
    let _event = handle.subscribe((peer_id, 20))?;
    drop(handle);
    session(
        peer.clone(),
        buckets,
        source,
        message_source,
        transport.clone(),
    )
    .await?;
    let (handle, source) = event_channel();
    peer.spawner.spawn(async move {
        let mut targets = HashSet::new();
        let mut events = Vec::new();
        for i in 0..U256::BITS - 1 {
            let d = (U256::from_le_bytes(rand::random()) | U256::ONE << (U256::BITS - 1)) >> i;
            let target = distance_from(&peer_id, d);
            // assert_eq!(buckets.index(&target), i as _);
            assert!(targets.insert(target));
            // let mut event = handle.subscribe((target, 20))?;
            // while event.option_next().await.is_some() {}
            let event = handle.subscribe((target, 20))?;
            events.push(event)
        }
        for mut event in events {
            while event.option_next().await.is_some() {}
        }
        Ok(())
    });
    session(peer, buckets, source, message_source, transport).await
}

fn hex_string(id: &Location) -> String {
    id.map(|n| format!("{n:02x}")).join("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_inversion() {
        let id = rand::random::<PeerId>();
        let d = U256::from_le_bytes(rand::random());
        assert_eq!(distance(&id, &distance_from(&id, d)), d);
    }

    fn ordered_closest() -> crate::Result<()> {
        let (secret_key, _) = secp256k1::generate_keypair(&mut rand::thread_rng());
        let center = PeerRecord::new(&Signer::from(secret_key), Addr::Untyped(Default::default()))?;
        let mut buckets = Buckets::new(center);
        for _ in 0..1000 {
            let (secret_key, _) = secp256k1::generate_keypair(&mut rand::thread_rng());
            buckets.insert(PeerRecord::new(
                &Signer::from(secret_key),
                Addr::Untyped(Default::default()),
            )?)
        }
        for _ in 0..1000 {
            let records = buckets.find_closest(&rand::random(), 20);
            assert_eq!(records.len(), 20)
        }
        Ok(())
    }

    #[test]
    fn ordered_closest_100() -> crate::Result<()> {
        for _ in 0..100 {
            ordered_closest()?
        }
        Ok(())
    }
}
