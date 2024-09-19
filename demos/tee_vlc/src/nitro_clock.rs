use std::{sync::Arc, time::Duration};
use bincode::Options;
use types::raw_wrapper::Payload;
use crypto::core::DigestHash;
use enclaves::nitro_secure::{HandleFn, NitroSecureModule as NitroSecure};
use vlc::ordinary_clock::{Clock, LamportClock, OrdinaryClock};
use derive_where::derive_where;
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc::{UnboundedReceiver, UnboundedSender}, time::Instant};
use tracing::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct Update<C>(pub C, pub Vec<C>, pub u64);

// feel lazy to define event type for replying
pub type UpdateOk<C> = (u64, C, Vec<Duration>);

#[derive(Debug, Clone, Default, derive_more::AsRef, Serialize, Deserialize)]
#[derive_where(PartialOrd, PartialEq)]
pub struct NitroEnclavesClock {
    #[as_ref]
    pub plain: OrdinaryClock,
    #[derive_where(skip)]
    pub document: Payload,
}

impl TryFrom<OrdinaryClock> for NitroEnclavesClock {
    type Error = anyhow::Error;

    fn try_from(value: OrdinaryClock) -> Result<Self, Self::Error> {
        anyhow::ensure!(value.is_genesis());
        Ok(Self {
            plain: value,
            document: Default::default(),
        })
    }
}

impl Clock for NitroEnclavesClock {
    fn reduce(&self) -> LamportClock {
        self.plain.reduce()
    }
}

// technically `feature = "aws-nitro-enclaves-attestation"` is sufficient for
// attestation, NSM API is only depended by `NitroSecureModule` that running
// inside enclaves image
#[cfg(feature = "nitro-enclaves")]
impl NitroEnclavesClock {
    pub fn verify(
        &self,
    ) -> anyhow::Result<Option<aws_nitro_enclaves_nsm_api::api::AttestationDoc>> {
        if self.plain.is_genesis() {
            return Ok(None);
        }
        use aws_nitro_enclaves_attestation::{AttestationProcess as _, AWS_ROOT_CERT};
        use aws_nitro_enclaves_nsm_api::api::AttestationDoc;
        let document = AttestationDoc::from_bytes(
            &self.document,
            AWS_ROOT_CERT,
            std::time::SystemTime::UNIX_EPOCH
                .elapsed()
                .unwrap()
                .as_secs(),
        )?;
        use DigestHash as _;
        anyhow::ensure!(
            document.user_data.as_ref().map(|user_data| &***user_data)
                == Some(&self.plain.sha256().to_fixed_bytes()[..])
        );
        Ok(Some(document))
    }

    pub fn worker() -> HandleFn {
        Arc::new(|buf, nsm, pcrs, write_sender| {
            Box::pin(async move {
                // IO action in tee is severe delay, just debug
                // println!("Received buffer: {:?}", buf);
                // let _ = io::stdout().flush();
               
                // if production env, need to remove time slot log
                let mut timers = Vec::new();
                if let Err(err) = async {
                    // 0. once action time
                    let full_start = Instant::now();

                    // 1. decode time
                    let start = Instant::now();
                    let Update(prev, merged, id) = bincode::options()
                        .deserialize::<Update<NitroEnclavesClock>>(&buf)?;                  
                    
                    let elapsed = start.elapsed();
                    timers.push(elapsed);
                    // println!("bincode deserialize: {:?}", elapsed);
                    // let _ = io::stdout().flush();
                    
                    // 2. verify clocks time
                    let start = Instant::now();
                    for clock in [&prev].into_iter().chain(&merged) {
                        if let Some(document) = clock.verify()? {
                            for (i, pcr) in pcrs.iter().enumerate() {
                                anyhow::ensure!(
                                    document.pcrs.get(&i).map(|pcr| &**pcr) == Some(pcr)
                                )
                            }
                        }
                    }

                    let elapsed = start.elapsed();
                    timers.push(elapsed);
                    // println!("verify clock: {:?}", elapsed);
                    // let _ = io::stdout().flush();

                    // 3. update clock time
                    let start = Instant::now();
                    let plain = prev
                        .plain
                        .update(merged.iter().map(|clock| &clock.plain), id);
                    
                    let elapsed = start.elapsed();
                    timers.push(elapsed);
                    // println!("Update clock: {:?}", elapsed);
                    // let _ = io::stdout().flush();
                    
                    // 4. gen clock with proof time
                    let start = Instant::now();
                    // let key_lens = plain.0.len();
                    // relies on the fact that different clocks always hash into different
                    // digests, hopefully true
                    let user_data = plain.sha256().to_fixed_bytes().to_vec();
                    let document = nsm.process_attestation(user_data)?;
                    let updated = NitroEnclavesClock {
                        plain,
                        document: Payload(document),
                    };

                    let elapsed = start.elapsed();
                    timers.push(elapsed);
                    // println!("Gen clock proof: {:?}", elapsed);
                    // let _ = io::stdout().flush();

                    let elapsed = full_start.elapsed();
                    timers.push(elapsed);
                    // println!("Total once time: {:?}, key is {:?}", elapsed, key_lens);
                    // let _ = io::stdout().flush();
                    
                    let buf = bincode::options().serialize(&(id, updated, timers))?;
                    write_sender.send(buf)?;
                    Ok(())
                }
                .await
                {
                    warn!("{err}")
                }
                Ok(())
            })
        })
    }

    pub async fn run(port: u32) -> anyhow::Result<()> {
        let handler: HandleFn = NitroEnclavesClock::worker();

        NitroSecure::run(port, handler).await
    }
}


pub async fn nitro_enclaves_portal_session(
    cid: u32,
    port: u32,
    mut events: UnboundedReceiver<Update<NitroEnclavesClock>>,
    sender: UnboundedSender<UpdateOk<NitroEnclavesClock>>,
) -> anyhow::Result<()> {
    use std::os::fd::AsRawFd;

    use bincode::Options;
    use nix::sys::socket::{connect, socket, AddressFamily, SockFlag, SockType, VsockAddr};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let fd = socket(
        AddressFamily::Vsock,
        SockType::Stream,
        SockFlag::empty(),
        None,
    )?;
    // this one is blocking, but should be instant, hopefully
    {
        let _span = tracing::debug_span!("connect").entered();
        connect(fd.as_raw_fd(), &VsockAddr::new(cid, port))?
    }
    let stream = std::os::unix::net::UnixStream::from(fd);
    stream.set_nonblocking(true)?;
    let stream = tokio::net::UnixStream::from_std(stream)?;
    let (mut read_half, mut write_half) = stream.into_split();
    let write_session = tokio::spawn(async move {
        while let Some(update) = events.recv().await {
            let buf = bincode::options().serialize(&update)?;
            write_half.write_u64_le(buf.len() as _).await?;
            write_half.write_all(&buf).await?
        }
        anyhow::Ok(())
    });
    let read_session = tokio::spawn(async move {
        loop {
            let len = read_half.read_u64_le().await?;
            let mut buf = vec![0; len as _];
            read_half.read_exact(&mut buf).await?;
            sender.send(bincode::options().deserialize(&buf)?)?
        }
        #[allow(unreachable_code)] // for type hinting
        anyhow::Ok(())
    });
    tokio::select! {
        result = write_session => return result?,
        result = read_session => result??
    }
    anyhow::bail!("unreachable")
}

#[cfg(feature = "nitro-enclaves")]
pub mod impls {

    use super::NitroEnclavesClock;
    use crate::{Clocked, Verify};

    impl<M: Send + Sync + 'static> Verify<()> for Clocked<M, NitroEnclavesClock> {
        fn verify_clock(&self, _: usize, (): &()) -> anyhow::Result<()> {
            self.clock.verify()?;
            Ok(())
        }
    }
}

pub fn try_connection(cid: u32, port: u32) -> anyhow::Result<tokio::net::UnixStream> {
    use nix::sys::socket::{connect, socket, AddressFamily, SockFlag, SockType, VsockAddr};
    use std::os::fd::AsRawFd;

    let fd = socket(
        AddressFamily::Vsock,
        SockType::Stream,
        SockFlag::empty(),
        None,
    )?;

    {
        let _span = tracing::debug_span!("connect").entered();
        connect(fd.as_raw_fd(), &VsockAddr::new(cid, port))?
    }

    let stream = std::os::unix::net::UnixStream::from(fd);
    stream.set_nonblocking(true)?;

    let stream = tokio::net::UnixStream::from_std(stream)?;
    Ok(stream)
}

pub async fn tee_start_listening(
    stream: tokio::net::UnixStream,
    mut events: UnboundedReceiver<Update<NitroEnclavesClock>>,
    sender: UnboundedSender<UpdateOk<NitroEnclavesClock>>,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let (mut read_half, mut write_half) = stream.into_split();

    let write_session = tokio::spawn(async move {
        while let Some(prompt) = events.recv().await {
            let buf = bincode::options().serialize(&prompt)?;
            write_half.write_u64_le(buf.len() as _).await?;
            write_half.write_all(&buf).await?;
        }
        anyhow::Ok(())
    });

    let read_session = tokio::spawn(async move {
        loop {
            let len = read_half.read_u64_le().await?;
            let mut buf = vec![0; len as _];
            read_half.read_exact(&mut buf).await?;
            sender.send(bincode::options().deserialize(&buf)?)?
        }
        #[allow(unreachable_code)] // for type hinting
        anyhow::Ok(())
    });

    tokio::select! {
        result = write_session => return result?,
        result = read_session => result??
    }

    anyhow::bail!("unreachable")
}
