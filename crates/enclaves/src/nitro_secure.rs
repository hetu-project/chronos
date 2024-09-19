use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

/// HandleCallbackFn is running handler behind in vsock.
/// params: input_buf, nsm (nitro secure module), pcrs, write_sender(reply sender)
pub type HandleFn = Arc<
    dyn Fn(
            Vec<u8>,
            Arc<NitroSecureModule>,
            [Vec<u8>; 3],
            UnboundedSender<Vec<u8>>,
        ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>>
        + Send
        + Sync,
>;

#[derive(Debug)]
pub struct NitroSecureModule(pub i32);

#[cfg(feature = "nitro-enclaves")]
impl NitroSecureModule {
    fn new() -> anyhow::Result<Self> {
        let fd = aws_nitro_enclaves_nsm_api::driver::nsm_init();
        anyhow::ensure!(fd >= 0);
        Ok(Self(fd))
    }

    pub fn process_attestation(&self, user_data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        use aws_nitro_enclaves_nsm_api::api::Request::Attestation;
        // some silly code to avoid explicitly mention `serde_bytes::ByteBuf`
        let mut request = Attestation {
            user_data: Some(Default::default()),
            nonce: None,
            public_key: None,
        };
        let Attestation {
            user_data: Some(buf),
            ..
        } = &mut request
        else {
            unreachable!()
        };
        buf.extend(user_data);
        match aws_nitro_enclaves_nsm_api::driver::nsm_process_request(self.0, request) {
            aws_nitro_enclaves_nsm_api::api::Response::Attestation { document } => Ok(document),
            aws_nitro_enclaves_nsm_api::api::Response::Error(err) => anyhow::bail!("{err:?}"),
            _ => anyhow::bail!("unimplemented"),
        }
    }

    fn describe_pcr(&self, index: u16) -> anyhow::Result<Vec<u8>> {
        use aws_nitro_enclaves_nsm_api::api::Request::DescribePCR;
        match aws_nitro_enclaves_nsm_api::driver::nsm_process_request(self.0, DescribePCR { index })
        {
            aws_nitro_enclaves_nsm_api::api::Response::DescribePCR { lock: _, data } => Ok(data),
            aws_nitro_enclaves_nsm_api::api::Response::Error(err) => anyhow::bail!("{err:?}"),
            _ => anyhow::bail!("unimplemented"),
        }
    }

    pub async fn run(port: u32, handler: HandleFn) -> anyhow::Result<()> {
        use std::os::fd::AsRawFd;

        use nix::sys::socket::{
            bind, listen, socket, AddressFamily, Backlog, SockFlag, SockType, VsockAddr,
        };
        use tokio::{
            io::{AsyncReadExt as _, AsyncWriteExt as _},
            sync::mpsc::unbounded_channel,
        };

        let nsm = std::sync::Arc::new(Self::new()?);
        let pcrs = [
            nsm.describe_pcr(0)?,
            nsm.describe_pcr(1)?,
            nsm.describe_pcr(2)?,
        ];

        let socket_fd = socket(
            AddressFamily::Vsock,
            SockType::Stream,
            SockFlag::empty(),
            None,
        )?;
        bind(socket_fd.as_raw_fd(), &VsockAddr::new(0xFFFFFFFF, port))?;
        // theoretically this is the earliest point to entering Tokio world, but i don't want to go
        // unsafe with `FromRawFd`, and Tokio don't have a `From<OwnedFd>` yet
        listen(&socket_fd, Backlog::new(64)?)?;
        let socket = std::os::unix::net::UnixListener::from(socket_fd);
        socket.set_nonblocking(true)?;
        let socket = tokio::net::UnixListener::from_std(socket)?;

        loop {
            let (stream, _) = socket.accept().await?;
            let (mut read_half, mut write_half) = stream.into_split();
            let (write_sender, mut write_receiver) = unbounded_channel::<Vec<_>>();

            let mut write_session = tokio::spawn(async move {
                while let Some(buf) = write_receiver.recv().await {
                    write_half.write_u64_le(buf.len() as _).await?;
                    write_half.write_all(&buf).await?;
                }
                anyhow::Ok(())
            });
            let nsm = nsm.clone();
            let pcrs = pcrs.clone();
            let handler = handler.clone(); 
            let mut read_session = tokio::spawn(async move {
                loop {
                    let task = async {
                        let len = read_half.read_u64_le().await?;
                        let mut buf = vec![0; len as _];
                        read_half.read_exact(&mut buf).await?;
                        anyhow::Ok(buf)
                    };
                    let buf = match task.await {
                        Ok(buf) => buf,
                        Err(err) => {
                            warn!("{err}");
                            return anyhow::Ok(());
                        }
                    };
                    let nsm_clone = nsm.clone();
                    let pcrs_clone = pcrs.clone();
                    let write_sender = write_sender.clone();
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handler(buf, nsm_clone, pcrs_clone, write_sender).await {
                            eprintln!("Error: {:?}", err);
                        }
                    });
                }
            });
            loop {
                let result = tokio::select! {
                    result = &mut read_session, if !read_session.is_finished() => result,
                    result = &mut write_session, if !write_session.is_finished() => result,
                    else => break,
                };
                if let Err(err) = result.map_err(Into::into).and_then(std::convert::identity) {
                    warn!("{err}")
                }
            }
        }
    }
}

#[cfg(feature = "nitro-enclaves")]
impl Drop for NitroSecureModule {
    fn drop(&mut self) {
        aws_nitro_enclaves_nsm_api::driver::nsm_exit(self.0)
    }
}
