use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{
    channel::EventSender,
    transport::{Addr, Message},
    Transport,
};

#[derive(Debug, Clone)]
pub struct UdpSocket(pub Arc<tokio::net::UdpSocket>);

impl UdpSocket {
    pub async fn bind(addr: Addr) -> crate::Result<Self> {
        let Addr::Socket(addr) = addr else {
            crate::bail!("unsupported {addr:?}")
        };
        Ok(Self(Arc::new(tokio::net::UdpSocket::bind(addr).await?)))
    }

    pub async fn listen_session<M>(self, event: EventSender<(Addr, M)>) -> crate::Result<()>
    where
        M: BorshDeserialize + Send + 'static,
    {
        let mut buf = vec![0; 65536];
        loop {
            let (len, remote) = tokio::select! {
                recv_from = self.0.recv_from(&mut buf) => recv_from?,
                () = event.0.closed() => break Ok(()),
            };
            let send = event.send((Addr::Socket(remote), borsh::from_slice::<M>(&buf[..len])?));
            if send.is_err() {
                eprintln!("listener closed channel {self:?}")
            }
        }
    }
}

impl<M> Transport<M> for UdpSocket
where
    M: BorshSerialize,
{
    fn addr(&self) -> Addr {
        Addr::Socket(self.0.local_addr().expect("retrievable local address"))
    }

    async fn send_to(&self, destination: Addr, message: M) -> crate::Result<()>
    where
        M: Message,
    {
        let Addr::Socket(destination) = destination else {
            crate::bail!("unsupported destination kind {destination:?}")
        };
        let buf = borsh::to_vec(&message)?;
        self.0.send_to(&buf, destination).await?;
        Ok(())
    }

    async fn send_to_all(
        &self,
        destinations: impl Iterator<Item = Addr> + Send,
        message: M,
    ) -> crate::Result<()>
    where
        M: Message,
    {
        let buf = borsh::to_vec(&message)?;
        for destination in destinations {
            let Addr::Socket(destination) = destination else {
                crate::bail!("unsupported destination kind {destination:?}")
            };
            self.0.send_to(&buf, destination).await?;
        }
        Ok(())
    }
}
