use std::future::Future;

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

use crate::channel;

/// Safer alternative to raw `tokio::spawn`.
///
/// It can be used to spawn the set of *background tasks* which effectively
/// "abort the program if anything goes wrong", that is
/// * They are possible to go wrong (so unwrapping does not fit)
/// * Besides going wrong they only have trivial result (either `()` or `!`), so
///   not bother to join them manually that for propogating the error
/// * If they goes wrong there's no way or no meaning to recover, that is, we
///   would not bother to repair the system after any of them goes wrong,
///   instead we just want to make sure the capture the error reliably and fail
///   the whole system as a whole
///
/// Candidates for background tasks includes socket listener, message sender,
/// tasks with forever loop blocking on `SubmitSource`, etc. It is advised to
/// spawn these tasks with monitoring their status instead of just detaching
/// them, to make the system more predicatable. The model here is similar to
/// spawn the tasks into a `JoinSet`, just this can be shared acorss multiple
/// users and waiter.
///
/// Background tasks should bring their own shutdown policy. It is expected that
/// all background tasks shut themselves down under certain circumstance, so we
/// can confirm that they actually work well during lifetime. Remember to drop
/// unused `BackgroundSpawner`, or it may block monitor's `wait` call.
#[derive(Debug, Clone)]
pub struct BackgroundSpawner {
    err_sender: UnboundedSender<crate::Error>,
}

impl BackgroundSpawner {
    pub fn spawn(
        &self,
        task: impl Future<Output = crate::Result<()>> + Send + 'static,
    ) -> JoinHandle<()> {
        let err_sender = self.err_sender.clone();
        let task = tokio::spawn(task);
        tokio::spawn(async move {
            let err = match task.await {
                Err(err) => err.into(),
                Ok(Err(err)) => err,
                _ => return,
            };
            if err_sender.send(err).is_err() {
                // eprintln!("error channel closed")
            }
        })
    }
}

#[derive(Debug)]
pub struct BackgroundMonitor {
    err_sender: UnboundedSender<crate::Error>,
    err_receiver: UnboundedReceiver<crate::Error>,
    watching: Watching,
}

#[derive(Debug)]
struct Watching(bool);

impl Default for BackgroundMonitor {
    fn default() -> Self {
        let (err_sender, err_receiver) = unbounded_channel();
        Self {
            err_sender,
            err_receiver,
            watching: Watching(true),
        }
    }
}

impl BackgroundMonitor {
    pub fn spawner(&self) -> BackgroundSpawner {
        BackgroundSpawner {
            err_sender: self.err_sender.clone(),
        }
    }

    pub async fn wait(mut self) -> crate::Result<()> {
        drop(self.err_sender);
        self.watching.0 = false;
        let mut channel_err = None;
        while let Some(err) = self.err_receiver.recv().await {
            if err.is::<channel::Error>() {
                channel_err = Some(err);
                continue;
            }
            if err.is::<tokio::sync::oneshot::error::RecvError>() {
                channel_err = Some(err);
                continue;
            }
            Err(err)?
        }
        if let Some(err) = channel_err {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            eprintln!("postponed reporting channel error");
            Err(err)
        } else {
            Ok(())
        }
    }

    pub async fn wait_task<T>(&mut self, task: impl Future<Output = T>) -> crate::Result<T> {
        let err = tokio::select! {
            result = task => return Ok(result),
            err = self.err_receiver.recv() => err.expect("error channel opens"),
        };
        self.watching.0 = false;
        Err(err)
    }

    pub fn start(
        on_err: impl FnOnce(crate::Error) + Send + 'static,
    ) -> (JoinHandle<()>, BackgroundSpawner) {
        let monitor = Self::default();
        let spawner = monitor.spawner();
        let handle = tokio::spawn(async move {
            if let Err(err) = monitor.wait().await {
                on_err(err)
            }
        });
        (handle, spawner)
    }
}

impl Drop for Watching {
    fn drop(&mut self) {
        assert!(!self.0, "dropping backgrond monitor when still watching")
    }
}
