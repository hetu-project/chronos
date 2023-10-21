use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::{
    crypto::{Sign, Signer},
    replication::{ClientIndex, ReplicaIndex},
    Receivers, To,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Addr {
    Replica(ReplicaIndex),
    Client(ClientIndex),
    //
}

#[derive(Debug, Clone)]
enum Event<M> {
    Message(Addr, Addr, M),
    LoopbackMessage(Addr, M),
    Timer(Addr),
}

pub type TimerId = u32;

#[derive(Debug)]
pub struct Context<M> {
    pub source: Addr,
    timeline: Arc<Mutex<Timeline<M>>>,
}

#[derive(Debug)]
struct Timeline<M> {
    now: Duration,
    id: u32,
    events: BTreeMap<(Duration, u32), Event<M>>,
    timers: HashMap<TimerId, Timer>,
}

#[derive(Debug)]
struct Timer {
    duration: Duration,
    key: (Duration, u32),
}

impl<M> Timeline<M> {
    fn add_event(&mut self, offset: Duration, event: Event<M>) {
        self.id += 1;
        let evicted = self.events.insert((self.now + offset, self.id), event);
        assert!(evicted.is_none())
    }

    fn add_timer_event(&mut self, offset: Duration, event: Event<M>) {
        self.add_event(offset, event);
        let evicted = self.timers.insert(
            self.id,
            Timer {
                duration: offset,
                key: (self.now + offset, self.id),
            },
        );
        assert!(evicted.is_none())
    }
}

impl<M> Context<M> {
    pub fn send<N>(&mut self, to: To, message: N)
    where
        M: Sign<N> + Clone,
    {
        let message = M::sign(message, &Signer::Simulated);
        let mut timeline = self.timeline.try_lock().unwrap();
        if matches!(to, To::Loopback | To::AddrsWithLoopback(_)) {
            timeline.add_event(
                Duration::ZERO,
                Event::LoopbackMessage(self.source, message.clone()),
            )
        }
        match to {
            To::Addr(addr) => {
                let crate::context::Addr::Simulated(addr) = addr else {
                    unimplemented!()
                };
                timeline.add_event(Duration::ZERO, Event::Message(addr, self.source, message))
            }
            To::Addrs(addrs) | To::AddrsWithLoopback(addrs) => {
                for addr in addrs {
                    let crate::context::Addr::Simulated(addr) = addr else {
                        unimplemented!()
                    };
                    assert_ne!(addr, self.source);
                    timeline.add_event(
                        Duration::ZERO,
                        Event::Message(addr, self.source, message.clone()),
                    )
                }
            }
            To::Loopback => {}
        }
    }

    pub fn set(&self, duration: Duration) -> TimerId {
        let mut timeline = self.timeline.try_lock().unwrap();
        timeline.add_timer_event(duration, Event::Timer(self.source));
        timeline.id
    }

    pub fn unset(&self, id: TimerId) {
        let mut timeline = self.timeline.try_lock().unwrap();
        let timer = timeline.timers.remove(&id).unwrap();
        timeline.events.remove(&timer.key).unwrap();
    }
}

#[derive(Debug)]
pub struct Dispatch<M> {
    timeline: Arc<Mutex<Timeline<M>>>,
}

impl<M> Dispatch<M> {
    pub fn register(&self, receiver: Addr) -> super::Context<M> {
        super::Context::Simulated(Context {
            source: receiver,
            timeline: self.timeline.clone(),
        })
    }

    pub fn deliver_event(&self, receivers: &mut impl Receivers<Message = M>) -> bool {
        let mut timeline = self.timeline.lock().unwrap();
        let Some(((now, id), event)) = timeline.events.pop_first() else {
            return false;
        };
        assert!(now >= timeline.now);
        timeline.now = now;
        use crate::context::Addr::Simulated;
        match event {
            Event::Message(receiver, remote, message) => {
                receivers.handle(Simulated(receiver), Simulated(remote), message)
            }
            Event::LoopbackMessage(receiver, message) => {
                receivers.handle_loopback(Simulated(receiver), message)
            }
            Event::Timer(receiver) => {
                let offset = timeline.timers[&id].duration;
                timeline.add_timer_event(offset, Event::Timer(receiver));
                receivers.on_timer(Simulated(receiver), crate::context::TimerId::Simulated(id))
            }
        }
        true
    }
}
