use std::{env::args, future::pending};

use neat::{
    context::{
        crypto::{Signer, Verifier, Verify},
        ordered_multicast::Variant,
        tokio::Dispatch,
        Addr, Receivers, To,
    },
    Context,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Subnode(SubnodeMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SubnodeMessage {
    Ping,
    Pong,

    Hello(Hello),
    HelloOk,
}

impl From<SubnodeMessage> for Message {
    fn from(value: SubnodeMessage) -> Self {
        Self::Subnode(value)
    }
}

impl Verify<()> for Message {
    fn verify(&self, _: &Verifier<()>) -> Result<(), neat::context::crypto::Invalid> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Hello {
    ping_addr: Addr,
}

struct Node {
    context: Context<Message>,
    subnode: Subnode,
}

struct Subnode {
    context: Context<SubnodeMessage>,
    client_addr: Option<Addr>,
}

impl Node {
    fn hello(&mut self, addr: Addr) {
        self.subnode.handle(
            self.context.addr(),
            Addr::Upcall,
            SubnodeMessage::Hello(Hello { ping_addr: addr }),
        )
    }
}

impl Receivers for Node {
    type Message = Message;

    fn handle(&mut self, receiver: Addr, remote: Addr, message: Self::Message) {
        println!("{message:?}");
        match message {
            Message::Subnode(message) => self.subnode.handle(receiver, remote, message),
        }
    }

    fn handle_loopback(&mut self, _: Addr, message: Self::Message) {
        println!("{message:?}");
        let Message::Subnode(SubnodeMessage::HelloOk) = message else {
            unreachable!()
        };
    }

    fn on_timer(&mut self, receiver: Addr, id: neat::context::TimerId) {
        self.subnode.on_timer(receiver, id)
    }
}

impl Receivers for Subnode {
    type Message = SubnodeMessage;

    fn handle(&mut self, _: Addr, remote: Addr, message: Self::Message) {
        println!("{message:?}");
        match message {
            SubnodeMessage::Hello(message) => {
                self.client_addr = Some(remote);
                self.context
                    .send(To::Addr(message.ping_addr), SubnodeMessage::Ping)
            }
            SubnodeMessage::Ping => self.context.send(To::Addr(remote), SubnodeMessage::Pong),
            SubnodeMessage::Pong => self
                .context
                .send(To::Addr(self.client_addr.unwrap()), SubnodeMessage::HelloOk),
            _ => unimplemented!(),
        }
    }

    fn on_timer(&mut self, _: Addr, _: neat::context::TimerId) {
        unreachable!()
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let runtime_handle = runtime.handle().clone();
    std::thread::spawn(move || runtime.block_on(pending::<()>()));
    let dispatch = Dispatch::new(runtime_handle, Variant::Unreachable);
    let context = dispatch.register::<Message>(
        Addr::Socket(args().nth(1).unwrap().parse().unwrap()),
        Signer::Simulated,
    );
    let mut node = Node {
        subnode: Subnode {
            context: context.subnode(),
            client_addr: None,
        },
        context,
    };
    if let Some("hello") = args().nth(2).as_deref() {
        node.hello(Addr::Socket(args().nth(3).unwrap().parse().unwrap()))
    }
    dispatch.run(&mut node, Verifier::Nop)
}
