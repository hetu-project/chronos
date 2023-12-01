## Modeling Concurrent Stateful Applications with Sessions

> Move to dedicated document if desired.

Stateful applications are usually implemented as event handlers that are driven by a tight event loop. When demanding concurrency, such model only permits detached concurrent tasks because there's no way to customize event loop to receive notification of concurrent tasks finish, which is undesirable.

Concurrent applications are usually implemented as stateless, *microservice* style tasks, which incurs error-prone and hard-to-tweak locking issues when dealing with shared mutable states.

This codebase tries to develop a programming pattern that is suitable for concurrent stateful applications. Code snippets are grouped into *sessions*, which contain logic that connected with causal dependencies. Code in different sessions are causally independent to each other, thus share no state and can be concurrently executed. Each session is an asynchronous task (or coroutine) that drives its own event loop. A session has full control of what to be expected from event loop and even when to block on receiving events.

Besides efficiently express concurrency, one bonus goal is to have *streamlined* code. Code skeleton for sessions:

```rust
async fn a_session(
    // immutable arguments
    // message channels and transportations
) -> crate::Result<()> {
    // declare mutable states

    // everything here happens sequentially, the code comes later executes later
}

async fn b_session() {
    // everything here is concurrent to everything in `a_session`
}

// example of complicated sessions which may take too many arguments as a 
// function
struct CState {
    // immutable arguments
    // message channels and transportations
    // mutable states
}

impl CState {
    pub fn new(
        // immutable arguments
        // message channels and transportations
    ) -> Self {
        Self {
            // move arguments
            // initialize mutable states
        }
    }

    pub async fn session(&mut self) {
        // similar to above
    }
}
```

**Common patterns of sessions.** A session run may contain arbitrary behaviors. However, the following patterns commonly appear as far as have seen, and the codebase provides several helpers for them.

Collector sessions. This is the most common and recommended pattern. These sessions collect information by receiving messages from (potentially multiple) channels and joining results from other sessions, until "reduce" the information into returning result. The sessions may be multistaged, collecting information from difference sources in different stages, and unnecessary to be in "one big event loop" style. These sessions determine their termination base on their local information, so do not require a shutdown signal input.

Generally these are the sessions with streaming input and one-shot output. Examples of the tasks of these sessions:

* (Verify and) collect quorum certificates
* Collect client requests and a produce block/ordering batch
* Transaction workload that issues one or more requests through a client sequentially. The workload may also verify intermediate results and conditionally issue requests base on results of previous requests
* Close-loop benchmark that repeatedly joins the above workload for certain duration

Listener sessions. This is the opposite of the previous pattern, and these are sessions with one-shot input and streaming output. The example of these sessions is network socket listeners. These sessions either loop forever and take an explicit shutdown signal on spawning, or they may shut down when downstream consumer is gone.

Server sessions. This is a variant of collector session, with only a slight difference that these sessions usually run forever by default and need to take a shutdown signal. (A simpler solution is to send the shutdown signal by closing network source channel.) Everything else is the same - these sessions can be viewed as collect network messages and return trivial results (or some metrics information). Just the major part of these sessions usually (have to) become a large event loop.

Service sessions. These are sessions that take streaming input and produce *corresponded* streaming output. Every output is replying one of the input. If the output and input are not correlated, it is possible that the session is incorrectly abstracted, and the design needs to be revised. The shutdown condition of these sessions are simply the closing of input source.

As their name suggests, service sessions provide RPC-like service to other local sessions. It is generally suggested to abstract logic into one of the patterns above, but in certain cases the service pattern may be the only feasible way, for example:

* Input/output type based polymorphism, inheriting the spirit of actor model
* Potentially concurrent invocation into the same mutable state
* Invoking into the same mutable state from multiple sessions, but none of them can own the state because of e.g. lifetime mismatch

Any single one of the above may have solutions e.g. `async_trait` for the first one, but any combination will make it much harder.

The examples of service sessions include replicated applications and protocol clients. It is tempting to implement protocol clients as "session generators", which seems to offer finer-grained control and be more elegant (which is always the most important concern obviously). They are finally decided to be service sessions mainly because there are still occasional needs of persist states across client invocations (e.g. last view number for PBFT), and the fact of both applications and clients are service sessions potentially results in a more composable system.

**Best practice of designing sessions.** (This section may evolve over time as I keep practicing writing sessions.) Some general steps to follow:

* Start with a stateful, single event loop structure
* Determine potential source of concurrency, i.e. the part of code whose result/effect is not needed to start the next iteration of event loop
* Categorize the code snippet into one of the session patterns
* Connect the sub-session back with channels and join handles

Take a simple RPC server implementing at-most-once semantic as an example. Starting with one single event loop:

```rust
fn server_session() -> crate::Result<()> {
    loop {
        // TODO
    }
}
```

Add event source for incoming network messages, and accept a service session handle as upper layer application:

```rust
fn server_session(
    // EventSource for incoming network messages
    // SubmitHandle for upper layer application
) -> crate::Result<()> {
    loop {
        // await on next network message
        // preprocess message e.g. deduplicated
        // await on invoking submit handle and wrap result into reply message
        // save a copy of reply for following preprocessing and response to client
    }
}

```

The session already looks good. But if you are willing to trade some simplicity for concurrency, notice the third line on loop body can be concurrent to the next iteration. However, the forth line depends on its result and cannot be detached out from the looping, so the best we can do is to a spawn-join pair on a collector session in its simplest form:

```rust

fn server_session(
    // EventSource for incoming network messages
    // SubmitHandle for upper layer application
) -> crate::Result<()> {
    // initialize join set for reply sessions
    loop {
        // select
        //   next network message =>
        //     preprocess message e.g. deduplicated
        //     spawn a reply session into join set which:
        //       invoke submit handle and wrap result into reply message
        //   next joined reply session from join set =>
        //     save a copy of reply for following preprocessing and response to client
    }
}
```

With careful analysis, the modified version actually has slightly different semantic: the saved copies of replies are more stalled than the original version. The two versions are equivalent if the clients work in close loops.

It's unnecessary to force UNIX "one thing at a time" style, KISS-style, small scale modular composable sessions, as long as concurrency is sufficiently developed. Sometimes the logic is monolithic by nature, and there's little thing we can do about it.

**The implementation of (supporting) sessions.** Session is an abstraction that requires only few things other than language's built-in asynchronous features, and this codebase relies on Tokio for those left things. Most essentially, concurrent primitives `spawn` and `select!` are deeply baked into sessions logic, kind of like extensions of the Rust language. Other than this, asynchronous channels are massively used with trivial wrapping for error handling. The background task management mimics `JoinSet`, but implemented with channels to enable sharing. The network functions and time utilities are also used, but only as normal dependent library.
