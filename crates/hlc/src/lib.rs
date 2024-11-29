//! An implementation of the
//! [Hybrid Logical Clock](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf)
//! for Rust.

use std::fmt::{self, Formatter, Display};
use std::sync::Mutex;
use std::time::{SystemTime, Duration, UNIX_EPOCH};

/// The `HLTimespec` type stores a hybrid logical timestamp (also called
/// timespec for symmetry with SystemTime).
///
/// Such a timestamp is comprised of an "ordinary" wall time and
/// a logical component. Timestamps are compared by wall time first,
/// logical second.
///
/// # Examples
///
/// ```
/// use hlc::HLTimespec;
/// let early = HLTimespec::new(1, 0, 0);
/// let middle = HLTimespec::new(1, 1, 0);
/// let late = HLTimespec::new(1, 1, 1);
/// assert!(early < middle && middle < late);
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub struct HLTimespec {
    wall: SystemTime,
    logical: u64,
}

impl HLTimespec {
    /// Creates a new hybrid logical timestamp with the given seconds,
    /// nanoseconds, and logical ticks.
    ///
    /// # Examples
    ///
    /// ```
    /// use hlc::HLTimespec;
    /// let ts = HLTimespec::new(1, 2, 3);
    /// assert_eq!(format!("{}", ts), "1.2+3");
    /// ```
    pub fn new(s: i64, ns: u32, l: u64) -> HLTimespec {
        let duration = Duration::new(s as u64, ns);
        let wall = UNIX_EPOCH + duration;  // start from unix epoch timestamp
        HLTimespec { wall, logical: l }
    }
}

impl Display for HLTimespec {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let duration_since_epoch = self.wall.duration_since(UNIX_EPOCH).map_err(|_| fmt::Error)?;
        let sec = duration_since_epoch.as_secs();
        let nsec = duration_since_epoch.subsec_nanos();

        write!(f, "{}.{:0>9}+{}", sec, nsec, self.logical)
    }
}

/// `State` is a hybrid logical clock.
///
/// # Examples
///
/// ```
/// use hlc::{HLTimespec, State};
/// let mut s = State::new();
/// println!("{}", s.get_time()); // attach to outgoing event
/// let ext_event_ts = HLTimespec::new(12345, 67, 89); // external event's timestamp
/// let ext_event_recv_ts = s.update(ext_event_ts);
/// ```
///
/// If access to the clock isn't serializable, a convenience method returns
/// a `State` wrapped in a `Mutex`:
///
/// ```
/// use hlc::State;
/// let mu = State::new_sendable();
/// {
///     let mut s = mu.lock().unwrap();
///     s.get_time();
/// }
/// ```
pub struct State<F> {
    s: HLTimespec,
    now: F,
}

impl State<()> {
    // Creates a standard hybrid logical clock, using `std::time::SystemTime` as
    // supplier of the physical clock's wall time.
    pub fn new() -> State<fn() -> SystemTime> {
        State::new_with(SystemTime::now)
    }

    // Returns the result of `State::new()`, wrapped in a `Mutex`.
    pub fn new_sendable() -> Mutex<State<fn() -> SystemTime>> {
        Mutex::new(State::new())
    }
}

impl<F: FnMut() -> SystemTime> State<F> {
    /// Creates a hybrid logical clock with the supplied wall time. This is
    /// useful for tests or settings in which an alternative clock is used.
    ///
    /// # Examples
    ///
    /// ```
    /// use hlc::{HLTimespec, State};
    /// let time = HLTimespec::new(0, 0, 0);
    /// let mut s = State::new_with(move || time.wall);
    /// let mut ts = s.get_time();
    /// assert_eq!(format!("{}", ts), "0.0+0");
    /// ```
    pub fn new_with(now: F) -> State<F> {
        State {
            s: HLTimespec {
                wall: UNIX_EPOCH,
                logical: 0,
            },
            now,
        }
    }

    /// Generates a timestamp from the clock.
    pub fn get_time(&mut self) -> HLTimespec {
        let s = &mut self.s;
        let wall = (self.now)();
        if s.wall < wall {
            s.wall = wall;
            s.logical = 0;
        } else {
            s.logical += 1;
        }
        s.clone()
    }

    /// Assigns a timestamp to an event which happened at the given timestamp
    /// on a remote system.
    pub fn update(&mut self, event: HLTimespec) -> HLTimespec {
        let (wall, s) = ((self.now)(), &mut self.s);

        if wall > event.wall && wall > s.wall {
            s.wall = wall;
            s.logical = 0
        } else if event.wall > s.wall {
            s.wall = event.wall;
            s.logical = event.logical + 1;
        } else if s.wall > event.wall {
            s.logical += 1;
        } else {
            if event.logical > s.logical {
                s.logical = event.logical;
            }
            s.logical += 1;
        }
        s.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{HLTimespec, State};

    fn hlts(s: i64, ns: u32, l: u64) -> HLTimespec {
        HLTimespec::new(s, ns, l)
    }

    #[test]
    fn it_works() {
        // Start with a reference time for tests
        let zero = hlts(0, 0, 0);
        let ops = vec![
            // Test cases in the form (wall, event_ts, outcome).
            (hlts(1, 0, 0), zero, hlts(1, 0, 0)),
            (hlts(1, 0, 0), zero, hlts(1, 0, 1)), // clock didn't move
            (hlts(0, 9, 0), zero, hlts(1, 0, 2)), // clock moved back
            (hlts(2, 0, 0), zero, hlts(2, 0, 0)), // finally ahead again
            (hlts(3, 0, 0), hlts(1, 2, 3), hlts(3, 0, 0)), // event happens, wall ahead
            (hlts(3, 0, 0), hlts(1, 2, 3), hlts(3, 0, 1)), // wall ahead but unchanged
            (hlts(3, 0, 0), hlts(3, 0, 1), hlts(3, 0, 2)), // event happens at wall
            (hlts(3, 0, 0), hlts(3, 0, 99), hlts(3, 0, 100)), // event with larger logical
            (hlts(3, 5, 0), hlts(4, 4, 100), hlts(4, 4, 101)), // event with larger wall
            (hlts(5, 0, 0), hlts(4, 5, 0), hlts(5, 0, 0)), // event behind wall
            (hlts(4, 9, 0), hlts(5, 0, 99), hlts(5, 0, 100)),
            (hlts(0, 0, 0), hlts(5, 0, 50), hlts(5, 0, 101)), // event at state
        ];

        // Prepare fake clock and create State.
        let mut times = ops.iter().rev().map(|op| op.0).collect::<Vec<HLTimespec>>();
        let mut s = State::new_with(move || {
            // Mock the time to return controlled values for testing
            times.pop().unwrap().wall
        });

        for op in &ops {
            let t = if op.1 == zero {
                s.get_time()
            } else {
                s.update(op.1)
            };
            assert_eq!(t, op.2);
        }
    }
}