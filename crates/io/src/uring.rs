//! The io_uring backend.
//!
//! One ring, one outstanding accept, one outstanding read per connection and
//! one write per submission. `wait` arms a `Timeout` completion so the
//! gateway's tick (heartbeats, cluster egress polling) still runs on an idle
//! cluster.
//!
//! Operations are identified by `user_data`: the high bit marks a timeout,
//! the low four bits are the operation kind, and the bits between are the
//! connection id.
//!
//! # Safety
//!
//! Two unsafe operations live here: `OwnedFd::from_raw_fd` for a descriptor
//! the kernel just accepted (we are the only owner), and `mem::zeroed` for
//! the `sockaddr_storage` the kernel writes into. The read and write buffers
//! stay in the connection map until their completion arrives, so the kernel's
//! raw pointers remain valid.

use std::collections::HashMap;
use std::io;
use std::mem::size_of;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::Duration;

use io_uring::{IoUring, opcode, types};

use crate::{Event, EventLoop, READ_BUFFER};

const RING_ENTRIES: u32 = 256;
const TIMEOUT_BIT: u64 = 1 << 63;
const KIND_MASK: u64 = 0x0f;
const KIND_ACCEPT: u64 = 0;
const KIND_READ: u64 = 1;
const KIND_WRITE: u64 = 2;
const KIND_TIMEOUT: u64 = 3;
const KIND_TIMEOUT_REMOVE: u64 = 4;

struct UringConnection {
    stream: TcpStream,
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    read_inflight: bool,
    write_inflight: bool,
    closing: bool,
}

/// `io_uring`: real completion operations.
pub struct UringLoop {
    ring: IoUring,
    listener: TcpListener,
    connections: HashMap<u64, UringConnection>,
    next_id: u64,
    accept_inflight: bool,
    accept_address: Box<libc::sockaddr_storage>,
    accept_length: libc::socklen_t,
    timeout_spec: Box<types::Timespec>,
    timeout_user: Option<u64>,
    timeout_generation: u64,
}

impl UringLoop {
    /// Bind `addr` and create the ring.
    ///
    /// # Errors
    ///
    /// The address cannot be bound, or io_uring is unavailable (for example
    /// `kernel.io_uring_disabled`).
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let ring = IoUring::new(RING_ENTRIES)?;
        // SAFETY: the storage is plain old data filled by the kernel before
        // the accept completion; zeroing it is a valid initial value.
        let accept_address = Box::new(unsafe { std::mem::zeroed::<libc::sockaddr_storage>() });
        let accept_length = libc::socklen_t::try_from(size_of::<libc::sockaddr_storage>())
            .unwrap_or(libc::socklen_t::MAX);
        Ok(Self {
            ring,
            listener,
            connections: HashMap::new(),
            next_id: 1,
            accept_inflight: false,
            accept_address,
            accept_length,
            timeout_spec: Box::new(types::Timespec::new()),
            timeout_user: None,
            timeout_generation: 0,
        })
    }

    fn push(&mut self, entry: io_uring::squeue::Entry) -> io::Result<()> {
        // SAFETY: `entry` is a fully initialised SQE. Every buffer it points
        // at is owned by this loop and lives until its completion: the read
        // and write buffers are in `connections`, the accept address and
        // timeout spec are boxed fields, and the listener outlives the ring.
        let pushed = unsafe { self.ring.submission().push(&entry) };
        if pushed.is_err() {
            // The local SQ is full; submit what is there and try once more.
            self.ring.submit()?;
            // SAFETY: as above; the entry was not consumed by the failed push.
            unsafe { self.ring.submission().push(&entry) }
                .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "submission queue full"))?;
        }
        Ok(())
    }

    fn arm_accept(&mut self) -> io::Result<()> {
        if self.accept_inflight {
            return Ok(());
        }
        self.accept_length = libc::socklen_t::try_from(size_of::<libc::sockaddr_storage>())
            .unwrap_or(libc::socklen_t::MAX);
        let address =
            &mut *self.accept_address as *mut libc::sockaddr_storage as *mut libc::sockaddr;
        let length = &mut self.accept_length as *mut libc::socklen_t;
        let entry = opcode::Accept::new(types::Fd(self.listener.as_raw_fd()), address, length)
            .build()
            .user_data(KIND_ACCEPT);
        self.push(entry)?;
        self.accept_inflight = true;
        Ok(())
    }

    fn arm_read(&mut self, id: u64) -> io::Result<()> {
        let Some(connection) = self.connections.get_mut(&id) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no such connection",
            ));
        };
        if connection.read_inflight || connection.closing {
            return Ok(());
        }
        let entry = opcode::Read::new(
            types::Fd(connection.stream.as_raw_fd()),
            connection.read_buffer.as_mut_ptr(),
            u32::try_from(READ_BUFFER).unwrap_or(u32::MAX),
        )
        .build()
        .user_data(operation(id, KIND_READ));
        connection.read_inflight = true;
        self.push(entry)
    }

    fn on_completion(&mut self, user_data: u64, result: i32, events: &mut Vec<Event>) {
        if user_data & TIMEOUT_BIT != 0 {
            if user_data & KIND_MASK == KIND_TIMEOUT && self.timeout_user == Some(user_data) {
                self.timeout_user = None;
            }
            return;
        }
        let id = user_data >> 4;
        match user_data & KIND_MASK {
            KIND_ACCEPT => {
                self.accept_inflight = false;
                if result < 0 {
                    events.push(Event::AcceptFailed(io::Error::from_raw_os_error(-result)));
                    return;
                }
                // SAFETY: `accept` returned a fresh descriptor whose ownership
                // has not been given to anyone else. `OwnedFd` is the only
                // owner from here on, and `TcpStream::from` takes it over.
                let owned = unsafe { OwnedFd::from_raw_fd(result) };
                let stream = TcpStream::from(owned);
                if let Err(error) = stream.set_nonblocking(true) {
                    events.push(Event::AcceptFailed(error));
                    return;
                }
                stream.set_nodelay(true).ok();
                let peer = stream
                    .peer_addr()
                    .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
                let id = self.next_id;
                self.next_id += 1;
                self.connections.insert(
                    id,
                    UringConnection {
                        stream,
                        read_buffer: vec![0_u8; READ_BUFFER],
                        write_buffer: Vec::new(),
                        read_inflight: false,
                        write_inflight: false,
                        closing: false,
                    },
                );
                events.push(Event::Accepted { id, peer });
            }
            KIND_READ => {
                if let Some(connection) = self.connections.get_mut(&id) {
                    connection.read_inflight = false;
                }
                events.push(Event::Read {
                    id,
                    result: completion_result(result),
                });
            }
            KIND_WRITE => {
                if let Some(connection) = self.connections.get_mut(&id) {
                    connection.write_inflight = false;
                }
                events.push(Event::Written {
                    id,
                    result: completion_result(result),
                });
            }
            _ => {}
        }
    }

    fn cleanup(&mut self) {
        self.connections.retain(|_, connection| {
            !(connection.closing && !connection.read_inflight && !connection.write_inflight)
        });
    }
}

fn operation(id: u64, kind: u64) -> u64 {
    (id << 4) | kind
}

fn completion_result(result: i32) -> io::Result<usize> {
    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(usize::try_from(result).unwrap_or(usize::MAX))
    }
}

impl EventLoop for UringLoop {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    fn start_accept(&mut self) -> io::Result<()> {
        self.arm_accept()
    }

    fn start_read(&mut self, id: u64) -> io::Result<()> {
        self.arm_read(id)
    }

    fn submit_write(&mut self, id: u64, bytes: &[u8]) -> io::Result<()> {
        let Some(connection) = self.connections.get_mut(&id) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no such connection",
            ));
        };
        if connection.write_inflight {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "a write is already in flight",
            ));
        }
        connection.write_buffer.clear();
        connection.write_buffer.extend_from_slice(bytes);
        let length = u32::try_from(connection.write_buffer.len()).unwrap_or(u32::MAX);
        let entry = opcode::Write::new(
            types::Fd(connection.stream.as_raw_fd()),
            connection.write_buffer.as_ptr(),
            length,
        )
        .build()
        .user_data(operation(id, KIND_WRITE));
        connection.write_inflight = true;
        self.push(entry)
    }

    fn read_data(&self, id: u64) -> &[u8] {
        self.connections
            .get(&id)
            .map_or(&[], |connection| connection.read_buffer.as_slice())
    }

    fn close(&mut self, id: u64) {
        if let Some(connection) = self.connections.get_mut(&id) {
            connection.closing = true;
        }
        self.cleanup();
    }

    fn wait(&mut self, timeout: Duration, events: &mut Vec<Event>) -> io::Result<()> {
        if let Some(user) = self.timeout_user.take() {
            let entry = opcode::TimeoutRemove::new(user)
                .build()
                .user_data(TIMEOUT_BIT | KIND_TIMEOUT_REMOVE);
            self.push(entry)?;
        }
        let timeout = timeout.max(Duration::from_micros(1));
        *self.timeout_spec = types::Timespec::from(timeout);
        self.timeout_generation = self.timeout_generation.wrapping_add(1);
        let user = TIMEOUT_BIT | ((self.timeout_generation & 0x00ff_ffff) << 8) | KIND_TIMEOUT;
        let entry = opcode::Timeout::new(&*self.timeout_spec)
            .build()
            .user_data(user);
        self.push(entry)?;
        self.timeout_user = Some(user);

        loop {
            match self.ring.submit_and_wait(1) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        let mut completed: Vec<(u64, i32)> = Vec::new();
        {
            let mut queue = self.ring.completion();
            for completion in &mut queue {
                completed.push((completion.user_data(), completion.result()));
            }
        }
        for (user_data, result) in completed {
            self.on_completion(user_data, result, events);
        }
        self.cleanup();
        Ok(())
    }
}
