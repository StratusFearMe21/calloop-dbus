use calloop::{
    generic::{Fd, Generic},
    {EventSource, InsertError, Interest, Mode, Poll, Readiness, Source},
};
use dbus::{
    arg::ReadAll,
    blocking::stdintf::org_freedesktop_dbus,
    blocking::{BlockingSender, Connection, LocalConnection, Proxy, SyncConnection},
    channel::{BusType, Channel, MatchingReceiver, Sender, Token},
    message::MatchRule,
    strings::{BusName, Path},
    Error, Message,
};
use log::{trace, warn};

use std::io;

mod filters;
use filters::Filters;

pub struct DBusSource {
    conn: Connection,
    watch: Generic<Fd>,
    filters: std::cell::RefCell<Filters<FilterCb>>,
}

pub struct LocalDBusSource {
    conn: LocalConnection,
    watch: Generic<Fd>,
    filters: std::cell::RefCell<Filters<LocalFilterCb>>,
}

pub struct SyncDBusSource {
    conn: SyncConnection,
    watch: Generic<Fd>,
    filters: std::sync::Mutex<Filters<SyncFilterCb>>,
}
macro_rules! sourceimpl {
    ($s: ident, $c: ident, $cb: ident $(, $ss:tt)*) => {

type $cb = Box<dyn FnMut(Message, &$s) -> bool $(+ $ss)* + 'static>;

impl $s {
    /// Create a new connection to the session bus.
    pub fn new_session() -> io::Result<Self> {
        Self::new(Channel::get_private(BusType::Session))
    }

    /// Create a new connection to the system-wide bus.
    pub fn new_system() -> io::Result<Self> {
        Self::new(Channel::get_private(BusType::System))
    }

    fn new(c: Result<Channel, Error>) -> io::Result<Self> {
        let mut channel = c.map_err(|_| {
            io::Error::new(io::ErrorKind::ConnectionRefused, "Failed to connet to DBus")
        })?;

        channel.set_watch_enabled(true);

        let watch_fd = channel.watch();

        let interest = match (watch_fd.read, watch_fd.write) {
            (true, true) => Interest::Both,
            (false, true) => Interest::Writable,
            (true, false) => Interest::Readable,
            (false, false) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "fd nether read nor write",
                ))
            }
        };

        let watch = Generic::from_fd(watch_fd.fd, interest, Mode::Level);
        Ok(Self {
            conn: channel.into(),
            watch,
            filters: Default::default(),
        })
    }

    /// Get the connection's unique name.
    ///
    /// It's usually something like ":1.54"
    pub fn unique_name(&self) -> BusName {
        self.conn.unique_name()
    }

    pub fn with_proxy<'a, 'b, D: Into<BusName<'a>>, P: Into<Path<'a>>>(
        &'b self,
        dest: D,
        path: P,
        timeout: std::time::Duration
    ) -> Proxy<'a, &'b Self> {
        Proxy { connection: self, destination: dest.into(), path: path.into(), timeout }
    }

    /// Request a name on the D-Bus.
    ///
    /// For detailed information on the flags and return values, see the libdbus documentation.
    pub fn request_name<'a, N: Into<BusName<'a>>>(
        &self,
        name: N,
        allow_replacement: bool,
        replace_existing: bool,
        do_not_queue: bool,
    ) -> Result<org_freedesktop_dbus::RequestNameReply, Error> {
        self.conn
            .request_name(name, allow_replacement, replace_existing, do_not_queue)
    }

    /// Release a previously requested name on the D-Bus.
    pub fn release_name<'a, N: Into<BusName<'a>>>(&self, name: N) -> Result<org_freedesktop_dbus::ReleaseNameReply, Error> {
        self.conn.release_name(name)
    }

    /// Adds a new match to the connection, and sets up a callback when this message arrives.
    ///
    /// The returned value can be used to remove the match. The match is also removed if the callback
    /// returns "false".
    // TODO: The callback should be that of DBus add_match with the calloop data added. We should
    // also provide a version of add_match with is API compatible with DBus add_match.
    pub fn add_match<S: ReadAll, F>(
        &self,
        match_rule: MatchRule<'static>,
        f: F,
    ) -> Result<dbus::channel::Token, dbus::Error>
    where
        F: FnMut(S, &Self, &Message) -> bool $(+ $ss)* + 'static,
    {
        let m = match_rule.match_str();
        self.conn.add_match_no_cb(&m)?;
        Ok(self.start_receive(match_rule, MakeSignal::make(f, m)))
    }

    /// Removes a previously added match and callback from the connection.
    pub fn remove_match(&self, id: Token) -> Result<(), Error> {
        let (mr, _) = self.stop_receive(id).ok_or_else(|| Error::new_failed("No match with id found"))?;
        self.conn.remove_match_no_cb(&mr.match_str())
    }

    pub fn process(&mut self, timeout: std::time::Duration) -> Result<bool, Error> {
        self.conn.process(timeout)
    }

    /// The Channel for this connection
    pub fn channel(&self) -> &Channel {
        self.conn.channel()
    }

    /// Insert this source into the given event loop with an adapder that ether panics on orphan
    /// events or just logs it at warn level. You probably only what this if you set eavesdrop on a
    /// MatchRule.
    pub fn quick_insert<Data: 'static>(
        self,
        handle: calloop::LoopHandle<Data>,
        panic_on_orphan: bool,
    ) -> Result<Source<$s>, InsertError<$s>> {
        handle.insert_source(self, move |msg, connection, _data| {
            match connection.filters_mut().get_matches(&msg) {
                Some((token, (_, callback))) => {
                    trace!("match on {:?}", &msg);
                    if callback(msg, &connection) {
                        return Some(*token)
                    }
                }
                None => {
                    if panic_on_orphan {
                        panic!("[calloop-dbus] Encountered an orphan event: {:#?}", msg,);
                    }
                    warn!("orphan {:#?}", msg);
                }
            }
            None
        })
    }
}

impl MatchingReceiver for $s {
    type F = $cb;

    fn start_receive(&self, m: MatchRule<'static>, f: Self::F) -> dbus::channel::Token {
        self.filters_mut().add(m, f)
    }

    fn stop_receive(&self, id: dbus::channel::Token) -> Option<(MatchRule<'static>, Self::F)> {
        self.filters_mut().remove(id)
    }
}

impl BlockingSender for $s {
    fn send_with_reply_and_block(&self, msg: Message, timeout: std::time::Duration) -> Result<Message, Error> {
        self.conn.send_with_reply_and_block(msg, timeout)
    }
}

impl Sender for $s {
    fn send(&self, msg: Message) -> Result<u32, ()> {
        self.conn.send(msg)
    }
}

impl<S: ReadAll, F: FnMut(S, &$s, &Message) -> bool $(+ $ss)* + 'static> MakeSignal<$cb, S, $s> for F {
    fn make(mut self, mstr: String) -> $cb {
        Box::new(move |msg: Message, es: &$s| {
            if let Ok(s) = S::read(&mut msg.iter_init()) {
                if self(s, es, &msg) {
                    return true
                };
                let _ = es.conn.remove_match_no_cb(&mstr);
                false
            } else {
                true
            }
        })
    }
}

impl EventSource for $s {
    type Event = Message;
    type Metadata = $s;
    type Ret = Option<Token>;

    fn process_events<F>(
        &mut self,
        _: Readiness,
        _: calloop::Token,
        mut callback: F,
    ) -> io::Result<()>
    where
        F: FnMut(Self::Event, &mut Self::Metadata) -> Self::Ret,
    {
        // read in all message and send quote ones
        self.conn
            .channel()
            .read_write(Some(std::time::Duration::from_millis(0)))
            .map_err(|()| {
                io::Error::new(io::ErrorKind::NotConnected, "DBus connection is closed")
            })?;

        // process each message
        while let Some(message) = self.conn.channel().pop_message() {
            if let Some(token) = callback(message, self) {
                self.conn.remove_match(token)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            }
        }

        self.conn.channel().flush();
        Ok(())
    }

    fn register(&mut self, poll: &mut Poll, token: calloop::Token) -> io::Result<()> {
        self.watch.register(poll, token)
    }

    fn reregister(&mut self, poll: &mut Poll, token: calloop::Token) -> io::Result<()> {
        self.watch.reregister(poll, token)
    }

    fn unregister(&mut self, poll: &mut Poll) -> io::Result<()> {
        self.watch.unregister(poll)
    }
}

    }
}

sourceimpl!(DBusSource, Connection, FilterCb, Send);
sourceimpl!(LocalDBusSource, LocalConnection, LocalFilterCb);
sourceimpl!(SyncDBusSource, SyncConnection, SyncFilterCb, Send, Sync);

impl DBusSource {
    fn filters_mut(&self) -> std::cell::RefMut<Filters<FilterCb>> {
        self.filters.borrow_mut()
    }
}

impl LocalDBusSource {
    fn filters_mut(&self) -> std::cell::RefMut<Filters<LocalFilterCb>> {
        self.filters.borrow_mut()
    }
}

impl SyncDBusSource {
    fn filters_mut(&self) -> std::sync::MutexGuard<Filters<SyncFilterCb>> {
        self.filters.lock().unwrap()
    }
}

/// Internal helper trait
pub trait MakeSignal<G, S, T> {
    /// Internal helper trait
    fn make(self, mstr: String) -> G;
}

#[test]
fn test_add_match() {
    use dbus::blocking::stdintf::org_freedesktop_dbus::PropertiesPropertiesChanged as Ppc;
    use dbus::message::SignalArgs;
    let c = DBusSource::new_session().unwrap();
    let x = c
        .add_match(Ppc::match_rule(None, None), |_: Ppc, _, _| true)
        .unwrap();
    c.remove_match(x).unwrap();
}

#[test]
fn test_conn_send_sync() {
    fn is_send<T: Send>(_: &T) {}
    fn is_sync<T: Sync>(_: &T) {}

    let c = SyncDBusSource::new_session().unwrap();
    is_send(&c);
    is_sync(&c);

    let c = DBusSource::new_session().unwrap();
    is_send(&c);
}

#[test]
fn test_peer() {
    let mut c = DBusSource::new_session().unwrap();

    let c_name = c.unique_name().into_static();
    use std::sync::Arc;
    let done = Arc::new(false);
    let d2 = done.clone();
    let j = std::thread::spawn(move || {
        let c2 = DBusSource::new_session().unwrap();

        let proxy = c2.with_proxy(c_name, "/", std::time::Duration::from_secs(5));
        let (s2,): (String,) = proxy
            .method_call("org.freedesktop.DBus.Peer", "GetMachineId", ())
            .unwrap();
        println!("{}", s2);
        assert_eq!(Arc::strong_count(&d2), 2);
        s2
    });
    assert_eq!(Arc::strong_count(&done), 2);

    for _ in 0..30 {
        c.process(std::time::Duration::from_millis(100)).unwrap();
        if Arc::strong_count(&done) < 2 {
            break;
        }
    }

    let s2 = j.join().unwrap();

    let proxy = c.with_proxy(
        "org.a11y.Bus",
        "/org/a11y/bus",
        std::time::Duration::from_secs(5),
    );
    let (s1,): (String,) = proxy
        .method_call("org.freedesktop.DBus.Peer", "GetMachineId", ())
        .unwrap();

    assert_eq!(s1, s2);
}
