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
use std::time::Duration;

mod filters;
use filters::Filters;

/// A event source connection to D-Bus, non-async version where callbacks are Send but not Sync.
pub struct DBusSource<Data: 'static> {
    conn: Connection,
    watch: Generic<Fd>,
    filters: std::cell::RefCell<Filters<FilterCb<Data>>>,
}

/// A event source conncetion to D-Bus, thread local + non-async version
pub struct LocalDBusSource<Data: 'static> {
    conn: LocalConnection,
    watch: Generic<Fd>,
    filters: std::cell::RefCell<Filters<LocalFilterCb<Data>>>,
}

/// A event source connection to D-Bus, Send + Sync + non-async version
pub struct SyncDBusSource<Data: 'static> {
    conn: SyncConnection,
    watch: Generic<Fd>,
    filters: std::sync::Mutex<Filters<SyncFilterCb<Data>>>,
}

macro_rules! sourceimpl {
    ($source: ident, $connection: ident, $callback: ident $(, $ss:tt)*) => {

type $callback<Data> = Box<dyn FnMut(Message, &$source<Data>, &mut Data) -> bool $(+ $ss)* + 'static>;

impl<Data> $source<Data> {
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

    pub fn with_proxy<'a, 'b, Dest: Into<BusName<'a>>, BusPath: Into<Path<'a>>>(
        &'b self,
        dest: Dest,
        path: BusPath,
        timeout: Duration
    ) -> Proxy<'a, &'b Self> {
        Proxy { connection: self, destination: dest.into(), path: path.into(), timeout }
    }

    /// Request a name on the D-Bus.
    ///
    /// For detailed information on the flags and return values, see the libdbus documentation.
    pub fn request_name<'a, Name: Into<BusName<'a>>>(
        &self,
        name: Name,
        allow_replacement: bool,
        replace_existing: bool,
        do_not_queue: bool,
    ) -> Result<org_freedesktop_dbus::RequestNameReply, Error> {
        self.conn
            .request_name(name, allow_replacement, replace_existing, do_not_queue)
    }

    /// Release a previously requested name on the D-Bus.
    pub fn release_name<'a, Name: Into<BusName<'a>>>(&self, name: Name) -> Result<org_freedesktop_dbus::ReleaseNameReply, Error> {
        self.conn.release_name(name)
    }

    /// Adds a new match to the connection, and sets up a callback when this message arrives.
    ///
    /// The returned value can be used to remove the match. The match is also removed if the callback
    /// returns "false".
    pub fn add_match<Args: ReadAll, Callback>(
        &self,
        match_rule: MatchRule<'static>,
        callback: Callback,
    ) -> Result<dbus::channel::Token, dbus::Error>
    where
        Callback: FnMut(Args, &Self, &Message) -> bool $(+ $ss)* + 'static,
    {
        let match_str = match_rule.match_str();
        self.conn.add_match_no_cb(&match_str)?;
        Ok(self.start_receive(match_rule, MakeSignal::make(callback, match_str)))
    }

    /// Adds a new match to the connection, and sets up a callback when this message arrives. This
    /// callback will be able to access the calloop user data.
    ///
    /// The returned value can be used to remove the match. The match is also removed if the callback
    /// returns "false".
    pub fn add_match_data<Args: ReadAll, Callback>(
        &self,
        match_rule: MatchRule<'static>,
        callback: Callback,
    ) -> Result<dbus::channel::Token, dbus::Error>
    where
        Callback: FnMut(Args, &Self, &Message, &mut Data) -> bool $(+ $ss)* + 'static,
    {
        let match_str = match_rule.match_str();
        self.conn.add_match_no_cb(&match_str)?;
        Ok(self.start_receive(match_rule, MakeDataSignal::make(callback, match_str)))
    }

    /// Removes a previously added match and callback from the connection.
    pub fn remove_match(&self, id: Token) -> Result<(), Error> {
        let (match_rule, _) = self.stop_receive(id).ok_or_else(|| Error::new_failed("No match with id found"))?;
        self.conn.remove_match_no_cb(&match_rule.match_str())
    }

    pub fn process(&mut self, timeout: Duration) -> Result<bool, Error> {
        self.conn.process(timeout)
    }

    /// The Channel for this connection
    pub fn channel(&self) -> &Channel {
        self.conn.channel()
    }

    /// Insert this source into the given event loop with an adapder that ether panics on orphan
    /// events or just logs it at warn level. You probably only what this if you set eavesdrop on a
    /// MatchRule.
    pub fn quick_insert(
        self,
        handle: calloop::LoopHandle<Data>,
        panic_on_orphan: bool,
    ) -> Result<Source<$source<Data>>, InsertError<$source<Data>>> {
        handle.insert_source(self, move |msg, connection, data| {
            match connection.filters_mut().get_matches(&msg) {
                Some((token, (_, callback))) => {
                    trace!("match on {:?}", &msg);
                    if callback(msg, &connection, data) {
                        return Some(*token)
                    }
                }
                None => {
                    if panic_on_orphan {
                        panic!("[calloop-dbus] Encountered an orphan event: {:#?}", msg);
                    }
                    warn!("orphan {:#?}", msg);
                }
            }
            None
        })
    }
}

impl<Data> MatchingReceiver for $source<Data> {
    type F = $callback<Data>;

    fn start_receive(&self, match_rule: MatchRule<'static>, callback: Self::F) -> dbus::channel::Token {
        self.filters_mut().add(match_rule, callback)
    }

    fn stop_receive(&self, id: dbus::channel::Token) -> Option<(MatchRule<'static>, Self::F)> {
        self.filters_mut().remove(id)
    }
}

impl<Data> BlockingSender for $source<Data> {
    fn send_with_reply_and_block(&self, msg: Message, timeout: Duration) -> Result<Message, Error> {
        self.conn.send_with_reply_and_block(msg, timeout)
    }
}

impl<Data> Sender for $source<Data> {
    fn send(&self, msg: Message) -> Result<u32, ()> {
        self.conn.send(msg)
    }
}

impl<Args: ReadAll, Callback: FnMut(Args, &$source<Data>, &Message, &mut Data) -> bool $(+ $ss)* + 'static, Data>
    MakeDataSignal<$callback<Data>, Args, $source<Data>> for Callback
{
    fn make(mut self, match_str: String) -> $callback<Data> {
        Box::new(move |msg: Message, event_source: &$source<Data>, data: &mut Data| {
            if let Ok(args) = Args::read(&mut msg.iter_init()) {
                if self(args, event_source, &msg, data) {
                    return true
                };
                let _ = event_source.conn.remove_match_no_cb(&match_str);
                false
            } else {
                true
            }
        })
    }
}

impl<Args: ReadAll, Callback: FnMut(Args, &$source<Data>, &Message) -> bool $(+ $ss)* + 'static, Data>
    MakeSignal<$callback<Data>, Args, $source<Data>> for Callback
{
    fn make(mut self, match_str: String) -> $callback<Data> {
        Box::new(move |msg: Message, event_source: &$source<Data>, _| {
            if let Ok(args) = Args::read(&mut msg.iter_init()) {
                if self(args, event_source, &msg) {
                    return true
                };
                let _ = event_source.conn.remove_match_no_cb(&match_str);
                false
            } else {
                true
            }
        })
    }
}

impl<Data> EventSource for $source<Data> {
    type Event = Message;
    type Metadata = $source<Data>;
    type Ret = Option<Token>;

    fn process_events<Callback>(
        &mut self,
        _: Readiness,
        _: calloop::Token,
        mut callback: Callback,
    ) -> io::Result<()>
    where
        Callback: FnMut(Self::Event, &mut Self::Metadata) -> Self::Ret,
    {
        // read in all message and send quote ones
        self.conn
            .channel()
            .read_write(Some(Duration::from_millis(0)))
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

impl<Data> DBusSource<Data> {
    fn filters_mut(&self) -> std::cell::RefMut<Filters<FilterCb<Data>>> {
        self.filters.borrow_mut()
    }
}

impl<Data> LocalDBusSource<Data> {
    fn filters_mut(&self) -> std::cell::RefMut<Filters<LocalFilterCb<Data>>> {
        self.filters.borrow_mut()
    }
}

impl<Data> SyncDBusSource<Data> {
    fn filters_mut(&self) -> std::sync::MutexGuard<Filters<SyncFilterCb<Data>>> {
        self.filters.lock().unwrap()
    }
}

/// Internal helper trait
pub trait MakeSignal<G, S, T> {
    /// Internal helper trait
    fn make(self, match_str: String) -> G;
}
///
/// Internal helper trait
pub trait MakeDataSignal<G, S, T> {
    /// Internal helper trait
    fn make(self, match_str: String) -> G;
}

#[test]
fn test_add_match() {
    use dbus::blocking::stdintf::org_freedesktop_dbus::PropertiesPropertiesChanged as Ppc;
    use dbus::message::SignalArgs;
    let source: DBusSource<usize> = DBusSource::new_session().unwrap();
    let token = source
        .add_match(Ppc::match_rule(None, None), |_: Ppc, _, _| true)
        .unwrap();
    source.remove_match(token).unwrap();
}

#[test]
fn test_conn_send_sync() {
    fn is_send<T: Send>(_: &T) {}
    fn is_sync<T: Sync>(_: &T) {}

    let source: SyncDBusSource<usize> = SyncDBusSource::new_session().unwrap();
    is_send(&source);
    is_sync(&source);

    let source: DBusSource<usize> = DBusSource::new_session().unwrap();
    is_send(&source);
}

#[test]
fn test_peer() {
    let mut source: DBusSource<usize> = DBusSource::new_session().unwrap();

    let source_name = source.unique_name().into_static();
    use std::sync::Arc;
    let done = Arc::new(false);
    let done2 = done.clone();
    let thread = std::thread::spawn(move || {
        let source2: DBusSource<usize> = DBusSource::new_session().unwrap();

        let proxy = source2.with_proxy(source_name, "/", Duration::from_secs(5));
        let (signal2,): (String,) = proxy
            .method_call("org.freedesktop.DBus.Peer", "GetMachineId", ())
            .unwrap();
        println!("{}", signal2);
        assert_eq!(Arc::strong_count(&done2), 2);
        signal2
    });
    assert_eq!(Arc::strong_count(&done), 2);

    for _ in 0..30 {
        source.process(Duration::from_millis(100)).unwrap();
        if Arc::strong_count(&done) < 2 {
            break;
        }
    }

    let s2 = thread.join().unwrap();

    let proxy = source.with_proxy("org.a11y.Bus", "/org/a11y/bus", Duration::from_secs(5));
    let (s1,): (String,) = proxy
        .method_call("org.freedesktop.DBus.Peer", "GetMachineId", ())
        .unwrap();

    assert_eq!(s1, s2);
}
