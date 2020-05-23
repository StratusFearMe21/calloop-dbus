use calloop::{
    generic::{Fd, Generic},
    {EventSource, InsertError, Interest, Mode, Poll, Readiness, Source},
};
use dbus::{
    blocking::stdintf::org_freedesktop_dbus,
    blocking::Connection,
    channel::{BusType, Channel, MatchingReceiver},
    message::MatchRule,
    strings::BusName,
    Error, Message,
};
use log::{debug, trace};

use std::cell::RefCell;
use std::io;

mod filters;
use filters::Filters;

type FilterCb = Box<dyn FnMut(Message, &Connection) -> bool + Send + 'static>;

pub struct DBusSource {
    conn: Connection,
    watch: Generic<Fd>,
    filters: RefCell<Filters<FilterCb>>,
}

impl DBusSource {
    pub fn new(bus_type: BusType) -> io::Result<Self> {
        let mut channel = Channel::get_private(bus_type).map_err(|_| {
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

    /// Insert this source into the given event loop with an adapder that ether panics on orphan
    /// events or just logs it at debug level. You probaly only what this if you set eavesdrop on a
    /// MatchRule.
    pub fn quick_insert<Data: 'static>(
        self,
        handle: calloop::LoopHandle<Data>,
        panic_on_orphan: bool,
    ) -> Result<Source<DBusSource>, InsertError<DBusSource>> {
        handle.insert_source(self, move |msg, _, _| {
            if panic_on_orphan {
                panic!("[calloop] Encountered an orphan event: {:#?}", msg,);
            } else {
                debug!("orphan {:#?}", msg);
            }
        })
    }

    fn filters_mut(&self) -> std::cell::RefMut<Filters<FilterCb>> {
        self.filters.borrow_mut()
    }

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

    pub fn add_match<'a, F>(
        &mut self,
        match_rule: MatchRule<'a>,
        f: F,
    ) -> Result<dbus::channel::Token, dbus::Error>
    where
        F: FnMut(Message, &Connection) -> bool + Send + 'static,
    {
        let token = self.start_receive(match_rule.static_clone(), Box::new(f));
        self.conn
            .add_match_no_cb(&match_rule.match_str().as_str())
            .map(|_| token)
    }
}

impl MatchingReceiver for DBusSource {
    type F = FilterCb;

    fn start_receive(&self, m: MatchRule<'static>, f: Self::F) -> dbus::channel::Token {
        self.filters_mut().add(m, f)
    }

    fn stop_receive(&self, id: dbus::channel::Token) -> Option<(MatchRule<'static>, Self::F)> {
        self.filters_mut().remove(id)
    }
}

impl EventSource for DBusSource {
    type Event = Message;
    type Metadata = ();
    type Ret = ();

    fn process_events<F>(
        &mut self,
        _: Readiness,
        _: calloop::Token,
        mut fcallback: F,
    ) -> io::Result<()>
    where
        F: FnMut(Self::Event, &mut Self::Metadata) -> Self::Ret,
    {
        self.conn
            .channel()
            .read_write(Some(std::time::Duration::from_millis(0)))
            .map_err(|()| {
                io::Error::new(io::ErrorKind::NotConnected, "DBus connection is closed")
            })?;

        while let Some(message) = self.conn.channel().pop_message() {
            let mut remove: Option<dbus::channel::Token> = None;
            if let Some((token, (_, callback))) = self.filters_mut().get_matches(&message) {
                trace!("match on message {:?}", &message);
                if !callback(message, &self.conn) {
                    remove = Some(*token);
                }
            } else {
                fcallback(message, &mut ());
            }
            if let Some(token) = remove {
                self.filters_mut().remove(token);
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
