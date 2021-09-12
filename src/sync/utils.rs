use futures::future::{FutureExt, Map};
use futures::channel::oneshot::{Canceled, Receiver, Sender, channel};

pub struct Condition {
    sender: Option<Sender<()>>,
    receiver: Option<Receiver<()>>,
}

impl Default for Condition {
    fn default() -> Self {
        let (sender, receiver) = channel();
        Condition { sender: Some(sender), receiver: Some(receiver) }
    }
}

pub type ConditionFuture = Map<Receiver<()>, fn(Result<(), Canceled>)>;

impl Condition {
    pub fn set(&mut self) {
        let sender = self.sender.take().expect("Condition:;set() called twice");
        sender.send(()).expect("Condition::set()");
    }

    pub fn wait(&mut self) -> ConditionFuture {
        fn error(r: Result<(), Canceled>) {
            r.expect("Condition::wait()");
        }
        self.receiver.take().expect("Condition::wait() called twice").map(error)
    }
}
