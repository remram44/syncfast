use futures::future::{FutureExt, Map};
use futures::channel::oneshot::{Canceled, Receiver, Sender, channel};
use std::path::Path;

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
        let sender = self.sender.take().expect("Condition::set() called twice");
        sender.send(()).expect("Condition::set()");
    }

    pub fn wait(&mut self) -> ConditionFuture {
        fn error(r: Result<(), Canceled>) {
            r.expect("Condition::wait()");
        }
        self.receiver.take().expect("Condition::wait() called twice").map(error)
    }
}

pub fn move_file(from: &Path, to: &Path) -> std::io::Result<()> {
    match std::fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(_) => {
            // Try copying then removing source file
            std::fs::copy(from, to)?;
            match std::fs::remove_file(from) {
                Ok(()) => Ok(()),
                Err(e) => {
                    std::fs::remove_file(to).ok(); // Ignore this result on purpose
                    Err(e)
                }
            }
        }
    }
}
