use crate::error::Error;
use crate::io::{PortalId, StatementId};
use crate::message::{self, BackendMessageFormat, Bind, Close, Parse, TransactionStatus};
use crate::PgConnection;

/// What became of the statements a connection was asked to run ahead of its next query.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Ahead {
    /// Nothing is queued.
    #[default]
    Clear,

    /// Queued: the next query this connection runs sends them first, in the same message.
    Queued,

    /// Sent with a query that didn't confirm them: it failed, its stream was dropped before it
    /// finished, or it ran inside or opened a transaction block, whose end decides their fate.
    Sent,

    /// Sent with a query that finished without an error, outside any transaction block before
    /// and after it, so they ran and committed with it.
    Committed,
}

#[derive(Debug, Default)]
pub(crate) struct AheadQueue {
    statements: Vec<String>,
    state: Ahead,
}

impl PgConnection {
    /// Queues statements, one per item, to run ahead of the next query this connection runs
    /// through its `Executor`, written in the same message as its Parse (when the statement isn't
    /// prepared yet) or its Bind: the server runs them first, and a statement among them that fails
    /// makes the query fail without running it. Their results are discarded. [`Self::ahead`]
    /// reports what became of them once that message is answered.
    #[doc(hidden)]
    pub fn run_ahead<'s>(&mut self, statements: impl IntoIterator<Item = &'s str>) {
        let queue = &mut self.inner.ahead;
        let before = queue.statements.len();
        queue.statements.extend(
            statements
                .into_iter()
                .map(str::trim)
                .filter(|statement| !statement.is_empty())
                .map(str::to_owned),
        );
        if queue.statements.len() > before {
            queue.state = Ahead::Queued;
        }
    }

    #[doc(hidden)]
    pub fn ahead(&self) -> Ahead {
        self.inner.ahead.state
    }

    /// Drops what is queued and forgets what became of what was sent.
    #[doc(hidden)]
    pub fn clear_ahead(&mut self) {
        self.inner.ahead = AheadQueue::default();
    }

    pub(crate) fn take_ahead(&mut self) -> Vec<String> {
        let queue = &mut self.inner.ahead;
        if queue.statements.is_empty() {
            return Vec::new();
        }
        queue.state = Ahead::Sent;
        std::mem::take(&mut queue.statements)
    }

    pub(crate) fn write_ahead(&mut self, statements: &[String]) -> Result<(), Error> {
        for query in statements {
            // a statement left prepared by a pipeline that failed after its Parse is closed first
            self.inner
                .stream
                .write_msg(Close::Statement(StatementId::AHEAD))?;
            self.inner.stream.write_msg(Parse {
                statement: StatementId::AHEAD,
                query,
                param_types: &[],
            })?;
            self.inner.stream.write_msg(Bind {
                portal: PortalId::UNNAMED,
                statement: StatementId::AHEAD,
                formats: &[],
                num_params: 0,
                params: &[],
                result_formats: &[],
            })?;
            self.inner.stream.write_msg(message::Execute {
                portal: PortalId::UNNAMED,
                limit: 0,
            })?;
        }
        Ok(())
    }

    // reads past the replies to `count` statements written by `write_ahead`, up to the last one's
    // CommandComplete; an error among them is returned
    pub(crate) async fn skip_ahead(&mut self, mut count: usize) -> Result<(), Error> {
        while count > 0 {
            if self.inner.stream.recv().await?.format == BackendMessageFormat::CommandComplete {
                count -= 1;
            }
        }
        Ok(())
    }

    pub(crate) fn idle(&self) -> bool {
        matches!(self.inner.transaction_status, TransactionStatus::Idle)
    }

    pub(crate) fn ran_ahead(&mut self, idle_before: bool) {
        if idle_before && self.idle() && self.inner.ahead.state == Ahead::Sent {
            self.inner.ahead.state = Ahead::Committed;
        }
    }
}
