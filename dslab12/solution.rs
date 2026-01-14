use crate::domain::{Action, ClientRef, Edit, EditRequest, Operation, ReliableBroadcastRef};
use module_system::Handler;

use std::collections::VecDeque;
use std::mem;

fn transform_action(a: &Action, ra: usize, b: &Action, rb: usize) -> Action {
    use Action::*;

    match (a, b) {
        (Insert { idx: p1, ch }, Insert { idx: p2, .. }) => {
            if p1 < p2 {
                Insert { idx: *p1, ch: *ch }
            } else if p1 == p2 && ra < rb {
                Insert { idx: *p1, ch: *ch }
            } else {
                Insert { idx: *p1 + 1, ch: *ch }
            }
        }

        (Delete { idx: p1 }, Delete { idx: p2 }) => {
            if p1 < p2 {
                Delete { idx: *p1 }
            } else if p1 == p2 {
                Nop
            } else {
                Delete { idx: *p1 - 1 }
            }
        }

        (Insert { idx: p1, ch }, Delete { idx: p2 }) => {
            if p1 <= p2 {
                Insert { idx: *p1, ch: *ch }
            } else {
                Insert { idx: *p1 - 1, ch: *ch }
            }
        }

        (Delete { idx: p1 }, Insert { idx: p2, .. }) => {
            if p1 < p2 {
                Delete { idx: *p1 }
            } else {
                Delete { idx: *p1 + 1 }
            }
        }

        (Nop, _) => Nop,
        (a, Nop) => a.clone(),
    }
}

#[derive(Clone)]
struct Entry {
    rank: usize,
    action: Action,
}

pub(crate) struct Process<const N: usize> {
    rank: usize,
    broadcast: Box<dyn ReliableBroadcastRef<N>>,
    client: Box<dyn ClientRef>,

    log: Vec<Entry>,
    pending: VecDeque<EditRequest>,

    current_round: usize,
    issued_local: bool,
    received_in_round: [bool; N],
    round_applied: Vec<Entry>,

    recv_count_from: [usize; N],
    buffer: VecDeque<Vec<Operation>>,
}

impl<const N: usize> Process<N> {
    pub(crate) fn new(
        rank: usize,
        broadcast: Box<dyn ReliableBroadcastRef<N>>,
        client: Box<dyn ClientRef>,
    ) -> Self {
        Self {
            rank,
            broadcast,
            client,
            log: Vec::new(),
            pending: VecDeque::new(),
            current_round: 0,
            issued_local: false,
            received_in_round: [false; N],
            round_applied: Vec::new(),
            recv_count_from: [0; N],
            buffer: VecDeque::new(),
        }
    }

    fn buffer_push(&mut self, round_idx: usize, msg: Operation) {

        let offset = round_idx - self.current_round;
        while self.buffer.len() <= offset {
            self.buffer.push_back(Vec::new());
        }
        self.buffer[offset].push(msg);
    }

    fn buffer_has_current(&self) -> bool {
        self.buffer.front().map_or(false, |v| !v.is_empty())
    }

    fn buffer_take_current(&mut self) -> Vec<Operation> {
        if self.buffer.is_empty() {
            Vec::new()
        } else {
            mem::take(&mut self.buffer[0])
        }
    }

    async fn send_edit_and_log(&mut self, rank: usize, action: Action) {
        self.client.send(Edit { action: action.clone() }).await;
        self.log.push(Entry { rank, action });
    }

    async fn issue_local_action_for_round(&mut self, action: Action) {
        self.issued_local = true;
        self.received_in_round[self.rank] = true;

        self.send_edit_and_log(self.rank, action.clone()).await;
        self.round_applied.push(Entry {
            rank: self.rank,
            action: action.clone(),
        });

        self.broadcast
            .send(Operation {
                process_rank: self.rank,
                action,
            })
            .await;
    }

    async fn issue_local_from_request(&mut self, mut req: EditRequest) {
        let start = req.num_applied.min(self.log.len());
        for i in start..self.log.len() {
            req.action = transform_action(&req.action, N, &self.log[i].action, self.log[i].rank);
        }
        self.issue_local_action_for_round(req.action).await;
    }

    async fn apply_remote_current_round(&mut self, op: Operation) {
        let sender = op.process_rank;

        if sender == self.rank {
            return;
        }
        if self.received_in_round[sender] {
            return;
        }

        let mut action = op.action;
        for e in &self.round_applied {
            action = transform_action(&action, sender, &e.action, e.rank);
        }

        self.send_edit_and_log(sender, action.clone()).await;
        self.round_applied.push(Entry { rank: sender, action });
        self.received_in_round[sender] = true;
    }

    fn round_complete(&self) -> bool {
        self.issued_local && self.received_in_round.iter().all(|x| *x)
    }

    fn start_next_round(&mut self) {
        self.issued_local = false;
        self.received_in_round = [false; N];
        self.round_applied.clear();
        self.current_round += 1;
        if !self.buffer.is_empty() {
            self.buffer.pop_front();
        }
    }

    async fn try_progress(&mut self) {
        loop {
            let mut progressed = false;

            if !self.issued_local {
                if let Some(req) = self.pending.pop_front() {
                    self.issue_local_from_request(req).await;
                    progressed = true;
                } else if self.buffer_has_current() {
                    self.issue_local_action_for_round(Action::Nop).await;
                    progressed = true;
                }
            }

            if self.issued_local {
                let ops = self.buffer_take_current();
                if !ops.is_empty() {
                    for op in ops {
                        self.apply_remote_current_round(op).await;
                    }
                    progressed = true;
                }
            }

            if self.round_complete() {
                self.start_next_round();
                progressed = true;
            }

            if !progressed {
                break;
            }
        }
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> {
    async fn handle(&mut self, msg: Operation) {
        let sender = msg.process_rank;
        let round_idx = self.recv_count_from[sender];
        self.recv_count_from[sender] += 1;

        self.buffer_push(round_idx, msg);
        self.try_progress().await;
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> {
    async fn handle(&mut self, request: EditRequest) {
        self.pending.push_back(request);
        self.try_progress().await;
    }
}
