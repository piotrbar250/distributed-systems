use std::time::Duration;

use tokio::time::Instant;

use assignment_3_test_utils::{ExecutorSender, IdentityMachine, RamStorage, make_idents, make_rafts};
use module_system::System;

#[tokio::main]
async fn main() {

    let mut system = System::new().await;

    let entry_data = vec![1, 2, 3, 4, 5];
    let processes = make_idents::<3>();
    let [ident_leader, ident_follower, _dead] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    // let [raft_leader, _raft_follower] = make_rafts(
    //     &mut system,
    //     [ident_leader, ident_follower],
    //     [100, 300],
    //     boot,
    //     |_| Box::new(IdentityMachine),
    //     |_| Box::<RamStorage>::default(),
    //     sender.clone(),
    //     async |id, mref| sender.insert(id, mref).await,
    //     &processes,
    // ).await;
    

    let [_raft_leader] = make_rafts(
        &mut system,
        [ident_leader],
        [100],
        boot,
        |_| Box::new(IdentityMachine),
        |_| Box::<RamStorage>::default(),
        sender.clone(),
        async |id, mref| sender.insert(id, mref).await,
        &processes,
    ).await;

    tokio::time::sleep(Duration::from_secs(3)).await;
}
