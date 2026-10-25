//! A session survives a process restart: the next sequences and the resend
//! window come back from disk.

use std::path::PathBuf;

use deepbook_fix::codec::{MessageBuilder, MsgType, parse, tag};
use deepbook_fix::journal::{FileJournal, FileSequenceStore};
use deepbook_fix::message::{ApplicationMessage, ExecType, ExecutionReport, OrdStatus};
use deepbook_fix::session::{Action, Session, SessionConfig};
use deepbook_types::Side;

const NOW: i64 = 1_792_931_696_000;

fn temporary_directory(name: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let path = std::env::temp_dir().join(format!(
        "deepbook-fix-it-{}-{name}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("temporary directory");
    path
}

fn logon(seq: u64) -> Vec<u8> {
    MessageBuilder::new(
        MsgType::Logon,
        "PEER",
        "DEEPBOOK",
        seq,
        "20261025-12:34:56.000",
    )
    .body_int(tag::ENCRYPT_METHOD, 0)
    .body_int(tag::HEART_BT_INT, 30)
    .build()
}

fn execution_report(exec_id: &str) -> ApplicationMessage {
    ApplicationMessage::ExecutionReport(ExecutionReport {
        exec_id: exec_id.to_owned(),
        order_id: "O-1".to_owned(),
        cl_ord_id: "C-1".to_owned(),
        exec_type: ExecType::New,
        ord_status: OrdStatus::New,
        symbol: "AAPL".to_owned(),
        side: Side::Buy,
        order_qty: 100,
        last_qty: None,
        last_px: None,
        leaves_qty: 100,
        cum_qty: 0,
        avg_px: 0,
        account: None,
        transact_time: None,
        text: None,
    })
}

#[test]
fn a_session_resumes_its_sequences_and_journal_from_disk() {
    let directory = temporary_directory("resume");
    let journal_path = directory.join("journal.bin");
    let sequences_path = directory.join("sequences");

    {
        let mut session = Session::new(
            SessionConfig::new("DEEPBOOK", "PEER", 30),
            FileJournal::open(&journal_path, 16).expect("journal"),
            FileSequenceStore::new(&sequences_path),
            NOW,
        )
        .expect("session");
        session.on_inbound(&logon(1), NOW).expect("logon");
        session
            .send_application(&execution_report("E-1"), NOW + 1)
            .expect("report");
        assert_eq!(3, session.next_out());
    }

    let mut session = Session::new(
        SessionConfig::new("DEEPBOOK", "PEER", 30),
        FileJournal::open(&journal_path, 16).expect("journal"),
        FileSequenceStore::new(&sequences_path),
        NOW + 2,
    )
    .expect("reopened session");
    assert_eq!(2, session.next_in());
    assert_eq!(3, session.next_out());

    // The peer reconnects with its next sequence (2); our Logon reply is the
    // next outbound message (3).
    let actions = session.on_inbound(&logon(2), NOW + 3).expect("logon");
    let reply = actions
        .iter()
        .find_map(|action| match action {
            Action::Send { raw, .. } => Some(parse(raw).expect("parse")),
            _ => None,
        })
        .expect("logon reply");
    assert_eq!(3, reply.seq_num().expect("seq"));
    assert_eq!(MsgType::Logon, reply.msg_type().expect("type"));

    // The report sent before the restart is still in the journal.
    let request = MessageBuilder::new(
        MsgType::ResendRequest,
        "PEER",
        "DEEPBOOK",
        3,
        "20261025-12:34:56.000",
    )
    .body_int(tag::BEGIN_SEQ_NO, 2)
    .body_int(tag::END_SEQ_NO, 2)
    .build();
    let actions = session.on_inbound(&request, NOW + 4).expect("resend");
    let resent = actions
        .iter()
        .find_map(|action| match action {
            Action::Send { raw, seq } => Some((*seq, parse(raw).expect("parse"))),
            _ => None,
        })
        .expect("resent report");
    assert_eq!(2, resent.0);
    assert!(resent.1.poss_dup());
    assert_eq!(
        Some(b"20261025-12:34:56.001".as_slice()),
        resent.1.orig_sending_time()
    );

    std::fs::remove_dir_all(directory).expect("cleanup");
}
