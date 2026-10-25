import java.io.FileInputStream;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import quickfix.ApplicationAdapter;
import quickfix.DefaultMessageFactory;
import quickfix.FileStoreFactory;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.field.ClOrdID;
import quickfix.field.ExecType;
import quickfix.field.HandlInst;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.NewOrderSingle;

/**
 * The QuickFIX/J half of M1d: log on as CLIENT, send one NewOrderSingle, check
 * the ExecutionReport, then log out.
 *
 * The class is deliberately a plain main rather than a JUnit suite: the
 * conformance runner starts a real gateway process and this program is the
 * external FIX client.
 */
public final class QuickFixInitiator {
    private static final CountDownLatch LOGGED_ON = new CountDownLatch(1);
    private static final CountDownLatch REPORTED = new CountDownLatch(1);
    private static volatile SessionID sessionId;
    private static volatile ExecutionReport report;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: QuickFixInitiator <quickfixj.cfg>");
        }
        SessionSettings settings = new SessionSettings(new FileInputStream(args[0]));

        ApplicationAdapter application = new ApplicationAdapter() {
            @Override
            public void onLogon(SessionID id) {
                sessionId = id;
                LOGGED_ON.countDown();
            }

            @Override
            public void onLogout(SessionID id) {
                // The run is over; the report latch already fired.
            }

            @Override
            public void fromApp(Message message, SessionID id) throws quickfix.FieldNotFound {
                if (message instanceof ExecutionReport executionReport) {
                    report = executionReport;
                    REPORTED.countDown();
                }
            }
        };

        SocketInitiator initiator = new SocketInitiator(
            application,
            new FileStoreFactory(settings),
            settings,
            new DefaultMessageFactory());
        initiator.start();
        try {
            if (!LOGGED_ON.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("QuickFIX/J did not log on");
            }

            NewOrderSingle order = new NewOrderSingle(
                new ClOrdID("QFJ-1"),
                new Side(Side.BUY),
                new TransactTime(LocalDateTime.now()),
                new OrdType(OrdType.LIMIT));
            order.set(new Symbol("AAPL"));
            order.set(new OrderQty(100.0));
            order.set(new Price(100.25));
            order.set(new TimeInForce(TimeInForce.DAY));
            order.set(new HandlInst(HandlInst
                .AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION));
            if (!Session.sendToTarget(order, sessionId)) {
                throw new AssertionError("QuickFIX/J refused to send the order");
            }

            if (!REPORTED.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("no ExecutionReport for QFJ-1");
            }
            ExecutionReport received = report;
            assertEquals("QFJ-1", received.getClOrdID().getValue(), "ClOrdID");
            assertEquals(ExecType.NEW, received.getExecType().getValue(), "ExecType");
            assertEquals(OrdStatus.NEW, received.getOrdStatus().getValue(), "OrdStatus");
            if (received.getOrderID().getValue().isEmpty()) {
                throw new AssertionError("empty OrderID");
            }
            if (received.getLeavesQty().getValue() != 100.0) {
                throw new AssertionError("unexpected LeavesQty " + received.getLeavesQty().getValue());
            }
        } finally {
            if (sessionId != null) {
                Session session = Session.lookupSession(sessionId);
                if (session != null) {
                    session.logout("conformance done");
                }
            }
            Thread.sleep(300);
            initiator.stop();
        }
        System.out.println("quickfixj: logon, NewOrderSingle and ExecutionReport OK");
    }

    private static void assertEquals(Object expected, Object actual, String what) {
        if (!expected.equals(actual)) {
            throw new AssertionError(what + ": expected " + expected + ", got " + actual);
        }
    }
}
