import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A byte-level FIX 4.4 probe for the two replayed behaviours QuickFIX/J's
 * session layer would otherwise hide: the resend of an application message
 * with PossDupFlag/OrigSendingTime plus an admin gap fill, and the logout when
 * a resend starts outside the journal window.
 *
 * Usage: RawProbe resend|over-window <host> <port>
 */
public final class RawProbe {
    private static final String SENDING_TIME = "20261025-12:34:56.000";

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: RawProbe resend|over-window <host> <port>");
        }
        String mode = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            send(out, "A", 1, "98=0\u0001108=30\u0001");
            Map<Integer, String> logon = parse(readFrame(in));
            assertField("35", "A", logon, "logon reply");

            if (mode.equals("resend")) {
                resend(out, in);
            } else if (mode.equals("over-window")) {
                overWindow(out, in);
            } else {
                throw new IllegalArgumentException("unknown mode " + mode);
            }
        }
        System.out.println("raw: " + mode + " OK");
    }

    private static void resend(OutputStream out, InputStream in) throws Exception {
        for (int index = 1; index <= 3; index++) {
            int seq = index + 1;
            send(out, "D", seq, orderBody("C-" + index));
            Map<Integer, String> report = parse(readFrame(in));
            assertField("35", "8", report, "ExecutionReport " + index);
            assertField("150", "0", report, "ExecType " + index);
        }

        // Ask for the logon reply (admin, seq 1) and the three reports.
        send(out, "2", 5, "7=1\u000116=4\u0001");

        Map<Integer, String> gapFill = parse(readFrame(in));
        assertField("35", "4", gapFill, "first resend frame is a SequenceReset");
        assertField("123", "Y", gapFill, "gap fill flag");
        assertField("36", "2", gapFill, "gap fill NewSeqNo");
        assertField("34", "1", gapFill, "gap fill sequence");

        for (int seq = 2; seq <= 4; seq++) {
            Map<Integer, String> resent = parse(readFrame(in));
            assertField("35", "8", resent, "application message " + seq + " is not gap-filled");
            assertField("34", Integer.toString(seq), resent, "resent sequence");
            assertField("43", "Y", resent, "PossDupFlag on " + seq);
            if (resent.get(122) == null || resent.get(122).isEmpty()) {
                throw new AssertionError("OrigSendingTime missing on " + seq);
            }
        }

        send(out, "5", 6, "58=bye\u0001");
        Map<Integer, String> logout = parse(readFrame(in));
        assertField("35", "5", logout, "logout reply");
    }

    private static void overWindow(OutputStream out, InputStream in) throws Exception {
        for (int index = 1; index <= 2; index++) {
            int seq = index + 1;
            send(out, "D", seq, orderBody("C-" + index));
            Map<Integer, String> report = parse(readFrame(in));
            assertField("35", "8", report, "ExecutionReport " + index);
        }

        // The journal keeps one entry; sequence 2 is already gone.
        send(out, "2", 4, "7=2\u000116=0\u0001");
        Map<Integer, String> logout = parse(readFrame(in));
        assertField("35", "5", logout, "over-window logout");
        String text = logout.get(58);
        if (text == null || !text.contains("outside the journal window")) {
            throw new AssertionError("logout text does not name the journal window: " + text);
        }
    }

    private static String orderBody(String clOrdId) {
        return "11=" + clOrdId + "\u0001"
            + "1=A-1\u0001"
            + "55=AAPL\u0001"
            + "54=1\u0001"
            + "38=100\u0001"
            + "40=2\u0001"
            + "44=100.25\u0001"
            + "59=0\u0001"
            + "60=" + SENDING_TIME + "\u0001";
    }

    private static void send(OutputStream out, String type, int seq, String body) throws Exception {
        String bodyFields = "35=" + type + "\u0001"
            + "49=CLIENT\u0001"
            + "56=DEEPBOOK\u0001"
            + "34=" + seq + "\u0001"
            + "52=" + SENDING_TIME + "\u0001"
            + body;
        String message = "8=FIX.4.4\u0001"
            + "9=" + bodyFields.length() + "\u0001"
            + bodyFields;
        int sum = 0;
        for (int index = 0; index < message.length(); index++) {
            sum += message.charAt(index);
        }
        message += String.format("10=%03d\u0001", sum % 256);
        out.write(message.getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private static String readFrame(InputStream in) throws Exception {
        StringBuilder frame = new StringBuilder();
        int total = -1;
        while (true) {
            int next = in.read();
            if (next < 0) {
                throw new AssertionError("connection closed before a complete frame: " + frame);
            }
            frame.append((char) next);
            if (total < 0) {
                int bodyLengthField = frame.indexOf("\u00019=");
                if (bodyLengthField >= 0) {
                    int end = frame.indexOf("\u0001", bodyLengthField + 3);
                    if (end >= 0) {
                        int bodyLength = Integer.parseInt(
                            frame.substring(bodyLengthField + 3, end));
                        int bodyStart = end + 1;
                        total = bodyStart + bodyLength + 7; // "10=ddd" + SOH
                    }
                }
            }
            if (total > 0 && frame.length() >= total) {
                return frame.substring(0, total);
            }
        }
    }

    private static Map<Integer, String> parse(String frame) {
        Map<Integer, String> fields = new LinkedHashMap<>();
        for (String field : frame.split("\u0001")) {
            int separator = field.indexOf('=');
            if (separator > 0) {
                fields.put(Integer.parseInt(field.substring(0, separator)),
                    field.substring(separator + 1));
            }
        }
        return fields;
    }

    private static void assertField(
        String tag, String expected, Map<Integer, String> fields, String what) {
        String actual = fields.get(Integer.parseInt(tag));
        if (!expected.equals(actual)) {
            throw new AssertionError(what + ": tag " + tag + " expected " + expected
                + ", got " + actual + " in " + fields);
        }
    }
}
