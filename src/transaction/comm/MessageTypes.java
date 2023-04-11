package transaction.comm;

import utils.TerminalColors;

public interface MessageTypes extends TerminalColors {
    public static final int OPEN_TRANSACTION = 1;
    public static final int CLOSE_TRANSACTION = 2;
    public static final int READ_REQUEST = 3;
    public static final int WRITE_REQUEST = 4;
    public static final int READ_REQUEST_RESPONSE = 5;
    public static final int TRANSACTION_COMMITTED = 6;
    public static final int TRANSACTION_ABORTED = 7;
}
