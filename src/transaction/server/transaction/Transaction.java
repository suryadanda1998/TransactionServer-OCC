package transaction.server.transaction;

import transaction.server.TransactionServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * class [Transaction] manages read/write operations and captures the transaction logs
 *
 * @author dheeraj
 */
public class Transaction {

    // information to track the transaction using OCC
    int transactionID;
    int transactionNumber;
    int lastCommittedTransactionNumber;

    // storage for tentative data
    List<Integer> readSet = new ArrayList<>();
    Map<Integer, Integer> writeSet = new HashMap<>();

    // buffer to keep track of logs
    StringBuffer log = new StringBuffer("");

    /**
     * Constructor for the Transaction class.
     *
     * @param transactionID the ID of the transaction.
     * @param lastCommittedTransactionNumber the transaction number of the last committed transaction.
     */
    Transaction(int transactionID, int lastCommittedTransactionNumber) {
        this.transactionID = transactionID;
        this.lastCommittedTransactionNumber = lastCommittedTransactionNumber;
    }

    /**
     * Reads the balance of the given account number.
     *
     * @param accountNumber the account number to read the balance of.
     * @return the balance of the given account number.
     */
    public int read(int accountNumber) {
        Integer balance;

        // check if value to be read was written by the same transaction
        balance = writeSet.get(accountNumber);

        // if not, read the committed version of it
        if(balance == null) {
            balance = TransactionServer.accountManager.read(accountNumber);
        }

        if(!readSet.contains(accountNumber)) {
            readSet.add(accountNumber);
        }

        return balance;
    }

    /**
     * Writes the given new balance to the account with the given account number.
     *
     * @param accountNumber the account number to write the new balance to.
     * @param newBalance the new balance to write to the account.
     * @return the new balance that was written to the account.
     */
    public int write(int accountNumber, int newBalance) {

        int oldBalance = read(accountNumber);

        if(!writeSet.containsKey(accountNumber)) {
            writeSet.put(accountNumber, newBalance);
        }
        return oldBalance;
    }

    /**
     * Returns the read set of the transaction.
     *
     * @return the read set of the transaction.
     */
    public List<Integer> getReadSet() {
        return readSet;
    }

    /**
     * Returns the write set of the transaction.
     *
     * @return the write set of the transaction.
     */
    public Map<Integer, Integer> getWriteSet() {
        return writeSet;
    }

    /**
     * Sets the transaction number of the transaction.
     *
     * @param transactionNumber the transaction number to set.
     */
    public void setTransactionNumber(int transactionNumber) {
        this.transactionNumber = transactionNumber;
    }

    /**
     * Returns the transaction number of the transaction.
     *
     * @return the transaction number of the transaction.
     */
    public int getTransactionNumber() {
        return transactionNumber;
    }

    /**
     * Returns the ID of the transaction.
     *
     * @return the ID of the transaction.
     */
    public int getTransactionID() {
        return transactionID;
    }

    /**
     * Returns the transaction number of the last committed transaction.
     *
     * @return the transaction number of the last committed transaction.
     */
    public int getLastCommittedTransactionNumber() {
        return lastCommittedTransactionNumber;
    }

    /**
     * Adds log message to the log buffer
     *
     * @param logMessage log message
     */
    public void log(String logMessage) {
        log.append(logMessage).append("\n");
    }

    public StringBuffer getLog() {
        return log;
    }
}
