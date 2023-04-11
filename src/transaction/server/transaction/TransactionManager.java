package transaction.server.transaction;

import transaction.comm.MessageTypes;
import transaction.server.TransactionServer;

import java.net.Socket;
import java.util.*;

/**
 * Class [TransactionManager] represents singleton transaction manager
 *
 * @author surya and dheeraj
 */
public class TransactionManager implements MessageTypes {

    // counter for transaction IDs
    static int transactionIdCounter = 0;

    // list of transactions
    static final List<Transaction> runningTransactions = new ArrayList<>();
    static final List<Transaction> abortedTransactions = new ArrayList<>();
    static final Map<Integer, Transaction> committedTransactions = new HashMap<>();

    // counter for transaction numbers
    static int transactionNumberCounter = 0;

    // Default constructor
    public TransactionManager() {}

    /**
     * Gets the list of aborted transactions
     *
     * @return the list of aborted transactions
     */
    public static List<Transaction> getAbortedTransactions() {
        return abortedTransactions;
    }

    /**
     * Run the transaction for an incoming client request
     *
     * @param client socket object representing connection to client
     */
    public synchronized void runTransaction(Socket client) {
        (new TransactionManagerWorker(client)).start();
    }

    /**
     * Validates the transaction by following OCC principle
     *
     * @param transaction Transaction to be validated
     *
     * @return a flag indicating whether the validation is successful or not
     */
    public static boolean validateTransaction(Transaction transaction) {
        int transactionNumber;
        int lastCommittedTransactionNumber;
        int transactionNumberIndex;

        List<Integer> readSet = transaction.getReadSet();
        Map<Integer, Integer> committedTransactionWriteSet;
        Iterator<Integer> readSetIterator;

        Transaction committedTransaction;
        Integer committedAccount;

        // assign transaction number
        transactionNumber = ++transactionNumberCounter;
        transaction.setTransactionNumber(transactionNumber);

        // get last committed transaction number; the number before this transaction is started
        lastCommittedTransactionNumber = transaction.getLastCommittedTransactionNumber();

        for(transactionNumberIndex = lastCommittedTransactionNumber+1; transactionNumberIndex < transactionNumber; transactionNumberIndex++) {

            // get transaction details which is already committed
            committedTransaction = committedTransactions.get(transactionNumberIndex);

            // make sure transaction with transactionNumberIndex was not aborted before
            if(committedTransaction != null) {

                // check our own read set against the write set of checked transaction
                committedTransactionWriteSet = committedTransaction.getWriteSet();

                readSetIterator = readSet.iterator();

                while(readSetIterator.hasNext()) {

                    // is an account in the read set part of the write set in the committedTransaction?
                    committedAccount = readSetIterator.next();

                    if(committedTransactionWriteSet.containsKey(committedAccount)) {
                        transaction.log("[TransactionManager.validateTransaction] Transaction #" +
                                transaction.getTransactionID() + "failed: r/w conflict of an Account #" + committedAccount
                                + " with Transaction #" + committedTransaction.getTransactionID());

                        return false;
                    }
                }
            }
        }

        transaction.log("[TransactionManager.validateTransaction] Transaction #" + transaction.getTransactionID() +
                " successfully validated");
        return true;
    }

    /**
     * writes the write set of a transaction into the operational data
     *
     * @param transaction Transaction to be written
     */
    public static void writeTransaction(Transaction transaction) {
        Map<Integer, Integer> transactionWriteSet = transaction.getWriteSet();
        int account;
        int balance;

        // get all the entries of a write set
        for (Map.Entry<Integer, Integer> entry : transactionWriteSet.entrySet()) {
            account = entry.getKey();
            balance = entry.getValue();

            TransactionServer.accountManager.write(account, balance);

            transaction.log("[TransactionManager.writeTransaction] Transaction #" + transaction.getTransactionID() +
                    " is written");
        }
    }
}
