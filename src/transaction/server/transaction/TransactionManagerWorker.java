package transaction.server.transaction;

import transaction.comm.Message;
import transaction.comm.MessageTypes;
import transaction.server.TransactionServer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static transaction.server.transaction.TransactionManager.committedTransactions;
import static transaction.server.transaction.TransactionManager.runningTransactions;

/**
 * class [TransactionManagerWorker] is responsible for handling network communication for
 * a particular transaction
 *
 * @author surya and dheeraj
 */
public class TransactionManagerWorker extends Thread implements MessageTypes {

    // network communication related fields
    Socket client;
    ObjectInputStream readFromNet;
    ObjectOutputStream writeToNet;
    Message message;

    // transaction related properties
    Transaction transaction = null;
    int accountNumber = 0;
    int balance = 0;

    // flag for jumping out of while loop after this transaction closed
    boolean keepGoing = true;

    // the constructor opens up the network channels to do operations on the data received
    TransactionManagerWorker(Socket client) {
        this.client = client;

        try {
            // open the streams
            readFromNet = new ObjectInputStream(client.getInputStream());
            writeToNet = new ObjectOutputStream(client.getOutputStream());
        } catch (IOException ex) {
            System.out.println("[TransactionManagerWorker.run] Failed to open object streams");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void run() {
        // loop is left when transaction closes
        while(keepGoing) {
            try {
                message = (Message) readFromNet.readObject();
            } catch (IOException | ClassNotFoundException | NullPointerException ex) {
                System.out.println("[TransactionManagerWorker.run] Message could not be read from object streams");
                ex.printStackTrace();
                System.exit(1);
            }

            // processing message
            switch (message.getType()) {
                // =====================================================================================================
                case OPEN_TRANSACTION:
                // =====================================================================================================
                    synchronized (runningTransactions) {
                        // create new transaction, assign a new transaction ID, also pass in the last assigned transaction number
                        transaction = new Transaction(++TransactionManager.transactionIdCounter,
                                TransactionManager.transactionNumberCounter);
                        // add the transaction to the list of current running transactions
                        runningTransactions.add(transaction);
                    }

                    // send communication back to the client
                    try {
                        writeToNet.writeObject(transaction.getTransactionID());
                        System.out.println("Transaction with ID " + transaction.getTransactionID() + " has started");
                    } catch (IOException e) {
                        System.err.println("[TransactionManagerWorker.run] OPEN_TRANSACTION #"
                                + transaction.getTransactionID() + " - Error writing transactionID to the client");
                    }

                    transaction.log("[TransactionManagerWorker.run] " + OPEN_COLOR + "OPEN_TRANSACTION" + RESET_COLOR
                            + " #" + transaction.getTransactionID());
                    break;

                // =====================================================================================================
                case CLOSE_TRANSACTION:
                // =====================================================================================================
                    synchronized (runningTransactions) {

                        // remove the transaction from the list of current running transactions
                        runningTransactions.remove(transaction);

                        if(TransactionManager.validateTransaction(transaction)) {

                            // add the transaction to the committed transactions
                            committedTransactions.put(transaction.getTransactionNumber(), transaction);

                            // write data to the operational data
                            TransactionManager.writeTransaction(transaction);

                            // send communication back to the client
                            try {
                                writeToNet.writeObject((Integer) TRANSACTION_COMMITTED);
                                System.out.println("Transaction with ID " + transaction.getTransactionID() + " has closed");
                            } catch (IOException e) {
                                System.err.println("[TransactionManagerWorker.run] CLOSE_TRANSACTION #"
                                        + transaction.getTransactionID() + " - Error writing transactionID to the client");
                            }

                            transaction.log("[TransactionManagerWorker.run] " + COMMIT_COLOR + "CLOSE_TRANSACTION" + RESET_COLOR
                                    + " #" + transaction.getTransactionID() + " - COMMITTED");
                        } else {
                            try {
                                writeToNet.writeObject((Integer) TRANSACTION_ABORTED);
                                System.out.println("Transaction with ID " + transaction.getTransactionID() + " has aborted");
                            } catch (IOException e) {
                                System.err.println("[TransactionManagerWorker.run] CLOSE_TRANSACTION #"
                                        + transaction.getTransactionID() + " - Error writing transactionID to the client");
                            }

                            transaction.log("[TransactionManagerWorker.run] " + ABORT_COLOR + "CLOSE_TRANSACTION" + RESET_COLOR
                                    + " #" + transaction.getTransactionID() + " - ABORTED");
                        }
                    }
                    keepGoing = false;
                    System.out.println(transaction.getLog());

                    int totalSum = 0;

                    for(int i=1; i <= TransactionServer.numberOfAccounts; i++) {
                        System.out.println("[TransactionManagerWorker.run] " + "Account #" + i + " balance is " + TransactionServer.accountManager.read(i));
                        totalSum += TransactionServer.accountManager.read(i);
                    }

                    System.out.println("[TransactionManagerWorker.run] " + "The total sum of all the accounts after " +
                            "transaction #" + transaction.getTransactionID() + " is " + totalSum);

                    break;
                // =====================================================================================================
                case READ_REQUEST:
                // =====================================================================================================
                    // read request
                    accountNumber = (Integer) message.getContent();
                    balance = transaction.read(accountNumber);

                    // send communication back to the client
                    try {
                        writeToNet.writeObject(new Message(READ_REQUEST_RESPONSE, balance));
                        System.out.println("Transaction with ID " + transaction.getTransactionID() + " READ - Account Number #" + accountNumber + " with balance " + balance);
                    } catch (IOException e) {
                        System.err.println("[TransactionManagerWorker.run] READ_TRANSACTION #"
                                + transaction.getTransactionID() + " - Error writing Account Balance to the client");
                    }

                    transaction.log("[TransactionManagerWorker.run] " + READ_COLOR + "READ_TRANSACTION" + RESET_COLOR
                            + " #" + transaction.getTransactionID() + " - READ");
                    break;
                // =====================================================================================================
                case WRITE_REQUEST:
                // =====================================================================================================
                    // write request
                    Object[] content = (Object[]) message.getContent();

                    // fetch account number and balance from the Message
                    accountNumber = (int) content[0];
                    balance = (int) content[1];

                    balance = transaction.write(accountNumber, balance);

                    // send communication back to the client
                    try {
                        writeToNet.writeObject(new Message(READ_REQUEST_RESPONSE, balance));
                        System.out.println("Transaction with ID " + transaction.getTransactionID() + " WRITE - Account Number #" + accountNumber + " with balance " + balance);
                    } catch (IOException e) {
                        System.err.println("[TransactionManagerWorker.run] READ_TRANSACTION #"
                                + transaction.getTransactionID() + " - Error writing WRITE RESPONSE to the client");
                    }

                    transaction.log("[TransactionManagerWorker.run] " + WRITE_COLOR + "WRITE_TRANSACTION" + RESET_COLOR
                            + " #" + transaction.getTransactionID() + " - WRITE");
                    break;
            }
        }
    }
}
