package transaction.client;

import transaction.comm.Message;
import transaction.comm.MessageTypes;
import transaction.exception.TransactionAbortedException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class TransactionServerProxy implements MessageTypes {

    String host;
    int port;

    private Socket dbConnection;
    private ObjectOutputStream writeToNet;
    private ObjectInputStream readFromNet;
    private Integer transactionID = 0;

    /**
     * custom constructor
     *
     * @param host IP address of the Transaction Server
     * @param port port of the Transaction Server
     */
    TransactionServerProxy(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * opens a transaction
     *
     * @return int transaction ID
     */
    public int openTransaction() {
        try {
            dbConnection = new Socket(host, port);
            writeToNet = new ObjectOutputStream(dbConnection.getOutputStream());
            readFromNet = new ObjectInputStream(dbConnection.getInputStream());
        } catch (IOException ex) {
            System.out.println("[TransactionServerProxy.openTransaction] Error occurred when opening object streams");
            ex.printStackTrace();
        }

        try {
            writeToNet.writeObject(new Message(OPEN_TRANSACTION, null));
            transactionID = (Integer) readFromNet.readObject();
        } catch (IOException | ClassNotFoundException | NullPointerException ex) {
            System.out.println("[TransactionServerProxy.openTransaction] Error occurred when writing/reading messages");
            ex.printStackTrace();
        }
        return transactionID;
    }

    /**
     * Requests this transaction to be closed
     *
     * @return the status, i.e. either TRANSACTION_COMMITTED OR TRANSACTION_ABORTED
     */
    public int closeTransaction() {
        int returnStatus = TRANSACTION_COMMITTED;

        try {
            writeToNet.writeObject(new Message(CLOSE_TRANSACTION, null));
            returnStatus = (int) readFromNet.readObject();

            readFromNet.close();
            writeToNet.close();
            dbConnection.close();
        } catch (Exception ex) {
            System.out.println("[TransactionServerProxy.closeTransaction] Error occurred");
            ex.printStackTrace();
        }
        return returnStatus;
    }

    /**
     * Reading a value from account
     *
     * @param accountNumber account number to read
     *
     * @return the balance of the account
     *
     * @throws TransactionAbortedException
     */
    public int read(int accountNumber) throws TransactionAbortedException {
        Message message = new Message(READ_REQUEST, accountNumber);

        try {
            writeToNet.writeObject(message);
            message = (Message) readFromNet.readObject();
        } catch (Exception ex) {
            System.out.println("[TransactionServerProxy.read] Error occurred");
            ex.printStackTrace();
        }

        if(message.getType() == READ_REQUEST_RESPONSE) {
            return (int) message.getContent();
        } else {
            throw new TransactionAbortedException();
        }
    }

    /**
     * Writes the amount to account
     *
     * @param accountNumber account number to write
     * @param amount amount to be written
     *
     * @throws TransactionAbortedException
     */
    public void write(int accountNumber, int amount) throws TransactionAbortedException {
        Object[] content = new Object[]{accountNumber, amount};
        Message message = new Message(WRITE_REQUEST, content);
        try {
            writeToNet.writeObject(message);
            message = (Message) readFromNet.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            System.out.println("[TransactionServerProxy.write] Error occurred: IOException | ClassNotFoundException");
            ex.printStackTrace();
            System.err.println("\n\n");
        }

        if(message.getType() == TRANSACTION_ABORTED) {
            // transaction is aborted
            throw new TransactionAbortedException();
        }
    }
}
