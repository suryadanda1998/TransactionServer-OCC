package transaction.server;

import transaction.server.account.AccountManager;
import transaction.server.transaction.TransactionManager;
import utils.NetworkUtilities;
import utils.PropertyHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Properties;

/**
 * The class [TransactionServer] is the server-side implementation of a transaction processing system.
 * It listens for incoming client connections and handles client requests by calling the appropriate
 * methods in the AccountManager and TransactionManager classes.
 *
 * @author surya
 */
public class TransactionServer implements Runnable {

    // manages account data for the transaction processing system.
    public static AccountManager accountManager;

    // manages transactions for the transaction processing system.
    public static TransactionManager transactionManager;

    // server connection
    static ServerSocket transactionServerSocket;

    public static int numberOfAccounts;

    // fetch serverIP
    String serverIP = NetworkUtilities.getMyIP();

    /**
     * The keepGoing variable is a flag that is used to control the execution of the server.
     */
    static boolean keepGoing = true;

    public TransactionServer(String propertiesFile) {
        Properties properties = null;

        try {
            properties = new PropertyHandler(propertiesFile);
        } catch (IOException e) {
            System.out.println("[TransactionServer.TransactionServer] didn't find the properties file");
            System.exit(1);
        }

        // get number of accounts
        numberOfAccounts = Integer.parseInt(properties.getProperty("NUMBER_OF_ACCOUNTS"));

        // get initial balance
        int initialBalance = Integer.parseInt(properties.getProperty("INITIAL_BALANCE"));

        // create account manager
        accountManager = new AccountManager(numberOfAccounts, initialBalance);
        System.out.println("[TransactionServer.TransactionServer] Account Manager created");

        // create transaction manager
        transactionManager = new TransactionManager();

        System.out.println("[TransactionServer.TransactionServer] Transaction Manager created");

        try {
            // get port
            int port = Integer.parseInt(properties.getProperty("PORT"));

            // create server socket
            transactionServerSocket = new ServerSocket(port, 50, InetAddress.getByName(serverIP));
        } catch (IOException e) {
            System.out.println("[TransactionServer.TransactionServer] couldn't create server socket");
            System.exit(1);
        }
    }

    /**
     * The run method is called when the TransactionServer object is executed in a separate thread.
     * It listens for incoming client connections and handles client requests by calling the runTransaction
     * method of the TransactionManager class.
     */
    @Override
    public void run() {
        try {
            while (true) {
                transactionManager.runTransaction(transactionServerSocket.accept());
            }
        } catch (IOException e) {
            System.err.println("[TransactionServer.run] Error while creating the socket");
        }
    }

    // entry point for the transaction server
    public static void main(String[] args) {
        String propertiesFile;

        try {
            propertiesFile = args[0];
        } catch (ArrayIndexOutOfBoundsException ex) {
            propertiesFile = "resources/server.properties";
        }
        new Thread(new TransactionServer(propertiesFile)).start();
    }
}
