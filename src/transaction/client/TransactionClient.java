package transaction.client;

import transaction.exception.TransactionAbortedException;
import utils.PropertyHandler;

import java.io.IOException;
import java.util.Properties;

/**
 * The class [TransactionClient] is a client for a distributed transaction system.
 * It reads properties from a properties file and creates transaction executors for the system.
 * The transaction executors are run in separate threads.
 *
 * @author surya and bhavana
 */
public class TransactionClient implements Runnable {

    private int numberOfAccounts;

    private int initialBalance;

    private String serverIP;

    private int serverPort;

    private int numberOfTransactions;

    private Properties properties;

    /**
     * custom constructor reads the properties from properties file and initializes the data
     *
     * @param propertiesFile the properties file to read the configs
     */
    public TransactionClient(String propertiesFile) {
        try {
            properties = new PropertyHandler(propertiesFile);
        } catch (IOException e) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read properties file");
            e.printStackTrace();
            System.exit(1);
        }
        serverIP = properties.getProperty("SERVER_IP");

        if(serverIP == null) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read SERVER_IP property");
            System.exit(1);
        }

        try {
            serverPort = Integer.parseInt(properties.getProperty("SERVER_PORT"));
        } catch (NumberFormatException ex) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read Server Port");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            numberOfAccounts = Integer.parseInt(properties.getProperty("NUMBER_OF_ACCOUNTS"));
        } catch (NumberFormatException ex) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read Number Of Accounts");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            initialBalance = Integer.parseInt(properties.getProperty("INITIAL_BALANCE"));
        } catch (NumberFormatException ex) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read Initial Balance");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            numberOfTransactions = Integer.parseInt(properties.getProperty("NUMBER_OF_TRANSACTIONS"));
        } catch (NumberFormatException ex) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read Number of Transactions");
            ex.printStackTrace();
            System.exit(1);
        }


    }

    @Override
    public void run() {
        // Create the transaction server proxy
        TransactionServerProxy transactionServerProxy;

        // Create the transaction executors and run them in separate threads
        for (int i = 0; i < numberOfTransactions; i++) {
            int accountA = 0;
            int accountB = 0;

            while((accountA == 0 || accountB == 0) || accountA == accountB) {
                accountA = (int) (Math.random() * numberOfAccounts) + 1;
                accountB = (int) (Math.random() * numberOfAccounts) + 1;
            }
            int amount = 5;

            // Create the transaction server proxy
            transactionServerProxy = new TransactionServerProxy(serverIP, serverPort);

            TransactionExecutor transactionExecutor = new TransactionExecutor(accountA, accountB, amount, transactionServerProxy);
            Thread transactionThread = new Thread(transactionExecutor);
            transactionThread.start();
        }
    }

    public static void main(String[] args) {
        String propertiesFile = null;

        try {
            propertiesFile = args[0];
        } catch (ArrayIndexOutOfBoundsException ex) {
            propertiesFile = "resources/client.properties";
        }

        new Thread(new TransactionClient(propertiesFile)).start();
    }


    private class TransactionExecutor implements Runnable {

        private final int accountA;
        private final int accountB;
        private final int amount;
        private final TransactionServerProxy transactionServerProxy;

        public TransactionExecutor(int accountA, int accountB, int amount, TransactionServerProxy transactionServerProxy) {
            this.accountA = accountA;
            this.accountB = accountB;
            this.amount = amount;
            this.transactionServerProxy = transactionServerProxy;
        }

        @Override
        public void run() {
            while (true) {
                int transactionId = transactionServerProxy.openTransaction();
                System.out.println("Transaction with ID " + transactionId +" has opened between " + accountA + " and " + accountB);
                try {
                    int balanceA = transactionServerProxy.read(accountA);
                    System.out.println("Transaction with ID " + transactionId + " READ - Account A balance " + balanceA);
                    int deductedBalance = balanceA - amount;
                    transactionServerProxy.write(accountA, deductedBalance);
                    System.out.println("Transaction with ID " + transactionId + " WRITE - Account A balance " + deductedBalance);

                    int balanceB = transactionServerProxy.read(accountB);
                    System.out.println("Transaction with ID " + transactionId + " READ - Account B balance " + balanceB);

                    transactionServerProxy.write(accountB, balanceB + amount);
                    System.out.println("Transaction with ID " + transactionId + " WRITE - Account B balance " + (balanceB + amount));

                    int status = transactionServerProxy.closeTransaction();
                    if (status == TransactionServerProxy.TRANSACTION_COMMITTED) {
                        System.out.println("Transaction committed successfully.");
                        break;
                    } else {
                        System.out.println("Transaction aborted. Retrying...");
                    }
                } catch (TransactionAbortedException ex) {
                    System.out.println("Transaction aborted. Retrying...");
                }
            }
        }
    }
}
