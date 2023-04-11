package transaction.server.account;

import java.util.ArrayList;
import java.util.List;

/**
 * class [AccountManager] maintains a list of accounts and provides methods to read and write account balance.
 *
 * @author bhavana and surya
 */
public class AccountManager {

    // list of accounts
    List<Account> accounts = new ArrayList<>();

    // default constructor
    public AccountManager() {
        this.accounts = new ArrayList<>();
    }

    /**
     * Custom constructor that initializes the list of accounts with a given number of accounts and an initial balance
     * for each account.
     *
     * @param numberOfAccounts The number of accounts to create.
     * @param initialBalance The initial balance for each account.
     */
    public AccountManager(int numberOfAccounts, int initialBalance) {
        for(int i = 1; i <= numberOfAccounts; i++) {
            Account account = new Account(i, initialBalance);
            this.accounts.add(account);
        }
    }

    /**
     * Returns the balance of the account with the specified account number.
     *
     * @param accountNumber The account number of the account to get the balance for.
     * @return The balance of the account, or null if the account is not found.
     */
    public Integer read(int accountNumber) {
        Account account = getAccountByAccountNumber(accountNumber);
        if(account != null) {
            return account.getBalance();
        } else {
            System.err.println("Invalid Account; Account# " + accountNumber + " doesn't exists");
            System.exit(1);
        }
        return -1;
    }

    /**
     * Writes the specified balance to the account with the specified account number.
     *
     * @param accountNumber The account number of the account to write the balance to.
     * @param balance The balance to write to the account.
     * @return True if the write was successful, false otherwise.
     */
    public boolean write(int accountNumber, int balance) {
        Account account = getAccountByAccountNumber(accountNumber);
        if(account != null) {
            account.setBalance(balance);
            return true;
        } else {
            System.err.println("Invalid Account; Account# " + accountNumber + " doesn't exists");
            System.exit(1);
        }
        return false;
    }

    /**
     * Helper method that returns the account with the specified account number.
     *
     * @param accountNumber The account number of the account to get.
     * @return The account with the specified account number, or null if the account is not found.
     */
    private Account getAccountByAccountNumber(int accountNumber) {
        for(Account account : accounts) {
            if(account.getAccountNumber() == accountNumber) {
                return account;
            }
        }
        // handle it saying AccountNotFound or Invalid account
        return null;
    }
}
