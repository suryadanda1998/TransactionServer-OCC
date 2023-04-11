package transaction.server.account;

/**
 * class [Account] represents the account number and the balance in the account.
 * Account gets manipulated by transaction
 *
 * @author bhavana
 */
public class Account {

    // the account number
    int accountNumber;

    // the balance in the account
    int balance;

    /**
     * Creates a new Account object with the given account number and balance.
     *
     * @param accountNumber the account number
     * @param balance the balance in the account
     */
    public Account(int accountNumber, int balance) {
        this.accountNumber = accountNumber;
        this.balance = balance;
    }

    /**
     * Sets the balance of the account to the specified amount.
     *
     * @param balance the new balance for the account
     */
    public void setBalance(int balance) {
        this.balance = balance;
    }

    /**
     * Returns the account number associated with this account.
     *
     * @return the account number
     */
    public int getAccountNumber() {
        return accountNumber;
    }

    /**
     * Returns the current balance of the account.
     *
     * @return the current balance
     */
    public int getBalance() {
        return balance;
    }
}