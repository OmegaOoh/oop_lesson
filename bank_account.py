account_database = []


def create_account(num, type, name, init_balance):
    """
        Create Dictionary of Account Detail, Append to account database(list).
        :param num: Account ID
        :param type: Type of account
        :param name: Owner name
        :param init_balance: initial balance when create an account
    """
    index = search_account_db(num)
    if index == -1:
        account = {}
        account["account_number"] = num
        account["type"] = type
        account["account_name"] = name
        account["balance"] = init_balance
        account_database.append(account)
    else:
        print("Account", num, "already exists")


def delete_account(num):
    """
        Delete Account by Number of an account.
        if account is found delete that account.
        :param num: Account ID
    """
    index = search_account_db(num)
    if index != -1:
        print("Deleting account:", account_database[index]["account_number"])
        del account_database[index]
    else:
        print(num, "invalid account number; nothing to be deleted.")


def search_account_db(num):
    """
        Find index of an account that contains given account number.
        :param num: Account ID
    """
    for i in range(len(account_database)):
        if account_database[i]["account_number"] == num:
            return i
    return -1


def deposit(account_num, amount):
    """
        Add money into account's balance
        :param account_num: Account ID
        :param amount: Money to Deposit
    """
    index = search_account_db(account_num)
    if index != -1:
        print("Depositing", amount, "to", account_database[index]["account_number"])
        account_database[index]["balance"] += amount
    else:
        print(account_num, "invalid account number; no deposit action performed.")


def withdraw(account_num, amount):
    """
        Take out money out of account's balance if account have enough in its balance
        :param account_num: Account ID
        :param amount: Money to withdraw
    """
    index = search_account_db(account_num)
    if index != -1:
        if account_database[index]["balance"] >= amount:
            print("Withdrawing", amount, "from", account_database[index]["account_number"])
            account_database[index]["balance"] -= amount
        else:
            print("withdrawal amount", amount, "exceeds the balance of", account_database[index]["balance"], "for", account_num, "account.")
    else:
        print(account_num, "invalid account number; no withdrawal action performed.")


def show_account(account_num):
    """
        print out information of a bank account
        :param account_num: account number
    """
    index = search_account_db(account_num)
    if index != -1:
        print("Showing details for", account_database[index]["account_number"])
        print(account_database[index])
    else:
        print(account_num, "invalid account number; nothing to be shown for.")


create_account("0000", "saving", "David Patterson", 1000)
create_account("0001", "checking", "John Hennessy", 2000)
create_account("0003", "saving", "Mark Hill", 3000)
create_account("0004", "saving", "David Wood", 4000)
create_account("0004", "saving", "David Wood", 4000)
print(account_database)
show_account('0003')
deposit('0003', 50)
show_account('0003')
withdraw('0003', 25)
show_account('0003')
delete_account('0003')
show_account('0003')
deposit('0003', 50)
withdraw('0001', 6000)
