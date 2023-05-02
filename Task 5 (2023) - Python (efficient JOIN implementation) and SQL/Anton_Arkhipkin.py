# the only additional library I decided to use - is decimal (BUT it is still standard =) )
# because it is the worst thing to use for money
# example: float('0.1') + float('0.2')

from decimal import Decimal

# we are working with the fixed CSV structure, so we could just refer the columns in the string by their number. 
# But to make it more flexible and support order/count of columns changes - we will do it a bit dynamic based on headers
        
def get_column_index(headers, column_name):
    for i in range(len(headers)):
        if headers[i].lower()==column_name.lower(): #column names as not case censitive
            return i

class TabularCSV:
    def __init__(self, **kwargs):
        self.filename = kwargs['filename']  
        self.sep = kwargs['sep']        
        with open(self.filename, "r") as f:
            first_line = f.readline().strip('\n')
            self.headers = first_line.strip('\n').split(sep)

class PerformTransactionsAgg:
    def __init__(self, **kwargs):
        self.users_headers = kwargs['users_tabular'].headers
        self.users_filename = kwargs['users_tabular'].filename
        self.transactions_headers = kwargs['transactions_tabular'].headers
        self.transactions_filename = kwargs['transactions_tabular'].filename
        self.active_users_set = self.get_active_users_set()
        self.transactions_agg = self.iterate_transactions()
        
    # users functions    
    def check_user_active(self, user_data):
        return user_data[get_column_index(self.users_headers, 'is_active')] == 'True' 

    def get_active_users_set(self):
        active_users = []
        with open(self.users_filename, "r") as f:
            next(f) # skiping headers
            for line in f:
                data = line.strip('\n').split(sep) #split line to list of separate values
                if self.check_user_active(data):
                    active_users.append(data[0])
        active_users_set = set(active_users)
        return active_users_set
    
    # transactions functions
    def new_transaction(self, transaction_category_id):
        transaction_template = {
            "transaction_category_id": transaction_category_id,
            "total_amount": Decimal('0.0'),
            "num_users": 0,
            "users_set": set([]),
        }
        return transaction_template
        
    def check_transaction_not_blocked(self, transaction_data):
        return transaction_data[get_column_index(self.transactions_headers, 'is_blocked')] == 'False'  
        
    def check_transaction_user_active(self, transaction_data):
        # return user_data[1].replace('\n','') == 'True' 
        return transaction_data[get_column_index(self.transactions_headers, 'user_id')] in self.active_users_set
    
    def update_transaction_category_metrics(self, transactions_agg, data_transaction):
        for cat in transactions_agg:
            if cat['transaction_category_id'] == data_transaction[get_column_index(self.transactions_headers, 'transaction_category_id')]:
                cat['total_amount'] += Decimal(data_transaction[get_column_index(self.transactions_headers, 'transaction_amount')])
                cat['users_set'].add(data_transaction[get_column_index(self.transactions_headers, 'user_id')])
                cat['num_users'] = len(cat['users_set'])
        return transactions_agg
        
    def iterate_transactions(self):
        transactions_agg = []
        with open(self.transactions_filename, "r") as f:
            next(f) # skiping headers
            for line in f:
                data = line.strip('\n').split(sep) # split line to list of separate values
                if self.check_transaction_not_blocked(data):
                    if self.check_transaction_user_active(data):
                        transaction_category_id = data[get_column_index(self.transactions_headers, 'transaction_category_id')]
                        if not any(t['transaction_category_id'] == transaction_category_id for t in transactions_agg):
                            transactions_agg.append(self.new_transaction(transaction_category_id))
                        transactions_agg = self.update_transaction_category_metrics(transactions_agg, data)
        return transactions_agg
        
        

if __name__ == '__main__':            
    sep = ","
    users_tabular = TabularCSV(filename="users.csv", sep=sep) 
    transactions_tabular = TabularCSV(filename="transactions.csv", sep=sep) 

    TransactionsAgg = PerformTransactionsAgg(users_tabular=users_tabular, transactions_tabular=transactions_tabular)
    output_list = TransactionsAgg.transactions_agg
    output_list.sort(key=lambda k: k['total_amount'], reverse=True)

    # print output
    print("transaction_category_id \tsum_amount \tnum_users")
    print("===== \t\t\t\t===== \t\t=====")
    for x in output_list:
        print(f"{x['transaction_category_id']}\t\t\t\t{x['total_amount']}\t\t{x['num_users']}")