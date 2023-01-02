import re

def get_data_prvdr_cust_id(filepath):
        file_path_list = filepath.split('/')
        cust_id = file_path_list[-1].split('_')[0]
        return cust_id.strip()

def clean_digits(data):
    data = str(data)
    if data == '' or data == 0 or data == None:
        return 0
    data = re.sub('[^-.0-9]+', '', data)
    return data
    
def find_balance_occurences(amt_list, bal_list, l):
    balance_occurences = 0
    len_of_comm_list = len(bal_list)-2
    # l = cols_to_check if (cols_to_check > 0 and len_of_comm_list > cols_to_check) else len_of_comm_list
    for i in range(0, l):
        try:
            previous_balance = float(bal_list[i])
            balance = float(bal_list[i+1])
            previous_amount = float(amt_list[i])
            amount = float(amt_list[i+1])
            if round(previous_balance - previous_amount) == round(balance):
                balance_occurences += 1
            if round(previous_balance + amount) == round(balance):
                balance_occurences += 1
            if round(balance - amount) == round(previous_balance):
                balance_occurences += 1
            if round(previous_balance + previous_amount) == round(balance):
                balance_occurences += 1
            # print(balance_occurences)
        except (ValueError, IndexError):
            continue
    return balance_occurences
