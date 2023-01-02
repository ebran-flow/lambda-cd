from mailer import send_mail, send_simple_mail
from mtn_common import find_balance_occurences, get_data_prvdr_cust_id
import boto3
import pandas as pd
from datetime import datetime as dt
from trp import Document
import re
import io
import os
import sqlalchemy
import pymysql
import unicodedata
import numpy as np
import json
import uuid
from urllib.parse import unquote_plus
from dotenv.main import load_dotenv
from common import invoke_next_lambda, df_to_sql, connect_to_database_engine, print_error 

def lambda_handler(event, context):
    # FUNCTIONS FOR EXCEL CLEANUP
    def is_date(data):
        if len(data)>=10:
            data = data[0:10]
            try:
                data = dt.strptime(data, '%d/%m/%Y')
                return True
            except ValueError:    
                try:
                    data = dt.strptime(data, '%Y-%m-%d')
                    return True
                except Exception:
                    return False
        return False    
    def is_negative(data):
        try:
            data = int(data)
        except ValueError:
            try:
                data = float(data)
            except ValueError:
                return False
        
        ret = True if data < 0 else False
        return ret

    def is_id(data):
        if isinstance(data, str):
            data = data.rstrip()
            if data.isnumeric():
                if len(data) == 11:
                    return True
            else:
                return False
        else:
                return False
    def is_type(data):
        # data = change_txn_type_format(data)
        if ('CASH_IN' in data or 'CASH_OUT' in data or 'TRANSFER' in data or
            "CUSTOM_WITHHOLDING_TAX" in data or 'EXTERNAL' in data or 'COMMISSIONING' in data):
            return True
        else:
            return False
    def remove_time(date):
        date = re.sub('[^-/0-9]+', '', date)
        date = date[0:10]
        return date
    def remove_dirty_date(data):
        try:
            dt.strptime(data, '%d/%m/%Y')
            return data
        except ValueError:
            try:
                dt.strptime(data, '%Y-%m-%d')
                return data
            except ValueError:
                return np.nan

    def clean_text(data):
        data = re.sub('[^:/@A-Za-z0-9]+', '', data)
        data = re.sub(r'^.*?F', 'F', data)
        text = data
        if len(data)>=16:
            text = data[0:16]
        return text
    def clean_digits(data):
        if data == '' or data == 0 or data == None:
            return 0
        data = re.sub('[^-.0-9]+', '', data)
        return data
    def check_first_row(sheet):
        row1 = sheet.iloc[0,:]
        for value in row1:
            if isinstance(value, str):
                if "Id" in value or "Type" in value:
                    sheet = sheet.drop(index='1', axis=0)
                    break
        return sheet
    def remove_unwanted_rows(sheet):
        # print("REMOVE: ", sheet)
        for i in range(1, 4):
            row = sheet.iloc[0,:]
            for value in row:
                if isinstance(value, str):
                    value = value.upper()
                    if 'ID' in value or 'TYPE' in value or 'SUBSCRIBER' in value or 'FID' in value:
                        # sheet = sheet.drop(index=i, axis=0)
                        sheet.drop(index=i, axis=0, inplace=True)
                        i = i+1
                        break
        # return sheet
    def rmv_digit_and_latin_txt(text):
        text = ''.join([i for i in text if not i.isdigit()])
        data = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return data
    def change_txn_type_format(text):
        text = text.upper()
        text = text.rstrip()
        text = re.sub('[^A-Z]+', '_', text)
        return text
    def remove_additional_minus(text):
        if isinstance(text, str):
            count = text.count('-')
            if count > 1:
                text = text.replace('-', '', count-1)
            return text
        else:
            return text
    def remove_extra_space(data):
        if isinstance(data, str):
            data = data.rstrip()
            return data
        else:
            return data
    def clean_table(sheet):
        remove_unwanted_rows(sheet)
        sheet['txn_date'] = sheet['txn_date'].replace(r'^\s*$', np.NaN, regex=True)
        sheet['txn_date'] = sheet['txn_date'].fillna(method='ffill').fillna(method='bfill')
        # sheet = check_first_row(sheet)
        sheet['txn_date'] = sheet['txn_date'].apply(remove_time)
        sheet['txn_date'] = sheet['txn_date'].apply(remove_dirty_date)
        sheet['txn_date'] = sheet['txn_date'].fillna(method='ffill').fillna(method='bfill')
        # sheet['from'] = sheet['from'].apply(clean_text)
        # sheet['to'] = sheet['to'].apply(clean_text)
        sheet['balance'] = sheet['balance'].apply(clean_digits)
        sheet['amount'] = sheet['amount'].apply(clean_digits)
        sheet['comms'] = 0
        sheet['txn_type'] = sheet['txn_type'].apply(rmv_digit_and_latin_txt)
        # sheet['txn_type'] = sheet['txn_type'].apply(change_txn_type_format)
        sheet['transform_status'] = "NOT_DONE"
        id = uuid.uuid4()
        sheet['export_run_id'] = f'{id}'
        # sheet = sheet.apply(remove_extra_space)
        return sheet, id

    def rename_label(old_label, new_label):
        # index = int(index)
        # column = index + 1
        # label = column_index[f'{column}']
        sheet.rename(columns = {old_label: new_label}, inplace=True)

    MAX_COLS_TO_CHECK = 0
    def find_column_names(sheet):
        has_id, has_date, has_type, has_amount= False, False, False, False

        sheet.replace('', np.nan, inplace=True)
        sheet.dropna(thresh=2 ,axis=0, inplace=True)
        sheet.replace(np.nan, '', inplace=True)

        ## txn_date, txn_type, amount
        for column in sheet.columns:
            this_column = sheet[column].to_list()
            len_of_column = len(this_column) - 1
            l = MAX_COLS_TO_CHECK if (MAX_COLS_TO_CHECK > 0 and len_of_column > MAX_COLS_TO_CHECK) else len_of_column

            id_occurences, date_occurences, type_occurences, amount_occurences = 0, 0, 0, 0
            for i in range(0, l):
                cell_data = this_column[i]
                if is_id(data=cell_data):
                    id_occurences += 1
                if is_date(data=cell_data):
                    date_occurences += 1
                if is_type(data=cell_data):
                    type_occurences += 1
                if is_negative(data=cell_data):
                    amount_occurences += 1
            occurences = {}
            occurences['txn_id'] = id_occurences
            occurences['txn_date'] = date_occurences
            occurences['txn_type'] = type_occurences
            occurences['amount'] = amount_occurences
            occurences['none'] = 1
            maximum_occurence = max(occurences,  key=occurences.get)
            if maximum_occurence != 'none':
                if maximum_occurence == 'txn_id': has_id = True
                elif maximum_occurence == 'txn_date': has_date = True
                elif maximum_occurence == 'txn_type': has_type = True
                elif maximum_occurence == 'amount': has_amount = True
   
                rename_label(old_label=column, new_label=maximum_occurence)
        ## balance
        # print(has_id, has_date, has_type, has_amount)
        if has_id and has_date and has_type and has_amount:
        # if True:
            return find_balance_column(sheet)
        else:
            raise Exception('ID or DATE or TYPE or AMOUNT column not found')
            # print("GELLO")

    def find_balance_column(sheet):
        has_balance = False
        remove_unwanted_rows(sheet)
        sheet['amount'] = sheet['amount'].apply(clean_digits)
        sheet['amount'] = sheet['amount'].apply(remove_additional_minus)
        amount_list = sheet['amount'].to_list()
        sheet['txn_type'] = sheet['txn_type'].apply(rmv_digit_and_latin_txt)
        sheet['txn_type'] = sheet['txn_type'].apply(lambda x: x.replace(' ', ''))
        txn_type_list = sheet['txn_type'].to_list()    
        print(txn_type_list)    
        
        for column in sheet.columns: 
            if column=="txn_id" or column=="txn_date" or column=="txn_type" or column=="amount":
                continue
            balance_occurences = 0
            txn_bal_list, comm_bal_list = [], []
            txn_amt_list, comm_amt_list = [], []
            
            
            sheet['temp_column'] = sheet[column].apply(clean_digits)
            balance_list = sheet['temp_column'].to_list()
            len_of_column = len(balance_list)
            for i in range(0, len_of_column):
                txn_type = txn_type_list[i]
                bal = balance_list[i]
                amt = amount_list[i]
                if "COMMISSIONING" in txn_type or "CUSTOM_WITHHOLDING_TAX" in txn_type or "TRANSFER" in txn_type: 
                    comm_bal_list.append(bal)
                    comm_amt_list.append(amt) 
                elif "CASH_IN" in txn_type or "CASH_OUT" in txn_type or "PAYMENT" in txn_type:
                    txn_bal_list.append(bal)
                    txn_amt_list.append(amt) 
            
            print("BALANCE AND AMOUNT")
            print(comm_amt_list)
            print(comm_bal_list)
            print(txn_amt_list)
            print(txn_bal_list)

            l = MAX_COLS_TO_CHECK if (MAX_COLS_TO_CHECK > 0 and len_of_column > MAX_COLS_TO_CHECK) else len_of_column
            comm_bal_occs = find_balance_occurences(comm_amt_list, comm_bal_list, l)
            txn_bal_occs = find_balance_occurences(txn_amt_list, txn_bal_list, l)
            
            balance_occurences = comm_bal_occs + txn_bal_occs
            print("COMMS BALANCE: ", comm_bal_occs)
            print("TXN BALANCE: ", txn_bal_occs)
            print("BALANCE OCCURENCES: ", balance_occurences)
            if balance_occurences >= 10:
                # print("LEN OF COLUMN", len_of_column)
                # print("NO OF BALANCES:", balance_occurences)
                has_balance = True  
                rename_label(old_label=column, new_label='balance')
                break
        
        if has_balance:
            sheet = sheet.drop(columns = ['temp_column'], axis=1)
            sheet = sheet[['txn_id', 'txn_date', 'txn_type', 'data_prvdr_cust_id', 'amount', 'balance']].copy()
            return sheet
        else:
            ## SEND EMAIL
            raise Exception('BALANCE column not found')
            # exit()

    def read_file_from_s3(key):
        load_dotenv()
        bucket_name = os.environ.get('BUCKET_NAME')
        aws_access_key_id=os.environ.get('ACCESS_KEY')
        aws_secret_access_key=os.environ.get('SECRET_KEY')
        region_name= os.environ.get('REGION_NAME')

        print(bucket_name, aws_access_key_id, aws_secret_access_key, region_name, key)
        # name = os.path.splitext(key)[0]
        # extension = os.path.splitext(key)[1]
        # if extension == '.pdf':
        #     convert_to_image(key, bucket_name)
            
        textract = boto3.client(
            'textract',
            aws_access_key_id=f"{aws_access_key_id}", 
            aws_secret_access_key=f"{aws_secret_access_key}",
            region_name=f"{region_name}") 
            
        return textract.analyze_document(
                        Document={
                            "S3Object": { 
                                "Bucket": f"{bucket_name}",
                                "Name": f"{key}"
                            }
                        },
                        FeatureTypes=["TABLES"])

        
      
    #MAIN FUNCTION
    # global key, sheet
    key = 0
    sheet = pd.DataFrame()
    try:
    # key = 0
        if event:
            key = event['Records'][0]['s3']['object']['key']
            key = unquote_plus(key)
            response = read_file_from_s3(key)
            
        else:
            # Code to read local file comes here
            response = context
            key = context
            # raise Exception('KEY ERROR')
            pass
        
        key = key + '.xlsx'
        doc = Document(response)
        # sheet = pd.DataFrame()
        # column_index = {'1': 'txn_id', '2': 'txn_date', '3': 'txn_type', '4': 'from', '5': 'from_name',
        #                 '6': 'to', '7': 'to_name', '8': 'amount', '9': 'balance', '10': 'currency'}
        column_index = {'1': 'FIRST', '2': 'SECOND', '3': 'THIRD', '4': 'FOURTH', '5': 'FIFTH', '6': 'SIXTH', '7': 'SEVENTH',
                        '8': 'EIGHT', '9': 'NINTH', '10': 'TENTH', '11': 'ELEVENTH', '12': 'TWELFTH', '13': 'THIRTEENTH'}
        # for page in doc.pages:
            # Print tables
        page = doc.pages[0]
        for table in page.tables:
            for r, row in enumerate(table.rows):
                for c, cell in enumerate(row.cells):
                    col = c + 1
                    row = r + 1
                    column = column_index[f"{col}"]
                    sheet.at[row, column] = cell.text

        sheet['data_prvdr_cust_id'] = get_data_prvdr_cust_id(key)
        print(sheet)
        sheet = find_column_names(sheet)
        final_df, run_id = clean_table(sheet)
        print(final_df.head(n=60).to_string(index=False))

        db_con = connect_to_database_engine('STMT')
        exp_arr = df_to_sql(final_df, 'mtn_cust_acc_stmts', db_con)
        print_error()
        if exp_arr:
            send_mail("ERROR IN MTN LOAD", key, exp_arr)
        data = {'run_id': f'{run_id}', 'key': f'{key}', 'sheet_format': 2}
        invoke_next_lambda(data, 'mtn_transform')
        if exp_arr:
                send_mail("ERROR IN MTN LOAD", key, exp_arr)
                
    except Exception as error:
        header = "ERROR IN MTN LOAD"
        message = getattr(error, "message", repr(error))
        print(message)
        if key!=0 and not sheet.empty:
        # if True:
            error_df = [{"df": sheet, "exception": message}]
            # print("KEY FOUND",key, message)
            send_mail(header, key, error_df)    
        else:
            # key = 'flow.xlsx'
            # print("KEY NOT FOUND",key, message)
            send_simple_mail(header, message)



    