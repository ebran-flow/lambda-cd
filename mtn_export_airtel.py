import pdfplumber
# from mtn_transform_airtel import lambda_handler2
import pandas as pd
import numpy as np
from urllib.parse import unquote_plus
import uuid
from common import chunkit, clean_df_db_dups, connect_to_database_engine, df_to_sql, get_obj_from_s3, invoke_next_lambda, print_error
from mtn_common import clean_digits, get_data_prvdr_cust_id
from datetime import datetime as dt
import re
import traceback
from io import BytesIO
from mailer import send_mail, send_simple_mail 


def lambda_handler(event, context): 
    def rename(df):
        df.rename(columns={
            'Transation ID': 'txn_id',
            'Date' : 'txn_date',
            'Transaction Type': 'txn_type',
            'Amount': 'amount',
            'Balance': 'balance',  
            'Description': 'description'          
        }, inplace=True)
        
        return df

    def clean(txn_df):
        # Remove trailing spaces in headers
        txn_df = txn_df.rename(columns=lambda x: x.rstrip())
        # Rename and copy only required columns
        txn_df = rename(txn_df)
        txn_df = txn_df[[ 'txn_id','txn_date', 'txn_type', 'amount', 'balance', 'description' ]].copy()

        # Replace newline with spaces
        txn_df.replace(r'\n',  ' ', regex=True, inplace=True)

        txn_df['balance'] = txn_df['balance'].apply(clean_digits)
        txn_df['amount'] = txn_df['amount'].apply(clean_digits)
        
        # Get data_prvdr_cust_id and run_id
        data_prvdr_cust_id = get_data_prvdr_cust_id(key)
        run_id = uuid.uuid4()

        txn_df = txn_df.assign( data_prvdr_cust_id=data_prvdr_cust_id,
                                comms=0,
                                export_run_id=run_id, 
                                transform_status='NOT_DONE')

        return txn_df, run_id

    def get_df_from_pdf(pdf_file):
        pdf = pdfplumber.open(pdf_file)
        pages = pdf.pages
        
        df = pd.DataFrame()
        for i in range(len(pages)):
            tbl = pages[i].extract_table()
            header  = tbl.pop(0)
            data = tbl

            temp_df = pd.DataFrame(data, columns = header)
            df = temp_df if df.empty else df.append(temp_df)
        return df


    # MAIN FUNCTION
    df = pd.DataFrame()
    key = 0
    try:
        if event:
            key = event['Records'][0]['s3']['object']['key']
            key = unquote_plus(key)
            pdf = get_obj_from_s3(key)
            pdf = BytesIO(pdf)
            df = get_df_from_pdf(pdf)
        else:
            key = context
            df = get_df_from_pdf(key)

        final_df, run_id = clean(df)
        print(final_df.head(n=10).to_string(index=False))
        
        dup_cols = ['balance', 'amount', 'txn_date', 'data_prvdr_cust_id'] 
        final_df.drop_duplicates(dup_cols, keep='last', inplace=True)
        
        db_con = connect_to_database_engine('STMT')
        final_df = clean_df_db_dups(final_df, 'mtn_cust_acc_stmts', db_con, dup_cols, filter_categorical_col= 'data_prvdr_cust_id')
        
        if final_df.empty:
            raise Exception('Duplicate File')

        exp_list = []
        for chunk in chunkit(final_df):
            exp = df_to_sql(chunk, 'mtn_cust_acc_stmts', db_con)
            exp_list.extend(exp) 
            # print_error()  
        db_con.dispose()
        
        if exp_list:
            send_mail("ERROR IN MTN EXPORT", key, exp_list)
            # print_error()

        data = { 'run_id': f'{run_id}', 'key': key }
        invoke_next_lambda(data, 'mtn_transform')
        # lambda_handler2(data, 0)
        

    except Exception:
        message = traceback.format_exc()
        print(message)
        header = 'ERROR IN MTN LOAD'
        send_simple_mail(header, message)
