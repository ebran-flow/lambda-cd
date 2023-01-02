import pandas as pd
from datetime import datetime as dt
import sqlalchemy
import numpy as np
import json
import boto3
import os
from common import df_to_sql, connect_to_database_engine, chunkit, insert_stmt_format, print_error, update_transform_status
from mailer import send_mail, send_simple_mail
import traceback

## START MAIN PROGRAM ##
def lambda_handler2(event, context):
    def parse_date(data):
        data = data[0:10]
        try:
            date = dt.strptime(data, '%d/%m/%Y').strftime("%Y-%m-%d")
            return date
        except ValueError:
            date = dt.strptime(data, '%Y-%m-%d').strftime("%Y-%m-%d")
            return date

    def get_5perc(amt):
        return amt * 0.05

    def get_comms_cash_in(amt):
        if amt < 500 : return get_5perc(amt)
        elif amt <= 2500 : return get_5perc(amt)
        elif amt <= 5000 : return 125
        elif amt <= 15000 : return 175
        elif amt <= 30000 : return 225
        elif amt <= 45000 : return 275
        elif amt <= 60000 : return 325
        elif amt <= 125000 : return 400
        elif amt <= 250000 : return 500
        elif amt <= 500000 : return 950
        elif amt <= 1000000 : return 1600
        elif amt <= 2000000 : return 3000
        elif amt <= 4000000 : return 3000
        elif amt <= 7000000 : return 3000
    
    def get_comms_cash_out(amt):
        if amt < 500 : return 100
        elif amt <= 2500 : return 100
        elif amt <= 5000 : return 125
        elif amt <= 15000 : return 200
        elif amt <= 30000 : return 300
        elif amt <= 45000 : return 400
        elif amt <= 60000 : return 500
        elif amt <= 125000 : return 650
        elif amt <= 250000 : return 1100
        elif amt <= 500000 : return 2200
        elif amt <= 1000000 : return 4400
        elif amt <= 2000000 : return 4400
        elif amt <= 4000000 : return 4400
        elif amt <= 7000000 : return 4400

    def transform_table(sheet):
        
        sheet['txn_date'] = sheet['txn_date'].apply(parse_date)

        sheet = sheet.assign(cr_amt=0, dr_amt=0, is_float=False, data_prvdr_code='UMTN')
        sheet['abs_amount'] = sheet['amount'].abs()
        
        for i, row in sheet.iterrows():
            abs_amount = sheet.at[i, 'abs_amount']
            amount = sheet.at[i, 'amount']
            txn_type = sheet.at[i, 'txn_type']
            txn_type = txn_type.strip()
            
            if amount > 0:
                sheet.loc[i, 'cr_amt'] = abs_amount
            elif amount < 0:
                sheet.loc[i, 'dr_amt'] = abs_amount
            
            if txn_type == "Withdraw Money":
                sheet.loc[i, 'comms'] = get_comms_cash_out(abs_amount)
            elif txn_type == "Deposit Money":
                sheet.loc[i, 'comms'] = get_comms_cash_in(abs_amount)
        
        sheet.rename(columns={
                                'id': 'ref_id',
                                'export_run_id': 'run_id'
                                },inplace=True)

        sheet.drop(['abs_amount', 'amount', 'transform_status', 'description'], axis = 1, inplace=True)
        return sheet

    ## MAIN FUNCTION
    file_key = 0
    file_key = event['key']
    try:
        db_con = connect_to_database_engine('STMT')
        run_id = event['run_id']
        table_name = 'mtn_cust_acc_stmts'
        query = f'SELECT * FROM {table_name} where transform_status="NOT_DONE" and export_run_id="{run_id}";'
        sheet = pd.read_sql(sql=query,con=db_con)
    
        sheet= transform_table(sheet)
        
        exp_list = []
        for chunk in chunkit(sheet):
            exp = df_to_sql(chunk, 'flow_cust_acc_stmts', db_con)
            exp_list.extend(exp) 
            # print_error()  

        key = 'transform_status'
        update_transform_status(db_con, table_name, key, run_id, exp_list)

        if exp_list:
            send_mail("ERROR IN MTN TRANSFORM", file_key, exp_list)
            # print_error()
        
        db_con.dispose()

    except Exception:
        header = "ERROR IN MTN TRANSFORM " + file_key
        message = traceback.format_exc()
        print(message)
        send_simple_mail(header, message)
