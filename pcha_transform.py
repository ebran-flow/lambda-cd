import pandas as pd
from datetime import datetime as dt
import sqlalchemy
import numpy as np
import os
from common import df_to_sql, connect_to_database_engine, chunkit, print_error, update_transform_status
from mailer import send_mail, send_simple_mail
import traceback
import time

## START MAIN PROGRAM ##
def lambda_handler2(event, context):
    def parse_date(data):
        data = data[0:10]
        try:
            date = dt.strptime(data, '%Y-%m-%d').strftime("%Y-%m-%d")            
            return date
        except ValueError:
            date = dt.strptime(data, '%d/%m/%Y').strftime("%Y-%m-%d")
            return date

    def get_comms(record):
            txn_type = record['txn_type'].strip()
            # if (txn_type == 'FIN_DEP_C_PROP') or (txn_type == 'FIN_DEP_C_TER') or (txn_type == 'FIN_PAG_CRED') or (txn_type == 'FIN_RETIROS_AHO'):
            if (txn_type == 'TOTAL_COMMS'):
                return record['cr_amt']
            else:
                return 0.0

    def transform_table():

        # sheet['txn_date'] = sheet['txn_date'].apply(parse_date)
        sheet['comms'] = 0
        sheet['is_float'] = False
        sheet['data_prvdr_code'] = 'PCHA'

        sheet_format = event['sheet_format']
        if sheet_format == 'COMMISSION_SHEET':
            sheet['comms'] = sheet.apply(get_comms, axis=1)

        sheet.rename(columns={
                                'id': 'ref_id',
                                'export_run_id': 'run_id',
                                'acc_num': 'data_prvdr_cust_id'
                                },inplace=True)
        
        sheet.drop(['transform_status', 'ruc_num'], axis = 1, inplace=True)

        return sheet

    ## MAIN FUNCTION
    start_time = time.time()
    
    file_key = 0
    file_key = event['key']
    try:
        db_con = connect_to_database_engine('STMT')
        run_id = event['run_id']
        table_name = 'pcha_cust_acc_stmts'
        query = f'SELECT * FROM {table_name} where transform_status="NOT_DONE" and export_run_id="{run_id}";'
        sheet = pd.read_sql(sql=query,con=db_con)
    
        sheet= transform_table()
        
        exp_list = []

        for chunk in chunkit(sheet, 1000):
            exp = df_to_sql(chunk, 'flow_cust_acc_stmts', db_con)
            exp_list.extend(exp) 
            # print_error()  
        

        key = 'transform_status'
        update_transform_status(db_con, table_name, key, run_id, exp_list)
        if exp_list:
            send_mail("ERROR IN PCHA TRANSFORM", file_key, exp_list)
        
        db_con.dispose()

        end_time = time.time()
        print(f'------TIME TAKEN: {end_time - start_time}')

    except Exception as error:
        header = "ERROR IN PCHA TRANSFORM " + file_key
        message = getattr(error, "message", repr(error))
        message = traceback.format_exc()
        print(message)
        send_simple_mail(header, message)
