from mailer import send_mail, send_simple_mail
# from pcha_transform import lambda_handler2
import uuid
import pandas as pd
import numpy as np
from common import chunkit, connect_to_database_engine, df_to_sql, get_obj_from_s3, invoke_next_lambda, print_error
import re
import datefinder
import os
from urllib.parse import unquote_plus
import traceback
import time
import io
import boto3

def lambda_handler(event, context):
    def last_txn_balance(table_name, ruc_num, start_date, end_date):        
        query = f"select balance, acc_num from {table_name} where ruc_num = '{ruc_num}' and txn_date between '{start_date}' and '{end_date}' order by txn_date desc limit 1;"
        last_txn = pd.read_sql(sql=query, con=db_con)
        if not last_txn.empty:
            last_balance = last_txn.at[0, 'balance']
            acc_num = last_txn.at[0, 'acc_num']
            
            return acc_num, last_balance
        else:
            return None, None

    def clean(df):
        df['txn_type'] = df['txn_type'].apply(lambda x : 'TOTAL_COMMS' if x == 'TOTAL_ESTAB.' else x)
        
        df['export_run_id'] = f'{run_id}'
        df['transform_status'] = 'NOT_DONE'
        
        dup_cols = ['txn_date', 'dr_amt', 'cr_amt', 'acc_num']
        df.drop_duplicates(dup_cols, keep='last', inplace=True)

        return df
    
    ##MAIN FUNCTION
    start_time = time.time()
    try:
        db_con = connect_to_database_engine('STMT')
        key = 0
        if event:
            key = event['Records'][0]['s3']['object']['key']
            key = unquote_plus(key)
            file = get_obj_from_s3(key)

        else:
            # Have to notify to appsupport
            key = context
            file = key
            # raise Exception('Event has no records')

        comms_excel = pd.ExcelFile(file)
        sheet_names = comms_excel.sheet_names

        final_df = pd.DataFrame()
        rucs_with_no_maps = list()
        run_id = uuid.uuid4()
        
        for sheet_name in sheet_names:
            comms_df = pd.read_excel(file, sheet_name)

            for row_index, column in comms_df.iterrows():
                for value in column:
                    if "RESUMEN DE CAJEROS CORRESPONSALES POR AGENCIA" in str(value):
                        
                        title_index = row_index
                        title_str = comms_df.iat[title_index, 0]

                        dates = list(datefinder.find_dates(title_str))

                        start_date = dates[0].replace(hour=00, minute=00, second=00).strftime('%Y-%m-%d %H:%M:%S')
                        end_date = dates[1].replace(hour=23, minute=59, second=59).strftime('%Y-%m-%d %H:%M:%S')

                        ruc_row = title_index + 2
                        ruc_row = comms_df.iat[ruc_row, 5]
                        ruc_num = ruc_row.split(':')[1].strip()

                        acc_num, last_balance= last_txn_balance('pcha_cust_acc_stmts', ruc_num, start_date, end_date)

                        if not acc_num:
                            rucs_with_no_maps.append(ruc_num)
                            continue              
                        
                        # sel_df = comms_df.iloc[title_index+5:title_index+9, [0,7]]
                        # sel_df.columns =['txn_type', 'cr_amt']

                        # sel_df = sel_df[sel_df['txn_type'].isin(['FIN_DEP_C_PROP', 'FIN_DEP_C_TER', 'FIN_PAG_CRED', 'FIN_RETIROS_AHO'])]

                        sel_df = comms_df.iloc[title_index+5:title_index+22, [0,7]]
                        sel_df.columns =['txn_type', 'cr_amt']

                        sel_df = sel_df[sel_df['txn_type'].isin(['TOTAL_ESTAB.'])]

                        sel_df['txn_date'] = end_date
                        sel_df['dr_amt'] = 0
                        sel_df['ruc_num'] = ruc_num
                        sel_df['balance'] = last_balance 
                        sel_df['acc_num'] = acc_num
                        
                        # print(sel_df)
                        if (final_df.empty):
                            final_df = sel_df
                        else:
                            final_df = final_df.append(sel_df)

        if not final_df.empty:
            final_df = clean(final_df)

            exp_list = []
            for chunk in chunkit(final_df, 500):
                exp = df_to_sql(chunk, 'pcha_cust_acc_stmts', db_con)
                exp_list.extend(exp) 
                # print_error()

            data = {'run_id': f'{run_id}', 'key': f'{key}', 'sheet_format': 'COMMISSION_SHEET'}
            # lambda_handler2(data, 0)
            invoke_next_lambda(data, 'pcha_transform')

            if exp_list:
                # print('DUPLICATES')
                send_mail("ERROR IN PCHA LOAD COMMS", key, exp)
                # print_error()
        
        if rucs_with_no_maps:
            header = f'[PCHA] UNMAPPED RUCS IN COMMS - {key}'
            send_simple_mail(header, rucs_with_no_maps)

        db_con.dispose()

        end_time = time.time()
        print(f'------TIME TAKEN: {end_time - start_time}')

    except Exception as error:
        header = "ERROR IN PCHA COMMS LOAD " + key
        message = getattr(error, "message", repr(error))
        message = traceback.format_exc()
        # print(message)
        send_simple_mail(header, message)