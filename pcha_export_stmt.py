import traceback
# from pcha_transform import lambda_handler2
from mailer import send_mail, send_simple_mail
from common import chunkit, clean_df_db_dups, connect_to_database_engine, df_to_sql, get_obj_from_s3, print_error
import pandas as pd
import boto3
import uuid
import os
from urllib.parse import unquote_plus
from common import invoke_next_lambda
import numpy as np
import time
import io

def lambda_handler(event, context):
    def read_df_from_file(file):
        # try:
            # txn_df = pd.read_excel(file, dtype={'cCodKardex': str})
        txn_df = pd.read_csv(file, dtype={'cCodKardex': str})
        return txn_df

    def get_req_columns(df):
        req_columns = ['dFecKardex', 'cHorKardex', 'cCodKardex', 'cDesCorta',
                       'nIngreso', 'nEgreso', 'nSalCapita', 'cCodCuenta']
        
        return df.drop(df.columns.difference(req_columns), axis=1)

    def rename(df):
        df.rename(columns={
                            'dFecKardex': 'txn_date',
                            'cHorKardex': 'txn_time',
                            'cCodKardex': 'txn_id',
                            'cDesCorta': 'txn_type',
                            'nIngreso': 'cr_amt',
                            'nEgreso': 'dr_amt',
                            'nSalCapita': 'balance',
                            'cCodCuenta': 'acc_num',                            
                            },inplace=True)

        return df
    
    def ruc_mapper(column):
        uniq_acc_nums = column.unique()
        acc_ruc_map = dict()
        for acc_num in uniq_acc_nums:
            query = f"select ruc_num from mapper where acc_num = {acc_num} limit 1"
            map_df = pd.read_sql(sql=query, con=db_con)
            if not map_df.empty:
                ruc_num = map_df.at[0, 'ruc_num']
                acc_ruc_map[acc_num] = ruc_num
            else:
                no_map_list.append(ruc_num)
                acc_ruc_map[acc_num] = None
        return acc_ruc_map
        
    def clean(txn_df):
        
        txn_df = get_req_columns(txn_df) 
        txn_df = rename(txn_df)

        acc_ruc_map = ruc_mapper(txn_df['acc_num'])

        txn_df['dr_amt'].fillna(0, inplace=True)
        txn_df['cr_amt'].fillna(0, inplace=True)
        txn_df['balance'].fillna(0, inplace=True) # Replace NaN values with 0
        
        txn_df['txn_date'] = txn_df['txn_date'] + ' ' + txn_df['txn_time']
        txn_df.drop([ 'txn_time' ], axis=1, inplace=True)

        txn_df.dropna(axis=0, subset=['acc_num'], inplace=True)
        
        txn_df['ruc_num'] = txn_df['acc_num'].apply(lambda acc_num: acc_ruc_map.get(acc_num, None))
        txn_df.dropna(axis=0, subset=['ruc_num'], inplace=True)
        
        id = uuid.uuid4()
        txn_df['export_run_id'] = f'{id}'
        txn_df['transform_status'] = 'NOT_DONE'                        
        
        return txn_df, id

    ##MAIN FUNCTION
    df = pd.DataFrame()
    key = 0
    start_time = time.time()
    try:
        if event:
            key = event['Records'][0]['s3']['object']['key']
            key = unquote_plus(key)
            excel = get_obj_from_s3(key)
            excel = io.StringIO(excel.decode('utf-8'))
            df = read_df_from_file(excel)
        else:
            key = context
            df = read_df_from_file(key)
            # raise Exception('Event has no records')

        # print(df)
        db_con = connect_to_database_engine('STMT')
        no_map_list = list()

        final_df, run_id = clean(df)   

        print(final_df.head(n=10).to_string(index=False))

        dup_cols = ['txn_date', 'dr_amt', 'cr_amt', 'acc_num', 'ruc_num']
        final_df.drop_duplicates(dup_cols, keep='last', inplace=True)
        
        final_df = clean_df_db_dups(final_df, 'pcha_cust_acc_stmts', db_con, dup_cols, filter_categorical_col='acc_num')
        
        exp_list = []
        for chunk in chunkit(final_df, 1000):
            exp = df_to_sql(chunk, 'pcha_cust_acc_stmts', db_con)
            exp_list.extend(exp) 
            # print_error()

        
        db_con.dispose()
        data = {'run_id': f'{run_id}', 'key': f'{key}', 'sheet_format': 'A/C Statement'}
        # lambda_handler2(data, 0)

        invoke_next_lambda(data, 'pcha_transform')
        if exp_list:
            send_mail("ERROR IN PCHA LOAD", key, exp)
        if no_map_list:
            send_simple_mail('RUCS WITH NO MAPPING', no_map_list)
        final_df, df, db_con, excel = None, None, None, None

        end_time = time.time()
        print(f'------TIME TAKEN: {end_time - start_time}')

    except Exception as error:
        header = "ERROR IN PCHA LOAD"
        message = getattr(error, "message", repr(error))
        message = traceback.format_exc()
        print(message)
        send_simple_mail(header, message)
        
        if key!=0 and not df.empty:
            error_df = [{"df": df, "exception": message}]
            send_mail(header, key, error_df)    
        else:
            send_simple_mail(header, message)



