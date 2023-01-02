import traceback
from common import connect_to_database_engine, df_to_sql, chunkit, drop_unwanted_columns, invoke_next_lambda, parse_date_in_series, trim_dataframe, update_lambda_status, update_transform_status, invoke_next_lambda
import pandas as pd
import numpy as np
from datetime import datetime as dt
from mailer import send_mail, send_simple_mail

def lambda_handler2(event, context):
    txn_types = {   
                    'Float Purchase': ['isw float distribution', 'isw paypoint agent', 'quickteller float purchase', 'wallet fund'],
                    'Commission': ['agent commission', 'wallet refund'],
                    'Float Adjustment': ['opening credit sum', 'opening debit sum'],
                    'Float Transfer': ['wallet deposit'],
                    'Recovery': ['wallet debit'],
                    'POS Float Load': ['paypoint cards'] 
                }

    def rename(df):
        df.rename(columns={
                            'time': 'txn_date',
                            'terminal': 'from',
                            'req_ref': 'txn_id', 
                            'biller': 'descr', 
                            'customer': 'to', 
                            'debit_amt': 'dr_amt', 
                            'credit_amt': 'cr_amt',
                            'export_run_id': 'run_id',
                            'id': 'ref_id'
                        }, inplace=True)
        return df

    def parse_date(data):
        data = data[0:10]
        date = dt.strptime(data, "%Y-%m-%d").strftime("%Y-%m-%d")
        return date

    def get_txn_type(descr):
        descr = (descr.strip()).lower()
        for txn_type in txn_types.keys():
            if any(txn_descr in descr for txn_descr in txn_types[txn_type]):
                return txn_type
        else:
            return 'Retail Txn'

    def clean_table(txn_df):
        txn_df = trim_dataframe(txn_df)
        txn_df = rename(txn_df)

        # txn_df['txn_date'] = txn_df['time'].apply(parse_date)
        txn_df['txn_date'] = parse_date_in_series(txn_df['txn_date'], ['%Y-%m-%d %H:%M'])
        txn_df['txn_date'] = txn_df['txn_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

        txn_df['txn_type'] = txn_df['descr'].apply(get_txn_type)

        txn_df = txn_df[~txn_df['txn_type'].str.contains('Float Adjustment', na=False)]
        txn_df = txn_df.assign(comms=0.0, is_float=0, acc_prvdr_code='UISG')

        # Insert Commission
        txn_df['comms'] = np.where(txn_df['txn_type'].str.lower() == "commission", txn_df['cr_amt'], txn_df['comms'])
        txn_df['is_float'] = np.where(txn_df['txn_type'].str.lower() == 'float purchase', 1, 0)

        required_columns = ['txn_id','txn_date','txn_type','dr_amt','cr_amt','comms','is_float','balance','acc_number','acc_prvdr_code','run_id','ref_id']
        txn_df = drop_unwanted_columns(txn_df, required_columns, 'keep')
        return txn_df

    def call_score_calc(acc_number, run_id):
        data = {
                    "country": "UGA",
                    "time_zone": "EAT",
                    "acc_number": acc_number,
                    "ap_code": "UISG",
                    "run_id": run_id,
                    "no_of_days": 90
                }
        invoke_next_lambda(data, 'score_calc')

    # MAIN FUNCTION
    try:
        file_key = event['key']
        db_con = connect_to_database_engine('STMT')
        run_id = event['run_id']
        table_name = 'uisg_cust_acc_stmts'
        query = f"SELECT * FROM {table_name} where transform_status='NOT_DONE' and export_run_id='{run_id}'"

        df = pd.read_sql(sql=query, con=db_con)
        if df.empty:
            raise Exception("No records in table")

        final_df = clean_table(df)

        dup_cols = ['acc_number','txn_date', 'balance', 'dr_amt', 'cr_amt', 'txn_id'] 
        final_df.drop_duplicates(dup_cols, keep='last', inplace=True)
        
        exp_list = []
        
        for chunk in chunkit(final_df):
            exp = df_to_sql(chunk, 'flow_cust_acc_stmts', db_con)
            exp_list.extend(exp)   

        key = 'transform_status'
        update_transform_status(db_con, table_name, key, run_id, exp_list)
        update_lambda_status(run_id, 'UGA','transform_success')
        db_con.dispose()

        acc_number = event['acc_number']
        call_score_calc(acc_number, run_id)
        if exp_list:
            send_mail("ERROR IN UISG TRANSFORM", file_key, exp)
        
    except Exception as error:
        error = repr(error)
        update_lambda_status(run_id, 'UGA', 'transform_failed',error[:120])
        header = f'ERROR IN UISG TRANSFORM '
        message = traceback.format_exc()
        print(message)        
        send_simple_mail(header, message)

