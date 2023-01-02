import os
import signal
from dotenv import load_dotenv
import traceback
from common import connect_to_database_engine, drop_unwanted_columns, get_exported_data, insert_df_to_db, handle_etl_process_failure, invoke_next_lambda, remove_duplicate_records, rollback_exported_data, timeout_handler, trim_dataframe, update_lambda_status, update_transform_status
import pandas as pd
import numpy as np
from datetime import datetime as dt
import pytz

TIME_ZONE = pytz.timezone("Africa/Kampala")

def rename(df):
    df.rename(columns={
        'transaction_id': 'txn_id',
        'phone_num': 'acc_number',
        'amount': 'amt',
        'description': 'descr',
        'time': 'time',
        'service': 'service',
        'export_run_id': 'run_id',
        'id': 'ref_id'
    }, inplace=True)
    return df

def timestamp_to_datetime(timestamp):
    timestamp = int(timestamp)
    return dt.fromtimestamp(timestamp, tz=TIME_ZONE).strftime("%Y-%m-%d %H:%M:%S")

def find_txn_type(data):
    if 'agent commission' in data:
        return 'Commission'
    elif 'Stock purchase' in data or 'Stock sent from' in data or 'Merchant bank deposit' in data:
        return 'Float Purchase'
    elif 'Stock sent to' in data:
        return 'Float Transfer'
    # elif 'cashin' in data or ('paid' in data and 'phone number:' in data):
    #     return 'Retail Txn'
    else:
        return 'Retail Txn'

def parse_date(data):
    data = data[0:10]
    date = dt.strptime(data, "%Y-%m-%d").strftime("%Y-%m-%d")
    return date

def transform_df(txn_df):
    txn_df = trim_dataframe(txn_df)
    # CONVERT TIMESTAMP TO DATE TIME
    txn_df['txn_date'] = txn_df['time'].apply(timestamp_to_datetime)
    txn_df.sort_values(by='txn_date', ascending=False, inplace=True)

    # SEPARATE BALANCE RECORD
    bal_descr = ['Opening balance', 'Closing balance', 'balance']
    # bal_descr = ['balance']
    bal_df = txn_df[txn_df.descr.str.contains('|'.join(bal_descr), na=False)]
    txn_df = txn_df[~txn_df.descr.str.contains('|'.join(bal_descr), na=False)]

    if txn_df.shape[0] == 0:
        raise Exception('Transaction txn_df is empty')
    elif bal_df.shape[0] == 0:
        raise Exception('Balance txn_df is empty')

    # CREDITED OR DEBITED
    txn_df = txn_df.assign(cr_amt=0, dr_amt=0, balance=0)

    txn_df.cr_amt = np.where(txn_df.amt > 0, txn_df.amt, txn_df.cr_amt)
    txn_df.dr_amt = np.where(txn_df.amt < 0, abs(txn_df.amt), txn_df.dr_amt)

    # CALCULATE RUNNING BALANCE
    bal_amt = bal_df.amt.values[0]

    txn_df_1st_row = txn_df.index[0] #FIRST_ROW_INDEX
    for row_index, column in txn_df.iterrows():
        if txn_df_1st_row == row_index:
            txn_df.at[row_index, 'balance'] = bal_amt
        else:
            previous_balance = txn_df.at[previous_index, 'balance']
            previous_amount = txn_df.at[previous_index, 'amt']
            txn_df.at[row_index, 'balance'] = previous_balance - previous_amount
        previous_index = row_index

    # ADD SMALLEST BALANCE TO MAKE ALL BALANCE POSITIVE
    smallest_balance = txn_df['balance'].min()
    if smallest_balance < 0:
        txn_df['balance'] = txn_df['balance'] + abs(smallest_balance)

    txn_df.sort_values(by='txn_date', ascending=True, inplace=True)

    txn_df = txn_df.assign(txn_type=np.nan, comms=0.0, is_float=0, acc_prvdr_code='CCA')
    # DETERMINE IF TXN_TYPE IS COMMISSION
    txn_df['txn_type'] = txn_df['descr'].apply(find_txn_type)

    # ENTER COMMISSION
    txn_df.comms = np.where(txn_df.txn_type == "COMMISSION", txn_df.amt, txn_df.comms)
    txn_df.is_float = np.where(txn_df.txn_type == 'Float Purchase', 1, 0)

    # txn_df['txn_date'] = txn_df['txn_date'].apply(parse_date)

    required_columns = ['txn_id','txn_date','txn_type','dr_amt','cr_amt','comms','is_float','balance','acc_number','acc_prvdr_code','run_id','ref_id']
    txn_df = drop_unwanted_columns(txn_df, required_columns, 'keep')
    return txn_df

def clean_df(df, db_con, transform_table):
    final_df = transform_df(df)

    dup_cols = ['acc_number','txn_date', 'balance', 'dr_amt', 'cr_amt', 'txn_id'] 
    final_df = remove_duplicate_records(final_df, transform_table, dup_cols, 'acc_number', db_con)
    return final_df 

def call_score_calc(event):
    data = {
                "country_code": "UGA",
                "time_zone": "EAT",
                "no_of_days": 90,
                "acc_prvdr_code": "CCA"
            }
    data.update(event)
    if os.environ.get('APP_ENV') == 'local':
        from score import score_calc
        score_calc(data, None)
    else:
        invoke_next_lambda(data, 'score_calc')
    
def main(event, context):
    load_dotenv()
    engine = connect_to_database_engine('STMT')
    df = pd.DataFrame()
    run_id = event['run_id']
    export_table, transform_table = 'cca_cust_acc_stmts', 'flow_cust_acc_stmts'

    try:
        acc_number = event['acc_number']
        # function_name = 'cca_transform'
        country_code, status, lambda_status =  'UGA', 'success', 'transform_success'
        os.environ["COUNTRY_CODE"] = country_code
        
        signal.signal(signal.SIGALRM, timeout_handler)
        if context:
            signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 10)

        with engine.begin() as db_con:
            df = get_exported_data(export_table, run_id, db_con)
            df = rename(df)

            df = clean_df(df, db_con, transform_table)
            print(df.head(n=10).to_string(index=False))

            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': transform_table, 'event': event, 'context': context}
            exp_list = insert_df_to_db(df, db_con, addl_data)

            update_transform_status(db_con, export_table, 'transform_status', run_id, exp_list)
        
            payload =   {
                            "country_code": country_code,
                            "flow_req_id": run_id,
                            "status": status,
                            "lambda_status": lambda_status,
                        }
            update_lambda_status(payload, event, context)
        call_score_calc(event)  

    except Exception as e:
        error_message = str(e)
        exception = traceback.format_exc()
        print(exception)        
        rollback_exported_data(export_table, run_id, engine)
        country_code, status, lambda_status = 'UGA', 'failed', 'transform_failed'
        payload =   {
                        "country_code": country_code,
                        "flow_req_id": run_id,
                        "status": status,
                        "lambda_status": lambda_status,
                        "error_message": error_message
                    }

        update_lambda_status(payload, event, context)
        files = [{'df': df, 'file_name': f"{acc_number}_{run_id}"}]
        handle_etl_process_failure(event, context, files)

    finally:
        if engine:
            engine.dispose()
        signal.alarm(0)

def lambda_handler2(event, context):
    main(event, context)

