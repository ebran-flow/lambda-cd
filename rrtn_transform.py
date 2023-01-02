import pandas as pd
import numpy as np
from common import remove_inessential_columns, invoke_score_calc, notify_process_status, clean_n_insert, remove_dup_in_df, cleanup, handle_transform_failure, set_timeout_signal, set_country_code_n_status, set_session, set_payload, get_exported_data, remove_duplicate_records, connect_to_database_engine, drop_unwanted_columns, insert_df_to_db, handle_etl_process_failure, invoke_next_lambda, parse_date_in_series, timeout_handler, trim_dataframe, update_lambda_status, update_transform_status, rollback_exported_data

TIME_ZONE = 'Africa/Kigali'
EXPECTED_DT_FORMATS = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']

def rename(df):
    rename_columns = { 
                        'date': 'txn_date', 
                        'amount': 'amount', 
                        'closing_balance': 'balance',
                    }
    df.rename(columns=rename_columns, inplace=True)
    return df

def insert_txn_type(df):
    df.txn_type = np.where(df.is_business_txn, 'Retail Txn', df.txn_type)
    df.txn_type = np.where(df.is_float_purchase, 'Float Purchase', df.txn_type)
    df.txn_type = np.where(df.is_float_transfer, 'Float Transfer', df.txn_type)
    df.txn_type = np.where(df.is_commission, 'Commission', df.txn_type)
    return df

def transform_df(txn_df, transform_table):
    txn_df = rename(txn_df)
    txn_df = trim_dataframe(txn_df)
    txn_df['txn_date'] = pd.to_datetime(txn_df['txn_date'], format='%Y-%m-%d %H:%M:%S.%f%z', utc=True)
    txn_df['txn_date'] = txn_df['txn_date'].dt.tz_convert(TIME_ZONE).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    txn_df = txn_df.assign(cr_amt=0, dr_amt=0, comms=0, txn_type=None, acc_prvdr_code='RRTN', is_float=False)

    txn_df.cr_amt = np.where(txn_df.is_credit, txn_df.amount, txn_df.cr_amt)
    txn_df.dr_amt = np.where(txn_df.is_debit, abs(txn_df.amount), txn_df.dr_amt)
    txn_df = insert_txn_type(txn_df)
    txn_df.comms = np.where(txn_df.txn_type == "Commission", txn_df.amount, txn_df.comms)

    txn_df.rename(columns={
                            'id': 'ref_id',
                            'export_run_id': 'run_id'
                            },inplace=True)
    
    txn_df = remove_inessential_columns(txn_df, transform_table)
    return txn_df

def clean_df(df, db_con, addl_data):
    transform_table = addl_data['table']
    df = transform_df(df, transform_table)
    df = remove_dup_in_df(df, db_con, transform_table)
    return df  

def transform(event, context, engine, df, country_code):

    run_id = event['run_id']
    export_table, transform_table = 'rrtn_cust_acc_stmts', 'flow_cust_acc_stmts'
    try:
        acc_number = event['acc_number']
        event = set_country_code_n_status(event, country_code, 'success', 'transform')

        set_timeout_signal(context, 10)
        with engine.begin() as db_con:  
            df = get_exported_data(export_table, run_id, db_con)

            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': transform_table, 'event': event, 'context': context}
            df, exp_list = clean_n_insert(df, db_con, addl_data, clean_df)
            
            update_transform_status(db_con, export_table, 'transform_status', run_id, exp_list)
            notify_process_status(event, context, df, ['lead_id'], None)
        invoke_score_calc(event, 90)

    except Exception as e:
        error_message = str(e)
        event = set_country_code_n_status(event, country_code, 'failed', 'transform')
        handle_transform_failure(event, context, df, ['lead_id'], error_message, export_table, engine)

    finally:
        cleanup(engine)
        return event

def main(event, context):
    df, engine, country_code = set_session('RWA')
    return transform(event, context, engine, df, country_code)

def lambda_handler2(event, context):
    return main(event, context)
