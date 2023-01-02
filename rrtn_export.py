import pandas as pd
from common import remove_dup_in_df, invoke_transform, clean_n_insert, notify_process_status, set_timeout_signal, set_country_code_n_status, cleanup, set_session, drop_unwanted_columns, handle_file_loading, trim_dataframe

TIME_ZONE= 'Africa/Kigali'

def process_file_and_get_df(object_key):
    contents = handle_file_loading(object_key)
    df = pd.read_json(contents)
    return df

def rename(df):
    rename_columns = { 
                        'date': 'date', 
                        'isBusinessTransaction': 'is_business_txn', 
                        'isFloatPurchase': 'is_float_purchase', 
                        'isFloatTransfer': 'is_float_transfer',
                        'isCommission': 'is_commission', 
                        'isCredit': 'is_credit', 
                        'isDebit': 'is_debit', 
                        'amount': 'amount', 
                        'closingBalance': 'closing_balance',
                        'description': 'description'
                    }

    df.rename(columns=rename_columns, inplace=True)
    return df

def insert_txn_id(df):
    df['temp_date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S.%f%z', utc=True)
    df['temp_date'] = df['temp_date'].dt.tz_convert(TIME_ZONE).dt.strftime('%Y%m%d%H%M%S')

    df['txn_id'] = df.apply(lambda record: f"dummy_{record['temp_date']}{str(int(abs(record['amount'])))}{record['acc_number']}", axis=1) 
    df = drop_unwanted_columns(df, ['temp_date'], 'remove')
    return df

def clean(txn_df, acc_number, run_id):
    txn_df = trim_dataframe(txn_df)
    txn_df = rename(txn_df)
    txn_df = txn_df.assign( acc_number= acc_number )
    txn_df = insert_txn_id(txn_df)
    # print(txn_df.columns)
    txn_df = txn_df.assign( export_run_id=run_id, transform_status='NOT_DONE' )
    return txn_df

def clean_df(df, db_con, addl_data):
    acc_number = addl_data['acc_number']
    run_id = addl_data['run_id']
    export_table = addl_data['table']

    df = clean(df, acc_number, run_id)
    df = remove_dup_in_df(df, db_con, export_table)
    return df

def export(event, context, engine, df, country_code):
    try:
        run_id = event['run_id'] = f"{event['flow_req_id']}"
        acc_number = event['acc_number']
        object_key = event['object_key']
        export_table = 'rrtn_cust_acc_stmts'
        event = set_country_code_n_status(event, country_code, 'success', 'export')
        set_timeout_signal(context, 10)

        with engine.begin() as db_con:
            df = process_file_and_get_df(object_key)
            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': export_table, 'event': event, 'context': context}
            df, exp_list = clean_n_insert(df, db_con, addl_data, clean_df)
            notify_process_status(event, context, df, ['lead_id'], None)
        invoke_transform(event)
        
    except Exception as e:
        error_message = str(e)
        event = set_country_code_n_status(event, country_code, 'failed', 'export')
        notify_process_status(event, context, df, ['lead_id'], error_message)

    finally:
        cleanup(engine)
        return event

def main(event, context):
    df, engine, country_code = set_session('RWA')
    return export(event, context, engine, df, country_code)

def lambda_handler(event, context):
    return main(event, context)