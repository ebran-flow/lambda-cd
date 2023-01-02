import numpy as np
from common import remove_inessential_columns, set_session, set_country_code_n_status, handle_transform_failure, cleanup, remove_dup_in_df, invoke_score_calc, notify_process_status, clean_n_insert, set_timeout_signal, get_exported_data, parse_date_in_series, trim_dataframe, update_transform_status

def remove_unsuccessful_txns(df):
    if ('status' in df.columns) and not(df['status'].isna().values.all()):
        df = df[df['status'].str.contains("Successful", na=False)]
        df.drop(['status'], axis = 1, inplace=True)
    return df

def transform_df(txn_df, transform_table):
    
    txn_df = trim_dataframe(txn_df)
    txn_df = remove_unsuccessful_txns(txn_df)
    # expected_dt_formats = ['%Y-%m-%d %H:%M:%S']
    # txn_df['txn_date'] = parse_date_in_series(txn_df['txn_date'], expected_dt_formats)
    # txn_df['txn_date'] = txn_df['txn_date'].dt.strftime('%Y-%m-%d')
    
    txn_df = txn_df.assign(cr_amt=0, dr_amt=0, comms=0, acc_prvdr_code='RMTN', is_float=False)

    txn_df.cr_amt = np.where(txn_df.amount > 0, txn_df.amount, txn_df.cr_amt)
    txn_df.dr_amt = np.where(txn_df.amount < 0, abs(txn_df.amount), txn_df.dr_amt)
    txn_df.comms = np.where(txn_df.txn_type == "Batch transfer", txn_df.amount, txn_df.comms)

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
    export_table, transform_table = 'rmtn_cust_acc_stmts', 'flow_cust_acc_stmts'
    try:
        acc_number = event['acc_number']
        event = set_country_code_n_status(event, country_code, 'success', 'transform')
        
        set_timeout_signal(context, 10)
        with engine.begin() as db_con:  
            df = get_exported_data(export_table, run_id, db_con)    
            
            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': transform_table, 'event': event, 'context': context}
            df, exp_list = clean_n_insert(df, db_con, addl_data, clean_df)
            
            update_transform_status(db_con, export_table, 'transform_status', run_id, exp_list)
            notify_process_status(event, context, df, ['lead_id','file_json'], None)
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
