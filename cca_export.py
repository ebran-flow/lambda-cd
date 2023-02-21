import pandas as pd
from common import FlowEmptyStatementException, parse_date_in_series, remove_dup_in_df, invoke_transform, clean_n_insert, notify_process_status, set_timeout_signal, set_country_code_n_status, cleanup, set_session, handle_file_loading, trim_dataframe

EXPECTED_DT_FORMATS = ['%Y/%m/%d %H:%M:%S']

def process_file_and_get_df(object_key):
    contents = handle_file_loading(object_key)
    df = pd.read_csv(contents)
    if df.empty:
        raise FlowEmptyStatementException('Statement is empty')
    return df

def rename(df):
    rename_columns = {
        "Agent" : "merchant_number",
        "Transaction_Amount" : "txn_amount",
        "Transaction Reference Id" : "txn_ref_id",
        "Transaction Type" : "txn_type",
        "Created at" : "txn_date"
    }

    df.rename(columns=rename_columns, inplace=True)
    return df

def clean(txn_df, acc_number, run_id):
    txn_df = trim_dataframe(txn_df)
    txn_df = rename(txn_df)
    txn_df['txn_date'] = parse_date_in_series(txn_df['txn_date'], EXPECTED_DT_FORMATS)

    txn_df = txn_df.assign(acc_number=acc_number,export_run_id=run_id,transform_status='NOT_DONE')
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
        export_table = 'cca_cust_acc_stmts'
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
    df, engine, country_code = set_session('UGA')
    return export(event, context, engine, df, country_code)

def lambda_handler(event, context):
    return main(event, context)
