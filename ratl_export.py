import pandas as pd
from common import cleanup, remove_inessential_columns, remove_dup_in_df, notify_process_status, clean_n_insert, invoke_transform, set_timeout_signal, set_country_code_n_status, set_session, handle_zip_files, check_acc_number, check_if_fmt_supported, trim_dataframe, parse_date_in_series, clean_numeric_columns

EXPECTED_DT_FORMATS = ['%Y-%m-%d', '%d/%m/%Y']
TXN_STMT_EXPECTED_WORDS = {
    'fmt_1' : ['transfer_id','entry_type','service_type','transfer_subtype','account_id','second_party_account_id','transfer_value','previous_balance','post_balance','transaction_type','requested_value','transfer_status','attr_1_value'],
}

def clean(txn_df, acc_number, run_id, export_table):
    txn_df = trim_dataframe(txn_df)

    header = txn_df.columns.values
    if not check_if_fmt_supported(TXN_STMT_EXPECTED_WORDS, header):
        raise Exception(f'The given stmt format is not supported or missing columns.\nColumns in Statement:{header}')

    txn_df['transfer_date'] = parse_date_in_series(txn_df['transfer_date'], EXPECTED_DT_FORMATS)
    txn_df['transfer_date'] = txn_df['transfer_date'].dt.strftime('%Y-%m-%d')

    print(txn_df['transfer_date'])

    txn_df = clean_numeric_columns(txn_df, ['transfer_value', 'post_balance'])
    txn_df['transfer_value'] = txn_df['transfer_value']
    txn_df['post_balance'] = txn_df['post_balance']

    check_acc_number(txn_df, 'account_id', 0.9, acc_number)

    txn_df = txn_df.assign( account_id=acc_number,
                            export_run_id=run_id, 
                            transform_status='NOT_DONE')

    txn_df = remove_inessential_columns(txn_df, export_table)
    return txn_df

def clean_df(df, db_con, addl_data):
    acc_number = addl_data['acc_number']
    run_id = addl_data['run_id']
    export_table = addl_data['table']

    df = clean(df, acc_number, run_id, export_table)
    df = remove_dup_in_df(df, db_con, export_table)
    return df

def process_file_and_get_df(object_key, file_json):
    file = handle_zip_files(object_key)
    
    file_type = file_json['files'][0]['file_type']
    file_extension = file_type.split('/')[-1]

    if file_extension == 'xlsx':
        df = pd.read_excel(file)
    else:
        raise Exception(f'File format not supported: {file_extension}')
        
    return df

def export(event, context, engine, df, country_code):
    try:
        run_id = event['run_id'] = f"{event['flow_req_id']}"
        acc_number = event['acc_number'] 
        file_json = event['file_json']
        object_key = event['object_key']
        export_table = 'ratl_cust_acc_stmts'
        event = set_country_code_n_status(event, country_code, 'success', 'export')
        set_timeout_signal(context, 10)

        with engine.begin() as db_con:
            df = process_file_and_get_df(object_key, file_json)
            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': export_table, 'event': event, 'context': context}
            df, exp_list = clean_n_insert(df, db_con, addl_data, clean_df)
            notify_process_status(event, context, df, ['lead_id','file_json'], None)
        invoke_transform(event)

    except Exception as e:
        error_message = str(e)
        event = set_country_code_n_status(event, country_code, 'failed', 'export')
        notify_process_status(event, context, df, ['lead_id','file_json'], error_message)
    finally:
        cleanup(engine)
        return event

def main(event, context):
    df, engine, country_code = set_session('RWA')
    return export(event, context, engine, df, country_code)

def lambda_handler(event, context):
    return main(event, context)