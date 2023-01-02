import zipfile
from dotenv import load_dotenv
import pandas as pd
from common import connect_to_database_engine, drop_unwanted_columns, fill_empty_cells, handle_file_loading, insert_df_to_db, handle_etl_process_failure, raise_no_new_records_exception, remove_duplicate_records, timeout_handler, trim_dataframe, update_lambda_status, invoke_next_lambda
import traceback
from fuzzywuzzy import process
import os
import signal
import xlrd3 as xlrd

def rename(df):
    date_match = process.extractOne('Date / Time', df.columns)[0] #fuzzymatch for date column
    df.rename(columns={
        'Transaction ID': 'txn_id',
        date_match : 'txn_date',
        'Transaction Type': 'txn_type',
        'Amount': 'amount',
        'Commision Amount': 'comms',
        'TAX': 'tax',
        'Float Balance': 'balance',
        'To Account': 'description'
    }, inplace=True)
    return df

def clean(txn_df, acc_number, run_id):
    txn_df = trim_dataframe(txn_df)
    txn_df = rename(txn_df)

    txn_df['comms'] = fill_empty_cells(txn_df['comms'], 0)
    txn_df['tax'] = fill_empty_cells(txn_df['tax'], 0)
    
    txn_df = txn_df.assign( acc_number=acc_number, 
                            export_run_id=run_id, 
                            transform_status='NOT_DONE')

    required_columns = [ 'txn_id','txn_date', 'txn_type', 'amount', 'comms', 'balance', 'description', 'acc_number', 'export_run_id', 'transform_status', 'tax']
    txn_df = drop_unwanted_columns(txn_df, required_columns, 'keep')
    return txn_df

def jsonify_worksheet(worksheet):
    rows_dict = []
    header = [cell.value for cell in worksheet.row(0)]
    for row_idx in range(1, worksheet.nrows):
        row_dict = {}
        NUMBER_TYPE = 2
        for col_idx, cell in enumerate(worksheet.row(row_idx)):
            cell_value = str(int(float(cell.value))) if cell.ctype == NUMBER_TYPE else cell.value
            row_dict[header[col_idx]] = cell_value
        rows_dict.append(row_dict)
    return rows_dict
    
def get_df_from_mtn_excel(contents):
    workbook = xlrd.open_workbook(file_contents=contents)
    sheet_name = workbook.sheet_names()[0]
    loaded_sheet = workbook.sheet_by_name(sheet_name)
    json_data = jsonify_worksheet(loaded_sheet)
    df = pd.DataFrame(json_data)
    return df

def get_df(object_key):
    zip_file = handle_file_loading(object_key)
    z = zipfile.ZipFile(zip_file)
    txn_file = z.namelist()[0]

    with z.open(txn_file) as f: contents = f.read()
    df = get_df_from_mtn_excel(contents)
    return df

def clean_df(df, db_con, addl_data):
    acc_number = addl_data['acc_number']
    run_id = addl_data['run_id']

    final_df = clean(df, acc_number, run_id)
    dup_cols = ['balance', 'amount', 'txn_date', 'acc_number'] 
    final_df = remove_duplicate_records(final_df, addl_data['table'], dup_cols, 'acc_number', db_con)
    raise_no_new_records_exception(final_df)
    return final_df

def invoke_transform(data):
    if os.environ.get('APP_ENV') == 'local':
        from mtn_transform import lambda_handler2
        lambda_handler2(data, 0)
    else:
        invoke_next_lambda(data, 'mtn_transform')

def main(event, context):
    load_dotenv()
    engine = connect_to_database_engine('STMT')
    df = pd.DataFrame()
    try:
        run_id = event['run_id'] = f"{event['flow_req_id']}"
        acc_number = event['acc_number'] 
        lead_id = event['lead_id']
        object_key = event['object_key']
        # function_name = 'mtn_export'
        export_table = 'mtn_cust_acc_stmts'
        country_code, status, lambda_status = 'UGA', 'success', 'export_success'
        os.environ["COUNTRY_CODE"] = country_code
        
        signal.signal(signal.SIGALRM, timeout_handler)
        if context:
            signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 10)

        addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': export_table, 'event': event, 'context': context}
        df = get_df(object_key)

        with engine.begin() as db_con:
            df = clean_df(df, db_con, addl_data)
            insert_df_to_db(df, db_con, addl_data)  

            payload =   {
                            "country_code": country_code,
                            "flow_req_id": run_id,
                            "status": status,
                            "lambda_status": lambda_status,
                            "lead_id": event['lead_id']
                        }
            update_lambda_status(payload, event, context)

        event.update({"sheet_format": 3})
        invoke_transform(event)
    
    except Exception as e:
        error_message = str(e)
        exception = traceback.format_exc()
        print(exception)

        country_code, status, lambda_status = 'UGA', 'failed', 'export_failed'
        payload =   {
                        "country_code": country_code,
                        "flow_req_id": run_id,
                        "status": status,
                        "lambda_status": lambda_status,
                        "lead_id": lead_id,
                        "error_message": error_message
                    }
        update_lambda_status(payload, event, context)
        files = [{'df': df, 'file_name': f"{acc_number}_{run_id}"}]
        handle_etl_process_failure(event, context, files)
    finally:
        if engine:
            engine.dispose()
        signal.alarm(0)

def lambda_handler(event, context):
    main(event, context)
