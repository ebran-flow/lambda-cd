import zipfile
from common import cleanup, invoke_transform, notify_process_status, remove_dup_in_df, clean_n_insert, set_timeout_signal, set_country_code_n_status, set_session, check_acc_number, handle_file_loading, trim_dataframe
import pandas as pd
from xlrd import XLRDError

def read_df_from_file(file):
    try:
        txn_df = pd.read_excel(file)
    except (ValueError, XLRDError) as error:
        message = getattr(error, "message", repr(error))
        if ("Excel file format cannot be determined" in message 
            or "Unsupported format, or corrupt file:" in message or "File is not a recognized excel file" in message):
            txn_df = pd.read_html(file)[0]
        else:	
            raise error	
    return txn_df

def rename(txn_df):
    txn_df.rename(columns={
                            'Transaction Date': 'transaction_date',
                            'ID': 'id1',
                            'Transaction ID': 'transaction_id',
                            'Product ID': 'product_id',
                            'Description': 'description',
                            'Dr. Amount': 'dr_amt',
                            'Cr. Amount': 'cr_amt',
                            'Balance': 'balance'
                            
                            },inplace=True)
    return txn_df

def clean_column(data):

    if(data.startswith("=\"")):
        data = data.replace("=", "")
        data = data.replace("\"", "")

    return data	

def clean(txn_df, acc_number, run_id):
    txn_df = trim_dataframe(txn_df)
    txn_df = rename(txn_df)
    
    obj_cols = txn_df.dtypes[txn_df.dtypes == 'object'].index.tolist()
    for obj_col in obj_cols:
        txn_df[obj_col] = txn_df[obj_col].apply(clean_column)   

    txn_df['dr_amt'] = txn_df['dr_amt'].fillna(0)
    txn_df['cr_amt'] = txn_df['cr_amt'].fillna(0)
    txn_df['balance'] = txn_df['balance'].fillna(0)
    
    check_acc_number(txn_df, 'id1', 0.9, acc_number)

    txn_df['export_run_id'] = run_id
    txn_df['transform_status'] = 'NOT_DONE'                        
    return txn_df

def clean_df(df, db_con, addl_data):
    acc_number = addl_data['acc_number']
    run_id = addl_data['run_id']
    export_table = addl_data['table']

    df = clean(df, acc_number, run_id)
    df = remove_dup_in_df(df, db_con, export_table)
    return df

def get_df(object_key):
    zip_file = handle_file_loading(object_key)
    z = zipfile.ZipFile(zip_file)
    txn_file = z.namelist()[0]

    with z.open(txn_file) as f: contents = f.read()
    df = read_df_from_file(contents)
    return df

def export(event, context, engine, df, country_code):
    try:
        run_id = event['run_id'] = f"{event['flow_req_id']}"
        acc_number = event['acc_number'] 
        object_key = event['object_key']
        export_table = 'uezm_cust_acc_stmts'
        event = set_country_code_n_status(event, country_code, 'success', 'export')
        set_timeout_signal(context, 10)
       
        addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': export_table, 'event': event, 'context': context}
        df = get_df(object_key)
        with engine.begin() as db_con:
            df, exp_list = clean_n_insert(df, db_con, addl_data, clean_df)
            notify_process_status(event, context, df, [], None)
        invoke_transform(event)
    
    except Exception as e:
        error_message = str(e)
        event = set_country_code_n_status(event, country_code, 'failed', 'export')
        notify_process_status(event, context, df, [], error_message)
        
    finally:
        cleanup(engine)
        return event

def main(event, context):
    df, engine, country_code = set_session('UGA')
    return export(event, context, engine, df, country_code)

def lambda_handler(event, context):
    return main(event, context)
