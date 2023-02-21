import os
import signal
from dotenv import load_dotenv
import pandas as pd
from common import invoke_next_lambda, connect_to_database_engine, get_obj_from_s3, insert_df_to_db, handle_etl_process_failure, raise_no_new_records_exception, remove_duplicate_records, timeout_handler, update_lambda_status
import traceback
import zipfile
from io import BytesIO

def rename(df):
    df.rename(columns={
        'Transaction ID': 'transaction_id',
        'Account Number': 'merchant_num',
        'Amount': 'amount',
        'Description': 'description',
        'Time': 'time',
        'Service': 'service',
    }, inplace=True)
    return df

def clean(txn_df, acc_number, run_id):

    txn_df = rename(txn_df)

    # Remove header rows inbetween txn records
    txn_df = txn_df[~txn_df.description.str.contains("Description", na=False)]

    remove_comms = ['deposit commission for transacting', 'withdraw commission for transacting', 'cashback  :']
    txn_df = txn_df[~txn_df.description.str.contains('|'.join(remove_comms), na=False)]
    
    # run_id = uuid.uuid4()
    txn_df = txn_df.assign(phone_num=acc_number, export_run_id=run_id, transform_status='NOT_DONE')
    return txn_df

def unzip_and_create_df(zip_key):

    file_name = f"CCA/{zip_key}.zip"
    s3_obj = get_obj_from_s3(file_name)
    buffer = BytesIO(s3_obj)

    z = zipfile.ZipFile(buffer)
    files_in_zip = z.namelist()

    if len(files_in_zip) > 1:   
        raise Exception("Zip file contains more than one file")
    else:
        csv_data = z.open(files_in_zip.pop())
        df = pd.read_csv(csv_data)

    return df

def clean_df(df, db_con, addl_data):
    acc_number = addl_data['acc_number']
    run_id = addl_data['run_id']

    final_df = clean(df, acc_number, run_id)
    dup_cols = ['phone_num', 'time', 'amount', 'transaction_id']
    final_df = remove_duplicate_records(final_df, addl_data['table'], dup_cols, 'phone_num', db_con)

    raise_no_new_records_exception(final_df)
    return final_df

def invoke_transform(data):
    if os.environ.get('APP_ENV') == 'local':
        from cca_transform import lambda_handler2
        lambda_handler2(data, 0)
    else:
        invoke_next_lambda(data, 'cca_transform')


def main(event, context):
    load_dotenv()
    df = pd.DataFrame()
    engine = connect_to_database_engine('STMT')

    try:
        run_id = event['run_id'] = event['flow_req_id']
        object_key = event['object_key']
        acc_number = event['acc_number']
        # function_name = 'cca_export'
        export_table = 'cca_cust_acc_stmts'
        country_code, status, lambda_status = 'UGA', 'success', 'export_success'
        os.environ["COUNTRY_CODE"] = country_code
        
        signal.signal(signal.SIGALRM, timeout_handler)
        if context:
            signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 10)

        df = unzip_and_create_df(object_key)
        addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': export_table, 'event': event, 'context': context}
        
        with engine.begin() as db_con:
            df = clean_df(df, db_con, addl_data)

            print(df.head(n=10).to_string(index=False))

            insert_df_to_db(df, db_con, addl_data)
        
            payload =   {
                            "country_code": country_code,
                            "flow_req_id": run_id,
                            "status": status,
                            "lambda_status": lambda_status,
                        }
            update_lambda_status(payload, event, context)
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
