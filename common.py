from io import BytesIO
import os
import re
import traceback
import zipfile
from dotenv.main import load_dotenv
import numpy as np
import sqlalchemy
from sqlalchemy import text
import pandas as pd
import boto3
import json
import requests
from datetime import datetime as dt
import signal

class Constants:
    
    COUNTRY_TO_TIME_ZONE = {
        'RWA': 'CAT',
        'UGA': 'EAT'
    }

    TABLES_CONFIG = {
        'flow_cust_acc_stmts': {
            'REQUIRED_COLUMNS': ['txn_id','txn_date','txn_type','dr_amt','cr_amt','comms','is_float','balance','acc_number','acc_prvdr_code','run_id','ref_id'],
            'UNIQUE_COLUMNS': ['acc_number','txn_date', 'balance', 'dr_amt', 'cr_amt', 'txn_id'],
            'FILTER_BY': 'acc_number',
            'FILTER_CONTINUOUS': 'txn_date',
        },

        'rmtn_cust_acc_stmts': {
            'REQUIRED_COLUMNS': ['txn_id','txn_date','status','txn_type','from','from_name','to','to_name','acc_number','amount','balance','transform_status','export_run_id'],
            'UNIQUE_COLUMNS': ['balance', 'amount', 'txn_date', 'acc_number'],
            'FILTER_BY': 'acc_number',
            'FILTER_CONTINUOUS': None,
        },

        'rbok_cust_acc_stmts': {
            'REQUIRED_COLUMNS': ['txn_id','txn_date','txn_type','descr','acc_number','cr_amt','dr_amt','balance','transform_status','export_run_id'],
            'UNIQUE_COLUMNS': ['balance', 'txn_id', 'txn_date', 'acc_number'],
            'FILTER_BY': 'acc_number',
            'FILTER_CONTINUOUS': None,
            'NUMERIC_COLUMNS': ['cr_amt', 'dr_amt', 'balance']
        },

        'ratl_cust_acc_stmts': {
            'REQUIRED_COLUMNS': ['export_run_id','transform_status','transfer_date','transfer_id','entry_type','service_type','transfer_subtype','account_id','second_party_account_id','transfer_value','previous_balance','post_balance','transaction_type','requested_value','transfer_status','attr_1_value'],
            'UNIQUE_COLUMNS': ['transfer_date','transfer_id','account_id','post_balance'],
            'FILTER_BY': 'account_id',
            'FILTER_CONTINUOUS': None,
        },

        'uezm_cust_acc_stmts': {
            'REQUIRED_COLUMNS': ['transaction_date','id1','transaction_id','product_id','description','dr_amt','cr_amt','balance','export_run_id','transform_status'],
            'UNIQUE_COLUMNS': ['dr_amt','cr_amt','transaction_date','transaction_id'],
            'FILTER_BY': 'id1',
            'FILTER_CONTINUOUS': None,
        },

        'rrtn_cust_acc_stmts': {
            'REQUIRED_COLUMNS': [],
            'UNIQUE_COLUMNS': ['date', 'amount', 'closing_balance'],
            'FILTER_BY': 'acc_number',
            'FILTER_CONTINUOUS': None,
        },
    }
    
class FlowInvalidDateException(Exception):
    pass

# LOAD ENV FILE
load_dotenv()

APP_URL = os.environ.get('APP_URL')
MAIL_ENDPOINT = '/internal_api/internal_mail' 
LAMBDA_STATUS_ENDPOINT = '/internal_api/lambda_status'

bucket_name = os.environ.get('BUCKET_NAME')
aws_access_key_id = os.environ.get('ACCESS_KEY')
aws_secret_access_key = os.environ.get('SECRET_KEY')
region_name = os.environ.get('REGION_NAME')

exp_arr = []

def connect_to_database_engine(DB_INSTANCE_NAME):
    db_username = os.environ.get(f'{DB_INSTANCE_NAME}_DB_USERNAME')
    db_password = os.environ.get(f'{DB_INSTANCE_NAME}_DB_PASSWORD')
    db_host = os.environ.get(f'{DB_INSTANCE_NAME}_DB_HOST')
    db_name = os.environ.get(f'{DB_INSTANCE_NAME}_DB_DATABASE')
    database_connection = sqlalchemy.create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_name}', future=True)
    return database_connection

def chunkit(df, chunk_size = 500): 
        num_chunks = len(df) // chunk_size
        if len(df) % chunk_size != 0:
            num_chunks += 1
        for i in range(num_chunks):
            yield df[i*chunk_size:(i + 1) * chunk_size]
        
def update_transform_status(db_con, table_name, key, run_id, exp_list):
    if exp_list:
        ids = []
        for exp in exp_list:
            df = exp['df']
            for row, index in df.iterrows():
                id = str(df.loc[row, 'ref_id'])
                ids.append(id)
        ids = "','".join(ids)
        ids = f"('{ids}')"

        failed_query = f"update {table_name} set {key}='FAILED' where export_run_id='{run_id}' and id in {ids};"
        success_query = f"update {table_name} set {key}='DONE' where export_run_id='{run_id}' and id not in {ids};"
        db_con.execute(text(failed_query))
        db_con.execute(text(success_query))

    else:
        success_query = f"update {table_name} set {key}='DONE' where export_run_id='{run_id}';"
        db_con.execute(text(success_query))
        
def df_to_sql(txn_df ,table_name, db_con, is_row_df = False, row_index = 0):
    try:
        txn_df.to_sql(con=db_con, name=f'{table_name}', if_exists='append', index=None)
                                        
    except Exception as e:
        if (e.__class__.__name__ == 'IntegrityError' ):
            if( is_row_df):
                # dup_arr.append(txn_df)
                
                print('Duplicate') #TODO PRINT EXCEPTION MSG
                # exp_arr.append({"df" : txn_df, "exception" : e})
                message = getattr(e, "message", repr(e))
                exp_arr.append({"df" : txn_df, "exception" : message})
            else:
                df_to_sql_each_row(txn_df, table_name, db_con)
        else:
            # exp_arr.append({"df" : txn_df, "exception" : e})
            message = getattr(e, "message", repr(e))
            exp_arr.append({"df" : txn_df, "exception" : message})
            print('Error')
    return exp_arr
    
def df_to_sql_each_row(txn_df, table_name, db_con):
	for index, row  in txn_df.iterrows():
		try:
			row_df = pd.DataFrame(row).transpose()
			df_to_sql(row_df, table_name, db_con, True ,index)
		except Exception as e:
			raise e

def clean_df_db_dups(df, tablename, engine, dup_cols=[],
                         filter_continuous_col=None, filter_categorical_col=None):
    """
    Remove rows from a dataframe that already exist in a database
    Required:
        df : dataframe to remove duplicate rows from
        engine: SQLAlchemy engine object
        tablename: tablename to check duplicates in
        dup_cols: list or tuple of column names to check for duplicate row values
    Optional:
        filter_continuous_col: the name of the continuous data column for BETWEEEN min/max filter
                               can be either a datetime, int, or float data type
                               useful for restricting the database table size to check
        filter_categorical_col : the name of the categorical data column for Where = value check
                                 Creates an "IN ()" check on the unique values in this column
    Returns
        Unique list of values from dataframe compared to database table
    """
    args = 'SELECT %s FROM %s' %(', '.join(['{0}'.format(col) for col in dup_cols]), tablename)
    args_contin_filter, args_cat_filter = None, None
    if filter_continuous_col is not None:
        # df[filter_continuous_col] = df[filter_continuous_col].astype('datetime64[ns]')
        args_contin_filter = """ %s BETWEEN '%s'
                                          AND '%s' """ %(filter_continuous_col,
                              df[filter_continuous_col].min(), df[filter_continuous_col].max())
        
    if filter_categorical_col is not None:
        args_cat_filter = ' %s in(%s)' %(filter_categorical_col,
                          ', '.join(["'{0}'".format(value) for value in df[filter_categorical_col].unique()]))

    if args_contin_filter and args_cat_filter:
        args += ' Where ' + args_contin_filter + ' AND' + args_cat_filter
    elif args_contin_filter:
        args += ' Where ' + args_contin_filter
    elif args_cat_filter:
        args += ' Where ' + args_cat_filter

    table_df = pd.read_sql(text(args), engine)
    table_col_dtypes = table_df.dtypes.to_dict()

    table_df = table_df.astype(str)
    for col in dup_cols:
        df[col] = df[col].astype(str)       
    
    df = pd.merge(df, table_df, how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)

    if not df.empty:
        for column in table_col_dtypes.keys():
            col_dtype = table_col_dtypes[column]
            df[column] = df[column].astype(col_dtype)
            df[column].replace('None', np.nan, inplace=True)   
            df[column].replace('nan', np.nan, inplace=True)

    return df
    
def print_error():
    if exp_arr:
        print(exp_arr)

def invoke_next_lambda(data, function_name):
    client = boto3.client(
        'lambda',
        aws_access_key_id=f"{aws_access_key_id}", 
        aws_secret_access_key=f"{aws_secret_access_key}",
        region_name=f"{region_name}") 
    
    aws_account_id = os.environ.get('AWS_ACCOUNT_ID')
    response = client.invoke(
                            FunctionName = f'arn:aws:lambda:{region_name}:{aws_account_id}:function:{function_name}',
                            InvocationType = 'Event',
                            Payload = json.dumps(data))
    return response

def get_obj_from_s3(key):
    client = boto3.client(
        's3',
        aws_access_key_id=f"{aws_access_key_id}",
        aws_secret_access_key=f"{aws_secret_access_key}",
        region_name=f"{region_name}")

    obj = client.get_object(Bucket=bucket_name, Key=key)['Body'].read()
    return obj

def update_lambda_status(payload, event, context):
    status_code, error = call_internal_api(APP_URL, LAMBDA_STATUS_ENDPOINT, payload)
    if status_code != 200:
        invoke_etl_api_failure_mail(event, context, error)
    return status_code, error

def trim_dataframe(df):
    df.columns = df.columns.str.strip()
    df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def drop_unwanted_columns(df, columns, type='remove'):
    if type == 'keep':
        df = df.loc[:,df.columns.isin(columns)]
    elif type == 'remove':
        df.drop(columns, axis=1, inplace=True, errors='ignore')
    return df

def remove_inessential_columns(df, table):
    REQUIRED_COLUMNS = Constants.TABLES_CONFIG[table]['REQUIRED_COLUMNS']
    return drop_unwanted_columns(df, REQUIRED_COLUMNS, 'keep')

def clean_digits(data, include_sign=True):
    data = (str(data)).strip()
    if include_sign:
        data = re.sub('[^-.0-9]+', '', data)
    else:
        data = re.sub('[^.0-9]+', '', data)
    
    if data == '' or data == 0 or data == None or data==np.NaN:
        return '0'

    return data

def clean_numeric_columns(df, columns_to_clean, include_sign=True):
    for column in columns_to_clean:
        df[column] = df[column].apply(clean_digits, include_sign=include_sign)
    return df

def get_max_occured_value_from_series(series, atleast_perc, value_to_find):
    total_count = series.count()
    unique_count = series.value_counts()
    max_occured_value = unique_count.first_valid_index()
    max_occured_value_count = unique_count.at[max_occured_value]

    if ( max_occured_value_count < ( atleast_perc * total_count) ):
        raise Exception(f'Unable to find the {value_to_find}')

    return max_occured_value

def get_lines_from_pdf_page(page, delimiter, lower_case=True):
        all_text = page.extract_text()
        lines = all_text.split(delimiter)
        if lower_case:
            lines =  [line.lower() for line in lines]
        return lines
        
def handle_file_loading(filename):
    if os.environ.get('APP_ENV') == 'local':
        with open(filename, 'rb') as f: contents = f.read()
    else:
        contents = get_obj_from_s3(filename)
    contents = BytesIO(contents)
    return contents

def parse_date_in_series(txn_date_series, formats):
    for format in formats:
        try:
            return pd.to_datetime(txn_date_series, format=format)
        except ValueError:
            continue
    raise Exception("Unable to parse date")

def parse_date_in_string(datestring, formats):
    for format in formats:
        try:
            return dt.strptime(datestring, format)
        except ValueError:
            continue
    raise FlowInvalidDateException(f"Unable to parse date: '{datestring}' using given formats: {formats}") 

def timeout_handler(_signal, _frame):
    raise Exception('Time exceeded')

def extract_files_to_temp(filename):
    myzipfile = handle_file_loading(filename)
    os.chdir('/tmp')
    with zipfile.ZipFile(myzipfile, 'r') as zip:
        zip.extractall()

def call_score_api(endpoint, payload):
    # url = os.environ.get('SCORE_APP_URL')
    url = APP_URL
    return call_internal_api(url, endpoint, payload)

def call_internal_api(url, endpoint, payload):
    username = os.environ.get('INTERNAL_API_USERNAME')
    token = os.environ.get('INTERNAL_API_TOKEN')
    
    api_response = requests.post(
        f"{url}{endpoint}",
        json=payload,
        headers={
            "username": username,
            "token": token,
            "purpose": "scoring"
        }
    )
    status_code = api_response.status_code
    try:
        if status_code == 200:
            body = api_response.json()
            data = body['data'] if 'data' in body else None
                
        else:
            data = api_response.json()['message']
            data = f"API Error:\n\tURL:{url}{endpoint}\n\tStatus Code: {status_code}\nThe error message: {data}"

    except requests.exceptions.JSONDecodeError as e:
        error_message = str(e)
        data = f"JsonDecodeError:\n\tURL:{url}{endpoint}\n\tStatus Code: {status_code}\nThe error message: {error_message}"

    return status_code, data

def fill_empty_cells(series, replace_with=0):
    series = series.replace(r'^\s*$', np.nan, regex=True)
    series.fillna(replace_with, inplace=True) # Replace NaN values with 0
    return series

def insert_df_to_db(df, db_con, addl_data):
    exp_list = []
    for chunk in chunkit(df):
        exp = df_to_sql(chunk, addl_data['table'], db_con)
        exp_list.extend(exp)  

    if exp_list:
        invoke_etl_db_failure_mail(addl_data, exp_list)

    return exp_list

def get_exported_data(table_name, run_id, db_con):
    query = f"SELECT * FROM {table_name} where transform_status='NOT_DONE' and export_run_id='{run_id}'"
    df = pd.read_sql(sql=text(query), con=db_con)
    if df.empty:
        raise Exception(f"No records in {table_name} table")
    return df

def rollback_exported_data(table_name, export_run_id, engine):
    db_con = engine.connect()
    query = f"DELETE FROM {table_name} WHERE export_run_id = '{export_run_id}'"
    db_con.execute(text(query))
    db_con.commit()
    db_con.close()

def remove_duplicate_records(df, table, dup_cols, filter_by, db_con, filter_continuous=None):
    df.drop_duplicates(dup_cols, keep='last', inplace=True)
    df = clean_df_db_dups(df, table, db_con, dup_cols, filter_continuous, filter_categorical_col=filter_by)
    return df

def raise_no_new_records_exception(df):
    if df.empty:
        raise Exception('No new records available in the provided sheet')

def invoke_mail_api(notify_type, payload):
    payload['view'] = 'etl_failure_notification'
    payload['notify_type'] = notify_type
    country_code = os.environ.get('COUNTRY_CODE')
    if country_code:
        payload['country_code'] = country_code
    call_internal_api(APP_URL, MAIL_ENDPOINT, payload)

def get_func_n_log_name(context):
    function_name = context.function_name if context else 'local_test'
    log_file = f"{context.log_group_name}/{context.log_stream_name}" if context else '/aws/lambda/test'
    return function_name, log_file

def set_basic_mail_info(event, context, exception):
    function_name, log_file = get_func_n_log_name(context)
    mail_info = {
        'acc_number': event['acc_number'],
        'function_name': function_name,
        'log_file': log_file,
        'event': event,
        'exception': exception,
    }
    return mail_info

def set_mail_attachments(payload, dfs, err_info=None):
    payload['mail_attachments'] = {}
    payload['mail_attachments']['dfs'] = dfs
    if err_info:
        payload['mail_attachments']['err_info'] = err_info
    return payload

def handle_etl_process_failure(event, context, files=None):
    exception = traceback.format_exc()
    print(exception)
    invoke_etl_process_failure_mail(event, context, exception, files)

def invoke_etl_process_failure_mail(event, context, exception, files=None):
    mail_info = set_basic_mail_info(event, context, exception)

    dfs = []
    if files:
        for file in files:
            df = file["df"]
            if df.empty: continue
            
            df_json = df.to_json(orient='split')
            dfs.append({
                'df': df_json,
                'file_name': file['file_name']
            })

    # mail_info = set_mail_attachments(mail_info, dfs)
    invoke_mail_api('etl_process', mail_info)     

def invoke_etl_db_failure_mail(data, errors):
    run_id = data['run_id']
    context = data['context']
    event = data['event']

    temp_dfs = []
    error_messages = []
    for error in errors:
        error_messages.append(error["exception"])
        temp_dfs.append(error["df"])
    final_df = pd.concat(temp_dfs)
    final_df = final_df.to_json(orient='split')

    mail_info = set_basic_mail_info(event, context, errors[0]['exception'])
    mail_info = set_mail_attachments(
        mail_info, 
        [{'df': final_df, 'file_name': f"{run_id}_stmt"}],
        {'errors': error_messages, 'file_name': f"{run_id}_errors"}
    ) 
    invoke_mail_api('etl_db', mail_info)     

def invoke_etl_api_failure_mail(event, context, exception):
    mail_info = set_basic_mail_info(event, context, exception)
    invoke_mail_api('etl_api', mail_info)

def check_stmt_fmt(expected_columns, columns_in_stmt):
    expected_columns = set(expected_columns)    
    common_words = set(columns_in_stmt).intersection(expected_columns)
    return common_words == expected_columns

def check_if_fmt_supported(expected_columns_config, columns_in_stmt):
    for format in expected_columns_config.keys():
        is_fmt_supported = check_stmt_fmt(expected_columns_config[format], columns_in_stmt)
        if is_fmt_supported:
            return format
    return False

def check_acc_number(df, acc_number_column, match_percent, acc_number):
    acc_number_from_stmt = get_max_occured_value_from_series(df[acc_number_column], match_percent, 'Account Number')
    if str(acc_number_from_stmt) != str(acc_number):
        raise Exception(f"Account number entered '{acc_number}' does not match with account number from Statement '{acc_number_from_stmt}'")
    return acc_number_from_stmt

def handle_zip_files(object_key):
    main_file = handle_file_loading(object_key)
    if os.environ.get('APP_ENV') == 'local':
        return main_file
    else:
        z = zipfile.ZipFile(main_file)
        txn_file = z.namelist()[0]
        with z.open(txn_file) as f: file = f.read()
        file = BytesIO(file)
        return file

def set_payload(event, optional_keys, error_message=None):
    payload = {
        "country_code": event['country_code'],
        "flow_req_id": event['run_id'],
        "status": event['status'],
        "lambda_status": event['lambda_status'],
    }

    for optional_key in optional_keys:
        payload[optional_key] = event[optional_key] if optional_key in event else None
    if error_message:
        payload['error_message'] = error_message
    return payload

def cleanup(engine, db_con=None):
    if engine:
        engine.dispose()
    if db_con:
        db_con.close()
    signal.alarm(0)

def set_timeout_signal(context, raise_before_remaining=10):
    """
    Sets the signal to interrupt execution if time exceeds a certain threshold.\n
    Required:
        context : The context input given to lambda
        raise_before_remaining : The remaining time in seconds before which the signal should fire
    """
    signal.signal(signal.SIGALRM, timeout_handler)
    if context:
        signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - raise_before_remaining)

def notify_process_status(event, context, df, optional_keys, error_message=None):
    """
    Notifies the FLOW API regarding the status of the ETL process so that it can perform the necessary operations
    Required:
        event : The event input given to lambda
        context : The context input given to lambda
        df : The dataframe generated from the transaction statement
        optional_keys : Additional keys to set to the payload
        error_message : Error message if the status is set as failed
    """
    acc_number = event['acc_number']
    run_id = event['run_id']
    payload = set_payload(event, optional_keys, error_message)
    update_lambda_status(payload, event, context)
    if error_message:
        files = [{'df': df, 'file_name': f"{acc_number}_{run_id}"}]
        handle_etl_process_failure(event, context, files)

def clean_n_insert(df, db_con, addl_data, clean_func):
    df = clean_func(df, db_con, addl_data)
    exp_list = insert_df_to_db(df, db_con, addl_data)
    return df, exp_list

def invoke_score_calc(event, no_of_days):
    data = {
        "time_zone": os.environ.get('TIME_ZONE'),
        "no_of_days": no_of_days
    }
    event.update(data)
    if os.environ.get('APP_ENV') == 'local':
        from score import score_calc
        score_calc(event, None)

def remove_dup_in_df(df, db_con, table):
    UNIQUE_COLUMNS = Constants.TABLES_CONFIG[table]['UNIQUE_COLUMNS']
    FILTER_BY = Constants.TABLES_CONFIG[table]['FILTER_BY']
    FILTER_CONTINUOUS = Constants.TABLES_CONFIG[table]['FILTER_CONTINUOUS']
    df = remove_duplicate_records(df, table, UNIQUE_COLUMNS, FILTER_BY, db_con, FILTER_CONTINUOUS)
    if table != 'flow_cust_acc_stmts':
        raise_no_new_records_exception(df)
    return df

def handle_transform_failure(event, context, df, optional_keys, error_message, export_table, engine):
    rollback_exported_data(export_table, event['run_id'], engine)
    notify_process_status(event, context, df, optional_keys, error_message)

def set_country_code_n_status(event, country_code, status, stage):
    event['country_code'], event['status'], event['lambda_status'] = country_code, status, f"{stage}_{status}"
    return event

def invoke_transform(event):
    if os.environ.get('APP_ENV') == 'local':
        ap_code = event['acc_prvdr_code']
        if ap_code == 'RMTN':
            from rmtn_transform import lambda_handler2
        elif ap_code == 'RBOK':
            from rbok_transform import lambda_handler2
        elif ap_code == 'RRTN':
            from rrtn_transform import lambda_handler2
        elif ap_code == 'RATL':
            from ratl_transform import lambda_handler2
        elif ap_code == 'UEZM':
            from uezm_transform import lambda_handler2
        lambda_handler2(event, None)

def set_session(country_code):
    load_dotenv()
    engine = connect_to_database_engine('STMT')
    df = pd.DataFrame()
    os.environ["COUNTRY_CODE"] = country_code
    os.environ["TIME_ZONE"] = Constants.COUNTRY_TO_TIME_ZONE[country_code]
    return df, engine, country_code

def insert_commission_records(txn_df, comms_df, date_field, required_common_months, month_to_offset, format):

    txn_df[date_field] =  pd.to_datetime(txn_df[date_field], format=format)
    comms_df[date_field] =  pd.to_datetime(comms_df[date_field], format=format)

    if month_to_offset != None:
        comms_df[date_field] = comms_df[date_field] + pd.offsets.DateOffset(months=month_to_offset)

    txn_months = txn_df[date_field].dt.strftime("%m %Y").unique().tolist()
    comms_months = comms_df[date_field].dt.strftime("%m %Y").unique().tolist()
    common_months = list(set(txn_months) & set(comms_months))
    if len(common_months) < required_common_months:
        raise Exception(f"The common months between commission and transaction statement is less than {required_common_months}. \nCommission months: {comms_months} \nTransaction months: {txn_months}")

    print(f"Common Months btw float and comms stmt: {common_months}")
    
    txn_df = txn_df[txn_df[date_field].dt.strftime('%m %Y').isin(common_months)]
    if month_to_offset != None:
        comms_df[date_field] = comms_df[date_field] + pd.offsets.DateOffset(months=-month_to_offset)

    txn_df = pd.concat([txn_df, comms_df])
    return txn_df
    
def main():
    print("Inside Common!")

if __name__ == "__main__":
    main()