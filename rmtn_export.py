import pandas as pd
from datetime import datetime as dt
import re
from common import FlowInvalidDateException, insert_commission_records, extract_files_to_temp, remove_inessential_columns, set_session, invoke_transform, remove_dup_in_df, set_country_code_n_status, clean_n_insert, notify_process_status, set_timeout_signal, cleanup, handle_zip_files, check_acc_number, clean_numeric_columns, parse_date_in_series, parse_date_in_string, trim_dataframe
import pdfplumber
from fuzzywuzzy import process

REPLACABLE_TYPES =   {
    'Float T': 'Float Transfer',
    'Batch t': 'Batch transfer',
    'External p': 'External payment',
    'External t': 'External transfer'
}
EXPECTED_DT_FORMATS = ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%m/%d/%Y %H:%M', '%d/%m/%Y %H:%M']
DT_FORMATS_IN_PDF = [dt_format.split(' ')[0] for dt_format in EXPECTED_DT_FORMATS]
REQUIRED_HEADERS = ['txn_date', 'txn_type', 'from', 'to', 'amount', 'balance']
EXPECTED_TXN_TYPES = ['float transfer', 'cash out', 'cash in', 'batch transfer', 'deposit', 'debit', 'transfer', 'reversal', 'external payment', 'payment', 'external transfer']
REQUIRED_COMMON_MONTHS = 3

def rename(df):
    columns = df.columns
    rename_columns = { 
                        'Id': 'txn_id',
                        'Date': 'txn_date',
                        'Status': 'status',
                        'Type': 'txn_type',
                        'From': 'from',
                        'From Name': 'from_name',
                        'To': 'to',
                        'To Name': 'to_name',
                        'Amount': 'amount',
                        'Fee': 'fee',
                        'Balance': 'balance'
    }
    
    from_name = process.extractOne('From Handler Name', columns)[0] 
    to_name = process.extractOne('To Handler Name', columns)[0] 

    rename_names2 = {
        from_name: 'from_name',
        to_name: 'to_name',
        'CurrencyBalance': 'balance'
    }
    df.rename(columns=rename_columns, inplace=True)
    df.rename(columns={k: v for k, v in rename_names2.items() if v not in df}, inplace=True)

    return df

def clean_txn_type(txn_type):
    for replacable_type in REPLACABLE_TYPES:
       if replacable_type in txn_type:
           return REPLACABLE_TYPES[replacable_type]
    return txn_type

def clean_msisdn_column(text):
    text = str(text)
    text = re.sub('[^0-9]+', '', text)
    return None if text == '' else f"FRI:{text}/MSISDN"

def predict_header(columns):
    for index, cell in enumerate(columns):
        try:
            if parse_date_in_string(cell, EXPECTED_DT_FORMATS): columns[index] = 'Date'
        except FlowInvalidDateException:
            if 'success' in cell.lower(): columns[index] = 'Status'
            only_numbers = re.sub('[^0-9]+', '', cell)
            if only_numbers.startswith('250'): 
                columns[index], columns[index+1], columns[index+2], columns[index+3] = 'From', 'From Name', 'To', 'To Name'
                continue
            if only_numbers != '':
                remaining_columns = len(columns) - index
                if remaining_columns == 3:
                    columns[index], columns[index+1], columns[index+2] = 'Amount', 'Fee', 'Balance' 
                elif remaining_columns == 2:
                    columns[index], columns[index+1] = 'Amount', 'Balance' 
            else:
                if cell.lower() in EXPECTED_TXN_TYPES: columns[index] = 'Type'
    return columns

def clean_header(df):
    # non_na_rows = df.shape[1] - MAX_NA_ALLOWED
    required_non_na_rows = 2
    df = df.dropna(thresh=required_non_na_rows)

    first_row = df.iloc[0]
    if 'Date' in first_row.to_list():
        new_header = first_row #grab the first row for the header
        df = df[1:] #take the data other than the header row
        df.columns = new_header
    
    if 'Date' not in df.columns:
        columns = df.columns.to_list()
        header = predict_header(columns)
    
        df.loc[-1] = df.columns
        df.index = df.index + 1
        df.sort_index(inplace=True)
        df.columns = header

    df = rename(df)
    header = df.columns.to_list()
    remaining = list(set(REQUIRED_HEADERS).difference(set(header)))
    if remaining: raise Exception(f"The required columns: {REQUIRED_HEADERS} are not present in statement: {header}")

    return df

def find_acc_number(record):
    msisdn = record['to'] if ( record['amount'] >= 0 ) else record['from']
    acc_number = re.sub('[^0-9]+', '', msisdn)[3:]

    if ('MSISDN' not in msisdn) or (len(acc_number) != 9):
        return None
    return acc_number

def get_txn_id(record):
    txn_date = dt.strptime(str(record['txn_date']), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
    return 'dummy_' + txn_date + str(abs(record['amount'])) + record['acc_number']

def perform_basic_cleaning(df):
    df = trim_dataframe(df)
    df = clean_header(df)
    df = df[~df['txn_date'].isnull()]

    df['txn_date'] = parse_date_in_series(df['txn_date'], EXPECTED_DT_FORMATS)
    df['txn_type'] = df['txn_type'].apply(clean_txn_type)
    df['from'] = df['from'].apply(clean_msisdn_column)
    df['to'] = df['to'].apply(clean_msisdn_column)

    df = clean_numeric_columns(df, ['amount', 'balance'])
    df['amount'] = df['amount'].astype(int)
    return df

def clean(txn_df, comms_df, acc_number, run_id, export_table):
    txn_df = perform_basic_cleaning(txn_df)
    if not comms_df.empty:
        comms_df = perform_basic_cleaning(comms_df)
        comms_df = comms_df[comms_df['txn_type'] == 'Batch transfer']
        txn_df = insert_commission_records(txn_df, comms_df, 'txn_date', REQUIRED_COMMON_MONTHS, -1, '%Y-%m-%d %H:%M:%s')

    txn_df['acc_number'] = txn_df.apply(find_acc_number, axis=1)
    check_acc_number(txn_df, 'acc_number', 0.9, acc_number)

    txn_df = txn_df.assign( acc_number=acc_number,
                            export_run_id=run_id, 
                            transform_status='NOT_DONE')

    if 'txn_id' not in txn_df.columns:
        txn_df['txn_id'] = txn_df.apply(get_txn_id, axis=1)
        
    txn_df = remove_inessential_columns(txn_df, export_table)
    return txn_df

def clean_df(df, db_con, addl_data):
    acc_number = addl_data['acc_number']
    run_id = addl_data['run_id']
    export_table = addl_data['table']
    comms_df = addl_data['comms_df']

    df = clean(df, comms_df, acc_number, run_id, export_table)
    df = remove_dup_in_df(df, db_con, export_table)
    return df

def get_header(page):
    words = page.extract_words(keep_blank_chars=True)
    header = []
    is_header = False
    for word in words:
        text = word['text']
        text_lower = text.lower()
        
        if text_lower == 'date' or text_lower == 'id':
            is_header=True
        if is_header:
            header.append(text)
        if 'balance' in text_lower:
            break
    return header

def get_header_n_vlines(page):
    words = page.extract_words(keep_blank_chars=True)
    header = []
    v_lines = []
    
    is_header = False
    for word in words:
        text = word['text']
        text_lower = text.lower()
        
        if text_lower == 'date' or text_lower == 'id':
            is_header=True
        if is_header and text_lower != 'currency':
            header.append(text)
            v_lines.append(word['x0'])
        if 'balance' in text_lower:
            v_lines.append(word['x1']+10)
            break
    return header, v_lines

def get_df_from_stmt_without_lines(pages, header, v_lines):
    records = []
    for page in pages:
        words = page.extract_words(keep_blank_chars=False)
        h_lines = []

        for word in words:
            text = word['text']
            try:
                text = text.strip()[0:10]
                parse_date_in_string(text, DT_FORMATS_IN_PDF)

                h_lines.append(word['top'])
                last_line = word['bottom']
            except FlowInvalidDateException:
                # if page.page_number != 1: continue
                # if text in header:
                #     v_lines.append(word['x0'])
                # if text == header[-1]:
                #     v_lines.append(word['x1']+10)
                continue

        h_lines.append(last_line)
        tbl = page.extract_table(table_settings = { "vertical_strategy": "explicit", 
                                                    "horizontal_strategy": "explicit",
                                                    "explicit_vertical_lines": v_lines,
                                                    "explicit_horizontal_lines": h_lines
                                                    })

        records.extend(tbl)
    df = pd.DataFrame(records, columns = header)
    return df

def handle_pdf_files(pages):
    records = []
    for i in range(len(pages)):
        tbl = pages[i].extract_table() 
        if i == 0:
            if not tbl: return pd.DataFrame()
            header = tbl.pop(0)
        records.extend(tbl)
    df = pd.DataFrame(records, columns = header)
    return df

def get_df(file_name, file_extension):
    if (file_extension == 'xlsx') or (file_extension == 'xls'):
        df = pd.read_excel(file_name)
    elif file_extension == 'csv':
        df = pd.read_csv(file_name)
    elif file_extension == 'pdf':
        pdf = pdfplumber.open(file_name)
        pages = pdf.pages
        df = handle_pdf_files(pages)
        if df.empty: 
            # header = get_header(pages[0])
            header, v_lines = get_header_n_vlines(pages[0])
            df = get_df_from_stmt_without_lines(pages, header, v_lines)
    else:
        raise Exception(f'File format not supported: {file_extension}')
    return df

def process_file_and_get_dfs(object_key, file_json):
    extract_files_to_temp(object_key)

    txn_file_name, txn_file_type = next(((file['file_name'], file['file_type']) for file in file_json['files'] if file["file_of"] == "txn_stmt"), False)
    # Check if comms stmt exist in the file_json template
    if any(file['file_of'] == 'comm_stmt' for file in file_json['files']): 
        comms_file_name, comms_file_type = next(((file['file_name'], file['file_type']) for file in file_json['files'] if file["file_of"] == "comm_stmt"), False) 
    else:
        comms_file_name, comms_file_type = None, None

    txn_file_extension = txn_file_type.split('/')[-1]
    txn_df = get_df(txn_file_name, txn_file_extension)

    # Checks if comms stmt is uploaded
    if comms_file_name:
        comms_file_extension = comms_file_type.split('/')[-1]
        comms_df = get_df(comms_file_name, comms_file_extension)
    else:
        comms_df = pd.DataFrame()

    return txn_df, comms_df

def export(event, context, engine, df, country_code):
    try:
        run_id = event['run_id'] = f"{event['flow_req_id']}"
        acc_number = event['acc_number'] 
        file_json = event['file_json']
        object_key = event['object_key']
        export_table = 'rmtn_cust_acc_stmts'
        event = set_country_code_n_status(event, country_code, 'success', 'export')
        set_timeout_signal(context, 10)
        
        with engine.begin() as db_con:
            df, comms_df  = process_file_and_get_dfs(object_key, file_json)
            addl_data = {'comms_df': comms_df, 'run_id': run_id,'acc_number': acc_number, 'table': export_table, 'event': event, 'context': context}
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
