import os
import numpy as np
import pandas as pd
import pdfplumber
from fuzzywuzzy import fuzz
from datetime import datetime as dt
from common import Constants, remove_inessential_columns, set_session, invoke_transform, clean_n_insert, remove_dup_in_df, notify_process_status, cleanup, set_timeout_signal, set_country_code_n_status, clean_digits, clean_numeric_columns, drop_unwanted_columns, extract_files_to_temp, get_lines_from_pdf_page, parse_date_in_series, parse_date_in_string, trim_dataframe

REQUIRED_COMMS_MONTHS = 3
TXN_STMT_EXPECTED_WORDS = {
    'internet_bnk_fmt_1' : ['narration', 'ref', 'number', 'in', 'money', 'out', 'balance'],
    'bnk_fmt_1' : ['book', 'reference', 'description', 'narration', 'value', 'debit', 'credit', 'closing', 'balance'],
    }

COMM_STMT_EXPECTED_WORDS = {
    'internet_bnk_fmt_1' : ['narration', 'ref', 'number', 'in', 'money', 'out', 'balance'],
    'bnk_fmt_1' : ['book', 'reference', 'description', 'narration', 'value', 'debit', 'credit', 'closing', 'balance'],
    'email_fmt_1' : ['transaction', 'type', 'number', 'value', 'commission'],
    }

STMT_COLUMN_RENAME = {
    'internet_bnk_fmt_1':   {
                                'Date': 'txn_date',
                                'Narration': 'descr',
                                'Ref Number': 'txn_id',
                                'Money In': 'cr_amt',
                                'Money Out': 'dr_amt',  
                                'Balance': 'balance'          
                            },
    'bnk_fmt_1':   {
                                'Book Date' : 'txn_date',
                                'Reference' : 'txn_id',
                                'Description' : 'txn_type',
                                'Narration' : 'descr',
                                'Value Date' : 'value_date',
                                'Debit' : 'dr_amt',
                                'Credit' : 'cr_amt',
                                'Closing Balance' : 'balance'
                            }
}
NUMERIC_COLUMNS = Constants.TABLES_CONFIG['rbok_cust_acc_stmts']['NUMERIC_COLUMNS']

def get_df_from_internet_bnk_fmt_1(pages):
    records = []
    for i in range(len(pages)):
        tbl = pages[i].extract_table()
        if not tbl:
            continue
        if i == 0:
            header = tbl.pop(0)
        records.extend(tbl)
    df = pd.DataFrame(records, columns = header)
    return df
    
def get_df_from_bnk_fmt_1(pages):
    header = ['Book Date', 'Reference', 'Description', 'Narration', 'Value Date', 'Debit', 'Credit', 'Closing Balance']
    records = []

    for page in pages:
        top_crop_perc = 0.265 if page.page_number == 1 else 0.11
        page_top = top_crop_perc * float(page.height)
        page_bottom = 0.932 * float(page.height)

        cropped_page = page.crop((0, page_top, page.width, page_bottom))

        vlines = [35.8, 90.5, 165.5, 228.8, 297.76, 355.78, 418.78, 478.8, 558.8]
        hlines = []

        fnd = cropped_page.find_tables(table_settings = {
                                        "vertical_strategy": "explicit", 
                                        "horizontal_strategy": "text",
                                        "explicit_vertical_lines": vlines
                                    })[0]

        cell_texts = cropped_page.extract_tables(table_settings = {
                                                "vertical_strategy": "explicit", 
                                                "horizontal_strategy": "text",
                                                "explicit_vertical_lines": vlines
                                            })[0]

        h_cell_borders = []
        for index, cell in enumerate(cell_texts):
            left_border, top_border, right_border, bottom_border  = 0, 1, 2, 3
            try:
                dt.strptime(cell[0], "%d %b %y")
                h_cell_borders.append(fnd.rows[index].cells[0][top_border])
            except ValueError:
                if 'balance at' in cell[1].lower():
                    h_cell_borders.append(fnd.rows[index].cells[0][top_border])
                elif (cell[2] != '') and (cell.count("") == len(cell)-1):
                    h_cell_borders.extend([
                                            fnd.rows[index].cells[0][top_border],
                                            fnd.rows[index].cells[0][bottom_border]
                                        ])
                continue

        hlines = [page_top] + h_cell_borders + [page_bottom]
        tbl = cropped_page.extract_tables(table_settings = {"vertical_strategy": "explicit", 
                                                            "horizontal_strategy": "explicit",
                                                            "explicit_vertical_lines": vlines,
                                                            "explicit_horizontal_lines": hlines
                                                        })[0]

        records.extend(tbl)
    df = pd.DataFrame(records, columns = header)
    return df
    
def check_format(line, expected_words):
    line = " ".join(line.split())
    words_in_line = line.split()
    common_words = list(set(words_in_line) & set(expected_words))
    if set(common_words) == set(expected_words):
        return True
    else:
        return False

def determine_stmt_format(page, stmt_type):
    lines = get_lines_from_pdf_page(page, '\n', True)
    for line in lines:
        line = " ".join(line.split())
        if (stmt_type == 'txn'):
            for stmt_fmt in TXN_STMT_EXPECTED_WORDS.keys():
                if check_format(line, COMM_STMT_EXPECTED_WORDS[stmt_fmt]):
                    return stmt_fmt
        elif (stmt_type == 'comm'):
            for stmt_fmt in COMM_STMT_EXPECTED_WORDS.keys():
                if check_format(line, COMM_STMT_EXPECTED_WORDS[stmt_fmt]):
                    return stmt_fmt
    return False 

def get_df_from_txn_stmt(txn_stmt_fmt, pages):
    if txn_stmt_fmt == 'internet_bnk_fmt_1':
        df = get_df_from_internet_bnk_fmt_1(pages)
    elif txn_stmt_fmt == 'bnk_fmt_1':
        df = get_df_from_bnk_fmt_1(pages)
    else:
        raise Exception('Statement format is not yet supported')
    return df

def rename(df, stmt_fmt):
    df.rename(columns=STMT_COLUMN_RENAME[stmt_fmt], inplace=True)
    return df	

def get_df_from_pdf_page(page):
    records = []
    tbl = page.extract_tables()[0]        
    header = tbl.pop(0)
    records.extend(tbl)
    df = pd.DataFrame(records, columns = header)
    return df

def find_substr_index(text, stmt_type):
    if stmt_type == 'float':
        remove_names = ['float', 'foat', '/float']
    elif stmt_type == 'comms':
        remove_names = ['commision', 'commission', 'commisio', 'cion acc']

    for name in remove_names:
        float_substr_index = text.find(name)
        if float_substr_index == -1:
            continue
        else:
            return float_substr_index
    return float_substr_index

def get_acc_name_from_pdf(page, stmt_type):        
    lines = get_lines_from_pdf_page(page, '\n', True)
    name = ''
    for index, line in enumerate(lines):
        #internet_bnk_fmt_1
        if ('opening balance' in line) and ('closing balance' in lines[index+2]):
            text = lines[index+1]
            float_substr_index = find_substr_index(text, stmt_type)
            name = text[0:float_substr_index].strip()
            break
        #bnk_fmt_1
        if ('account name :' in line) and ('account number :' in lines[index-1]):
            text = line + lines[index+1]
            float_substr_index = find_substr_index(text, stmt_type)
            text = text[0:float_substr_index].strip()
            name = text.split(':')[1].strip()
            break
        if (stmt_type == 'comms') and ('agent report' in line):
            text = f"{lines[index]} {lines[index+1]}"
            comms_name_index = text.find('for')
            name = text[comms_name_index+3:].strip()
            break

    name = " ".join(name.split())
    if name == '':
        raise Exception(f'Unable to locate Account Holder name in {stmt_type} statement')
    return name

def get_float_acc_number(page):
    lines = get_lines_from_pdf_page(page, '\n', True)
    acc_number = ''
    for line in lines:
        #internet_bnk_fmt_1 and bnk_fmt_1
        if ('accountnumber' in line) or ('account number' in line):
            text = line.split(':')[1].strip()
            acc_number = text.split(' ')[0].strip()
            break
        elif ('account no' in line):
            text = line.split(':')[1].strip()
            text = " ".join(text.split())
            acc_number = text.split(' ')[2].strip()
            break
    
    acc_number = " ".join(acc_number.split())
    if acc_number == '':
        raise Exception('Unable to locate Account number in Float statement')
    return acc_number

def get_float_acc_details(txn_stmt_fmt, txn_pdf_pages, acc_number):
    page_1 = txn_pdf_pages[0]
    float_acc_details = dict()
    if txn_stmt_fmt != 'bnk_fmt_1':
        float_acc_number = get_float_acc_number(page_1)
        validate_acc_number(float_acc_number, acc_number)
        float_acc_details['float_acc_number'] = float_acc_number
    else:
        float_acc_details['float_acc_number'] = acc_number

    float_acc_details['float_acc_name'] = get_acc_name_from_pdf(page_1, 'float')
    float_acc_details['df'] = get_df_from_txn_stmt(txn_stmt_fmt, txn_pdf_pages)
    return float_acc_details

def check_comms_and_float_name(float_acc_name, comms_acc_name):
    if fuzz.token_sort_ratio(comms_acc_name, float_acc_name) < 90:
        raise Exception(f"Commission name '{comms_acc_name}' does not match with Float Statement Name '{float_acc_name}'")
    return comms_acc_name

def validate_acc_number(float_stmt_number, acc_number_entered):
    if float_stmt_number != acc_number_entered:
        raise Exception(f"Account number entered '{acc_number_entered}' does not match with account number from Float Statement '{float_stmt_number}'")

def get_comms_acc_name_from_monthly_report_pdf(pages):
    df = get_df_from_pdf_page(pages[1])
    try:
        names = df['Outlet'].values
        comms_acc_name = names[names != np.array(None)][0]
    except KeyError:
        comms_acc_name = get_acc_name_from_pdf(pages[0], 'comms')
    comms_acc_name = comms_acc_name.lower()
    return comms_acc_name
    
def get_comms_amt_pdf(page):
    comms_df = get_df_from_pdf_page(page)
    comms_df['Commission'] = comms_df['Commission'].apply(clean_digits)
    comms_amts = comms_df[comms_df['Transaction type'] == 'TOTAL']['Commission']

    comms_amts = comms_amts.to_list()
    comms_amts = [int(comms_amt) for comms_amt in comms_amts]
    return clean_digits(sum(comms_amts))

def get_month_last_date(datestring):
    possible_formats = ['%B %Y', '%d/%m/%Y']
    date_obj = parse_date_in_string(datestring, possible_formats)
    import calendar
    import datetime
    return datetime.date(date_obj.year, date_obj.month, calendar.monthrange(date_obj.year, date_obj.month)[-1])

def get_comms_date_pdf(page):
    lines = get_lines_from_pdf_page(page, '\n', True)
    for line in lines:
        if ('monthly agent report of' in line):
            text = line.replace('monthly agent report of', '').strip()
            text = text.split(' ')
            datestring = f"{text[0]} {text[1]}"
            break
        if ('aggregated agent report' in line):
            text = line.replace('aggregated agent report', '').strip()
            text = text.split('-')[1].strip()
            datestring = text.split(' ')[0].strip()
            break
    else:
        raise Exception('Unable to locate Commission Date in Commission statement')
    return get_month_last_date(datestring)

def get_email_fmt_1_comms_df(pages, float_acc_details):
    page_1 = pages[0]
    
    comms_name = get_comms_acc_name_from_monthly_report_pdf(pages)
    comms_name = check_comms_and_float_name(float_acc_details['float_acc_name'], comms_name)

    comms_datetime = get_comms_date_pdf(page_1)
    comms_amt = get_comms_amt_pdf(page_1)
    txn_id = get_txn_id(comms_datetime, comms_amt, float_acc_details['float_acc_number'])

    header = ['txn_id','txn_date','txn_type','descr','cr_amt','dr_amt','balance']
    data = [[txn_id, comms_datetime, 'Commission', None, comms_amt, 0, 0]]
    comms_df = pd.DataFrame(data=data, columns=header)
    return comms_df

def filter_comms_records(df):
    filter_comms = ['agent commission payment', 'aa loan payoff', 'cash advance commission']
    comms_df = df[df.txn_type.str.lower().str.contains('|'.join(filter_comms), na=False)]
    txn_df = df[~df.txn_type.str.lower().str.contains('|'.join(filter_comms), na=False)]
    return txn_df, comms_df

def get_bnk_fmt_1_comms_df(pages, float_acc_details):
    comms_acc_name = get_acc_name_from_pdf(pages[0], 'comms')
    comms_name = check_comms_and_float_name(float_acc_details['float_acc_name'], comms_acc_name)
    comms_df = get_df_from_bnk_fmt_1(pages)
    comms_df = perform_basic_cleaning(comms_df)
    comms_df = rename(comms_df, 'bnk_fmt_1')

    comms_df = clean_numeric_columns(comms_df, NUMERIC_COLUMNS)
    comms_df = addl_clean_bnk_fmt_1(comms_df)
    txn_df, comms_df = filter_comms_records(comms_df)

    comms_df = comms_df.assign(balance=0)   
    return comms_df

def get_internet_bnk_fmt_1_comms_df(pages, float_acc_details):
    comms_acc_name = get_acc_name_from_pdf(pages[0], 'comms')
    comms_name = check_comms_and_float_name(float_acc_details['float_acc_name'], comms_acc_name)
    comms_df = get_df_from_internet_bnk_fmt_1(pages)
    comms_df = perform_basic_cleaning(comms_df)
    comms_df = rename(comms_df, 'internet_bnk_fmt_1')

    comms_df = clean_numeric_columns(comms_df, NUMERIC_COLUMNS)
    comms_df = addl_clean_internet_bnk_fmt_1(comms_df)
    txn_df, comms_df = filter_comms_records(comms_df)
    
    comms_df = comms_df.assign(balance=0)   
    return comms_df

def get_df_from_comm_stmt(pdf_file, comms_pdf_format, float_acc_details):
    pdf = pdfplumber.open(pdf_file)
    pages = pdf.pages
    if comms_pdf_format == 'internet_bnk_fmt_1':
        comms_df = get_internet_bnk_fmt_1_comms_df(pages, float_acc_details)
    elif comms_pdf_format == 'email_fmt_1':
        comms_df = get_email_fmt_1_comms_df(pages, float_acc_details)
    elif comms_pdf_format == 'bnk_fmt_1':
        comms_df = get_bnk_fmt_1_comms_df(pages, float_acc_details)
    return comms_df

def get_comms_df(files_details, float_acc_details):
    all_comms_df = pd.DataFrame()
    for file_details in files_details:
        file_of = file_details['file_of']
        file_name = file_details['file_name']
        stmt_fmt = file_details['stmt_fmt']

        if file_of != 'txn_stmt':
            temp_df = get_df_from_comm_stmt(file_name, stmt_fmt, float_acc_details)
            all_comms_df = temp_df if all_comms_df.empty else pd.concat([all_comms_df,temp_df])
    return all_comms_df

def get_txn_type(descr):
    text = descr.split('/')[0]
    text = text.strip()
    txn_type = None if text == '' else text
    return txn_type

def get_txn_id(date_obj, amount, acc_number):
    txn_date = date_obj.strftime('%Y%m%d%H%M%S')
    return 'dummy_' + txn_date + clean_digits(amount) + acc_number

def insert_commission_records(txn_df, comms_df):
    txn_df['txn_date'] =  pd.to_datetime(txn_df['txn_date'], format='%Y-%m-%d')
    comms_df['txn_date'] =  pd.to_datetime(comms_df['txn_date'], format='%Y-%m-%d')

    txn_months = txn_df['txn_date'].dt.strftime("%m %Y").unique().tolist()
    comms_months = comms_df['txn_date'].dt.strftime("%m %Y").unique().tolist()
    common_months = list(set(txn_months) & set(comms_months))

    print(f"Common Months btw float and comms stmt: {common_months}")
    txn_df = pd.concat([txn_df, comms_df])
    txn_df = txn_df[txn_df['txn_date'].dt.strftime('%m %Y').isin(common_months)]

    if txn_df.empty:
        raise Exception(f"The commission months {comms_months} and transaction months {txn_months} don't match")
    months_in_df = txn_df['txn_date'].dt.strftime("%m").unique().tolist()
    print(f"Months in df after removing uncommon months: {months_in_df}")

    if len(months_in_df) < REQUIRED_COMMS_MONTHS:
        raise Exception(f"The common months between commission and transaction statement is less than {REQUIRED_COMMS_MONTHS}. \nCommission months: {comms_months} \nTransaction months: {txn_months}")
        
    txn_df['txn_date'] = txn_df['txn_date'].dt.strftime('%Y-%m-%d')
    comms_df['txn_date'] = comms_df['txn_date'].dt.strftime('%Y-%m-%d')
    return txn_df

def perform_basic_cleaning(df):
    df = df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True)
    # Drop empty rows
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df.dropna(how='all', inplace=True)

    df = trim_dataframe(df)
    return df

def addl_clean_internet_bnk_fmt_1(df):
    df['txn_type'] = df['descr'].apply(get_txn_type)
    df = df[~df['txn_date'].str.contains('Date', na=False)]
    return df

def addl_clean_bnk_fmt_1(df):
    non_na_rows = df.shape[1] - 2
    df = df.dropna(thresh=non_na_rows)
    df = df[~df['txn_date'].str.contains('Book Date', na=False)]

    filter_bal = [ 'balance at period start', 'balance at period end' ]
    df = df[~df['txn_id'].str.lower().str.contains('|'.join(filter_bal), na=False)]

    df['txn_date'] = parse_date_in_series(df['txn_date'], ["%d %b %y"])
    df['txn_date'] = df['txn_date'].dt.strftime('%Y-%m-%d')
    return df

def separate_comms_from_df(df, comms_df):
    txn_df, comms_in_float_stmt = filter_comms_records(df)
    if comms_df.empty:
        comms_df = comms_in_float_stmt
    return txn_df, comms_df

def handle_addl_clean(df, txn_stmt_fmt, comms_df):
    if txn_stmt_fmt == 'internet_bnk_fmt_1':
        df = addl_clean_internet_bnk_fmt_1(df)
    elif txn_stmt_fmt == 'bnk_fmt_1':
        df = addl_clean_bnk_fmt_1(df)
    df, comms_df = separate_comms_from_df(df, comms_df)
    return df, comms_df

def clean(txn_df, addl_data):
    txn_df = perform_basic_cleaning(txn_df)
    txn_df = rename(txn_df, addl_data['txn_stmt_fmt'])
    txn_df, comms_df = handle_addl_clean(txn_df, addl_data['txn_stmt_fmt'],addl_data['comms_df'])

    acc_number = addl_data['acc_number']
    txn_df = insert_commission_records(txn_df, comms_df)
    txn_df = txn_df.assign(acc_number=acc_number, export_run_id=addl_data['run_id'], transform_status='NOT_DONE')

    export_table = addl_data['table']
    txn_df = clean_numeric_columns(txn_df, NUMERIC_COLUMNS, False)
    txn_df = remove_inessential_columns(txn_df, export_table)
    return txn_df

def get_txn_pdf(file_json):
    txn_stmt_file = next((file['file_name'] for file in file_json['files'] if file["file_of"] == "txn_stmt"), False)
    if not txn_stmt_file:
        raise Exception('Transaction Statement is not uploaded')
    return pdfplumber.open(txn_stmt_file)

def validate_files(file_json, txn_pdf):
    files = file_json['files']
    files_details = list()
    validation_status = 'failed'
    for index, file in enumerate(files):
        file_details = dict()
        file_name = file['file_name']
        if file_name and os.path.exists(file_name):
            file_of = file['file_of']
            if  file_of == 'txn_stmt':
                stmt_fmt = determine_stmt_format(txn_pdf.pages[0], 'txn')
            else:
                comm_pdf = pdfplumber.open(file_name)
                stmt_fmt = determine_stmt_format(comm_pdf.pages[0], 'comm')
            if stmt_fmt:
                file_json['files'][index]['file_err'] = None
            else:
                file_json['files'][index]['file_err'] = f"Uploaded {file['file_label']} format is not supported"
            
            file_details['file_name'] = file_name
            file_details['file_of'] = file_of
            file_details['stmt_fmt'] = stmt_fmt
            files_details.append(file_details)

    for file in files:
        if file['file_err']:
            return validation_status, file_json, files_details

    validation_status = 'success'
    return validation_status, file_json, files_details

def clean_df(df, db_con, addl_data):
    df = clean(df, addl_data)
    df = remove_dup_in_df(df, db_con, addl_data['table'])
    return df
    
def get_txn_n_comms_df(object_key, file_json, acc_number):
    extract_files_to_temp(object_key)
    txn_pdf = get_txn_pdf(file_json)

    validation_status, file_json, files_details = validate_files(file_json, txn_pdf)
    if validation_status == 'failed':
        raise Exception('One or more file stmt formats not supported') 
    
    txn_stmt_fmt = next((file_details['stmt_fmt'] for file_details in files_details if file_details["file_of"] == "txn_stmt"), False)    
    float_acc_details = get_float_acc_details(txn_stmt_fmt, txn_pdf.pages, acc_number)
    df = float_acc_details.pop('df')
    del txn_pdf
    comms_df = get_comms_df(files_details, float_acc_details)
    return df, comms_df, txn_stmt_fmt

def export(event, context, engine, df, country_code):
    try:
        run_id = event['run_id'] = f"{event['flow_req_id']}"
        acc_number = event['acc_number']
        file_json = event['file_json']
        object_key = event['object_key']
        export_table = 'rbok_cust_acc_stmts'
        event = set_country_code_n_status(event, country_code, 'success', 'export')

        set_timeout_signal(context, 10)   
        df, comms_df, txn_stmt_fmt = get_txn_n_comms_df(object_key, file_json, acc_number)
        addl_data = {'run_id': run_id,'acc_number': acc_number,'comms_df': comms_df,'txn_stmt_fmt': txn_stmt_fmt, 'table': export_table, 'event': event, 'context': context }

        with engine.begin() as db_con:
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
