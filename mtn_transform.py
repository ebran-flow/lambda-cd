import signal
from dotenv import load_dotenv
import pandas as pd
import os
from common import connect_to_database_engine, drop_unwanted_columns, fill_empty_cells, get_exported_data, insert_df_to_db, handle_etl_process_failure, invoke_next_lambda, parse_date_in_series, remove_duplicate_records, rollback_exported_data, timeout_handler, update_lambda_status, update_transform_status
import traceback

def get_comms_cash_in(amt):
    if amt < 500 : return 0
    elif amt <= 2500 : return round(amt*0.04, 2)
    elif amt <= 5000 : return 110
    elif amt <= 15000 : return 175
    elif amt <= 30000 : return 225
    elif amt <= 45000 : return 275
    elif amt <= 60000 : return 325
    elif amt <= 125000 : return 400
    elif amt <= 250000 : return 500
    elif amt <= 500000 : return 950
    elif amt <= 1000000 : return 1600
    elif amt <= 2000000 : return 3000
    elif amt <= 4000000 : return 3000
    elif amt <= 7000000 : return 3000

def get_comms_cash_out(amt):
    if amt < 500 : return 0
    elif amt <= 2500 : return 100
    elif amt <= 5000 : return 120
    elif amt <= 15000 : return 200
    elif amt <= 30000 : return 275
    elif amt <= 45000 : return 360
    elif amt <= 60000 : return 450
    elif amt <= 125000 : return 600
    elif amt <= 250000 : return 1000
    elif amt <= 500000 : return 2000
    elif amt <= 1000000 : return 3900
    elif amt <= 2000000 : return 4400
    elif amt <= 4000000 : return 4500
    elif amt <= 7000000 : return 5000

def get_comms_bill_payment_general(amt):
    if amt < 500 : return 0
    elif amt <= 2500 : return 35
    elif amt <= 5000 : return 100
    elif amt <= 15000 : return 200
    elif amt <= 30000 : return 220
    elif amt <= 45000 : return 250
    elif amt <= 60000 : return 285
    elif amt <= 125000 : return 440
    elif amt <= 250000 : return 520
    elif amt <= 500000 : return 1000
    elif amt <= 1000000 : return 2000
    elif amt <= 2000000 : return 4000
    elif amt <= 4000000 : return 8000
    elif amt <= 7000000 : return 12000

def get_comms_bill_payment_mtn(amt):
    return round(amt*0.0444, 2)
        
def get_comms(sheet):
    txn_ids = sheet['txn_id'].to_list()
    distinct_txn_ids = list(set(txn_ids))
    for txn_id in distinct_txn_ids:
        commission_amount = 0
        for row_index, columns in sheet.iterrows():
            if sheet.loc[row_index, 'txn_id'] == txn_id:
                if 'COMMISSIONING' in sheet.loc[row_index, 'txn_type']:
                    commission_amount = sheet.loc[row_index, 'amount']  
                    continue
        for row_index, columns in sheet.iterrows():
            if sheet.loc[row_index, 'txn_id'] == txn_id:
                if 'CASH_IN' in sheet.loc[row_index, 'txn_type'] or 'CASH_OUT' in sheet.loc[row_index, 'txn_type'] or 'PAYMENT' in sheet.loc[row_index, 'txn_type']:
                    sheet.loc[row_index, 'comms'] = commission_amount       
                    continue
    return sheet
def drop_commissioning_and_tax(sheet):
    commissioning_indexes = sheet[sheet['txn_type'] == 'COMMISSIONING'].index
    tax_indexes = sheet[sheet['txn_type'] == 'CUSTOM_WITHHOLDING_TAX'].index  
    sheet.drop(commissioning_indexes, inplace = True)      
    sheet.drop(tax_indexes, inplace = True)   
    return sheet

def calculate_tax(comms):
    return comms*0.1

def deduct_tax_from_commission(df):
    df['comms'] = fill_empty_cells(df['comms'])
    df['tax'] = fill_empty_cells(df['tax'])

    if (df['tax'] == 0).all():
        df['tax'] = df['comms'].apply(calculate_tax)
    
    df['comms'] = df['comms'] - df['tax']
    return df
        
def transform_df(sheet, sheet_format):        
    sheet['txn_date'] = parse_date_in_series(sheet['txn_date'], ['%Y-%m-%d %H:%M'])
    sheet['txn_date'] = sheet['txn_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    sheet['abs_amount'] = sheet['amount'].abs()

    sheet = sheet.assign(   cr_amt=0, 
                            dr_amt=0, 
                            is_float=False,
                            acc_prvdr_code='UMTN')

    if sheet_format ==3 and (sheet['comms'] == 0).all(): #Check if all values in comms are zero
        sheet_format = 1

    for i, row in sheet.iterrows():
        abs_amount = sheet.at[i, 'abs_amount']
        amount = sheet.at[i, 'amount']
        txn_type = sheet.at[i, 'txn_type']
        txn_type = txn_type.strip()
        
        # D - R - Y -> Donot Repeat Yourself
        
        dr_types = ['CASH_IN', 'BILL_PAYMENT', 'ADJUSTMENT', 'DEBIT']
        cr_types = ['CASH_OUT', 'DEPOSIT', 'REVERSAL', 'TRANSFER_FROM_VOUCHER', 'EXTERNAL_TRANSFER', 'BATCH_TRANSFER']
        if txn_type in cr_types:
            sheet.at[i,'cr_amt'] = abs_amount
        elif txn_type in dr_types:
            sheet.at[i,'dr_amt'] = abs_amount
        elif txn_type == 'TRANSFER':
            if amount > 0:
                sheet.loc[i, 'cr_amt'] = abs_amount
            elif amount < 0:
                sheet.loc[i, 'dr_amt'] = abs_amount
        else:
            if amount > 0:
                sheet.loc[i, 'cr_amt'] = abs_amount
            elif amount < 0:
                sheet.loc[i, 'dr_amt'] = abs_amount
        
        if (sheet_format == 1):
            if txn_type == "CASH_OUT" or txn_type == "WITHDRAW_MONEY":
                sheet.loc[i, 'comms'] = get_comms_cash_out(abs_amount)
            elif txn_type == "CASH_IN" or txn_type == "DEPOSIT_MONEY":
                sheet.loc[i, 'comms'] = get_comms_cash_in(abs_amount)
            elif txn_type == "BILL PAYMENT":
                if 'mtn' in sheet.loc[i, 'description'].lower() or 'africell' in sheet.loc[i, 'description'].lower():
                    if 'easyload' not in sheet.loc[i, 'description'].lower():
                        sheet.loc[i, 'comms'] = get_comms_bill_payment_mtn(abs_amount)
                elif 'umeme' in sheet.loc[i, 'description'].lower() or 'nwsc' in sheet.loc[i, 'description'].lower():
                    sheet.loc[i, 'comms'] = get_comms_bill_payment_general(abs_amount)

    sheet = deduct_tax_from_commission(sheet)

    if sheet_format == 2:
        sheet = get_comms(sheet)
        sheet = drop_commissioning_and_tax(sheet)
    
    sheet.rename(columns={
                            'id': 'ref_id',
                            'export_run_id': 'run_id'
                            },inplace=True)
    
    required_columns = ['txn_id','txn_date','txn_type','dr_amt','cr_amt','comms','is_float','balance','acc_number','acc_prvdr_code','run_id','ref_id']
    sheet = drop_unwanted_columns(sheet, required_columns, 'keep')

    return sheet

def clean_df(df, db_con, sheet_format, transform_table):
    final_df = transform_df(df, sheet_format)

    dup_cols = ['acc_number','txn_date', 'balance', 'dr_amt', 'cr_amt', 'txn_id'] 
    final_df = remove_duplicate_records(final_df, transform_table, dup_cols, 'acc_number', db_con)
    
    return final_df    

def call_score_calc(event):
    data = {
                "country_code": "UGA",
                "time_zone": "EAT",
                "no_of_days": 90,
                "acc_prvdr_code": "UMTN"
            }
    data.update(event)
    if os.environ.get('APP_ENV') == 'local':
        from score import score_calc
        score_calc(data, None)
    else:
        invoke_next_lambda(data, 'score_calc')

def main(event, context):
    load_dotenv()
    engine = connect_to_database_engine('STMT')
    df = pd.DataFrame()
    run_id = event['run_id']
    export_table, transform_table = 'mtn_cust_acc_stmts', 'flow_cust_acc_stmts'
    try:
        acc_number = event['acc_number']
        # function_name = 'mtn_transform'
        country_code, status, lambda_status =  'UGA', 'success', 'transform_success'
        os.environ["COUNTRY_CODE"] = country_code
        
        signal.signal(signal.SIGALRM, timeout_handler)
        if context:
            signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 10)
        
        with engine.begin() as db_con:
            df = get_exported_data(export_table, run_id, db_con)
            df = clean_df(df, db_con, event['sheet_format'], transform_table)

            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': transform_table, 'event': event, 'context': context}
            exp_list = insert_df_to_db(df, db_con, addl_data)    

            update_transform_status(db_con, export_table, 'transform_status', run_id, exp_list)
            payload =   {
                            "country_code": country_code,
                            "flow_req_id": run_id,
                            "status": status,
                            "lambda_status": lambda_status,
                            "lead_id": event['lead_id'],
                        }
            update_lambda_status(payload, event, context)
        call_score_calc(event)

    except Exception as e:
        error_message = str(e)
        exception = traceback.format_exc()
        print(exception)
        rollback_exported_data(export_table, run_id, engine)
        country_code, status, lambda_status = 'UGA', 'failed', 'transform_failed'
        payload =   {
                        "country_code": country_code,
                        "flow_req_id": run_id,
                        "status": status,
                        "lambda_status": lambda_status,
                        "lead_id": event['lead_id'],
                        "error_message": error_message
                    }

        update_lambda_status(payload, event, context)
        files = [{'df': df, 'file_name': f"{acc_number}_{run_id}"}]
        handle_etl_process_failure(event, context, files)

    finally:
        if engine:
            engine.dispose()
        signal.alarm(0)

def lambda_handler2(event, context):
    main(event, context)
