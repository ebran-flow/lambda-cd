import numpy as np
from common import FlowEmptyStatementException, remove_inessential_columns, invoke_score_calc, notify_process_status, clean_n_insert, remove_dup_in_df, cleanup, handle_transform_failure, set_timeout_signal, set_country_code_n_status, set_session, get_exported_data, parse_date_in_series, trim_dataframe, update_transform_status

EXPECTED_DT_FORMATS = ['%Y-%m-%d %H:%M:%S']

def rename(df):
    rename_columns = {
        'txn_ref_id': 'txn_id', 
        'txn_amount': 'amount', 
    }
    df.rename(columns=rename_columns, inplace=True)
    return df

def find_txn_type(data):
    data = (data.strip()).lower()
    if 'agent commission' in data:
        return 'Commission'
    elif 'float received' in data or 'manual bank top up' in data:
        return 'Float Purchase'
    elif 'float sent' in data:
        return 'Float Transfer'
    elif 'reversal' in data:
        return 'Reversal'
    else:
        return 'Retail Txn'

def set_running_balance(txn_df):
    txn_df.sort_values(by='txn_date', ascending=False, inplace=True)
    # Separate Balance Records
    bal_descr = ['opening balance', 'closing balance', 'balance']

    bal_df = txn_df[txn_df.txn_type.str.lower().str.contains('|'.join(bal_descr), na=False)]
    txn_df = txn_df[~txn_df.txn_type.str.lower().str.contains('|'.join(bal_descr), na=False)]
    if txn_df.shape[0] == 0:
        raise FlowEmptyStatementException('Transaction txn_df is empty')
    elif bal_df.shape[0] == 0:
        raise Exception('Balance txn_df is empty')

    # Calculate running balance
    txn_df = txn_df.assign(balance=0)
    bal_amount = bal_df.amount.values[0]

    txn_df_1st_row = txn_df.index[0] #First Row Index
    for row_index, column in txn_df.iterrows():
        if txn_df_1st_row == row_index:
            txn_df.at[row_index, 'balance'] = bal_amount
        else:
            previous_balance = txn_df.at[previous_index, 'balance']
            previous_amount = txn_df.at[previous_index, 'amount']
            txn_df.at[row_index, 'balance'] = previous_balance - previous_amount
        previous_index = row_index

    # Add smallest balance to make all balance positive
    smallest_balance = txn_df['balance'].min()
    if smallest_balance < 0:
        txn_df['balance'] = txn_df['balance'] + abs(smallest_balance)
    return txn_df

def transform_df(txn_df, transform_table):
    txn_df = rename(txn_df)
    txn_df = trim_dataframe(txn_df)
    txn_df['txn_date'] = parse_date_in_series(txn_df['txn_date'], EXPECTED_DT_FORMATS)

    txn_df = txn_df.assign(cr_amt=0, dr_amt=0, comms=0, acc_prvdr_code='CCA', is_float=False)

    txn_df = set_running_balance(txn_df)
    txn_df['txn_type'] = txn_df['txn_type'].apply(find_txn_type)

    txn_df.cr_amt = np.where(txn_df.amount > 0, txn_df.amount, txn_df.cr_amt)
    txn_df.dr_amt = np.where(txn_df.amount < 0, abs(txn_df.amount), txn_df.dr_amt)
    txn_df.comms = np.where(txn_df.txn_type == 'Commission', txn_df.amount, txn_df.comms)

    txn_df.is_float = np.where(txn_df.txn_type == 'Float Purchase', 1, 0)

    txn_df.sort_values(by='txn_date', ascending=True, inplace=True)
    txn_df['txn_date'] = txn_df['txn_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    txn_df.rename(columns={
        'id': 'ref_id',
        'export_run_id': 'run_id'
    }, inplace=True)
    
    txn_df = remove_inessential_columns(txn_df, transform_table)
    return txn_df

def clean_df(df, db_con, addl_data):
    transform_table = addl_data['table']
    df = transform_df(df, transform_table)
    df = remove_dup_in_df(df, db_con, transform_table)
    return df  

def transform(event, context, engine, df, country_code):

    run_id = event['run_id']
    export_table, transform_table = 'cca_cust_acc_stmts', 'flow_cust_acc_stmts'
    try:
        acc_number = event['acc_number']
        event = set_country_code_n_status(event, country_code, 'success', 'transform')

        set_timeout_signal(context, 10)
        with engine.begin() as db_con:  
            df = get_exported_data(export_table, run_id, db_con)

            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': transform_table, 'event': event, 'context': context}
            df, exp_list = clean_n_insert(df, db_con, addl_data, clean_df)
            
            update_transform_status(db_con, export_table, 'transform_status', run_id, exp_list)
            notify_process_status(event, context, df, ['lead_id'], None)
        invoke_score_calc(event, 90)

    except Exception as e:
        error_message = str(e)
        event = set_country_code_n_status(event, country_code, 'failed', 'transform')
        handle_transform_failure(event, context, df, ['lead_id'], error_message, export_table, engine)

    finally:
        cleanup(engine)
        return event

def main(event, context):
    df, engine, country_code = set_session('UGA')
    return transform(event, context, engine, df, country_code)

def lambda_handler2(event, context):
    return main(event, context)
