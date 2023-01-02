from common import handle_transform_failure, clean_n_insert, remove_inessential_columns, remove_dup_in_df, invoke_score_calc, notify_process_status, set_timeout_signal, set_country_code_n_status, cleanup, set_session, get_exported_data, parse_date_in_series, trim_dataframe, update_transform_status
import numpy as np

def rename(txn_df):
    txn_df.rename(columns={
                            'transaction_date': 'txn_date',
                            'id1': 'acc_number',
                            'transaction_id': 'txn_id',
                            'description': 'descr',
                            },inplace=True)
    txn_df.drop(['product_id'],axis=1,inplace=True)
    return txn_df

def get_comms(record):
    if record['descr'].rstrip() == "NT Comm Batch Payout":
        if record['cr_amt']:
            return record['cr_amt']
        elif record['dr_amt']:
            return record['dr_amt']
    else:
        return 0.0

def chk_float(amt):
    if amt == 0:
        return False
    else:
        return True

def get_float(record):
    text = record['descr'].rstrip()
    if (text == 'NT Comm Batch Payout') or (text == 'Transfer Comm') or ('Airtel' in text):
        if 'Withdrawal' in text or 'Comm' in text:
            return record['cr_amt']
        else:
            return 0.0
    else:
        return record['cr_amt']

def transform_df(txn_df, transform_table):

    txn_df = trim_dataframe(txn_df)
    txn_df = rename(txn_df)
    txn_df['txn_date'] = parse_date_in_series(txn_df['txn_date'], ["%d/%m/%Y %I:%M:%S %p"])
    txn_df['txn_date'] = txn_df['txn_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # txn_df['is_float'] = 0
    txn_df = txn_df.assign(acc_prvdr_code='UEZM',txn_type=None,comms=0)
    txn_df['float_amt'] = txn_df.apply(get_float, axis=1)
    txn_df['is_float'] = txn_df['float_amt'].apply(chk_float)
    txn_df['dr_amt'] = txn_df['dr_amt'].apply(lambda x : np.absolute(x))
    txn_df['comms'] = txn_df.apply(get_comms, axis=1)

    txn_df.rename(columns={
                                'id': 'ref_id',
                                'export_run_id': 'run_id'
                                },inplace=True)
    txn_df.rename(columns={
                            'id': 'ref_id',
                            'export_run_id': 'run_id',
                            },inplace=True)

    txn_df = remove_inessential_columns(txn_df, transform_table)
    return txn_df

def clean_df(df, db_con, addl_data):
    transform_table = addl_data['table']
    df = transform_df(df, transform_table)
    df = remove_dup_in_df(df, db_con, transform_table)
    return df

def transform(event, context, engine, df, country_code):
    
    run_id = event['run_id']
    export_table, transform_table = 'uezm_cust_acc_stmts', 'flow_cust_acc_stmts'
    try:
        acc_number = event['acc_number']
        event = set_country_code_n_status(event, country_code, 'success', 'transform')

        set_timeout_signal(context, 10)
        with engine.begin() as db_con:
            df = get_exported_data(export_table, run_id, db_con)

            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': transform_table, 'event': event, 'context': context}
            df, exp_list = clean_n_insert(df, db_con, addl_data, clean_df)

            update_transform_status(db_con, export_table, 'transform_status', run_id, exp_list)
            notify_process_status(event, context, df, [], None)
        invoke_score_calc(event, 90)

    except Exception as e:
        error_message = str(e)
        event = set_country_code_n_status(event, country_code, 'failed', 'transform')
        handle_transform_failure(event, context, df, [], error_message, export_table, engine)
        
    finally:
        cleanup(engine)
        return event

def main(event, context):
    df, engine, country_code = set_session('UGA')
    return transform(event, context, engine, df, country_code)

def lambda_handler2(event, context):
    return main(event, context)
