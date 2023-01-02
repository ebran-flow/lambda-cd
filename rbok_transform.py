from common import notify_process_status, invoke_score_calc, cleanup, remove_inessential_columns, remove_dup_in_df, clean_n_insert, handle_transform_failure, set_timeout_signal, set_session, set_country_code_n_status, get_exported_data, parse_date_in_series, trim_dataframe, update_transform_status
import numpy as np

def rename(df):
    df.rename(columns={
                        'export_run_id': 'run_id',
                        'id': 'ref_id'
                    }, inplace=True)
    return df

def transform_df(txn_df, transform_table):
    txn_df = trim_dataframe(txn_df)
    txn_df = rename(txn_df)
    txn_df = txn_df.assign(comms=0.0, is_float=0, acc_prvdr_code='RBOK')

    txn_df['txn_date'] = parse_date_in_series(txn_df['txn_date'], ['%Y-%m-%d'])
    txn_df['txn_date'] = txn_df['txn_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    comms_txn_types = ['commission', 'agent commission payment', 'aa loan payoff', 'cash advance commission']
    txn_df['comms'] = np.where(txn_df['txn_type'].str.lower().str.contains('|'.join(comms_txn_types)), txn_df['cr_amt'], txn_df['comms'])
    txn_df['is_float'] = np.where(txn_df['txn_type'].str.lower() == 'float purchase', 1, 0)

    txn_df = remove_inessential_columns(txn_df, transform_table)
    return txn_df

def clean_df(df, db_con, addl_data):
    transform_table = addl_data['table']
    df = transform_df(df, transform_table)
    df = remove_dup_in_df(df, db_con, transform_table)
    return df

def transform(event, context, engine, df, country_code):
    run_id = event['run_id']
    export_table, transform_table = 'rbok_cust_acc_stmts', 'flow_cust_acc_stmts'
    try:
        acc_number = event['acc_number']   
        event = set_country_code_n_status(event, country_code, 'success', 'transform')
        set_timeout_signal(context, 10)

        with engine.begin() as db_con:
            df = get_exported_data(export_table, run_id, db_con)

            addl_data = {'run_id': run_id,'acc_number': acc_number, 'table': transform_table, 'event': event, 'context': context}
            df, exp_list = clean_n_insert(df, db_con, addl_data, clean_df)

            update_transform_status(db_con, export_table, 'transform_status', run_id, exp_list)
            notify_process_status(event, context, df, ['lead_id','file_json'], None)
        invoke_score_calc(event, 90)

    except Exception as e:
        error_message = str(e)
        event = set_country_code_n_status(event, country_code, 'failed', 'transform')
        handle_transform_failure(event, context, df, ['lead_id'], error_message, export_table, engine)

    finally:
        cleanup(engine)
        return event

def main(event, context):
    df, engine, country_code = set_session('RWA')
    return transform(event, context, engine, df, country_code)

def lambda_handler2(event, context):
    return main(event, context)
