from io import StringIO
import re
import pandas as pd
from common import invoke_next_lambda, clean_df_db_dups, connect_to_database_engine, df_to_sql, chunkit, get_obj_from_s3, trim_dataframe, update_lambda_status
import traceback
from mailer import send_mail, send_simple_mail
from datetime import datetime as dt

def lambda_handler(event, context):
    def rename(df):
        df.rename(columns={
                            'Time': 'time', 
                            'Terminal': 'terminal', 
                            'Request Ref': 'req_ref', 
                            'TranType': 'tran_type', 
                            'Biller': 'biller', 
                            'Customer': 'customer',
                            'Narration': 'narration', 
                            'Resp Code': 'resp_code', 
                            'Surchage': 'surcharge', 
                            'Debit Amt': 'debit_amt', 
                            'Credit Amt': 'credit_amt',
                            'Balance': 'balance'
                        }, inplace=True)        
        return df

    def parse_date(data):
        date = dt.strptime(data, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M:%S")
        return date

    def clean(txn_df):
        txn_df = trim_dataframe(txn_df)
        txn_df = rename(txn_df)     
        # txn_df['time'] = txn_df['time'].apply(parse_date)
        txn_df = txn_df.assign(acc_number=acc_number, export_run_id=run_id, transform_status='NOT_DONE')
        return txn_df

    def remove_extra_commas(obj):
        """
        Some cells had commas inside them in a bracket. This is used to remove the contents inside the bracket.
        Required:
            key : S3 Object.
        Returns
            Returns String as file.
        """
        encoding = 'utf-8'
        text = obj.decode(encoding)
        data = re.sub("[\(].*?[\)]", "", text) 
        return StringIO(data)


    def get_obj_and_create_df(file_name):
        s3_obj = get_obj_from_s3(file_name)
        buffer = remove_extra_commas(s3_obj)
        return pd.read_csv(buffer)

    # MAIN FUNCTION
    df = pd.DataFrame()
    key = '0'
    try:
        if event:
            run_id = event['flow_req_id']
            object_key = event['object_key']
            acc_number = event['ap_cust_id']
            key = f"{acc_number} - {object_key}" 
            df = get_obj_and_create_df(object_key)
        else:
            key = context['key']
            acc_number = context['ap_cust_id']
            run_id = context['flow_req_id']
            df = pd.read_csv(key)

        db_con = connect_to_database_engine('STMT')
        final_df = clean(df)

        dup_cols = ['balance','req_ref','time','acc_number']
        final_df = clean_df_db_dups(final_df, 'uisg_cust_acc_stmts', db_con, dup_cols, filter_categorical_col= 'acc_number')

        exp_list = []
        for chunk in chunkit(final_df):
            exp = df_to_sql(chunk, 'uisg_cust_acc_stmts', db_con)
            exp_list.extend(exp)
        db_con.dispose()
    
        update_lambda_status(run_id, 'UGA','export_succes')
        data = {"run_id": f"{run_id}", "key": key, "acc_number": acc_number}
        invoke_next_lambda(data, 'uisg_transform')
        
        # from uisg_transform import lambda_handler2
        # lambda_handler2(data, None)
        final_df, df, db_con = None, None, None

    
    except Exception as error:
        error = repr(error)
        message = traceback.format_exc()
        print(message)

        update_lambda_status(run_id, 'UGA','export_failed',error[:120])

        header = f'ERROR IN UISG LOAD - {key}'
        if not df.empty:
            error_df = [{"df": df, "exception": message}]
            send_mail(header, key, error_df)
        else:
            send_simple_mail(header, message)