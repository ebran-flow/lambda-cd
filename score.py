from common import set_timeout_signal, set_session, notify_process_status, set_country_code_n_status, cleanup, call_score_api
import pandas as pd
import numpy as np
from datetime import datetime as dt
from sqlalchemy import text

def score_calc(event, context):
    def csf_normal_val(csf_type, value):
        for cs_factor in cs_factors_arr:
            if cs_factor['csf_type'] == csf_type and value >= cs_factor['value_from'] and value < cs_factor['value_to']:
                return cs_factor['normal_value']
        else:
            return 0

    def scorecard_ft(record, csf_types):
        for csf_type in csf_types:
            record[csf_type] = csf_normal_val(csf_type, record[f'__{csf_type}'])
        return record

    def format_date(date):
        f_date = date.strftime('%Y-%m-%d')
        return f_date

    def get_meta_df(acc_number):

        acc_number_str = "'"+acc_number+"'"

        sql = "select acc_number, date(min(txn_date)) meta_txn_start_date, "\
            "date(max(txn_date)) meta_txn_end_date, count(distinct date(txn_date)) meta_txn_days, "\
            f"1+DATEDIFF(max(txn_date), min(txn_date)) meta_cal_days  from {table_name}" + "\
                        where acc_number  = " + acc_number_str + ""

        if(no_of_days > 0):
            sql = sql + " and txn_date > (select date_sub(date(max(txn_date)),  interval " + str(no_of_days-1) + " day) from " + table_name + "\
                where acc_number = " + acc_number_str + ");"

        meta_df = pd.read_sql_query(text(sql),  con=db_con)

        meta_df['meta_txn_start_date'] = meta_df['meta_txn_start_date'].apply(
            format_date)
        meta_df['meta_txn_end_date'] = meta_df['meta_txn_end_date'].apply(
            format_date)
        return meta_df

    def get_average_roi(df_flow, N):

        df_ND_ROI = df_flow.groupby(
            [pd.Grouper(key='txn_date', freq=N+'D')]).agg({'comms': 'sum', 'balance': 'mean'})

        # Compute the ROI

        df_ND_ROI['__'+N+'_day_avg_roi'] = df_ND_ROI['comms'] / \
            df_ND_ROI['balance']

        df_ND_ROI['__'+N+'_day_avg_roi'] = df_ND_ROI['__'+N +
                                                     '_day_avg_roi'].apply(lambda x: 0 if x == np.inf else x)

        df_ND_ROI.drop(['comms', 'balance'], axis=1, inplace=True)

        df_ND_average = pd.DataFrame(df_ND_ROI.mean()).transpose()

        return df_ND_average

    def get_total_float(df_flow, N):

        df_max_bal = df_flow.groupby(
            [pd.Grouper(key='txn_date', freq=N+'D')]).agg({'balance': 'max'})

        df_avg_float = df_flow[df_flow['is_float'] == True].groupby(
            [pd.Grouper(key='txn_date', freq=N+'D')]).agg({'cr_amt': 'mean'})

        df_data = [{'__'+N+'_day_avg_max_bal': df_avg_float['cr_amt'].mean(), '__'+N+'_day_avg_float_purchase': df_max_bal['balance'].mean()}]
        df_ND_FLOAT_avg = pd.DataFrame(df_data)

        return df_ND_FLOAT_avg
    
    def float_pur_freq(df_float):
        df_float = df_float.groupby(
            [pd.Grouper(key='txn_date', freq='1D')]).agg({'cr_amt': 'count'})

        df_float.reset_index(level=['txn_date'], inplace=True)
        
        num_of_purchs = df_float['cr_amt'][:-1].sum()

        df_float_diff = df_float.diff()
        df_float_diff.dropna(inplace=True)

        df_float_diff['txn_days'] = df_float_diff['txn_date'].dt.days
        total_float_days = df_float_diff['txn_days'].sum()

        float_pur_freq = total_float_days / num_of_purchs

        data = {'avg_float_pur_freq': [float_pur_freq]}
        float_pur_freq_df = pd.DataFrame(data)
        float_pur_freq_df['avg_float_pur_freq'] = float_pur_freq_df['avg_float_pur_freq'].apply(lambda x: 0 if x == np.inf else x)

        
        return float_pur_freq_df

    def float_per_day(df_float):
        
        df_float = df_float.groupby(
            [pd.Grouper(key='txn_date', freq='1D')]).agg({'cr_amt': 'sum'})

        sum_of_float = df_float['cr_amt'][:-1].sum()

        df_float.reset_index(level=['txn_date'], inplace=True)
        df_float_diff = df_float.diff()
        df_float_diff.dropna(inplace=True)

        df_float_diff['txn_days'] = df_float_diff['txn_date'].dt.days
        total_float_days = df_float_diff['txn_days'].sum()

        float_per_day = sum_of_float / total_float_days

        data = {'avg_float_per_day': [float_per_day]}
        float_per_day_df = pd.DataFrame(data)
        float_per_day_df['avg_float_per_day'] = float_per_day_df['avg_float_per_day'].apply(lambda x: 0 if x == np.inf else x)

        return float_per_day_df

    def get_average_txn(df_flow, N):
        df_retail_tx = df_flow[df_flow['is_float'] == False]

        df_ND_ret_tx = df_retail_tx.groupby(
            [pd.Grouper(key='txn_date', freq=N+'D')]).count()

        df_ND_ret_tx['__'+N+'_day_avg_txns'] = df_ND_ret_tx['is_float']
        df_ND_ret_tx = df_ND_ret_tx[['__'+N+'_day_avg_txns']]
        df_ND_ret_tx = pd.DataFrame(df_ND_ret_tx.mean()).transpose()

        return df_ND_ret_tx

    def get_avg_balance(df_flow, N):
        df_ND_bal = df_flow.groupby(
            [pd.Grouper(key='txn_date', freq=N+'D')]).agg({'balance': 'mean'})

        df_ND_bal.rename(columns={'balance': '__'+N+'_day_avg_bal'}, inplace=True)
        df_ND_average = pd.DataFrame(df_ND_bal.mean()).transpose()

        df_ND_average = df_ND_average.round(2) ## TO AVOID SCIENTIFIC NOTATION

        return df_ND_average
    
    def get_avg_comms(df_flow, N, alias=None):

        if alias:
            factor_name = f'__{alias}'
        else:
            factor_name = f'__{N}_day_avg_comms'

        # Take only comms records
        df_flow = df_flow.loc[df_flow['comms'] > 0]
        if df_flow.empty:
            data = [{factor_name: 0}]
            return pd.DataFrame(data)

        freq =  "1M" if ( N == '30' ) else f"{N}D"
        df_ND_comms = df_flow.groupby(
            [pd.Grouper(key='txn_date', freq=freq)]).agg({'comms': 'sum'})   

        df_ND_comms.rename(columns={'comms': factor_name}, inplace=True)
        df_ND_average = pd.DataFrame(df_ND_comms.mean()).transpose()
        return df_ND_average

    def get_avg_max_bal(df_flow, N):

        df_ND_max_bal = df_flow.groupby(
            [pd.Grouper(key='txn_date', freq=N+'D')]).agg({'balance': 'max'})

        df_ND_max_bal.rename(columns={'balance': '__'+N+'_day_avg_max_bal'}, inplace=True)
        df_ND_average = pd.DataFrame(df_ND_max_bal.mean()).transpose()

        df_ND_average = df_ND_average.round(2) ## TO AVOID SCIENTIFIC NOTATION
        
        return df_ND_average
    
    def get_avg_dr_amt(df_flow, N):

        df_ND_avg_dr_amt = df_flow.groupby(
            [pd.Grouper(key='txn_date', freq=N+'D')]).agg({'dr_amt': 'sum'})

        df_ND_avg_dr_amt.rename(columns={'dr_amt': '__'+N+'_day_avg_dr_amt'}, inplace=True)
        df_ND_average = pd.DataFrame(df_ND_avg_dr_amt.mean()).transpose()

        df_ND_average = df_ND_average.round(2) ## TO AVOID SCIENTIFIC NOTATION

        return df_ND_average
    
    def max_return_per_day(df_float): # MAX(DAILY_SUM(DR) - DAILY_SUM(CR))
        
        df_float = df_float.groupby(
            [pd.Grouper(key='txn_date', freq='1D')]).agg({'cr_amt': 'sum', 'dr_amt': 'sum'})
        
        df_float['daily_returns'] = df_float['dr_amt'] - df_float['cr_amt']

        max_return = df_float['daily_returns'].max()

        data = {'factor1': [max_return]}
        daily_return_max_df = pd.DataFrame(data)
        
        return daily_return_max_df

    def max_return_per_day1(df_float): # MAX(DAILY_SUM(DR)) - AVG(DAILY_SUM(CR))
        
        df_float = df_float.groupby(
            [pd.Grouper(key='txn_date', freq='1D')]).agg({'cr_amt': 'sum', 'dr_amt': 'sum'})
        
        max_daily_debit = df_float['dr_amt'].max()
        avg_daily_credit = df_float['cr_amt'].mean()

        daily_return = max_daily_debit - avg_daily_credit
        data = {'factor2': [daily_return]}
        daily_return_max_df = pd.DataFrame(data)
        return daily_return_max_df

    def avg_return_per_day(df_float): # AVG(DAILY_SUM(DR)) - AVG(DAILY_SUM(CR))
        
        df_float = df_float.groupby(
            [pd.Grouper(key='txn_date', freq='1D')]).agg({'cr_amt': 'sum', 'dr_amt': 'sum'})
        
        avg_daily_debit = df_float['dr_amt'].mean()
        avg_daily_credit = df_float['cr_amt'].mean()

        avg_daily_return = avg_daily_debit - avg_daily_credit

        data = {'factor3': [avg_daily_return]}
        avg_daily_return_df = pd.DataFrame(data)

        return avg_daily_return_df

    def get_factors_df(df_flow, csf_types):
        factors_df = pd.DataFrame()

        df_for_txns = df_flow[['txn_date', 'is_float']]
        df_for_roi = df_flow[['txn_date', 'comms', 'balance']]

        for csf_type in csf_types:
            factor_df = pd.DataFrame()
            days = csf_type.split('_')[0]
            if 'avg_roi' in csf_type:
                factor_df = get_average_roi(df_for_roi, days)
            elif 'avg_comms' in csf_type:
                factor_df = get_avg_comms(df_for_roi, days)
            elif 'monthly_comms' in csf_type:
                factor_df = get_avg_comms(df_for_roi, '30', 'monthly_comms')
            elif 'avg_txns' in csf_type:
                factor_df = get_average_txn(df_for_txns, days)
            if factor_df.empty:
                raise Exception(f"Function to get data for the csf_type: '{csf_type}' doesn't exist")

            factors_df = factor_df if factors_df.empty else factors_df.join(factor_df)

        return factors_df        
    
    def process_cust(acc_number, required_csf_types):
        acc_number_str = "'"+acc_number+"'"

        sql = f"select acc_number, txn_date, comms, balance, is_float  from {table_name} where acc_number = " + acc_number_str + ""

        if(no_of_days > 0):
            sql = sql + " and txn_date > (select date_sub(date(max(txn_date)), interval " + str(no_of_days-1) + " day) \
                            from " + table_name + " where acc_number = "+acc_number_str + ")"
        df_flow = pd.read_sql_query(text(sql),  con=db_con)

        df_flow['txn_date'] = pd.to_datetime(df_flow['txn_date'])
        output_frame = get_factors_df(df_flow, required_csf_types)        

        output_frame['acc_number'] = acc_number
        output_frame = output_frame.apply(scorecard_ft, csf_types=required_csf_types, axis=1)

        meta_df = get_meta_df(acc_number)
        output_frame = pd.merge(output_frame, meta_df,
                                how='left', on=['acc_number'])

        return output_frame

    # CALL INTERNAL APIs
    def get_cs_model_factor_info(country_code, cs_model_code):
        endpoint = '/internal_api/admin/score_model/cs_model_factor_info'
        payload =   {   
                        "country_code": country_code,
                        "cs_model_code": cs_model_code,
                    }
        status_code, data = call_score_api(endpoint, payload)
        if status_code != 200:
            raise Exception(f"API Error: {endpoint} - {data}")
        return data
    
    def get_scoring_model(country_code, acc_number, acc_prvdr_code):
        endpoint = '/internal_api/admin/score_model/scoring_model'
        payload = { "country_code": country_code,
                    "acc_number": acc_number,
                    "acc_prvdr_code": acc_prvdr_code,                    
                    }
        status_code, data = call_score_api(endpoint, payload)
        if status_code != 200:
            raise Exception(f"API Error: {endpoint} - {data}")
        return data

    def get_score_and_result(cust_score_factors):
        endpoint = '/internal_api/admin/score_model/calculate_score_and_insert_csf_values'
        payload =  {    "country_code": country_code,
                        "acc_number": acc_number,
                        "acc_prvdr_code": acc_prvdr_code,
                        "run_id": run_id,
                        "score_model": cs_model_code,
                        "customer_details": customer_details,
                        "cust_score_factors": cust_score_factors
                        
                    }
        status_code, data = call_score_api(endpoint, payload)
        if status_code != 200:
            raise Exception(f"API Error: {endpoint} - {data}")
        return data

    def format_csfs(csf_types, factors_dict):
        factors = []
        # factors_done = []
        for csf_type in csf_types:
            normal_value = factors_dict[f"{csf_type}"]
            gross_value = factors_dict[f"__{csf_type}"]
            factor = {}
            factor['csf_type'] = csf_type
            # factors_done.append(csf_type)
            # factors_done.append('__'+csf_type)
            factor['n_val'] = int(normal_value)
            factor['g_val'] = float(round(gross_value, 2))
            factors.append(factor)

        # factors_not_done = list(set(factors_dict) - set(factors_done))
        # factors_not_done = [x for x in factors_not_done if not x.startswith('meta')]
        # factors_not_done.remove('acc_number')

        # for csf_type in factors_not_done:
        #     # normal_value = 0
        #     gross_value = factors_dict[f"{csf_type}"]
        #     factor = {}
        #     factor['csf_type'] = csf_type.strip('__')
        #     factor['csf_normal_value'] = round(gross_value, 2)
        #     factor['csf_gross_value'] = round(gross_value, 2)
        #     factors.append(factor)

        return factors

    def format_cust_score_factors(factors_df, csf_types):
        keys = factors_df.columns.to_list()
        values = factors_df.loc[0, :].values.tolist()
        factors_dict = dict(zip(keys, values))

        csf_values_records = format_csfs(csf_types, factors_dict)        
        return csf_values_records
    
    def get_past_performance_factors(cs_factors):
        past_performance_factors = []
        for cs_factor in cs_factors:
            if cs_factor['csf_group'] == 'past_performance':
                past_performance_factors.append(cs_factor['csf_type'])
        return list(set(past_performance_factors))

    def get_required_csf_types(cs_factors_data):
        required_csf_types = []
        for cs_model_data in cs_factors_data['cs_model_weightages']:
            required_csf_types.append(cs_model_data['csf_type'])

        past_performance_csf_types = get_past_performance_factors(cs_factors_data['cs_factor_values'])
        required_csf_types = list(set(required_csf_types) - set(past_performance_csf_types))

        return required_csf_types

    def get_cs_factors_and_csf_types(country_code, cs_model_code):
        cs_factors_and_weightages = get_cs_model_factor_info(country_code, cs_model_code)
        cs_factors_arr = cs_factors_and_weightages['cs_factor_values']
        if not cs_factors_arr:
            raise Exception("No records in cs_factor_values table")

        required_csf_types = get_required_csf_types(cs_factors_and_weightages)
        if not required_csf_types:
            raise Exception("No csf_types in cs_model_weightages table")
        return cs_factors_arr, required_csf_types

    def calculate_cust_score_factors(acc_number, required_csf_types):
        this_frame = process_cust(acc_number, required_csf_types)
        print(this_frame.head(n=1).to_string(index=False))
        return format_cust_score_factors(this_frame, required_csf_types)
    
    def check_if_acc_number_exists(table_name, db_con, acc_number, acc_prvdr_code):
        sql = f"select distinct acc_number as acc_number from {table_name} where acc_number='{acc_number}' and acc_prvdr_code = '{acc_prvdr_code}'"
        acc_number_exists = pd.read_sql_query(text(sql), con=db_con)
        if acc_number_exists.empty:
            raise Exception(f"No records in {table_name} table for acc_number: '{acc_number}'")

    # MAIN()
    df, engine, country_code = set_session(event['country_code'])
    try:
        db_con = engine.connect()
        time_zone = event['time_zone']
        acc_number = event['acc_number']
        acc_prvdr_code = event['acc_prvdr_code']
        run_id = event['run_id']
        no_of_days = event.get('no_of_days', 90)
        table_name = 'flow_cust_acc_stmts'
        event = set_country_code_n_status(event, country_code, 'success', 'score_calc')
        
        set_timeout_signal(context, 10)

        check_if_acc_number_exists(table_name, db_con, acc_number, acc_prvdr_code)
        customer_details = get_scoring_model(country_code, acc_number, acc_prvdr_code)
        cs_model_code = customer_details.pop('cs_model_code')

        cs_factors_arr, required_csf_types = get_cs_factors_and_csf_types(country_code, cs_model_code)
        cust_score_factors = calculate_cust_score_factors(acc_number, required_csf_types)
        scoring_result = get_score_and_result(cust_score_factors)
        print(scoring_result)

        event['result'] = scoring_result['result']
        notify_process_status(event, context, df, ['lead_id','result'], None)

    except Exception as e:
        error_message = str(e)
        print(error_message)
        event = set_country_code_n_status(event, country_code, 'failed', 'score_calc')
        notify_process_status(event, context, df, ['lead_id'], error_message)

    finally:
        cleanup(engine, db_con)
        return event

def main(): ## For Local
    UGA_aps = ['UMTN', 'CCA', 'UEZM', 'UISG']
    RWA_aps = ['RMTN', 'RBOK', 'RRTN']

    def get_country_code_and_time_zone(acc_prvdr_code):
        if acc_prvdr_code in UGA_aps:
            return 'UGA', 'EAT'
        if acc_prvdr_code in RWA_aps:
            return 'RWA', 'CAT'
        raise Exception(f'Country_code not set for acc_prvdr_code: {acc_prvdr_code}')
        
    ap_ids = ['791515671']
    acc_prvdr_code = 'RMTN'
    country_code, time_zone = get_country_code_and_time_zone(acc_prvdr_code)

    import shortuuid
    for ap_id in ap_ids:
        event = {
            'country_code': country_code,
            'time_zone': time_zone,
            'acc_number': ap_id,
            'acc_prvdr_code': acc_prvdr_code,
            'run_id': str(shortuuid.uuid()),
            'no_of_days': 90
        }

    score_calc(event, 0)

if __name__ == '__main__':
    main()