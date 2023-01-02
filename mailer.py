from flask import Flask, render_template
from flask_mail import Mail, Message
import pandas as pd
import io
import os
from dotenv.main import load_dotenv

def export_excel(df):
  with io.BytesIO() as buffer:
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, index=False)
    return buffer.getvalue()    

def send_mail(header, key, errors):
    with app.app_context():
        msg = Message(header, sender = sender_mail, recipients = error_mail_receivers)
        dfs = []
        error_message = []
        for error in errors:
            error_message.append(error["exception"])
            dfs.append(error["df"])
        
        final_df = pd.concat(dfs)
        # message = '\n'.join([i for i in error_message[:5]])
        message = '\n'.join([i for i in error_message])
        data = export_excel(final_df)
        excep = errors[0]['exception']
        msg.html = render_template('error.html', message=excep)
        msg.attach("errors.txt","text/plain", data=message)
        msg.attach(f'{key}.xlsx', "excel/xlsx", data=data)
                                                                      
        mail.send(msg)

def send_simple_mail(header, message):
    with app.app_context():
        msg = Message(header, sender = sender_mail, recipients = error_mail_receivers)
        msg.body = message                                          
        mail.send(msg)

def trigger_score_mail(header, data):
    with app.app_context():
        msg = Message(header, sender = sender_mail, recipients = score_mail_receivers)
        # msg.body = message
        msg.html = render_template('score_results.html', model_code=data['model_code'], customer_details=data['customer_details'])
        df = export_excel(data['df'])
        msg.attach(f'{header}.xlsx', "excel/xlsx", data=df)
        mail.send(msg)

def send_score_error_mail(header, data):
    with app.app_context():
        with app.app_context():
            msg = Message(header, sender = sender_mail, recipients = error_mail_receivers)
            msg.html = render_template('score_errors.html', errors=data)
            mail.send(msg)

def send_etl_failure_mail(data, receivers, files=None):
    with app.app_context():
        header = f"[ETL] - Error on {data['function_name']} for {data['acc_number']}"
        msg = Message(header, sender = sender_mail, recipients = receivers)
        
        if files:
            for file in files:
                df = file["df"]
                if df.empty: continue
                excel_data = export_excel(df)
                file_name = file['file_name']
                msg.attach(f'{file_name}.xlsx', "excel/xlsx", data=excel_data)
        
        msg.html = render_template('etl_failure.html', data=data)
        mail.send(msg)

def send_db_error_mail(data, receivers, errors):
    with app.app_context():
        header = f"[ETL] - DB Error on {data['function_name']} for {data['acc_number']}"
        msg = Message(header, sender = sender_mail, recipients = receivers)
        run_id = data['run_id']
        dfs = []
        error_message = []
        for error in errors:
            error_message.append(error["exception"])
            dfs.append(error["df"])
        final_df = pd.concat(dfs)

        message = '\n'.join([i for i in error_message])
        excel_data = export_excel(final_df)
        data['err_message'] = errors[0]['exception']

        msg.html = render_template('db_insert_error.html', data=data)
        msg.attach(f"{run_id}_errors.txt","text/plain", data=message)
        msg.attach(f"{run_id}_stmt.xlsx", "excel/xlsx", data=excel_data)

        mail.send(msg)

def send_api_error_mail(data, receivers):
    with app.app_context():
        header = f"[ETL] - API Error on {data['function_name']} for {data['acc_number']}"
        msg = Message(header, sender = sender_mail, recipients = receivers)
        msg.html = render_template('api_failure.html', data=data)
        mail.send(msg)

app = Flask(__name__)
load_dotenv()

sender_mail = os.environ.get('MAIL_SENDER')

app.config['MAIL_SERVER']= os.environ.get('MAIL_SERVER')
app.config['MAIL_USERNAME'] = sender_mail
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_PORT'] = 465

error_mail_receivers = os.environ.get('ERROR_MAIL_RECEIVERS')
error_mail_receivers = error_mail_receivers.split(',')

score_mail_receivers = os.environ.get('SCORE_MAIL_RECEIVERS')
score_mail_receivers = score_mail_receivers.split(',')

mail = Mail(app)