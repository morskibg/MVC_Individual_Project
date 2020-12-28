import os
from threading import Thread
from flask import current_app, flash
from flask_mail import Message
from app import app
from app import mail


def send_async_email(app, msg):
    with app.app_context():
        mail.send(msg)


def send_email(recipients, file_data, subject = "From GED automated invoice sender", sender = app.config['MAIL_DEFAULT_SENDER'], 
                text_body = 'Това е автоматично генерирано съобщение. Моля не отговаряйте ! За въпроси и уточнения използвайте : office@grandenergy.net ~ This is automated invoice sender! Please, DO NOT respond to this email address! For further clarification, please contact: office@grandenergy.net', 
                html_body = "<b>Това е автоматично генерирано съобщение. Моля не отговаряйте ! За въпроси и уточнения използвайте : office@grandenergy.net ~ This is automated invoice sender! Please, DO NOT respond to this email address! For further clarification, please contact: office@grandenergy.net</b>" ):
       
    msg = Message(subject, sender=sender, recipients=recipients,extra_headers={'Return-Receipt-To': sender,'Disposition-Notification-To':sender})
    msg.body = text_body
    msg.html = html_body

    for curr_file in file_data:
        print(f'curr_file --- {curr_file}')
        path = curr_file[0]
        ###### modifying inv reference file name to remove cyrillic letters 
        try:
            filename = curr_file[1].split('_',1)[0] + '_' + curr_file[1].rsplit('_',1)[1]
        except:
            filename = curr_file[1]
        else:
            f2 = curr_file[2].split('.')[0]
            filename = curr_file[1].split('_',1)[0] + f'_{f2}_' + curr_file[1].rsplit('_',1)[1]
        #############################################
        try:
            with open( os.path.join(path, curr_file[1]),'rb') as fp:  
                msg.attach(filename, 'application/octect-stream', fp.read())  
        except:
            flash(f'Missing file: {curr_file[1]}. Not sending !','danger')
            return
        

    Thread(target=send_async_email,args=(current_app._get_current_object(), msg)).start()

    if len(file_data) > 1:
        flash(f'Invoice {file_data[0][1]} and {file_data[1][1]} were sent to: {recipients} has been sent ', 'success')
    else:
        flash(f'File {file_data[0][1]} was sent to: {recipients} ', 'info')