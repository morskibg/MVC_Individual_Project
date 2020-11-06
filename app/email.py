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
                text_body = 'This is automated invoice sender! Please, DO NOT respond to this email address! For further clarification, please contact: office@grandenergy.net', 
                html_body = "<b>This is automated invoice sender! Please, DO NOT respond to this email address! For further clarification, please contact: office@grandenergy.net</b>" ):
       
    msg = Message(subject, sender=sender, recipients=recipients,extra_headers={'Return-Receipt-To': sender,'Disposition-Notification-To':sender})
    msg.body = text_body
    msg.html = html_body

    for curr_file in file_data:
        path = curr_file[0].split('/',1)[1]       
        
        try:
            filename = curr_file[1].split('_',1)[0] + '_' + curr_file[1].rsplit('_',1)[1]
        except:
            filename = curr_file[1]
        else:
            filename = curr_file[1].split('_',1)[0] + f'_{curr_file[2]}_' + curr_file[1].rsplit('_',1)[1]
        
        with app.open_resource(os.path.join(path, curr_file[1])) as fp:   
            msg.attach(filename, 'application/octect-stream', fp.read())  
       
    Thread(target=send_async_email,
        args=(current_app._get_current_object(), msg)).start()

    flash(f'Message to: {recipients} has been sent ', 'info')