from flask_mail import Message
from app import mail
from threading import Thread

def send_async_email(app, msg):
    with app.app_context():
        mail.send(msg)

def send_email(subject,  recipients, text_body, html_body, atachments = None):
    msg = Message(subject, recipients=recipients)
    msg.body = text_body
    msg.html = html_body
    Thread(target=send_async_email, args=(app, msg)).start()

# msg.attach(filename="something.pdf",disposition="attachment",content_type="application/pdf",data=fh.read())