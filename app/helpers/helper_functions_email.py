# # from flask import current_app
# # from flask_mail import Message
# # from app import mail
# # from app.decorators import async

# # @async
# # def send_async_email(app, msg):
# #     with app.app_context():
# #         mail.send(msg)

# # def send_email(subject, sender, recipients, text_body, html_body):
# #     msg = Message(subject, sender=sender, recipients=recipients)
# #     msg.body = text_body
# #     msg.html = html_body
# #     send_async_email(app, msg)
# import os
# from threading import Thread
# from flask import current_app
# from flask_mail import Message
# from app import app
# from app import mail


# def send_async_email(app, msg):
#     with app.app_context():
#         mail.send(msg)


# def send_email(recipients, file_data, subject = "From GED automated invoice sender", sender = app.config['MAIL_DEFAULT_SENDER'], 
#                 text_body = 'This is automated invoice sender! Please, DO NOT respond to this email address! For further clarification, please contact: openmarket@grandenergy.net', 
#                 html_body = "<b>This is automated invoice sender! Please, DO NOT respond to this email address! For further clarification, please contact: openmarket@grandenergy.net</b>" ):
       
#     msg = Message(subject, sender=sender, recipients=recipients)
#     msg.body = text_body
#     msg.html = html_body
#     for p in file_data:
#         with app.open_resource(os.path.join(p[0], p[1])) as fp:   
#             msg.attach(p[1], 'application/octect-stream', fp.read())  
        
#         # Thread(target=send_async_email,
#         #     args=(current_app._get_current_object(), msg)).start()

