import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.flaskenv'))

class Config(object):
	SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess-ha@_'
	SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI') 		
	SQLALCHEMY_TRACK_MODIFICATIONS = False
	MAIL_SERVER = os.environ.get('MAIL_SERVER')
	MAIL_PORT = int(os.environ.get('MAIL_PORT') or 25)
	MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS') is not None
	MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
	MAIL_DEFAULT_SENDER = os.environ.get('MAIL_DEFAULT_SENDER')
	MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
	ADMINS = ['d.petkov@grandenergy.net']
	INV_REFS_PATH = 'static/inv_ref_files'
	INTEGRA_INDIVIDUAL_PATH = 'static/integra_individual_files' 
	INTEGRA_FOR_UPLOAD_PATH = 'static/integra_for_upload' 
	PDF_INVOICES_PATH = 'static/created_pdf_invoices'
	REPORTS_PATH = 'static/reports'
	TEMP_INVOICE_PATH = 'static/temp_invoice_from_csv'
	TEMP_INVOICE_NAME = 'temp_invoice.xlsx'

