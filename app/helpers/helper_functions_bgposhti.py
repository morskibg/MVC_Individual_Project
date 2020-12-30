import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import *
from flask import  flash
from app.models import *  
from app import app
from app.helpers.helper_functions import (convert_date_to_utc,) 


def cash_receipt_generation(data):
    
    res = 'Фактура ' + data['INVOICE'] + ' ' + data['creation_date'] + '\\nПотребител N '+ data['ANUM'] + '\\n' + data['NAME'] + '\\n' + data['notes_for_r'] + '\\n@PRINCIPAL@\\n@TOTAL@\\nБлагодарим Ви!'   
    return res

def upload_file_generation(raw_df):

    df = raw_df[:-1]
    df = df.iloc[df['ANUM'].first_valid_index():df['ANUM'].last_valid_index()+1]    
    df['ADDRES'] = df['ADDRES'].str.replace('"', '')
    str_format = "%Y%m%d%H%M%S"
    date_time = dt.datetime.utcnow().strftime(str_format)
    rows = df.shape[0]
    total =  '{:.2f}'.format(df['AMOUNT'].sum())  
    df = df.fillna('няма')
    df['CUR'] = 'BGN'
    eof = f'EOF:{date_time}:{rows}:{total}'
    invoices = (
        db.session.query(
            Invoice.creation_date, Contractor.eic, Invoice.id
        )
        .join(Contractor,Contractor.id == Invoice.contractor_id)
        .filter(Invoice.id.in_(df['INVOICE']))
        .all()
    )
    invoices_df = pd.DataFrame.from_records(invoices, columns=invoices[0].keys())
    
    invoices_df['notes_for_r'] = invoices_df['creation_date'].apply(lambda x: 'Ел. енергия за: ' + x.strftime('%m-%Y'))
    invoices_df['creation_date'] = invoices_df['creation_date'].apply(lambda x: x.strftime('%d.%m.%Y')) 
    invoices_df['id'] = invoices_df['id'].astype(str)
    
    df = df.merge(invoices_df, left_on='INVOICE', right_on='id', how = 'left' )
    df['AMOUNT'] = df['AMOUNT'].apply(lambda x: '{:.2f}'.format(x))
    df['notes'] = df.apply(lambda x: 'Фактура: ' + x['INVOICE'] + '/' + x['creation_date'] , axis = 1)
    df = df[['ANUM', 'NAME', 'ADDRES', 'PHONE', 'eic','INVOICE','creation_date', 'notes','CUR','AMOUNT','notes_for_r']]
    df['intr'] = 0
    
    df['other_sum'] = 0
    df['cash_receipt'] = df.apply(lambda x: cash_receipt_generation(x), axis = 1)
    df.drop(columns = ['notes_for_r'], inplace = True)
    str_format = "%Y%m%d"
    date_time = dt.datetime.utcnow().strftime(str_format)
    filename = f'BP{date_time}.txt'
    full_path = os.path.join(os.path.join(app.root_path, app.config['BGPOSHTI_FOR_UPLOAD_PATH']),filename)
    df.to_csv(full_path, sep='|', index=False, header=False)
    with open(full_path,'a') as fh:                
        fh.write(f'{eof}\n')

    

