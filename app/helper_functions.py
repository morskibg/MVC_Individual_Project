import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from .models import *

def convert_date_to_utc(time_zone, dt_str, t_format = "%Y-%m-%d"):
    if(dt_str == ''):
        return None
    if  isinstance(dt_str, dt.date):
        dt_str = dt_str.strftime(t_format)
    naive = dt.datetime.strptime (dt_str, t_format)
    local = pytz.timezone (time_zone)
    local_date = local.localize(naive, is_dst=True)
    return local_date.astimezone(pytz.utc)

def validate_ciryllic(data):
    
    no_digit_internal_id = re.sub(r'[\d]', '', str(data))
    for c in no_digit_internal_id:
        asci = ord(c)
        if( (asci >= 65)&(asci <= 90)|(asci >= 97)&(asci <= 122)) :
            return False
    else:
        return True 

def get_contract_by_internal_id(internal_id):
    return Contract.query.filter(Contract.internal_id == internal_id).first()

def set_contarct_dates(contract, activation_date):

    new_start_date =  convert_date_to_utc("Europe/Sofia",activation_date) 
    new_end_date = new_start_date + dt.timedelta(hours =contract.duration_in_days * 24 + 23)
    contract.update({'start_date':new_start_date, 'end_date':new_end_date})

def get_subcontract_by_itn_and_start_date(itn, start_date):
    return SubContract.query.filter(SubContract.itn == itn, SubContract.start_date <= start_date, SubContract.end_date >= start_date).all()


                   
