import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from .models import *
from decimal import Decimal
from flask import g


MONEY_ROUND = 2

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
        if((asci >= 65)&(asci <= 90)|(asci >= 97)&(asci <= 122)) :
            return False
    else:
        return True 

def get_contract_by_internal_id(internal_id):
    return Contract.query.filter(Contract.internal_id == internal_id).first()

def get_erp_id_by_name(name):
    return Erp.query.filter(func.lower(Erp.name) == func.lower(name)).first().id

def set_contarct_dates(contract, activation_date):

    new_start_date =  convert_date_to_utc("Europe/Sofia",activation_date) 
    new_end_date = new_start_date + dt.timedelta(hours =contract.duration_in_days * 24 + 23)
    contract.update({'start_date':new_start_date, 'end_date':new_end_date})

def get_address(address):

    address = address.lower() if address.lower() != '' else 'none'
    curr_address = AddressMurs.query.filter(AddressMurs.name == address).first()
    if curr_address is None:            
        curr_address = AddressMurs(name = address)
        # curr_address.save()
    
    return curr_address

def get_invoicing_group(invoice_group_name):

    curr_inv_group = InvoiceGroup.query.filter(func.lower(InvoiceGroup.name) == func.lower(invoice_group_name)).first()
    # if curr_inv_group is None:
    #     curr_inv_group = InvoiceGroup(name = invoice_group_name, contractor_id = contract.contractor_id)
    return curr_inv_group

def get_measuring_type(measuring_type):

    curr_measuring_type = MeasuringType.query.filter(MeasuringType.code == measuring_type).first()
    # if curr_inv_group is None:
    #     curr_inv_group = InvoiceGroup(name = invoice_group_name, contractor_id = contract.contractor_id)
    return curr_measuring_type


def get_itn_meta(row):

    curr_itn_meta = ItnMeta.query.filter(ItnMeta.itn == row['itn']).first()
    
    if curr_itn_meta is  None:
        curr_itn_meta = ItnMeta(
            itn = row['itn'], 
            description = row['description'] if not pd.isnull(row['description']) else None, 
            grid_voltage = row['grid_voltage'],
            address = get_address(row['address']), 
            erp_id = get_erp_id_by_name(row['erp']), 
            is_virtual = row['is_virtual'], 
            virtual_parent_itn = row['virtual_parent_itn'] if not pd.isnull(row['virtual_parent_itn']) else None)
    # curr_itn_meta.save()
    return curr_itn_meta    

def get_subcontract(row, curr_contract, df):

    curr_sub_contract =  SubContract.query.filter(SubContract.itn == row['itn'], \
                                                        SubContract.start_date <= convert_date_to_utc("Europe/Sofia",row['activation_date']), \
                                                        SubContract.end_date >= convert_date_to_utc("Europe/Sofia",row['activation_date'])).all()
    if len(curr_sub_contract) == 0:
        
        forcasted_vol = None
        if df.get(row['itn']) is not None:
            forcasted_vol = Decimal(str(df[row['itn']]['forcasted_volume'].sum()))
            g.forcasted_schedule = df[row['itn']]
        else:
            forcasted_vol = Decimal(str(row['forecast_montly_consumption']))
        curr_sub_contract = SubContract(itn = row['itn'],
                                    contract_id = curr_contract.id, \
                                    object_name = '',\
                                    price = round(Decimal(str(row['price'])), MONEY_ROUND), \
                                    invoice_group_id = get_invoicing_group(row['invoice_group']).id, \
                                    measuring_type_id = get_measuring_type(row['measuring_type']).id, \
                                    start_date = convert_date_to_utc("Europe/Sofia",row['activation_date']).replace(tzinfo=None),\
                                    end_date =  curr_contract.end_date.replace(tzinfo=None), \
                                    zko = round(Decimal(str(row['zko'])), MONEY_ROUND), \
                                    akciz = round(Decimal(str(row['akciz'])), MONEY_ROUND), \
                                    has_grid_services = row['has_grid_services'], \
                                    has_spot_price = row['has_spot_price'], \
                                    has_balancing = row['has_balancing'], \
                                    forecast_vol = forcasted_vol)
        
    elif len(curr_sub_contract) > 1:
        flash(f'Error ! Overlaping subcontracts with itn {itn} and activation date {activation_date}','error')
    else:
        pass
    return curr_sub_contract

def generate_utc_time_series(start_date, end_date):

    start_date_utc = convert_date_to_utc("Europe/Sofia",start_date)
    end_date_utc = convert_date_to_utc("Europe/Sofia",end_date) + dt.timedelta(hours = 23)
    time_series = pd.date_range(start = start_date_utc, end = end_date_utc, freq='H', tz='UTC')
    
    # df = pd.DataFrame(time_series, columns = ['date_time'])
    # df.set_index(time_series, inplace = True)
    return time_series


# def generate_schedule(itn, start_date, end_date, forecasted_volume, price):
    



                   
