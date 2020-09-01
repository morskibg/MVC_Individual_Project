import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import g, flash
from app.models import *  #(Contract, Erp, AddressMurs, InvoiceGroup, MeasuringType, ItnMeta, SubContract, )

MONEY_ROUND = 2



def convert_date_from_utc(time_zone, dt_obj, is_string = True, t_format = "%Y-%m-%d %H:%M:%S"):
    if(dt_obj is None):
        return None
    if isinstance(dt_obj, str):
        dt_obj = dt.datetime.strptime(dt_obj, t_format)   
    utc = pytz.timezone('UTC')
    new_zone = pytz.timezone(time_zone)
    dt_obj = utc.localize(dt_obj)
    dt_obj = dt_obj.astimezone(new_zone)  
    if is_string:
        dt_obj = dt_obj.strftime("%Y-%m-%d")
    return dt_obj

def convert_date_to_utc(time_zone, dt_str, t_format = "%Y-%m-%d"):
    if(dt_str == ''):
        return None
    if  isinstance(dt_str, dt.date):
        dt_str = dt_str.strftime(t_format)
    naive = dt.datetime.strptime (dt_str, t_format)
    local = pytz.timezone (time_zone)
    local_date = local.localize(naive, is_dst=True)
    return local_date.astimezone(pytz.utc).replace(tzinfo=None)

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

# def get_subcontracts_by_itn_and_utc_date(itn, date):

#     curr_sub_contract =  SubContract.query.filter(SubContract.itn == itn, \
#                                                 SubContract.start_date <= date, \
#                                                 SubContract.end_date >= date).all()
#     return curr_sub_contract

def get_subcontracts_by_itn_and_utc_dates(itn, start_date, end_date):

    appl_sub_contracts =  SubContract.query \
                            .filter((SubContract.itn == itn) & ~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) \
                            .order_by(SubContract.start_date) \
                            .all()

    return appl_sub_contracts

def has_overlaping_subcontracts(itn, date):
    sub_contract =  SubContract.query \
                    .filter(SubContract.itn == itn, \
                        SubContract.start_date <= date, \
                        SubContract.end_date >= date) \
                    .all()
    return True if len(sub_contract) > 1 else False


def generate_subcontract(row, curr_contract, df):

    activation_date = convert_date_to_utc("Europe/Sofia",row['activation_date']).replace(tzinfo=None)
    curr_sub_contract =  SubContract.query.filter(SubContract.itn == row['itn'], \
                                                        SubContract.start_date <= activation_date, \
                                                        SubContract.end_date >= activation_date).all()
    if len(curr_sub_contract) == 0:
        
        forcasted_vol = None

        if df.get(row['itn']) is not None and get_measuring_type(row['measuring_type']).code in ['DIRECT','UNDIRECT']:
            forcasted_vol = Decimal(str(df[row['itn']]['forcasted_volume'].sum()))
            g.forcasted_schedule = df[row['itn']]
        else:
            if row['forecast_montly_consumption'] is None:
                flash('No forcasted volume provided or measuring type mismatch.','danger')
            else:
                forcasted_vol = Decimal(str(row['forecast_montly_consumption']))

        curr_sub_contract = SubContract(itn = row['itn'],
                                    contract_id = curr_contract.id, \
                                    object_name = '',\
                                    price = round(Decimal(str(row['price'])), MONEY_ROUND), \
                                    invoice_group_id = get_invoicing_group(row['invoice_group']).id, \
                                    measuring_type_id = get_measuring_type(row['measuring_type']).id, \
                                    start_date = convert_date_to_utc("Europe/Sofia",row['activation_date']),\
                                    end_date =  curr_contract.end_date, \
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

    return time_series

def create_subcontract_df(itn,activation_date, internal_id, measuring_type, invoice_group, price, zko, 
                                akciz, has_grid_services, has_spot_price,  
                                object_name, is_virtual,virtual_parent_itn, forecast_montly_consumption,has_balancing):
    
    df = pd.DataFrame([{'itn':itn,'activation_date':activation_date, 'internal_id':internal_id, 'measuring_type':measuring_type, 
                        'invoice_group':invoice_group, 'price':price, 'zko':zko,'akciz':akciz, 'has_grid_services':has_grid_services,
                        'has_spot_price':has_spot_price, 'object_name':object_name, 
                        'forecast_montly_consumption':forecast_montly_consumption,'has_balancing':has_balancing}])
    return df
    
def check_and_load_hourly_schedule(form):
    # print(f'in check_and_load_hourly_schedule:  {form.measuring_type.data.code}', file=sys.stdout)
    if form.measuring_type.data.code in ['DIRECT','UNDIRECT']:
        df = pd.read_excel(request.files.get('file_'))
        if set(df.columns).issubset(['date','forcasted_volume']):
            forcasted_vol = Decimal(str(df['forcasted_volume'].sum()))
            g.forcasted_schedule = df
        else:
            flash('Wrong file format for forcasted volume upload','danger') 
            return redirect(url_for('create_subcontract'))      
    elif form.forecast_vol.data == '':
        flash('No forcasted volume provided or measuring type mismatch.','danger')
        return redirect(url_for('create_subcontract')) 
    else:
        forcasted_vol = Decimal(str(form.forecast_vol.data))
        flash(f'forcasted volume = {forcasted_vol}','info')
    return forcasted_vol

def get_remaining_forecat_schedule(itn, date):
    remaining_schedule = ItnSchedule.query.filter(ItnSchedule.itn == itn, ItnSchedule.utc > date).all()
    list_of_dict = []
    for schedule in remaining_schedule: 
                            
                list_of_dict.append(dict(itn = schedule.itn, 
                                utc = schedule.utc,                                                      
                                forecast_vol = schedule.forecast_vol,
                                reported_vol = schedule.reported_vol,
                                price = schedule.price))

    g.remaining_schedule_list_of_dict = list_of_dict
    
    return remaining_schedule

def apply_collision_function(new_subcontract, old_subcontract):

    if new_subcontract.start_date <= old_subcontract.start_date and new_subcontract.end_date >= old_subcontract.end_date:
        old_is_inner(old_subcontract)
        print('old_is_inner', file = sys.stdout)
    elif new_subcontract.start_date > old_subcontract.start_date and new_subcontract.end_date > old_subcontract.end_date:
        old_is_left_wing(new_subcontract,old_subcontract)
        print('old_is_left_wing', file = sys.stdout)
    elif new_subcontract.start_date < old_subcontract.start_date and new_subcontract.end_date < old_subcontract.end_date:
        old_is_right_wing(new_subcontract,old_subcontract)
        print('old_is_right_wing', file = sys.stdout)
    elif new_subcontract.start_date > old_subcontract.start_date and new_subcontract.end_date < old_subcontract.end_date:
        new_is_inner(new_subcontract,old_subcontract)
        print('new_is_inner', file = sys.stdout)
    elif new_subcontract.start_date == old_subcontract.start_date and new_subcontract.end_date < old_subcontract.end_date:
        new_is_left_inner(new_subcontract,old_subcontract)
        print('new_is_left_inner', file = sys.stdout)
    elif new_subcontract.start_date > old_subcontract.start_date and new_subcontract.end_date == old_subcontract.end_date:
        new_is_right_inner(new_subcontract,old_subcontract)
        print('new_is_right_inner', file = sys.stdout)
    else:
        print('!!!! unhandeled condition !!!!', file = sys.stdout)





def old_is_inner(old_subcontract):
    db.session.delete(old_subcontract)
    db.session.commit()

def old_is_left_wing(new_subcontract,old_subcontract):
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})

def old_is_right_wing(new_subcontract,old_subcontract):
    old_subcontract.update({'start_date':new_subcontract.end_date + dt.timedelta(hours = 1)})

def new_is_inner(new_subcontract,old_subcontract):
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})

def new_is_left_inner(new_subcontract,old_subcontract):
    old_subcontract.update({'start_date':new_subcontract.end_date + dt.timedelta(hours = 1)})

def new_is_right_inner(new_subcontract,old_subcontract):
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})




    



                   
