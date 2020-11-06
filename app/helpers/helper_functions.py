import sys, pytz, datetime as dt
import pandas as pd
import os
import glob as gl
import os.path
import xlrd
import time,re
from decimal import Decimal,ROUND_HALF_UP

from sqlalchemy.exc import ProgrammingError
from flask import g, flash
from app.models import *  #(Contract, Erp, AddressMurs, InvoiceGroup, MeasuringType, ItnMeta, SubContract, )
from app.helpers.helper_function_excel_writer import (INV_REFS_PATH, INTEGRA_INDIVIDUAL_PATH, INTEGRA_FOR_UPLOAD_PATH)
                        
                                      


MONEY_ROUND = 9


def stringifyer(df):
    """check first row value of each colum if is datetime or decimal and convert them to string if yes"""
    for col in df.columns:
        if isinstance(df[col].iloc[0], dt.date): 
            
            df[col] = df.apply(lambda x: x[col].strftime('%Y-%m-%d %H:%M:%S'), axis=1)
        elif isinstance(df[col].iloc[0], Decimal):            
            
            df[col] = df.apply(lambda x: str(x[col]), axis=1)
        
def date_format_corector(df, columns):   
    for col in columns:
        df[col] = df[col].apply(lambda x: x.replace('.','/') if(isinstance(x,str) and ('.' in x)) else x)
        df[col] = df[col].apply(lambda x: dt.datetime.strptime(x, '%d/%m/%Y') if isinstance(x,str) else x)
    return df

def update_or_insert(df, table_name, remove_nan = False):
    """Perform bulk insert on duplicate update of pandas df to mysql table. Support native for MySql NULL insertion.
        Requirements: 1.Dataframe columns MUST be exactly the same and in the same order as SQL table.
                      2.The NULL values in datafreme MUST be respresentet by np.nan or string 'NULL' 
                      
        Input:engine, dataframe, sql table name
        Output: none"""
    # print(f'from ENTERING update_or_insert ---- > df is  {df}')
    if df.empty:
        print(f'from update_or_insert ---- > df is empty {df}')
        return
    if remove_nan:
        df = df.fillna(0)
    stringifyer(df)        
    SPLIT_SIZE = 40000
    df.fillna(value='NULL', inplace = True)
    fields = str(tuple(df.columns.values))
    fields = re.sub('[\']',"",fields)    
    fields = re.sub('(,\))',")",fields)
    tuples_ = [tuple(r) for r in df.to_numpy()]

    tuple_tokens =[tuples_[i : i + SPLIT_SIZE] for i in range(0, len(tuples_), SPLIT_SIZE)]
    for t in tuple_tokens:
        tuples_to_insert = str(t)    
        tuples_to_insert = re.sub('(NULL)',"#NULL#",tuples_to_insert)
        tuples_to_insert = re.sub('[\[\]]|(\'#)|(#\')',"", tuples_to_insert)
        tuples_to_insert = re.sub('(,\))',")", tuples_to_insert)
        tuples_to_insert = re.sub("%","%%", tuples_to_insert)        
        # tuples_to_insert = re.sub(':',"::", tuples_to_insert)  
        sql_str = f"INSERT INTO {table_name} {fields} VALUES {tuples_to_insert} ON DUPLICATE KEY UPDATE {','.join([x + ' = VALUES(' + x + ')' for x in df.columns.values])} "
        # print(f'from update_or_insert ----> before commit to db {sql_str}')
        db.session.execute(sql_str)
        db.session.commit()
       


def convert_date_from_utc(time_zone, dt_obj, is_string = True, t_format = "%Y-%m-%d %H:%M:%S"):
    if(dt_obj is None):
        return None
    if isinstance(dt_obj, str):
        dt_obj = dt.datetime.strptime(dt_obj, t_format)   
    utc = pytz.timezone('UTC')
    new_zone = pytz.timezone(time_zone)
    dt_obj = utc.localize(dt_obj)
    dt_obj = dt_obj.astimezone(new_zone).replace(tzinfo=None)  
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

def get_tariff_offset(input_date, time_zone, t_format = "%Y-%m-%d"):

    naive = dt.datetime.strptime (input_date, t_format) 
    local = pytz.timezone (time_zone)
    local_date = local.localize(naive, is_dst=True)
    return local_date.utcoffset() / dt.timedelta(hours=1)

def validate_ciryllic(data):

    if data.lower() == 'none':
        return True

    
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

    new_start_date =  convert_date_to_utc(TimeZone.query.filter(TimeZone.id == contract.time_zone_id).first().code, activation_date) 
    new_end_date = new_start_date + dt.timedelta(hours = contract.duration_in_days * 24 - 1)
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


def create_itn_meta(row):

    curr_itn_meta = ItnMeta.query.filter(ItnMeta.itn == row['itn']).first()
    
    if curr_itn_meta is not None:
        return None
    else:
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
    

        # curr_itn_meta.save()
    # #print(curr_itn_meta, file = sys.stdout)
    
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

def generate_forecast_schedule(measuring_type, itn, forecast_vol, weekly_forecast_df, activation_date_utc, curr_contract, tariff, has_spot_price, subcontract_end_date = None): 
    """ generate time schedule with tariff prices and forecast volumes and insert to ItnScheduleTemp"""
                                # curr_measuring_type, itn, row['forecast_montly_consumption'], forecast_df, activation_date_utc, curr_contract, curr_tariff, sub_end_date_utc

    # generate_forecast_schedule(curr_measuring_type, itn, row['forecast_montly_consumption'], forecast_df, activation_date_utc, curr_contract, tariffs, sub_end_date_utc)   

    time_zone = TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code
    # print(f'time zone: {time_zone}, sub_und_date: {subcontract_end_date}')
    local_start_date = convert_date_from_utc(time_zone, activation_date_utc, False)    
    local_end_date = convert_date_from_utc(time_zone, curr_contract.end_date, False) 
    end_date_utc = curr_contract.end_date 

    if subcontract_end_date is not None:        
        local_end_date = convert_date_from_utc(time_zone, subcontract_end_date, False)
        end_date_utc = subcontract_end_date
    # print(f'local end date: {local_end_date}')
    # #print(f'from generate_forecast_schedule ---> end date is {local_end_date}')
    time_series = pd.date_range(start = local_start_date, end = local_end_date , freq='h', tz = time_zone)
    #print(f'from generate_forecast_schedule time_series is : {time_series}')
    #print(f'{local_start_date} --- {local_end_date} --- {TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code}')
    forecast_df = pd.DataFrame(time_series, columns = ['utc'])
    forecast_df['weekday'] = forecast_df['utc'].apply(lambda x: x.strftime('%A'))
    forecast_df['hour'] = forecast_df['utc'].apply(lambda x: x.hour)

    # forecast_df.set_index('utc', inplace = True)
    # forecast_df.index = forecast_df.index.tz_convert('UTC').tz_localize(None)
    # forecast_df.reset_index(inplace = True) 

    if has_spot_price:
        # print(f'from generate_forecast_schedule sdate {activation_date_utc} --- {end_date_utc}')
        ibex_records = db.session.query(IbexData.utc.label('utc_ibex'), IbexData.price.label('ibex_price')).filter(IbexData.utc >= activation_date_utc, IbexData.utc <= end_date_utc).all()
        ibex_df = pd.DataFrame.from_records(ibex_records, columns = ibex_records[0].keys()) 
        ibex_df.set_index('utc_ibex', inplace = True) 
        # print(f'from generate_forecast_schedule ibex_df is :\n {ibex_df.head()}')
    else:
        ibex_df = pd.DataFrame()

    if measuring_type.code in ['DIRECT','UNDIRECT']:

        if weekly_forecast_df is not None: 
            # #print(f'weekly_forecast_df is not None: {forecast_df.head()}')   
            forecast_df = pd.merge(forecast_df, weekly_forecast_df, on = ['weekday','hour'], how = 'right' )
            forecast_df.drop_duplicates(subset = 'utc', keep = 'first', inplace = True)         
            forecast_df['forecast_vol'] = forecast_df['forecasted_vol'].apply(lambda x: Decimal(str(x)) * Decimal('1000'))
        else:
            forecast_df['forecast_vol'] = Decimal('0') 
        # #print(f'from direct version: {forecast_df.head()}')
    else:
        
        stp_df = pd.read_sql(StpCoeffs.query.filter(StpCoeffs.measuring_type_id == measuring_type.id).statement, db.session.bind)
        stp_df.set_index('utc', inplace = True)
        stp_df.index = stp_df.index.tz_localize('UTC').tz_convert(time_zone)
        stp_df.reset_index(inplace = True) 

        if not stp_df.empty:
            forecast_df = forecast_df.merge(stp_df, on = 'utc', how = 'left')
            forecast_df = forecast_df.fillna(0)
            forecast_df['forecast_vol'] = forecast_df['value'].apply(lambda x: Decimal(str(x)) * Decimal(str(forecast_vol)))
            # #print(f'from stp, forecast_vol = {forecast_vol}')
        else:    
            forecast_df['forecast_vol'] = Decimal(str(forecast_vol))
        # #print(f'from stp version: {forecast_df.head()}')

    forecast_df['itn'] = itn
    # forecast_df['price'] = price
    forecast_df['consumption_vol'] = -1
    # forecast_df = forecast_df[['itn','utc','forecast_vol', 'consumption_vol','price']]
    delete_sch = ItnScheduleTemp.__table__.delete()
    db.session.execute(delete_sch)
    forecast_df.set_index('utc', inplace = True)

    forecast_df.index = forecast_df.index.tz_convert('UTC').tz_localize(None)
    forecast_df['tariff_id'] = tariff.id
    forecast_df['settelment_vol'] = -1

    if has_spot_price:
        print(f'IN HAS SPOT PRICE !!!!!!!!!!!!!!!!!!')
        forecast_df = forecast_df.merge(ibex_df, left_index=True, right_index=True)
        forecast_df['price'] = forecast_df.apply(lambda x: generate_tariff_hours(x.name, tariff) + x['ibex_price'] / 1000, axis = 1) # x.name because use index
        forecast_df.drop(columns = ['ibex_price'], inplace = True)
    else:
        forecast_df['price'] = forecast_df.apply(lambda x: generate_tariff_hours(x.name, tariff), axis = 1) # x.name because use index

    # forecast_df['price'] = forecast_df.apply(lambda x: generate_tariff_hours(x.name, tariff), axis = 1)
    forecast_df.reset_index(inplace = True)   
    forecast_df.rename(columns={'index':'utc'}, inplace = True)  
    forecast_df = forecast_df[['itn', 'utc', 'forecast_vol', 'consumption_vol', 'price', 'settelment_vol', 'tariff_id']]    
    update_or_insert(forecast_df, ItnScheduleTemp.__table__.name)
    # print(f'from generate_forecast_schedule. Uploaded to ItnCheduleTemp Head: \n{forecast_df.head()}')
    # print(f'from generate_forecast_schedule. Uploaded to ItnCheduleTemp Tail: \n{forecast_df.tail()}')

def generate_tariff_hours(date, tariff):

    if tariff.name == 'single_tariff':
        return tariff.price_day

    if(date.hour > 4) & (date.hour <= 20):
            # dnevna tarifa
        if ((tariff.name == 'peak_tariff') & (((date.hour > 5) & (date.hour <= 9)) | ((date.hour > 15) & (date.hour <= 19)))):
            # vyrhova tarifa
            print(f'in vyrhova {date.hour}')
            return tariff.price_peak
        else:
            return tariff.price_day
    else:
        # no6tna tarifa
        return tariff.price_night

def check_and_load_hourly_schedule(measuring_type_code, itn, form_price, forecast_vol, form_forecast_df, activation_date, curr_contract):

    #print(f'in check_and_load_hourly_schedule:  {form_price}', file=sys.stdout)

    if measuring_type_code in ['DIRECT','UNDIRECT']:       

        forcasted_df = validate_forecasting_df(form_forecast_df, itn)
        forecasted_vol = upload_forecasted_schedule_to_temp_db(forcasted_df, itn, form_price, activation_date, curr_contract)
      
    elif forecast_vol == '':

        flash('No forcasted volume provided or measuring type mismatch.','danger')
        return redirect(url_for('create_subcontract')) 
    else:
        forecasted_vol = Decimal(str(forecast_vol)) 
        flash(f'forcasted volume in kWh= {forecasted_vol}','info')
    return forecasted_vol


def upload_forecasted_schedule_to_temp_db(forecasted_schedule_df, itn, price, activation_date, curr_contract):


    local_start_date = activation_date.to_pydatetime()
    
    local_end_date = convert_date_from_utc(TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code,curr_contract.end_date, False)   
    
    time_series = pd.date_range(start = local_start_date, end = local_end_date , freq='H', \
                                tz = TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code)
    
    
    df = pd.DataFrame(time_series, columns = ['utc'])
    df['weekday'] = df['utc'].apply(lambda x: x.strftime('%A'))
    df['hour'] = df['utc'].apply(lambda x: x.hour)
    df.set_index('utc', inplace = True)
    df.index = df.index.tz_convert('UTC').tz_localize(None)
    df.reset_index(inplace = True) 
    
    if forecasted_schedule_df is not None: 
        # forecasted_schedule_df['forecasted_vol'] = forecasted_schedule_df['forecasted_vol'].apply(lambda x: Decimal(str(x)) * Decimal('1000'))
        df = pd.merge(df, forecasted_schedule_df, on = ['weekday','hour'], how = 'right' )
        # df.drop_duplicates(subset = 'utc', keep = 'first', inplace = True)         
        df['forecast_vol'] = df['forecasted_vol'].apply(lambda x: Decimal(str(x)) * Decimal('1000'))
        forecasted_vol = Decimal(str(forecasted_schedule_df['forecasted_vol'].sum()))
    else:
        flash(f'No forcasted volume provided for ITN {itn}. Zerro will be inserted !','danger')
        df['forecast_vol'] = Decimal(str('0'))
        forecasted_vol = Decimal(str('0'))
    
    df['itn'] =itn
    df['price'] = price 
    df['consumption_vol'] = -1
    df = df[['itn','utc','forecast_vol','consumption_vol','price']]
    
    df['utc'] = df['utc'].astype(str)
    ItnScheduleTemp.query.delete()
    db.session.commit()
    #print(df, file = sys.stdout)
    bulk_list = df.to_dict(orient='records')    
    db.session.bulk_insert_mappings(ItnScheduleTemp, bulk_list)
    return forecasted_vol


def validate_forecasting_df(df, itn):
    
    if df.get(itn) is None:
        schedule_df = None if df.get('all') is None else (df.get('all') if set(df.get('all')).issubset(['weekday', 'hour','forecasted_vol']) else None)             
    else:
        schedule_df = df.get(itn) if set(df.get(itn)).issubset(['weekday','hour','forecasted_vol']) else None     
    return schedule_df

def validate_subcontracts_dates(start_date_utc, end_date_utc, current_contract):

    if (start_date_utc >= current_contract.end_date) | (end_date_utc <= current_contract.start_date):
        return None, None
    s_date = max(start_date_utc, current_contract.start_date)
    e_date = min(end_date_utc, current_contract.end_date)
    #print(f'FROM validate_subcontracts_dates ::: s_date = {s_date} <-----> e_date = {e_date}')
    return s_date, e_date

def create_invoicing_group(name, description, contractor_id):

    name = name.strip()
    description = description.strip()
    curr_inv_group = InvoiceGroup.query.filter(InvoiceGroup.name == name, InvoiceGroup.contractor_id == contractor_id).first()
    if curr_inv_group is not None:
        print(f'founded such a invoicing group - returning {curr_inv_group}')
        return curr_inv_group

    curr_inv_group = InvoiceGroup(name = name, contractor_id = contractor_id, description = description)
    curr_inv_group.save()
    return curr_inv_group

def create_tariff(name, price_day, price_night = 0, price_peak = 0, lower_limit = 0, upper_limit = 0):
    
    name = name.strip()
    print(f'{name} --- {price_day} --- {price_night} --- {price_peak} --- {lower_limit} --- {upper_limit}')    
    curr_tariff = Tariff.query.filter(Tariff.name == name, Tariff.price_day == Decimal(str(price_day)) / Decimal('1000'), 
                                        Tariff.price_night == Decimal(str(price_night)) / Decimal('1000'),
                                        Tariff.price_peak == Decimal(str(price_peak)) / Decimal('1000'),
                                        Tariff.lower_limit == Decimal(str(lower_limit)) / Decimal('1000'),
                                        Tariff.upper_limit == Decimal(str(upper_limit)) / Decimal('1000')).first()
    if curr_tariff is not None:
        print(f'founded such a tariff - returning {curr_tariff}')
        return curr_tariff
                 
    if (name == 'single_tariff') | (name == 'double_tariff') | (name == 'peak_tariff'):
        
        curr_tariff = Tariff(name = name, 
                            price_day = Decimal(str(price_day)) / Decimal('1000'), 
                            price_night = Decimal(str(price_night)) / Decimal('1000'), 
                            price_peak = Decimal(str(price_peak)) / Decimal('1000'), 
                            lower_limit = Decimal(str(lower_limit)) / Decimal('1000'),
                            upper_limit = Decimal(str(upper_limit)) / Decimal('1000'),
                            )
        curr_tariff.save() 
        print(f'SAVING {curr_tariff}') 
        return curr_tariff  

    elif(name == 'ibex'):
        #to do
        pass

    elif(name == 'custom'):
        #to do
        pass

    else:
        flash(f'Wrong tariff: {name} !','danger')
        print(f'Wrong tariff: {name} !')

    return curr_tariff

def generate_subcontract_from_file(row, curr_contract, df, curr_itn_meta):

    activation_date_utc = convert_date_to_utc(TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code,row['activation_date'])
    sub_end_date_utc = curr_contract.end_date # convert_date_to_utc(TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code, curr_contract.end_date)    

    curr_sub_contract =  SubContract.query.filter(SubContract.itn == row['itn'], \
                                                        SubContract.start_date <= activation_date_utc, \
                                                        SubContract.end_date >= activation_date_utc).all()
    if len(curr_sub_contract) == 0:
        
        itn = curr_itn_meta.itn
        activation_date_utc, sub_end_date_utc = validate_subcontracts_dates(activation_date_utc, sub_end_date_utc, curr_contract)

        if activation_date_utc is None:
            flash('Wrong dates according the contract !','danger')
            return None
            
        curr_measuring_type = get_measuring_type(row['measuring_type']) 

        if curr_measuring_type is None:
            flash(f'Wrong measuring type from ITN upload {itn}. Zerro will be inserted !','danger')
            print(f'Wrong measuring type from ITN upload {itn}. Zerro will be inserted !')
            return None        
        
        curr_tariff = create_tariff(row['tariff_name'], row['price_day'], row['price_night']) 
        curr_inv_group = create_invoicing_group(row['invoice_group_name'], row['invoice_group_description'], curr_contract.contractor_id)      

        forecast_df = validate_forecasting_df(df, itn)
        
        generate_forecast_schedule(curr_measuring_type, itn, row['forecast_montly_consumption'], forecast_df, activation_date_utc, curr_contract, curr_tariff, row['has_spot_price'], sub_end_date_utc)
       
        # try:
        curr_sub_contract = (SubContract(itn = itn,
                                    contract_id = curr_contract.id, 
                                    object_name = '',                                    
                                    invoice_group_id = get_invoicing_group(row['invoice_group_name']).id, 
                                    measuring_type_id = get_measuring_type(row['measuring_type']).id, 
                                    start_date = activation_date_utc,
                                    end_date =  sub_end_date_utc,
                                    zko = round(Decimal(str(row['zko'])) , MONEY_ROUND),
                                    akciz = round(Decimal(str(row['akciz'])) , MONEY_ROUND),
                                    has_grid_services = row['has_grid_services'],
                                    has_spot_price = row['has_spot_price'],
                                    has_balancing = row['has_balancing'],
                                    make_invoice = row['make_invoice']))           
        # except:
        #     flash(f'Unsuccesiful sub_contract creation from ITN upload {itn}. Skipping !','danger')
        #     print(f'Unsuccesiful sub_contract creation from ITN upload {itn}. Skipping !')
        #     return None

        
    elif len(curr_sub_contract) > 1:
        flash(f'Error ! Overlaping subcontracts with itn {itn} and activation date {activation_date_utc}','error')
        print(f'Error ! Overlaping subcontracts with itn {itn} and activation date {activation_date_utc}')
    else:
        pass
    return curr_sub_contract

def generate_provisional_subcontract(input_df, curr_subcontract):

    new_start_date =  dt.datetime.strptime(str(input_df.iloc[0]['utc']), "%Y-%m-%d %H:%M:%S") 

    upload_remaining_forecat_schedule (curr_subcontract.itn, new_start_date - dt.timedelta(hours = 1), curr_subcontract.end_date)
    
    
    new_measuring_type = get_measuring_type('UNDIRECT') 

    provisional_sub_contract = (SubContract(itn = curr_subcontract.itn,
                            contract_id = curr_subcontract.contract_id, 
                            object_name = curr_subcontract.object_name,                            
                            invoice_group_id = curr_subcontract.invoice_group_id, 
                            measuring_type_id = new_measuring_type.id, 
                            start_date = new_start_date ,
                            end_date =  curr_subcontract.end_date, 
                            zko = curr_subcontract.zko , 
                            akciz = curr_subcontract.akciz, 
                            has_grid_services = curr_subcontract.has_grid_services, 
                            has_spot_price = curr_subcontract.has_spot_price, 
                            has_balancing = curr_subcontract.has_balancing))
    print(f'from generate_provisional_subcontract %%%%%%%%%%%% \n old sub end date: {new_start_date - dt.timedelta(hours = 1)} \n new sub start date :{new_start_date}')                       
    curr_subcontract.update({'end_date':new_start_date - dt.timedelta(hours = 1)})
    provisional_sub_contract.save()  
    

def convert_datetime64_to_datetime(dt_obj):

    if not isinstance(dt_obj, np.datetime64):
        #print(f'NOT NP,DATETIME, actual is :{type(dt_obj)}', file = sys.stdout)
        return None
    return dt.datetime.strptime(np.datetime_as_string(dt_obj,unit='s'), '%Y-%m-%dT%H:%M:%S')

def generate_utc_time_series(start_date, end_date, tz ="Europe/Sofia"):

    start_date_utc = convert_date_to_utc(tz, start_date)
    end_date_utc = convert_date_to_utc(tz, end_date) + dt.timedelta(hours = 23)
    time_series = pd.date_range(start = start_date_utc, end = end_date_utc, freq='H', \
                                    tz = tz).tz_convert('UTC').tz_localize(None)

    return time_series
    


def upload_remaining_forecat_schedule(itn, remaining_start_date, remaining_end_date):
    remaining_schedule = (db.session
                            .query(ItnSchedule.itn,
                                    ItnSchedule.utc,
                                    ItnSchedule.forecast_vol,
                                    ItnSchedule.consumption_vol,
                                    ItnSchedule.price,
                                    ItnSchedule.settelment_vol,
                                    ItnSchedule.tariff_id) 
                                .filter(ItnSchedule.itn == itn, ItnSchedule.utc > remaining_start_date, ItnSchedule.utc <= remaining_end_date) 
                                .all())
    
    # list_of_dict = []
    # for schedule in remaining_schedule: 
                            
    #             list_of_dict.append(dict(itn = schedule.itn, 
    #                             utc = schedule.utc,                                                      
    #                             forecast_vol = schedule.forecast_vol,
    #                             consumption_vol = schedule.consumption_vol,
    #                             price = schedule.price,
    #                             settelment_vol = schedule.settelment_vol,
    #                             tariff_id = schedule.tariff_id))
    if len(remaining_schedule) == 0:
        print(f'Remaining schedule is 0. Subcontracts are in order !')
    else:
        df = pd.DataFrame.from_records(remaining_schedule, columns = remaining_schedule[0].keys()) 
        stringifyer(df)
        list_of_dict =  df.to_dict(orient='records')
        # print(f'list of dict \n {list_of_dict}')
        ItnScheduleTemp.query.delete()
        db.session.commit()
        db.session.bulk_insert_mappings(ItnScheduleTemp, list_of_dict)
    
def apply_collision_function(new_subcontract, old_subcontract, measuring_type_code, itn, forecast_vol, form_forecast_df, activation_date, curr_contract):

    if new_subcontract.start_date <= old_subcontract.start_date and new_subcontract.end_date >= old_subcontract.end_date:
        old_is_inner(old_subcontract)
        #print('old_is_inner', file = sys.stdout)
    elif new_subcontract.start_date > old_subcontract.start_date and new_subcontract.end_date > old_subcontract.end_date:
        old_is_left_wing(new_subcontract,old_subcontract)
        #print('old_is_left_wing', file = sys.stdout)
    elif new_subcontract.start_date < old_subcontract.start_date and new_subcontract.end_date < old_subcontract.end_date:
        old_is_right_wing(new_subcontract,old_subcontract)
        #print('old_is_right_wing', file = sys.stdout)
    elif new_subcontract.start_date > old_subcontract.start_date and new_subcontract.end_date < old_subcontract.end_date:

        upload_remaining_forecat_schedule(new_subcontract.itn, new_subcontract.end_date, old_subcontract.end_date)
        # #print(f'ADD_start_date = {new_subcontract.end_date + dt.timedelta(hours = 1)} ----- ADD_end_date = {old_subcontract.end_date}', file = sys.stdout)  
        #           
        additional_sub_contract = SubContract(itn = old_subcontract.itn,
                            contract_id = old_subcontract.contract_id, \
                            object_name = old_subcontract.object_name,\
                            # price = old_subcontract.price , \
                            invoice_group_id = old_subcontract.invoice_group_id, \
                            measuring_type_id = old_subcontract.measuring_type_id, \
                            start_date = new_subcontract.end_date + dt.timedelta(hours = 1) ,\
                            end_date =  old_subcontract.end_date, \
                            zko = old_subcontract.zko , \
                            akciz = old_subcontract.akciz, \
                            has_grid_services = old_subcontract.has_grid_services, \
                            has_spot_price = old_subcontract.has_spot_price, \
                            has_balancing = old_subcontract.has_balancing)
                            
        #print(additional_sub_contract, file = sys.stdout)
          
        new_is_inner(new_subcontract,old_subcontract)     
        additional_sub_contract.save()        
        #print('new_is_inner', file = sys.stdout)
    elif new_subcontract.start_date == old_subcontract.start_date and new_subcontract.end_date < old_subcontract.end_date:
        new_is_left_inner(new_subcontract,old_subcontract)
        #print('new_is_left_inner', file = sys.stdout)
    elif new_subcontract.start_date > old_subcontract.start_date and new_subcontract.end_date == old_subcontract.end_date:
        new_is_right_inner(new_subcontract,old_subcontract)
        #print('new_is_right_inner', file = sys.stdout)
    else:
        print('!!!! unhandeled condition from apply_collision_function !!!!', file = sys.stdout)


def old_is_inner(old_subcontract):
    db.session.delete(old_subcontract)

def old_is_left_wing(new_subcontract,old_subcontract):
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})

def old_is_right_wing(new_subcontract,old_subcontract):
    old_subcontract.update({'start_date':new_subcontract.end_date + dt.timedelta(hours = 1)})

def new_is_inner(new_subcontract,old_subcontract):
    # #print(f'from new_is_inner: new_sub: {new_subcontract} ----------- old_sub: {old_subcontract}')
    # #print(f'from new_is_inner: old_sub updated end date: {new_subcontract.start_date - dt.timedelta(hours = 1)}')
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})

def new_is_left_inner(new_subcontract,old_subcontract):
    #print(f'from new_is_left_inner, old_subcontract_start_date = {old_subcontract.start_date} <------>  old_subcontract_end_date = {old_subcontract.end_date}')
    old_subcontract.update({'start_date':new_subcontract.end_date + dt.timedelta(hours = 1)})

def new_is_right_inner(new_subcontract,old_subcontract):
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})


def validate_input_df(df):
    

    start_idx = df[df.columns[0]].first_valid_index()
    end_idx = df[df.columns[0]].last_valid_index()
    # print(f'Start valid idx --> {start_idx}\n End valid idx ---> {end_idx}')
    df = df[start_idx:end_idx+1].copy()
    df = df.rename(columns=lambda x: x.strip())
    
    return df

def get_excel_files(path):

    file_list = []
    for root, dirs, files in os.walk(path):            
        for filename in files:
            if filename.endswith('.xlsx') & (filename.find('~') == -1):
                # print('root-->',root, 'dirs --->',dirs, 'FILES>>>>>>>',files)
                file_list.append(filename)
                
    return file_list  

def delete_excel_files(path, files, is_delete_all):
    print(f'{files}')    
    custom_del_files = []
    if not is_delete_all:
        custom_del_files = files               

    for root, dirs, files in os.walk(path):            
        for filename in files:
            if filename.endswith('.xlsx') & (filename.find('~') == -1) :
                
                if is_delete_all:
                    os.remove(os.path.join(root, filename))                            
                    
                elif filename in custom_del_files:   
                                       
                    os.remove(os.path.join(root, filename))
                        
                else:
                    continue
                print(f'File: {filename} removed !')   

def parse_integra_csv(df):
    

    list_df = df[['DocNumber','RepFileName']]
    
    list_df = list_df.drop_duplicates(subset = ["DocNumber"], keep = 'first')
    
    list_df.set_index('DocNumber', drop = True, inplace = True)
    list_df = list_df.dropna()
    
    res = list_df.to_records().tolist()

    return res

def create_df_from_integra_csv(csv_file):

    raw_df = pd.read_csv(csv_file, sep = '|')
    appl_numbers = list(set(raw_df[raw_df['StockName'] == 'НАЧИСЛЕН АКЦИЗ']['DocNumber']))
    df = raw_df[raw_df['DocNumber'].isin(appl_numbers)]    

    return df

def get_files(path, file_type):

    file_list = []
    file_type = f'.{file_type}'
    for root, dirs, files in os.walk(path):            
        for filename in files:
            if filename.endswith(file_type) & (filename.find('~') == -1):
                # print('root-->',root, 'dirs --->',dirs, 'FILES>>>>>>>',files)
                file_list.append(filename)
                
    return file_list  

def update_ibex_data(start_date, end_date):
   
    try:
        ibex_df = IbexData.download_from_ibex_web_page(start_date, end_date)
    except:
        print(f'No IBEX data for chossen period : {start_date} - {end_date}')
    else:
        stringifyer(ibex_df)
        bulk_update_list = ibex_df.to_dict(orient='records')
        # print(f' IN IBEX {bulk_update_list}')
        db.session.bulk_update_mappings(IbexData, bulk_update_list)
        db.session.commit()



def update_schedule_prices(start_date, end_date):
    
    
    
    # valid_ibex_last_date = (db.session.query(IbexData.utc, IbexData.price).filter(IbexData.price == 0).order_by(IbexData.utc).first()[0])
    spot_itns = (
        db.session
            .query(SubContract.itn.label('sub_itn'))                                 
            .filter(SubContract.start_date <= end_date, SubContract.end_date > end_date) 
            .filter(SubContract.has_spot_price) #!!!!!!!!!!!!!!!!!!!!!!                           
            .distinct(SubContract.itn) 
            .subquery())

    records = (
        db.session
            .query(ItnSchedule.itn, ItnSchedule.utc, ItnSchedule.tariff_id, 
                    Tariff.price_day.label('tariff_price'), IbexData.price.label('ibex_price'))
            .join(spot_itns, spot_itns.c.sub_itn == ItnSchedule.itn)                    
            .join(IbexData, IbexData.utc == ItnSchedule.utc) 
            .join(Tariff,Tariff.id == ItnSchedule.tariff_id )               
            .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)
            .all()
        )
    df = pd.DataFrame.from_records(records, columns = records[0].keys()) 
    df['price'] = df.apply(lambda x: Decimal(str(x['tariff_price'])) + (Decimal(str(x['ibex_price'])) / Decimal('1000')), axis = 1)
    df.drop(columns = ['ibex_price'], inplace = True)
    stringifyer(df)
    bulk_update_list = df.to_dict(orient='records')  
    print(f'Enter updating schedule prices for {df.shape[0]} records.')  
    db.session.bulk_update_mappings(ItnSchedule, bulk_update_list)
    db.session.commit()
    

    



                   
