import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal,ROUND_HALF_UP

from sqlalchemy.exc import ProgrammingError
from flask import g, flash
from app.models import *  #(Contract, Erp, AddressMurs, InvoiceGroup, MeasuringType, ItnMeta, SubContract, )


MONEY_ROUND = 9


def stringifyer(df):
    """check first row value of each colum if is datetime or decimal and convert them to string if yes"""
    for col in df.columns:
        if isinstance(df[col].iloc[0], dt.date): 
            
            df[col] = df.apply(lambda x: x[col].strftime('%Y-%m-%d %H:%M:%S'), axis=1)
        elif isinstance(df[col].iloc[0], Decimal):            
            
            df[col] = df.apply(lambda x: str(x[col]), axis=1)
        
        
def update_or_insert(df, table_name):
    """Perform bulk insert on duplicate update of pandas df to mysql table. Support native for MySql NULL insertion.
        Requirements: 1.Dataframe columns MUST be exactly the same and in the same order as SQL table.
                      2.The NULL values in datafreme MUST be respresentet by np.nan or string 'NULL' 
                      
        Input:engine, dataframe, sql table name
        Output: none"""
    # print(f'from ENTERING update_or_insert ---- > df is  {df}')
    if df.empty:
        print(f'from update_or_insert ---- > df is empty {df}')
        return
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
        tuples_to_insert = re.sub('%',"%%", tuples_to_insert)
        # tuples_to_insert = re.sub(':',"::", tuples_to_insert)  
        sql_str = f"INSERT INTO {table_name} {fields} VALUES {tuples_to_insert} ON DUPLICATE KEY UPDATE {','.join([x + ' = VALUES(' + x + ')' for x in df.columns.values])} "
        # print(f'from update_or_insert ----> before commit to db')
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

def generate_forecast_schedule(measuring_type, itn, forecast_vol, weekly_forecast_df, activation_date_utc, curr_contract, tariff, subcontract_end_date = None): 
    """ generate time schedule with tariff prices and forecast volumes and insert to ItnScheduleTemp"""

    # generate_forecast_schedule(curr_measuring_type, itn, row['forecast_montly_consumption'], forecast_df, activation_date_utc, curr_contract, tariffs, sub_end_date_utc)   

    time_zone = TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code
    print(f'time zone: {time_zone}, sub_und_date: {subcontract_end_date}')
    local_start_date = convert_date_from_utc(time_zone, activation_date_utc, False)    
    local_end_date = convert_date_from_utc(time_zone, curr_contract.end_date, False)  

    if subcontract_end_date is not None:        
        local_end_date = convert_date_from_utc(time_zone, subcontract_end_date, False)
    print(f'local end date: {local_end_date}')
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
    forecast_df['price'] = forecast_df.apply(lambda x: generate_tariff_hours(x.name, tariff), axis = 1)
    forecast_df.reset_index(inplace = True)    
    forecast_df = forecast_df[['itn', 'utc', 'forecast_vol', 'consumption_vol', 'price', 'settelment_vol', 'tariff_id']]    
    update_or_insert(forecast_df, ItnScheduleTemp.__table__.name)
    print(f'from generate_forecast_schedule. Uploaded to ItnCheduleTemp Head: \n{forecast_df.head()}')
    print(f'from generate_forecast_schedule. Uploaded to ItnCheduleTemp Tail: \n{forecast_df.tail()}')


    # forecast_df['tariff_id'] = tariff.id
    # forecast_df['price'] = forecast_df.apply(lambda x: generate_regards_dst_hours(x.name, tariff), axis = 1)
    # forecast_df['settelment_vol'] = -1
    # # if(tariff.name == 'single_tariff'):
    # #     forecast_df['price'] = tariff.price_day

    # # elif(tariff.name == 'double_tariff'):
    # #     forecast_df['price'] = forecast_df.apply(lambda x: generate_regards_dst_hours(x.name, tariff), axis = 1)
    # #     # forecast_df.loc[(forecast_df.index.hour > 6) & (forecast_df.index.hour <= 22), 'price'] = tariff.price_day

    # forecast_df.index = forecast_df.index.tz_convert('UTC').tz_localize(None)
    # forecast_df.reset_index(inplace = True)    
    # forecast_df = forecast_df[['itn', 'utc', 'forecast_vol', 'consumption_vol', 'price', 'settelment_vol', 'tariff_id']]    
    # update_or_insert(forecast_df, ItnScheduleTemp.__table__.name)
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


# def generate_regards_dst_hours(date, tariff):
#     # print(f' in generate_regards_dst_hours {date} ---> {tariff}')
#     if tariff.name == 'single_tariff':
#         return tariff.price_day

#     if (date.month >= 4) & (date.month <= 10):
#         # lqtno chasovo vreme
#         if(date.hour > 7) & (date.hour <= 23):
#             # dnevna tarifa
#             if ((tariff.name == 'peak_tariff') & (((date.hour > 8) & (date.hour <= 12)) | ((date.hour > 18) & (date.hour <= 22)))):
#                 # vyrhova tarifa
#                 print(f'in vyrhova {date.hour}')
#                 return tariff.price_peak
#             else:
#                 return tariff.price_day
#         else:
#             # no6tna tarifa
#             return tariff.price_night
#     else:
#         # zimno chasovo vreme
#         if(date.hour > 6) & (date.hour <= 22):
#             # dnevna tarifa
#             if ((tariff.name == 'peak_tariff') & (((date.hour > 8) & (date.hour <= 11)) | ((date.hour > 20) & (date.hour <= 21)))):
#                 # vyrhova tarifa
#                 return tariff.price_peak
#             else:
#                 return tariff.price_day
#         else:
#             # no6tna tarifa
#             return tariff.price_night


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

def create_tariff(name, price_day, price_night = 0, price_peak = 0):

    print(f'{name} --- {price_day} --- {price_night} --- {price_peak}')    
    curr_tariff = Tariff.query.filter(Tariff.name == name, Tariff.price_day == Decimal(str(price_day)) / Decimal('1000'), Tariff.price_night == Decimal(str(price_night)) / Decimal('1000'), Tariff.price_peak == Decimal(str(price_peak)) / Decimal('1000')).first()
    if curr_tariff is not None:
        print(f'founded such a tariff - returning {curr_tariff}')
        return curr_tariff
    
    if (name == 'single_tariff') | (name == 'double_tariff') | (name == 'peak_tariff'):
        curr_tariff = Tariff(name = name, price_day = Decimal(str(price_day)) / Decimal('1000'), price_night = Decimal(str(price_night)) / Decimal('1000'), price_peak = Decimal(str(price_peak)) / Decimal('1000') )
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

    # price = Decimal(str(price)) / Decimal('1000')    

    # if curr_tariff is not None:
    #     flash(f'Updating existing tarrif for itn: {itn}, start date: {start_date}, end date: {end_date}, start hour: {start_hour} and end hour: {end_hour}.','danger')
    #     print(f'Updating existing tarrif for itn: {itn}, start date: {start_date}, end date: {end_date}, start hour: {start_hour} and end hour: {end_hour}.')
    #     curr_tariff.update({'name':name, 'price':price})  

    # else:
    #     unique_key = itn + start_date.strftime("%Y-%m-%d %H:%M:%S") + end_date.strftime("%Y-%m-%d %H:%M:%S") + str(start_hour) + str(end_hour)
    #     curr_tariff = Tariff(itn = itn, start_date = start_date, end_date = end_date, name = name, price = price, start_hour = start_hour, end_hour = end_hour, unique_key = unique_key)
    #     curr_tariff.save()

    return curr_tariff

def generate_subcontract_from_file(row, curr_contract, df, curr_itn_meta):

    activation_date_utc = convert_date_to_utc(TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code,row['activation_date'])
    sub_end_date_utc = curr_contract.end_date # convert_date_to_utc(TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code, curr_contract.end_date)    

    curr_sub_contract =  SubContract.query.filter(SubContract.itn == row['itn'], \
                                                        SubContract.start_date <= activation_date_utc, \
                                                        SubContract.end_date >= activation_date_utc).all()
    if len(curr_sub_contract) == 0:
        #print(f'metaaaaa ---> {curr_itn_meta}')
        itn = curr_itn_meta.itn
        activation_date_utc, sub_end_date_utc = validate_subcontracts_dates(activation_date_utc, sub_end_date_utc, curr_contract)

        if activation_date_utc is None:
            flash('Wrong dates according the contract !','danger')
            return None
            # print(f'from generate_subcontract_from_file ---- activation_date_utc = {activation_date_utc}<---> sub_end_date_utc = {sub_end_date_utc}')
        curr_measuring_type = get_measuring_type(row['measuring_type']) 

        if curr_measuring_type is None:
            flash(f'Wrong measuring type from ITN upload {itn}. Zerro will be inserted !','danger')
            print(f'Wrong measuring type from ITN upload {itn}. Zerro will be inserted !')
            return None              
        
        # if row.price.is_nan() :
        #     print(f'from decimal isna {row.price}')           
        #     price = Decimal('0')
        #     flash(f'Wrong price from ITN upload {itn}. Zerro will be inserted !','danger')
        #     print(f'Wrong price from ITN upload {itn}. Zerro will be inserted !')
        # else:
        #     (f'PRICEEEEEEEEEEEEEEEEE -------> is not evaluate to NONE {row.price}')
        #     price = round(Decimal(str(row['price'])) , MONEY_ROUND)
        
        curr_tariff = create_tariff(row['tariff_name'], row['price_day'], row['price_night'])
       

        forecast_df = validate_forecasting_df(df, itn)
        # #print(f'from generate_subcontract_from_file -> forecast_df: {forecast_df.head()}', file = sys.stdout)
        generate_forecast_schedule(curr_measuring_type, itn, row['forecast_montly_consumption'], forecast_df, activation_date_utc, curr_contract, curr_tariff, sub_end_date_utc)
       
        # try:
        curr_sub_contract = SubContract(itn = itn,
                                    contract_id = curr_contract.id, \
                                    object_name = '',\
                                    # price = price, \
                                    invoice_group_id = get_invoicing_group(row['invoice_group']).id, \
                                    measuring_type_id = get_measuring_type(row['measuring_type']).id, \
                                    start_date = activation_date_utc,\
                                    end_date =  sub_end_date_utc, \
                                    zko = round(Decimal(str(row['zko'])) , MONEY_ROUND), \
                                    akciz = round(Decimal(str(row['akciz'])) , MONEY_ROUND), \
                                    has_grid_services = row['has_grid_services'], \
                                    has_spot_price = row['has_spot_price'], \
                                    has_balancing = row['has_balancing'])            
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
    


def upload_remaining_forecat_schedule(itn, new_subcontract_end_date, old_subcontract_end_date):
    remaining_schedule = ItnSchedule.query \
                                    .filter(ItnSchedule.itn == itn, ItnSchedule.utc > new_subcontract_end_date, ItnSchedule.utc <= old_subcontract_end_date) \
                                    .all()
    list_of_dict = []
    for schedule in remaining_schedule: 
                            
                list_of_dict.append(dict(itn = schedule.itn, 
                                utc = schedule.utc,                                                      
                                forecast_vol = schedule.forecast_vol,
                                consumption_vol = schedule.consumption_vol,
                                price = schedule.price,
                                settelment_vol = schedule.settelment_vol))
    #print('delete temp table from upload_remaining_forecat_schedule', file = sys.stdout)
    ItnScheduleTemp.query.delete()
    db.session.commit()
    db.session.bulk_insert_mappings(ItnScheduleTemp, list_of_dict)
    #print('bulk_insert_mappings temp table from upload_remaining_forecat_schedule', file = sys.stdout)

    # return remaining_schedule

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
    df = df[start_idx:end_idx+1].copy()
    #print(f'input_df validated {df.iloc[0]}')
    return df



# def validate_itn_row(row):


#     activation_date_utc = convert_date_to_utc(TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code,row['activation_date'])
#     sub_end_date_utc = convert_date_to_utc(TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code, curr_contract.end_date)    

#     curr_sub_contract =  SubContract.query.filter(SubContract.itn == row['itn'], \
#                                                         SubContract.start_date <= activation_date_utc, \
#                                                         SubContract.end_date >= activation_date_utc).all()

#     invoice_group_id = get_invoicing_group(row['invoice_group']).id, \
#     measuring_type_id = get_measuring_type(row['measuring_type']).id, \
#     start_date = activation_date_utc,\
#     end_date =  sub_end_date_utc, \
#     zko = round(Decimal(str(row['zko'])) , MONEY_ROUND), \
#     akciz = round(Decimal(str(row['akciz'])) , MONEY_ROUND), \
#     has_grid_services = row['has_grid_services'], \
#     has_spot_price = row['has_spot_price'], \
#     has_balancing = row['has_balancing'])                      






    



                   
