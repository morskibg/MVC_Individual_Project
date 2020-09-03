import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import g, flash
from app.models import *  #(Contract, Erp, AddressMurs, InvoiceGroup, MeasuringType, ItnMeta, SubContract, )

MONEY_ROUND = 2


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
    
#     BAD_CHAR_DICT = {'#':' ','%':'%%','(':'\(',')':'\)',"'":"\'",'"':'\"',',':'\,'}
    
#     for i in BAD_CHAR_DICT.items():
#         df = replace_char_in_df(df , i[0], i[1])
#     date_time_stringifyer(df)

#     for col in df.columns:
#         if isinstance(df[col].iloc[0], pd.datetime):               
#             df[col] = df.apply(lambda x: x[col].strftime('%Y-%m-%d %H:%M:%S'), axis=1)

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
        sql_str = f"INSERT INTO {table_name} {fields} VALUES {tuples_to_insert} ON DUPLICATE KEY UPDATE {','.join([x + ' = VALUES(' + x + ')' for x in df.columns.values])} "
        
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

def upload_forecasted_schedule_to_temp_db(forecasted_schedule_df, itn, price, activation_date, curr_contract):


    local_start_date = activation_date.to_pydatetime()
    local_end_date = curr_contract.end_date
    
    time_series = pd.date_range(start = local_start_date, end = local_end_date + dt.timedelta(hours = 23) , freq='H', tz = 'EET')
    
    df = pd.DataFrame(time_series, columns = ['utc'])
    df['weekday'] = df['utc'].apply(lambda x: x.strftime('%A'))
    df['hour'] = df['utc'].apply(lambda x: x.hour)
    df.set_index('utc', inplace = True)
    df.index = df.index.tz_convert('UTC').tz_localize(None)
    df.reset_index(inplace = True) 
    
    if forecasted_schedule_df is not None:   
        df = pd.merge(df, forecasted_schedule_df, on = ['weekday','hour'], how = 'right' )
        # df.drop_duplicates(subset = 'utc', keep = 'first', inplace = True)         
        df['forecast_vol'] = df['forecasted_vol'].apply(lambda x: Decimal(str(x)))
        forecasted_vol = Decimal(str(forecasted_schedule_df['forecasted_vol'].sum()))
    else:
        flash(f'No forcasted volume provided for ITN {itn}. Zerro will be inserted !','danger')
        df['forecast_vol'] = Decimal(str('0'))
        forecasted_vol = Decimal(str('0'))
    
    df['itn'] =itn
    df['price'] = price
    df['reported_vol'] = -1
    df = df[['itn','utc','forecast_vol','reported_vol','price']]
    
    df['utc'] = df['utc'].astype(str)
    ItnScheduleTemp.query.delete()
    db.session.commit()
    print(df, file = sys.stdout)
    bulk_list = df.to_dict(orient='records')    
    db.session.bulk_insert_mappings(ItnScheduleTemp, bulk_list)
    return forecasted_vol



   
    
    # forecasted_vol = Decimal(str(forecasted_schedule_df['forecasted_volume'].sum()))
    
    # time_series = pd.date_range(start = forecasted_schedule_df.iloc[0]['date'].to_pydatetime(), \
    #                             end = forecasted_schedule_df.iloc[-1]['date'].to_pydatetime(), \
    #                             freq='H', tz = 'EET').tz_convert('UTC').tz_localize(None)        
    # schedule_df = pd.DataFrame(time_series, columns = ['utc'])
    # schedule_df['itn'] = itn 
    # schedule_df['price'] = price
    # schedule_df['reported_vol'] = -1

    # forecasted_schedule_df.set_index('date', inplace = True)
    # forecasted_schedule_df.index = forecasted_schedule_df.index.tz_localize('EET', ambiguous='infer').tz_convert('UTC').tz_localize(None)
    # forecasted_schedule_df.reset_index(inplace = True)
    # forecasted_schedule_df.rename(columns = {forecasted_schedule_df.columns[0]:'utc'}, inplace = True)
    # schedule_df = schedule_df.merge(forecasted_schedule_df, on = 'utc', how = 'left')
    
    # schedule_df = schedule_df.fillna(0)
    # schedule_df['forecast_vol'] = schedule_df['forecasted_volume'].apply(lambda x: Decimal(str(x)))
    # schedule_df.drop(columns = 'forecasted_volume', inplace = True)
    # schedule_df['utc'] = schedule_df['utc'].astype(str)
    # ItnScheduleTemp.query.delete()
    # db.session.commit()
    
    # bulk_list = schedule_df.to_dict(orient='records')    
    # db.session.bulk_insert_mappings(ItnScheduleTemp, bulk_list)
    # return forecasted_vol


def convert_weekly_schedule(schedule_df, itn ):

    
    local_start_date = schedule_df.iloc[0]['start_date']
    local_end_date = schedule_df.iloc[0]['end_date']
    time_series = pd.date_range(start = local_start_date, end = local_end_date + dt.timedelta(hours = 23) , freq='H', tz = 'EET')
    
    df = pd.DataFrame(time_series, columns = ['utc'])
    df['weekday'] = df['utc'].apply(lambda x: x.strftime('%A'))
    df.set_index('utc', inplace = True)
    df.index = df.index.tz_convert('UTC').tz_localize(None)
    df.reset_index(inplace = True)    
    df = df.merge(schedule_df, on = 'weekday', how = 'left' )
    df.drop(columns = ['start_date','end_date','weekday','hour'], inplace = True)
    df['itn'] =itn
    
    df['reported_vol'] = -1
    df = df[['itn','utc','forecasted_vol','reported_vol']]
    return df


def generate_subcontract(row, curr_contract, df):

    
    activation_date = convert_date_to_utc("Europe/Sofia",row['activation_date']).replace(tzinfo=None)
    curr_sub_contract =  SubContract.query.filter(SubContract.itn == row['itn'], \
                                                        SubContract.start_date <= activation_date, \
                                                        SubContract.end_date >= activation_date).all()
    if len(curr_sub_contract) == 0:
        
        forecasted_vol = None

        if get_measuring_type(row['measuring_type']).code in ['DIRECT','UNDIRECT']:
            forcasted_df = df.get(row['itn']) if df.get(row['itn']) is not None else df.get('all')
            forecasted_vol = upload_forecasted_schedule_to_temp_db(forcasted_df, row['itn'], row['price'], row['activation_date'], curr_contract)
            ##g.forcasted_schedule = df[row['itn']]
        else:
            if row['forecast_montly_consumption'] is None:
                flash(f'No forcasted volume provided or measuring type mismatch for ITN {row.itn}. Zerro will be inserted !','danger')
                forecasted_vol = Decimal(str('0'))
            else:
                forecasted_vol = Decimal(str(row['forecast_montly_consumption']))

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
                                    forecast_vol = forecasted_vol)
        
    elif len(curr_sub_contract) > 1:
        flash(f'Error ! Overlaping subcontracts with itn {itn} and activation date {activation_date}','error')
    else:
        pass
    return curr_sub_contract

def convert_datetime64_to_datetime(dt_obj):

    if not isinstance(dt_obj, np.datetime64):
        print(f'NOT NP,DATETIME, actual is :{type(dt_obj)}', file = sys.stdout)
        return None
    return dt.datetime.strptime(np.datetime_as_string(dt_obj,unit='s'), '%Y-%m-%dT%H:%M:%S')

def generate_utc_time_series(start_date, end_date, tz = "Europe/Sofia"):

    start_date_utc = convert_date_to_utc(tz, start_date)
    end_date_utc = convert_date_to_utc(tz, end_date) + dt.timedelta(hours = 23)
    time_series = pd.date_range(start = start_date_utc, end = end_date_utc, freq='H', \
                                    tz = tz).tz_convert('UTC').tz_localize(None)

    return time_series

# def create_subcontract_df(itn,activation_date, internal_id, measuring_type, invoice_group, price, zko, 
#                                 akciz, has_grid_services, has_spot_price,  
#                                 object_name, is_virtual,virtual_parent_itn, forecast_montly_consumption,has_balancing):
    
#     df = pd.DataFrame([{'itn':itn,'activation_date':activation_date, 'internal_id':internal_id, 'measuring_type':measuring_type, 
#                         'invoice_group':invoice_group, 'price':price, 'zko':zko,'akciz':akciz, 'has_grid_services':has_grid_services,
#                         'has_spot_price':has_spot_price, 'object_name':object_name, 
#                         'forecast_montly_consumption':forecast_montly_consumption,'has_balancing':has_balancing}])
#     return df
    
def check_and_load_hourly_schedule(form):


    # print(f'in check_and_load_hourly_schedule:  {form.measuring_type.data.code}', file=sys.stdout)
    if form.measuring_type.data.code in ['DIRECT','UNDIRECT']:
        df = pd.read_excel(request.files.get('file_'), sheet_name=None)

        if df.get(form.itn.data.itn) is not None and set(df.get(form.itn.data.itn)).issubset(['date','forecasted_volume']):

        # if set(df.get(row['itn'])).issubset(['date','forecasted_volume']):
            # forecasted_vol = Decimal(str(df['forecasted_volume'].sum()))
            # g.forcasted_schedule = df
            forecasted_vol = upload_forecasted_schedule_to_temp_db(df[form.itn.data.itn], form.itn.data.itn, form.price.data)
        else:
            flash('Wrong file format for forcasted volume upload','danger') 
            return redirect(url_for('create_subcontract'))      
    elif form.forecast_vol.data == '':
        flash('No forcasted volume provided or measuring type mismatch.','danger')
        return redirect(url_for('create_subcontract')) 
    else:
        forecasted_vol = Decimal(str(form.forecast_vol.data))
        flash(f'forcasted volume = {forecasted_vol}','info')
    return forecasted_vol

def get_remaining_forecat_schedule(itn, new_subcontract_end_date, old_subcontract_end_date):
    remaining_schedule = ItnSchedule.query \
                                    .filter(ItnSchedule.itn == itn, ItnSchedule.utc > new_subcontract_end_date, ItnSchedule.utc <= old_subcontract_end_date) \
                                    .all()
    list_of_dict = []
    for schedule in remaining_schedule: 
                            
                list_of_dict.append(dict(itn = schedule.itn, 
                                utc = schedule.utc,                                                      
                                forecast_vol = schedule.forecast_vol,
                                reported_vol = schedule.reported_vol,
                                price = schedule.price))
    print('delete temp table from get_remaining_forecat_schedule', file = sys.stdout)
    ItnScheduleTemp.query.delete()
    db.session.commit()
    db.session.bulk_insert_mappings(ItnScheduleTemp, list_of_dict)
    print('bulk_insert_mappings temp table from get_remaining_forecat_schedule', file = sys.stdout)
    # g.pop('remaining_schedule_list_of_dict', None)                          
    # print(f'g.remaining_schedule_list_of_dict loaded ---> {list_of_dict[0]}')
    # print(f'g.remaining_schedule_list_of_dict loaded last---> {list_of_dict[len(list_of_dict) - 1]}')
    # g.remaining_schedule_list_of_dict = list_of_dict
    
    return remaining_schedule

def apply_collision_function(new_subcontract, old_subcontract, form):

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
        remaining_schedule = get_remaining_forecat_schedule(new_subcontract.itn, new_subcontract.end_date, old_subcontract.end_date)
        
        forecasted_vol = check_and_load_hourly_schedule(form)  
        print(f'ADD_start_date = {new_subcontract.end_date + dt.timedelta(hours = 1)} ----- ADD_end_date = {old_subcontract.end_date}', file = sys.stdout)            
        additional_sub_contract = SubContract(itn = old_subcontract.itn,
                            contract_id = old_subcontract.contract_id, \
                            object_name = old_subcontract.object_name,\
                            price = old_subcontract.price, \
                            invoice_group_id = old_subcontract.invoice_group_id, \
                            measuring_type_id = old_subcontract.measuring_type_id, \
                            start_date = new_subcontract.end_date + dt.timedelta(hours = 1) ,\
                            end_date =  old_subcontract.end_date, \
                            zko = old_subcontract.zko, \
                            akciz = old_subcontract.akciz, \
                            has_grid_services = old_subcontract.has_grid_services, \
                            has_spot_price = old_subcontract.has_spot_price, \
                            has_balancing = old_subcontract.has_balancing, \
                            forecast_vol = forecasted_vol)
        print(additional_sub_contract, file = sys.stdout)
          
        new_is_inner(new_subcontract,old_subcontract)     
        additional_sub_contract.save()        
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

def old_is_left_wing(new_subcontract,old_subcontract):
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})

def old_is_right_wing(new_subcontract,old_subcontract):
    old_subcontract.update({'start_date':new_subcontract.end_date + dt.timedelta(hours = 1)})

def new_is_inner(new_subcontract,old_subcontract):
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})

def new_is_left_inner(new_subcontract,old_subcontract):
    print(f'from new_is_left_inner, old_subcontract_start_date = {old_subcontract.start_date} <------>  old_subcontract_end_date = {old_subcontract.end_date}')
    old_subcontract.update({'start_date':new_subcontract.end_date + dt.timedelta(hours = 1)})

def new_is_right_inner(new_subcontract,old_subcontract):
    old_subcontract.update({'end_date':new_subcontract.start_date - dt.timedelta(hours = 1)})




    



                   
