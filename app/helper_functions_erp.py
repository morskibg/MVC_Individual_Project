import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash
from app.models import *    
from app.helper_functions import update_or_insert, stringifyer, convert_date_to_utc
import collections
from zipfile import ZipFile
from io import BytesIO

from sqlalchemy.exc import ProgrammingError


def replace_char(df, bad_char, good_char):
    df = df.applymap(lambda x: x.replace(bad_char,good_char) if(isinstance(x,str) and (bad_char in x)) else x)
    return df
                      
def get_invoice_data(df, file_name):  

    # print('in get_invoice_data', file = sys.stdout)
    # df = pd.read_fwf(zip_obj.open(file_name), encoding="cp1251", engine='python',nrows=0)
    df = df.columns.str.extract(r'(\d+)[^0-9]+([\d.]+)').dropna()
    
    if df.empty:       
        raw_data = file_name.rsplit('_',1)[1].rsplit('.',1)
        match = re.search(r'\d+', raw_data[0])
        date_time_obj = dt.datetime.strptime(match.group(0), '%Y%m%d')
#         date_time_obj = dt.datetime.strptime(raw_data[0], '%Y%m%d')         
        return (raw_data[0],date_time_obj)

    else:
        if('.' in df[1].iloc[0]):
            date_time_obj = dt.datetime.strptime(str(df[1].iloc[0]), '%d.%m.%Y')         
            return (df[0].iloc[0],date_time_obj)
        else:
            date_time_obj = dt.datetime.strptime(str(df[1].iloc[0]), '%Y%m%d')         
            return (df[0].iloc[0],date_time_obj)                                            

def insert_erp_invoice(df):

    erp_inv_df = df[['number','date','event','correction_note']].copy()
    erp_inv_df = erp_inv_df.fillna('')
    erp_inv_df.drop_duplicates(subset = ['number','correction_note','event','date'], keep = 'first', inplace = True)
    erp_inv_df.reset_index(inplace = True, drop = True)
    # erp_inv_df['correction_note'] = erp_inv_df['correction_note'].apply(lambda x: 0 if x == 0.0 else x)
    erp_inv_df['date'] =erp_inv_df['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    print(f'erp_inv_df \n {erp_inv_df}')
    erp_inv_df['composite_key'] = erp_inv_df['correction_note'].apply(str) + erp_inv_df['event'].apply(str) + erp_inv_df['number'].apply(str) + erp_inv_df['date']
    erp_inv_df = erp_inv_df.fillna('')

    # print(f'AAAAAAAAAAAAAAAAAAAAAA   erp_inv_df to DB AAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n{erp_inv_df}')
    # print(f'BBBBBBBBBBBBBBBBBBBBBBBBB   has NULL BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\n{erp_inv_df.isnull().any().any()}')
    
    update_or_insert(erp_inv_df, ErpInvoice.__table__.name)

    return erp_inv_df

def reader_csv(df, file_name):   
    
        df.columns = df.columns.str.replace('"','')
        

        df = replace_char(df,'"','')
        df = replace_char(df,',','.')

        col_names = ['erp_code','1','6','content','subscriber_number','10','7','customer_number','8','9','itn','electric_meter_number','start_date',
                     'end_date','4','scale_number','scale_code','scale_type','time_zone','new_readings','old_readings','readings_difference','constant','correction','storno',
                     'total_amount','tariff','calc_amount','price','value','correction_note','event']
        df.columns = col_names
        cols_to_drop = ['1','4','6','7','8','9','10','erp_code']
        df = df.drop(cols_to_drop, axis = 1)
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
         
        # print(f'FROM CSV READER \n{df.dtypes}')
        df['start_date'] = pd.to_datetime(df['start_date'], format = '%d.%m.%Y')
        df['end_date'] = pd.to_datetime(df['end_date'], format = '%d.%m.%Y')

        df = df.fillna(0)

        df['calc_amount'] = df['calc_amount'].apply(Decimal)
        df['price'] = df['price'].apply(Decimal)
        df['value'] = df['value'].apply(Decimal)

        df['new_readings'] = df['new_readings'].apply(Decimal)
        df['old_readings'] = df['old_readings'].apply(Decimal)
        df['readings_difference']= df['readings_difference'].apply(Decimal)
        df['storno'] = df['storno'].apply(Decimal)
        df['total_amount'] = df['total_amount'].apply(Decimal)
       
        inv_data = get_invoice_data(df, file_name)
       
        df['number'] = pd.Series(inv_data[0], index=df.index)
        df['date'] = pd.Series(inv_data[1], index=df.index)
        df['correction_note'] = df['correction_note'].apply(lambda x: 0 if x == 0.0 else x)
        return df  

def get_tech_point(df, erp_invoice = None):   
    
    try:

        df = df[df['content'] == 'Техническа част'].copy()
        
        df['old_readings'] = df.apply(lambda x: x['new_readings'] - x['readings_difference'] if ((x['old_readings'] == Decimal('0')) & \
                                                (~pd.isnull(x['new_readings'])) & (~pd.isnull(x['readings_difference']))) else Decimal('0'), axis = 1)
        
        cols_to_drop = ['content','tariff','calc_amount','price','value','event','correction_note']    
        df = df.drop(cols_to_drop, axis = 1)
        df.drop_duplicates(subset=['itn', 'start_date','new_readings','total_amount'],keep='first',inplace = True) 
        # df['scale_code'] = df['scale_code'].apply(lambda x: x.str.strip() if x is not None else x)
        # df = df.fillna(0)
        if(erp_invoice is not None):
            erp_invoice = erp_invoice[erp_invoice['event'] == '']
            df = df.merge(erp_invoice, on = 'number', how = 'left')
            df.drop(columns = ['date_x', 'date_y', 'event', 'correction_note','composite_key','number'], inplace = True)
            df.rename(columns = {'id':'erp_invoice_id'}, inplace = True)
        
    except Exception as e: 
        print(e)
    else:
        # print(f'from tech ----> df: \n {df}')
        # print(f'df tech has null ---- > {df.erp_invoice_id.isnull().values.any()}')
        return df

def get_distrib_point(df, erp_invoice_df = None):   

    try:
    
        df = df[df['content'] == 'Разпределение']

        cols_to_drop = ['content','subscriber_number','customer_number','electric_meter_number','scale_number','scale_code','scale_type','time_zone','new_readings',
                        'old_readings','readings_difference','constant','correction','storno','total_amount']
        df = df.drop(cols_to_drop, axis = 1)

        df.drop_duplicates(subset=['itn', 'start_date','end_date','price','value'],keep='first',inplace = True)
        # df = df.fillna(0)
        # print(f'from get_distrib_point before if  ------> df : \n{df}')
        if(erp_invoice_df is not None):
        
            df['composite_key'] = df['correction_note'].apply(str) + df['event'].apply(str) + df['number'].apply(str) + df['date'].apply(str)
            # print(f'COMPOSITE KEY -----------------> {df.composite_key}')
            # print(f'correction_note KEY -----------------> {df.correction_note}')

            
            df = df.merge(erp_invoice_df, on = 'composite_key', how = 'left')
            # print(df, file = sys.stdout)
            df.drop(columns = ['correction_note_y','event_x', 'number_x', 'date_x', 'number_y', 'date_y', 'event_y','correction_note_x','composite_key'], inplace = True)
            df.rename(columns = {'id':'erp_invoice_id'}, inplace = True)
            # erp_invoice_df.to_excel('erp_invoice_df.xlsx')
            

    except Exception as e: 
        print(e)
    else:
        # print(f'from distr ----> df: \n {df}')
        return df     

def insert_mrus(raw_df, file_name):  
            
    input_df = reader_csv(raw_df, file_name)
    
    input_df['date'] = input_df['date'].apply(lambda x: convert_date_to_utc('EET', x))
    # print(f'input df insert erp invoice \n{input_df}')
    insert_erp_invoice(input_df)      


    erp_invoice_df =  pd.read_sql(ErpInvoice.query.statement, db.session.bind)   

    print(f'BBBBBBBBBBBBBBBB {erp_invoice_df}')
    tech_tbl = pd.DataFrame()
    distr_tbl = pd.DataFrame()

    tech_point = get_tech_point(input_df, erp_invoice_df)
    distrib_point = get_distrib_point(input_df, erp_invoice_df)

    try:
        if((distr_tbl.empty) & (distrib_point is not None)):           
            distr_tbl = distrib_point

        elif distrib_point is not None:           
            distr_tbl = distr_tbl.append(distrib_point, ignore_index=True)

        else:
            print(f'empty distribution point')

    except Exception as e:
        print(f'distribution is None {e}')

    try:
        if((tech_tbl.empty) & (tech_point is not None)):
            tech_tbl = tech_point  
                        
        elif tech_point is not None:           
            tech_tbl = tech_tbl.append(tech_point, ignore_index=True) 

        else:
            print(f'empty tech point')

    except Exception as e:
        print(f'tech is None {e}')

    if distr_tbl.empty:
        print(f'distrib table is empty')   

    else:
        try:
            have_all_itns_meta(distr_tbl['itn'].values)
            distr_tbl = distr_tbl.replace(np.nan,0)
            # print(f'distrib_tbl to DB \n{distr_tbl}')
            update_or_insert(distr_tbl, Distribution.__table__.name)
        except Exception as e:
            print(f'Exception from writing distribution to DB, with message: {e}')

    if tech_tbl.empty:
        print(f'tech table is empty') 

    else:
        try:       
            have_all_itns_meta(tech_tbl['itn'].values)
            # print(f'tech_tbl to DB \n{tech_tbl}')
            tech_tbl = tech_tbl.replace(np.nan,0)
            update_or_insert(tech_tbl, Tech.__table__.name)
        except Exception as e:
            print(f'Exception from writing tech to DB, with message: {e}') 

def insert_settlment_cez(zip_obj,separator): 

    PASSWORD = 'XY3R9TAPAB4BZEKDTU9L'
    ENCODING = 'utf-8'

    # ordered_dict = order_files_by_date(zip_obj)
    ordered_dict = order_files_by_size(zip_obj)
    db_stp_records = 0
    # print(ordered_dict, file = sys.stdout)
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
            
            print(f'in ZIP $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ {file_name}')           

            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)

            dfs_csv_dict = {text_file.filename: pd.read_csv(inner_zip.open(text_file.filename,pwd=bytes(PASSWORD, ENCODING)),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.csv')}
           
            for key in dfs_csv_dict.keys():
                try:
                    df = dfs_csv_dict[key]

                except Exception as e:
                    print(f'File {key} was NOT proceeded .Exception message: {e}!') 

                else:
                    insert_mrus(df, key)            

        elif file_name.endswith('.xlsx'):
            
            print(f'in XLXS $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ {file_name}')            
            try:
                df = pd.read_excel(zip_obj.read(file_name))
                initial_rows_count = df.shape[0]
                # df.columns = df.columns.str.strip()
                
                df.drop(['DD.MM.YYYY hh:mm','Име на Клиент, ЕСО:','Сетълмент период:'], axis=1, inplace = True)
                
                df_cols = df.columns[1:]
                # print(f'cols ------> {df_cols}')
                df_cols = [x.replace('.','/') if(isinstance(x,str) and ('.' in x)) else x for x in df_cols]
                
                s_date = df_cols[0] if isinstance(df_cols[0], dt.date) else dt.datetime.strptime(df_cols[0], '%d/%m/%Y %H:%M')
                e_date = df_cols[-1] if isinstance(df_cols[-1], dt.date) else dt.datetime.strptime(df_cols[-1], '%d/%m/%Y %H:%M')
                # print(f's date, e date {s_date- dt.timedelta(hours = 1)} --- {e_date- dt.timedelta(hours =1)}')
                time_series = pd.date_range(start = s_date - dt.timedelta(hours = 1), end = e_date - dt.timedelta(hours =1), tz = 'EET', freq='h')
                # print(f'Time series {time_series}')
                df.columns = time_series.insert(0,df.columns[0])
                
                df = pd.melt(df, id_vars=['Уникален Идентификационен Номер:'], var_name = ['utc'], value_name = 'consumption_vol')
                df.rename(columns={'Уникален Идентификационен Номер:': 'itn'}, inplace = True)
                df.set_index(pd.DatetimeIndex(df['utc']), inplace = True)
                df.drop(columns= 'utc', inplace = True)

            except Exception as e:
                print(f'File {file_name} was NOT proceeded .Exception message: {e}!')  

            else: 
                try:
                    df.index = df.index.tz_convert('UTC').tz_convert(None)

                except Exception as e:
                    print(f'Exception from cez hourly loading: {e}')

                else:
                    if(not df.empty):
                        df.reset_index(inplace = True)
                        df = df.dropna()
                    
                        min_date = min(df['utc']).to_pydatetime()                    
                        max_date = max(df['utc']).to_pydatetime() 

                        incoming_stp_records = get_incoming_stp_records(df,min_date, max_date)

                        if (len(incoming_stp_records) > 0) & (len(incoming_stp_records) >= (initial_rows_count // 2)):
                            
                            if db_stp_records == 0:
                                db_stp_records = get_stp_from_db('CEZ', min_date, max_date)                              
                                get_missing_points(incoming_stp_records, db_stp_records)
       
                            get_extra_points(incoming_stp_records, db_stp_records)

                            stp_records_df = pd.DataFrame.from_records(incoming_stp_records, columns=incoming_stp_records[0].keys()) 
                                             
                            update_stp_settelment_vol(df, stp_records_df, incoming_stp_records) 
                            update_stp_consumption_vol(stp_records_df, min_date, max_date)  

                        else:
                            db_non_stp_records = get_non_stp_from_db('CEZ', min_date, max_date)
                            incoming_non_stp_records = get_incoming_non_stp_records(df,min_date, max_date)
                            get_missing_points(incoming_non_stp_records, db_non_stp_records)
                            get_extra_points(incoming_non_stp_records, db_non_stp_records)

                            update_non_stp_consumption_settelment_vol(df, min_date, max_date)                   
 
def get_missing_points(incoming_records, db_records):

    incoming_records = set([x[0] for x in incoming_records])
    db_records = set([x[0] for x in db_records])
    res = db_records - incoming_records
    print(f'missing incomng data points ---> {res}')

    return res

def get_extra_points(incoming_records, db_records):

    incoming_records = set([x[0] for x in incoming_records])
    db_records = set([x[0] for x in db_records])
    res = incoming_records - db_records
    print(f'extra incoming data ---> {res}')

    return res

def get_distribution_stp(erp_name ,start_date, end_date):

    start = time.time()
    distribution_stp_records = (
        db.session 
            .query(Distribution.itn)   
            .join(SubContract, SubContract.itn == Distribution.itn)                       
            .join(MeasuringType)
            .join(ItnMeta, ItnMeta.itn == Distribution.itn)    
            .join(Erp, Erp.id == ItnMeta.erp_id)         
            .join(ErpInvoice,ErpInvoice.id == Distribution.erp_invoice_id)
            .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ErpInvoice.date >= start_date, ErpInvoice.date <= end_date)
            .filter(Erp.name == erp_name)
            .distinct()
            .all()
    )
    
    end = time.time()
    print(f'Time elapsed for get_distribution_stp : {end - start}  !')
    return  distribution_stp_records

def get_distribution_non_stp(erp_name ,start_date, end_date):

    start = time.time()
    distribution_non_stp_records = (
        db.session 
            .query(Distribution.itn)   
            .join(SubContract, SubContract.itn == Distribution.itn)                       
            .join(MeasuringType)
            .join(ItnMeta, ItnMeta.itn == Distribution.itn)    
            .join(Erp, Erp.id == ItnMeta.erp_id)         
            .join(ErpInvoice,ErpInvoice.id == Distribution.erp_invoice_id)
            .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
            .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ErpInvoice.date >= start_date, ErpInvoice.date <= end_date)
            .filter(Erp.name == erp_name)
            .distinct()
            .all()
    )
    
    end = time.time()
    print(f'Time elapsed for get_distribution_non_stp : {end - start}  !')
    return  distribution_non_stp_records

def get_distribution_stp_records(erp_name ,start_date, end_date):

    start = time.time()
    distribution_stp_records = (
        db.session 
            .query(Distribution.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id'))   
            .join(SubContract, SubContract.itn == Distribution.itn)                       
            .join(MeasuringType)
            .join(ItnMeta, ItnMeta.itn == Distribution.itn)  
            .join(ItnSchedule, ItnSchedule.itn == Distribution.itn) 
            .join(Erp, Erp.id == ItnMeta.erp_id)         
            .join(ErpInvoice,ErpInvoice.id == Distribution.erp_invoice_id)
            .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ErpInvoice.date >= start_date, ErpInvoice.date <= end_date)
            .filter(Erp.name == erp_name)
            .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)
            .distinct()
            .all()
    )
    
    end = time.time()
    print(f'Time elapsed for get_distribution_stp : {end - start}  !')
    return  distribution_stp_records

def get_incoming_stp_records(input_df,min_date, max_date):   
    start = time.time()
    stp_records = (
        db.session 
            .query(SubContract.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id')) 
            .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
            .join(MeasuringType) 
            .filter(SubContract.itn.in_(input_df.itn),SubContract.start_date <= min_date, SubContract.end_date >= max_date) 
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date) 
            .distinct()
            .all()
    )
    
    end = time.time()
    print(f'Time elapsed for create stp_records : {end - start}  !')
    return  stp_records

def get_incoming_non_stp_records(input_df,min_date, max_date):   
    start = time.time()
    stp_records = (
        db.session 
            .query(SubContract.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id')) 
            .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
            .join(MeasuringType) 
            .filter(SubContract.itn.in_(input_df.itn),SubContract.start_date <= min_date, SubContract.end_date >= max_date) 
            .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date) 
            .distinct()
            .all()
    )
    
    end = time.time()
    print(f'Time elapsed for create stp_records : {end - start}  !')
    return  stp_records


def resolve_poins_colision(input_df, min_date, max_date):
    
    colision_records = (
        db.session 
            .query(SubContract.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id')) 
            .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
            .join(MeasuringType) 
            .filter(SubContract.itn.in_(input_df.itn),SubContract.start_date <= min_date, SubContract.end_date >= max_date) 
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT')))             
            .filter(ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date) 
            .distinct()
            .all()
    )
    if len(colision_records) ==  0:
        return []
    colision_point_non_stp_df = pd.DataFrame.from_records(colision_records, columns=colision_records[0].keys())

    colision_points = [x[0] for x in colision_records]
    print(f'from col {colision_points}')

    for colision_point in colision_points:
        
        temp_input_df = input_df[input_df['itn'] == colision_point]

        if colision_point_non_stp_df.empty:
            continue
        
        temp_input_df = temp_input_df.merge(colision_point_non_stp_df, on = 'itn', how = 'left')

        temp_input_df['settelment_vol'] = temp_input_df['consumption_vol']
        temp_input_df.drop(columns = 'measuring_id', inplace = True)
         
        print(f'from collision df \n{temp_input_df}')
        update_reported_volume(temp_input_df, ItnSchedule.__table__.name)

    return colision_points

    
def update_non_stp_consumption_settelment_vol(input_df, min_date, max_date):

    colision_points = resolve_poins_colision(input_df, min_date, max_date)

    if len(colision_points) > 0:

        non_stp_records = (
            db.session 
                .query(SubContract.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id')) 
                .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
                .join(MeasuringType)
                .filter(~SubContract.itn.in_(colision_points)) 
                .filter(SubContract.itn.in_(input_df.itn),SubContract.start_date <= min_date, SubContract.end_date >= max_date) 
                .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
                .filter(ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date) 
                .distinct()
                .all()
        ) 
    else:
        non_stp_records = (
            db.session 
                .query(SubContract.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id')) 
                .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
                .join(MeasuringType)                
                .filter(SubContract.itn.in_(input_df.itn),SubContract.start_date <= min_date, SubContract.end_date >= max_date) 
                .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
                .filter(ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date) 
                .distinct()
                .all()
        ) 
           
    
    if len(non_stp_records) > 0:

        non_stp_records_df = pd.DataFrame.from_records(non_stp_records, columns=non_stp_records[0].keys()) 
        non_stp_df = input_df.merge(non_stp_records_df, on = 'itn', how = 'right') 
        non_stp_df['settelment_vol'] = non_stp_df['consumption_vol']
        non_stp_df.drop(columns = 'measuring_id', inplace = True)
        print(f'Non STP df = \n {non_stp_df}')
        update_reported_volume(non_stp_df, ItnSchedule.__table__.name)

def update_stp_settelment_vol(input_df, stp_records_df, stp_records):
                         
    stp_df = input_df[input_df['itn'].isin([x[0] for x in stp_records])]
    stp_df = stp_df.merge(stp_records_df, on = 'itn', how = 'left') 
    stp_df.rename(columns = {'consumption_vol':'settelment_vol'}, inplace = True)  
    stp_df.drop(columns = 'measuring_id', inplace = True)  
    # print(f' FROM update stp settelment volume. stp_df = \n {stp_df}')                  
    update_reported_volume(stp_df, ItnSchedule.__table__.name)

def update_stp_consumption_vol(stp_records_df, min_date, max_date, is_settelment_the_same= False):
    # print(f'@@@@@@@@@@@@@@@@@@@ TOTAL CONSUMPTION DF @@@@@@@@@@@@@@@@@@ \n{stp_records_df}')


    # invoice_start_date = min_date.replace(min_date.year, min_date.month, 11,0,0,0)
    # invoice_end_date = max_date.replace(max_date.year, max_date.month + 1, 10,23,0,0)

    invoice_start_date = min_date + dt.timedelta(hours = (10 * 24 + 1))
    invoice_end_date = max_date + dt.timedelta(hours = (10 * 24))

    total_consumption_records = (
        db.session
            .query(Distribution.itn.label('itn'), 
                func.sum(Distribution.calc_amount).label('total_consumption')) 
            
            .join(ErpInvoice, ErpInvoice.id == Distribution.erp_invoice_id)   
            .filter(Distribution.itn.in_(stp_records_df['itn']))      
            .filter(Distribution.tariff.in_(['Достъп','Пренос през електропреносната мрежа', 'Разпределение'])) 
            .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date) 
            .group_by(Distribution.itn)
            .all()
    )
    
    if len(total_consumption_records) > 0:
        total_consumption_df = pd.DataFrame.from_records(total_consumption_records, columns=total_consumption_records[0].keys())
        print(f'@@@@@@@@@@@@@@@@@@@ TOTAL CONSUMPTION DF @@@@@@@@@@@@@@@@@@ \n {total_consumption_df}')
        total_consumption_df = total_consumption_df.merge(stp_records_df, on = 'itn', how = 'right')
        
        missing_points = total_consumption_df[total_consumption_df['total_consumption'].isnull()]['itn']   
        
        total_consumption_df['total_consumption'] = total_consumption_df['total_consumption'].apply(lambda x: Decimal('0') if pd.isnull(x) else x)
        print(f'Missing point from input CSV files regard input settelment file \n{missing_points}')

        stp_coeffs_records = (
            db.session.query(StpCoeffs.utc, StpCoeffs.value.label('stp_coeff'), StpCoeffs.measuring_type_id.label('measuring_id'))            
                .filter(StpCoeffs.utc >= min_date, StpCoeffs.utc <= max_date)
                .all()
        ) 
        
        if len(stp_coeffs_records) > 0:
            stp_coeffs_df = pd.DataFrame.from_records(stp_coeffs_records, columns=stp_coeffs_records[0].keys())        
            stp_df =  total_consumption_df.merge(stp_coeffs_df, on = 'measuring_id', how = 'left')
            # print(f'update consumption \n {stp_df}')
            stp_df.fillna(0)
            stp_df['consumption_vol'] = stp_df['total_consumption'] * stp_df['stp_coeff']
            
            # stp_df = stp_df[['itn', 'utc', 'consumption_vol','tariff_id']]
            # print(f' FROM update stp consumption volume. stp_df = \n {stp_df}')
            if is_settelment_the_same:
                stp_df['settelment_vol'] = stp_df['consumption_vol']
            # a = stp_df[stp_df['itn'] == 'BG5521900615500000000000002008098']
            print(f'dates ---> {invoice_start_date}   {invoice_end_date}')
            print(f'@@@@@@@@@@@@@@@@@@@ UPDATE DF @@@@@@@@@@@@@@@@@@ \n {stp_df}')
            update_reported_volume(stp_df, ItnSchedule.__table__.name)
            

    #     else:
    #         print(f'stp coefs records is 0')
    # else:
    #     print(f'stp total consumption is 0')

def insert_settlment_e_pro(zip_obj, separator):

    ordered_dict = order_files_by_date(zip_obj)
    
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
            # continue
            print(file_name, file = sys.stdout)
            
            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)
         
            dfs = {text_file.filename: pd.read_excel(inner_zip.read(text_file.filename))
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.xlsx')}
            incomming_points = []
            for key in dfs.keys():               
                
                # try:                     
                df = dfs[key]
                df.columns = df.columns.str.strip()
                client_name = [x for x in df.columns if(x.find('Unnamed:') == -1)][0]
                itn = df.iloc[:1][client_name].values[0].split(': ')[1]                       
                
                df = df.rename(columns={client_name:'1','Unnamed: 1':'2', 'Unnamed: 2':'3'})        
                
                df_for_db = create_db_df_eepro_evn(df, itn)

                min_date = min(df_for_db['utc']).to_pydatetime()                    
                max_date = max(df_for_db['utc']).to_pydatetime()
                
                if(not df_for_db.empty):

                    incoming_non_stp_records = get_incoming_non_stp_records(df_for_db,min_date, max_date)
                    incomming_points.append(incoming_non_stp_records)
                    # print(f'e pro \n{df_for_db}')
                    update_non_stp_consumption_settelment_vol(df_for_db, min_date, max_date)
                    
                else:
                    print('Values in file ', key, ' was only 0 !')

                # except Exception as e: 
                #     print(f'File {key} was NOT proceeded .Exception message: {e}!')
            
            
        elif file_name.endswith('.csv'):

            try:
                print(f'E PRO csv reading {file_name}')
                df = pd.read_csv(zip_obj.open(file_name),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)

            except Exception as e:
                print(f'File {file_name} was NOT proceeded .Exception message: {e}!') 

            else:
                insert_mrus(df, file_name) 
                print(f'from e pro csv reading ----- >\n{df}')

        elif file_name == '070CIN03.xlsx':
            proceed_e_pro_stp_excel_file(zip_obj, file_name)

    print(f'FROM NON stp E-Pro')
    
    incomming_points = [x[0] for x in incomming_points if len(x) > 0]
    # print(f'INCOMING POINTS -----------------------> \n{incomming_points}')
    db_non_stp_records = get_non_stp_from_db('E-PRO', min_date, max_date)
    get_missing_points(incomming_points, db_non_stp_records)
    get_extra_points(incomming_points, db_non_stp_records)
                    
def proceed_e_pro_stp_excel_file(zip_obj, file_name):
    
    summary_df = pd.DataFrame()
    file_obj = [x for x in zip_obj.infolist() if x.filename == file_name][0]   
    df = pd.read_excel(zip_obj.read(file_obj.filename), sheet_name = None)  
    s_date = e_date = None
    for key in df.keys():
        curr_df = df[key]
        if curr_df.empty:
            continue
        # print(f' KEY ---> {key}')
        curr_df.drop(curr_df.columns[len(curr_df.columns)-1], axis=1, inplace=True)
        curr_df.columns = [x.strip() for x in curr_df.columns]
        curr_df.drop(curr_df.tail(1).index,inplace=True)
        curr_df = curr_df.fillna(0)
        curr_df.drop(['dd.MM.YYYY HH:mm','Име на клиент'], axis=1, inplace = True)
        
        df_cols = curr_df.columns[1:]
        df_cols = [x.replace('.','/') if(isinstance(x,str) and ('.' in x)) else x for x in df_cols]
        s_date = df_cols[0] if isinstance(df_cols[0], dt.date) else dt.datetime.strptime(df_cols[0], '%d/%m/%Y %H:%M')
        e_date = df_cols[-1] if isinstance(df_cols[-1], dt.date) else dt.datetime.strptime(df_cols[-1], '%d/%m/%Y %H:%M')
        # print(f'from e_pro stp \n{df_cols[0]} \n{df_cols[-2]}')
        time_series = pd.date_range(start = s_date - dt.timedelta(hours =1), end = e_date - dt.timedelta(hours =1), tz = 'EET', freq='h')
        # print(f'from e_pro stp \n{time_series}')
        curr_df.columns = time_series.insert(0,curr_df.columns[0])
        
        curr_df = pd.melt(curr_df, id_vars=['Уникален номер'], var_name = ['utc'], value_name = 'settelment_vol')
        curr_df.rename(columns={'Уникален Идентификационен Номер:': 'itn'}, inplace = True)
        curr_df.set_index(pd.DatetimeIndex(curr_df['utc']), inplace = True)
        curr_df.drop(columns= 'utc', inplace = True)
        try:
            curr_df.index = curr_df.index.tz_convert('UTC').tz_convert(None)
            s_date = convert_date_to_utc('EET', s_date)
            e_date = convert_date_to_utc('EET', e_date)
            # print(f'dates {s_date} --- {e_date}')

        except Exception as e:
                print(f'Exception from e-pro stp loading: {e}')

        else:
            if(not curr_df.empty):
                curr_df.reset_index(inplace = True)
                if summary_df.empty:
                    summary_df = curr_df
                else:
                    summary_df = summary_df.append(curr_df, ignore_index=True)
                    
    if summary_df.empty:
        print(f'empty file for e-pro stp')

    else:
        summary_df.rename(columns = {'Уникален номер':'itn'}, inplace = True)
        # stp_records = incoming_stp_records(summary_df, s_date, e_date)
        incoming_stp_records = get_incoming_stp_records(summary_df,s_date, e_date)
                        
        if len(incoming_stp_records) > 0:  
            print(f'FROM stp E-Pro')                      
            stp_records_df = pd.DataFrame.from_records(incoming_stp_records, columns=incoming_stp_records[0].keys()) 
            db_stp_records = get_stp_from_db('E-PRO', s_date, e_date)                              
            get_missing_points(incoming_stp_records, db_stp_records)
            get_extra_points(incoming_stp_records, db_stp_records)
            # a = summary_df[summary_df.itn == '32Z4800110120529']
            # print(f'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ stp_records_df  \n{a}')

            update_stp_settelment_vol(summary_df, stp_records_df, incoming_stp_records)             
            update_stp_consumption_vol(stp_records_df, s_date, e_date)  
                
def insert_settlment_evn(zip_obj,separator):
    
    # PASSWORD = '8yc#*3-Q5ADt'
    PASSWORD = '79+Kg+*rLA7P'
    ENCODING = 'utf-8'
    
    ordered_dict = order_files_by_size(zip_obj)
    distribution_stp_records = 0
    distribution_non_stp_records = 0
    incoming_non_stp_records = []
    # ordered_dict = order_files_by_date(zip_obj)
    print(ordered_dict, file = sys.stdout)
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
            print(file_name, file = sys.stdout)

            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)

            dfs_csv_dict = {text_file.filename: pd.read_csv(inner_zip.open(text_file.filename,pwd=bytes(PASSWORD, ENCODING)),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.csv')}
           
            for key in dfs_csv_dict.keys():
                try:
                    df = dfs_csv_dict[key]

                except Exception as e:
                    print(f'File {key} was NOT proceeded .Exception message: {e}!') 

                else:
                    insert_mrus(df, key)

            dfs_dict = {text_file.filename: pd.read_excel(inner_zip.read(text_file.filename,pwd=bytes(PASSWORD, ENCODING)))
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.xlsx')}
        
            for key in dfs_dict.keys():
                try:
                    df = dfs_dict[key]
                    df.columns = df.columns.str.strip()
                    ITN = df.iloc[:1].values[0][0] 
                    
                    # df = df.rename(columns={'Гранд Енерджи Дистрибюшън ЕООД':'1','Unnamed: 1':'2', 'Unnamed: 2':'3'})
                    df = df.rename(columns={'Юропиан Трейд Оф Енерджи АД':'1','Unnamed: 1':'2', 'Unnamed: 2':'3'})
                    df_for_db = create_db_df_eepro_evn(df, ITN) 
                    # print(f'df for db \n{df_for_db}')                   
                    if(not df_for_db.empty):  
                        min_date = min(df_for_db['utc']).to_pydatetime()                    
                        max_date = max(df_for_db['utc']).to_pydatetime()
                        print(f'min max date {min_date} --- {max_date}')                         
                        
                        if distribution_stp_records == 0:

                            db_stp_records = get_stp_from_db('EVN', min_date, max_date)                            
                            
                            db_non_stp_records = get_non_stp_from_db('EVN', min_date, max_date)
                           
                            # min_date_inv = min_date.replace(min_date.year, min_date.month, 11,0,0,0)
                            # max_date_inv = max_date.replace(max_date.year, max_date.month + 1, 10,23,0,0) 

                            invoice_start_date = min_date + dt.timedelta(hours = (10 * 24 + 1))
                            invoice_end_date = max_date + dt.timedelta(hours = (10 * 24))

                            distribution_stp_records = get_distribution_stp_records('EVN',min_date,max_date)

                            stp_records_df = pd.DataFrame.from_records(distribution_stp_records, columns=distribution_stp_records[0].keys())
                            
                            update_stp_consumption_vol(stp_records_df, min_date, max_date, True)

                            distribution_non_stp_records = get_distribution_non_stp('EVN',invoice_start_date, invoice_end_date)
                            # print(f'stp records df \n{stp_records_df}')

                        if(not df_for_db.empty):
                            
                            incoming_non_stp_records.append(list(zip(set(df_for_db.itn), )))
                            update_non_stp_consumption_settelment_vol(df_for_db, min_date, max_date)
                        
                    else:
                        print('Values in file ', key, ' was only 0 !')
        
                except Exception as e:
                    print(f'Exception from EVN xlsx upload. File {key} was NOT proceeded .Exception message: {e}! \n {df_for_db}') 

    

    incoming_non_stp_records = [x[0] for x in incoming_non_stp_records if len(x) > 0]

    get_missing_points(distribution_stp_records, db_stp_records)  
    get_extra_points(distribution_stp_records, db_stp_records)
    get_missing_points(incoming_non_stp_records, db_non_stp_records)  
    get_extra_points(incoming_non_stp_records, db_non_stp_records)


def create_db_df_eepro_evn(df, ITN):      
    try:
        df = df.fillna(0)
        df = df.iloc[3:]
        # print(f'entering create_db_df_eepro_evn df :\n -------> {df}')
        df['1'] = df['1'].apply(lambda x: x.replace('.','/'))
        
        is_manufacturer = True if df['3'].mean() != 0 else False
        df_for_db= pd.DataFrame(columns=['itn','utc','consumption_vol']) 
        
        df_for_db['utc'] = pd.to_datetime(df['1'], format = '%d/%m/%Y %H:%M')
        df_for_db['itn'] = ITN
        
        df_for_db['consumption_vol'] = df['3'].astype(float) if is_manufacturer else df['2'].astype(float)  
        df_for_db.set_index('utc', inplace = True)
        df_for_db.index = df_for_db.index.shift(periods=-1, freq='h').tz_localize('EET', ambiguous='infer').tz_convert('UTC').tz_convert(None)  

        df_for_db.reset_index(inplace = True)
    except Exception as e:
        print(f'Exception from create_db_df_eepro_evn: {e}')

    # print(f'from create_db_df_eepro_evn df_for_db ITN======{ITN}\n ---->{df_for_db}')   
    return df_for_db 

def get_stp_from_db(erp_name, start_date, end_date):

    start = time.time()
    stp_records = (
        db.session 
            .query(SubContract.itn) 
            .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
            .join(MeasuringType) 
            .join(ItnMeta, ItnMeta.itn == SubContract.itn)
            .join(Erp)
            .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(Erp.name == erp_name)
            .distinct()
            .all()
    )
    
    end = time.time()
    print(f'Time elapsed for get_stp_from_db : {end - start}  !')
    return  stp_records

def get_non_stp_from_db(erp_name, start_date, end_date):

    start = time.time()
    non_stp_records = (
        db.session 
            .query(SubContract.itn) 
            .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
            .join(MeasuringType) 
            .join(ItnMeta, ItnMeta.itn == SubContract.itn)
            .join(Erp)
            .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
            .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(Erp.name == erp_name)
            .distinct()
            .all()
    )
    
    end = time.time()
    print(f'Time elapsed for get_non_stp_from_db : {end - start}  !')
    return  non_stp_records

def get_df_eso_direct(path_csv):

    df = pd.read_csv(path_csv,sep=';',skiprows=2)
    itn = df[df.eq('RefBGCode').any(1)].values[0][1]
    df = df[4:]
    df = df[['Type','Consumption']]
    df = df.applymap(lambda x: x.replace(',','.') if(isinstance(x,str) and (',' in x)) else x)
    for idx in range(1, len(df.columns)):
        df[df.columns[idx]] = df[df.columns[idx]].astype(float)
    df['Type'] = [x.split('-')[0].strip() for x in df['Type']]
    df['Type'] = pd.to_datetime(df['Type'], format = "%d.%m.%Y %H:%M")
    df.set_index('Type', inplace = True)
    df.index = df.index.tz_localize('EET', ambiguous='infer').tz_convert('UTC').tz_convert(None) 
    df.reset_index(inplace = True)
    df['ITN_Id'] = itn
    df.rename(columns = {'Type':'Utc','Consumption':'Reported_Volume'}, inplace = True)
    df = df[['ITN_Id','Utc','Reported_Volume']]
    return df

def fill_direct(dir_path):
    
    for root,dirs, files in os.walk(dir_path):    
        for filename in files:
            if filename.endswith('.csv'):

                try:
                    df = get_df_eso_direct(os.path.join(root, filename))
                    update_reported_volume(df, ItnSchedule.__table__.name)
                    # update_or_insert(engine, df, SCHEDULE_TABLE)
                except Exception as e:
                    print('File ', filename, ' was NOT proceeded !')
                    print (str(e))
# def print_info(archive_name):
#     zf = zipfile.ZipFile(archive_name)
#     for info in zf.infolist():
#         print info.filename
#         print '\tComment:\t', info.comment
#         print '\tModified:\t', datetime.datetime(*info.date_time)
#         print '\tSystem:\t\t', info.create_system, '(0 = Windows, 3 = Unix)'
#         print '\tZIP version:\t', info.create_version
#         print '\tCompressed:\t', info.compress_size, 'bytes'
#         print '\tUncompressed:\t', info.file_size, 'bytes'
#         print

def order_files_by_date(zip_obj):
    raw_dict = {}
    for info in zip_obj.infolist():                
        # print(f' {info.filename}----{dt.datetime(*info.date_time)}', file = sys.stdout)
        raw_dict[dt.datetime(*info.date_time)] = info.filename   
    ordered_dict = collections.OrderedDict(sorted(raw_dict.items()))
    # for k, v in ordered_dict.items(): print(f' key: {k} ----> value {v}', file = sys.stdout)
    xlsx_dict = {k:v for (k,v) in ordered_dict.items() if v.split('.')[1] == 'xlsx'}
    if xlsx_dict:        
        csv_dict = {k:v for (k,v) in ordered_dict.items() if v.split('.')[1] == 'csv'}
        zip_dict = {k:v for (k,v) in ordered_dict.items() if v.split('.')[1] == 'zip'}
        csv_dict.update(zip_dict)
        csv_dict.update(xlsx_dict)
        for k, v in csv_dict.items(): print(f' from xlsx --> key: {k} ----> value {v}', file = sys.stdout)
        # return csv_dict
    else:
        csv_dict = {}
        non_csv_dict = {}
        for k, v in ordered_dict.items():

            inner_zfiledata = BytesIO(zip_obj.read(v))
            inner_zip =  ZipFile(inner_zfiledata)
            print(f'inner ZIP ---> {inner_zip.infolist()[0]}')
            if inner_zip.infolist()[0].filename.endswith('.csv'):
                csv_dict[k] = v
            else:
                non_csv_dict[k] = v

        csv_dict.update(non_csv_dict)
        for k, v in csv_dict.items(): print(f' key: {k} ----> value {v}', file = sys.stdout)
    return csv_dict
    # sum([zinfo.file_size for zinfo in  zip_obj.filelist])
    # for zf in zip_obj.filelist:
    #     print(f'file size {zf.filename}     -> {zf.file_size}')
    # return ordered_dict

def order_files_by_size(zip_obj):
    raw_dict = {}
    for info in zip_obj.infolist():  
        raw_dict[info.file_size] = info.filename   
    ordered_dict = collections.OrderedDict(sorted(raw_dict.items(),  reverse=True))
    # for k, v in ordered_dict.items(): print(f' key: {k} ----> value {v}', file = sys.stdout)
    xlsx_dict = {k:v for (k,v) in ordered_dict.items() if v.split('.')[1] == 'xlsx'}
    if xlsx_dict:        
        csv_dict = {k:v for (k,v) in ordered_dict.items() if v.split('.')[1] != 'xlsx'}
        csv_dict.update(xlsx_dict)
        for k, v in csv_dict.items(): print(f' key: {k} ----> value {v}', file = sys.stdout)
        return csv_dict
    else:
        csv_dict = {}
        non_csv_dict = {}
        for k, v in ordered_dict.items():

            inner_zfiledata = BytesIO(zip_obj.read(v))
            inner_zip =  ZipFile(inner_zfiledata)
            print(f'inner ZIP ---> {inner_zip.infolist()[0]}')
            if inner_zip.infolist()[0].filename.endswith('.csv'):
                csv_dict[k] = v
            else:
                non_csv_dict[k] = v

        csv_dict.update(non_csv_dict)





    # for k, v in csv_dict.items(): print(f' key: {k} ----> value {v}', file = sys.stdout)
    for k, v in csv_dict.items(): print(f' key: {k} ----> value {v}', file = sys.stdout)
    return csv_dict

def update_reported_volume(df, table_name):
    
    print(f'########################## ENTERING UPDATE REPORTED VOLUME #############################')
    start = time.time()

    stringifyer(df)
    bulk_update_list = df.to_dict(orient='records')    
    db.session.bulk_update_mappings(ItnSchedule, bulk_update_list)
    db.session.commit()
    end = time.time()
    print(f'Time elapsed for bulk update is : {end - start} for {df.shape[0]} rows and {df.shape[1]} columns !') 
   
    print(f'########################## Finiiiiiiish UPDATE REPORTED VOLUME #############################')

def have_all_itns_meta(series_itn):

    all_metas = ItnMeta.query.with_entities(ItnMeta.itn).all()
    all_metas = set([x[0] for x in all_metas])
    # print(f'%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% ALL ITNS = {all_metas}')
    print(f'All metas len = {len(all_metas)}')
    print(f'All series itn len = {len(set(series_itn))}') 
    
    if set(series_itn).issubset(all_metas):
        print(f'all have metas')
        return True
    else:
        print(f'NOT all have metas')
        print(f'ZOMBIE ITN \n{set(series_itn) - all_metas}')
        
        return False


def get_tariff_schedule(itn, start_date, end_date):
    pass

# if tariff.name == 'single_tariff':
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

# def get_invoice_data(zip_obj, file_name):  

#     # print('in get_invoice_data', file = sys.stdout)
#     df = pd.read_fwf(zip_obj.open(file_name), encoding="cp1251", engine='python',nrows=0)
#     df = df.columns.str.extract(r'(\d+)[^0-9]+([\d.]+)').dropna()
    
#     if df.empty:       
#         raw_data = file_name.rsplit('_',1)[1].rsplit('.',1)
#         match = re.search(r'\d+', raw_data[0])
#         date_time_obj = dt.datetime.strptime(match.group(0), '%Y%m%d')
# #         date_time_obj = dt.datetime.strptime(raw_data[0], '%Y%m%d')         
#         return (raw_data[0],date_time_obj)

#     else:
#         if('.' in df[1].iloc[0]):
#             date_time_obj = dt.datetime.strptime(str(df[1].iloc[0]), '%d.%m.%Y')         
#             return (df[0].iloc[0],date_time_obj)
#         else:
#             date_time_obj = dt.datetime.strptime(str(df[1].iloc[0]), '%Y%m%d')         
#             return (df[0].iloc[0],date_time_obj)         


# def insert_settlment_cez(zip_obj): 

#     ordered_dict = order_files_by_date(zip_obj)
#     for date_created, file_name in ordered_dict.items():
#         if file_name.endswith('.xlsx'):
#             print(f'From insert_settlment_cez: file name is:  {file_name}') 
#             df = pd.read_excel(zip_obj.read(file_name)) 

#             df = df.fillna(0)
#             df.drop(['DD.MM.YYYY hh:mm','Име на Клиент, ЕСО:','Сетълмент период:'], axis=1, inplace = True)
            
#             df_cols = df.columns[1:]
#             df_cols = [x.replace('.','/') if(isinstance(x,str) and ('.' in x)) else x for x in df_cols]
#             s_date = df_cols[0] if isinstance(df_cols[0], dt.date) else dt.datetime.strptime(df_cols[0], '%d/%m/%Y %H:%M')
#             e_date = df_cols[-1] if isinstance(df_cols[-1], dt.date) else dt.datetime.strptime(df_cols[-1], '%d/%m/%Y %H:%M')
#             # print(f's date, e date {s_date- dt.timedelta(hours = 1)} --- {e_date- dt.timedelta(hours =1)}')
#             time_series = pd.date_range(start = s_date - dt.timedelta(hours = 1), end = e_date - dt.timedelta(hours =1), tz = 'EET', freq='h')
#             # print(f'Time series {time_series}')
#             df.columns = time_series.insert(0,df.columns[0])
            
#             df = pd.melt(df, id_vars=['Уникален Идентификационен Номер:'], var_name = ['utc'], value_name = 'consumption_vol')
#             df.rename(columns={'Уникален Идентификационен Номер:': 'itn'}, inplace = True)
#             df.set_index(pd.DatetimeIndex(df['utc']), inplace = True)
#             df.drop(columns= 'utc', inplace = True)
            
#             try:
#                 df.index = df.index.tz_convert('UTC').tz_convert(None)
#             except Exception as e:
#                 print(f'Exception from cez hourly loading: {e}')

#             else:
#                 if(not df.empty):
#                     df.reset_index(inplace = True)
                   
#                     min_date = min(df['utc']).to_pydatetime()                    
#                     max_date = max(df['utc']).to_pydatetime()                    
                   
#                     if file_name.find('СТП') != -1:

#                         stp_records = incoming_stp_records(df,min_date, max_date)
                        
#                         if len(stp_records) > 0:                        
#                             stp_records_df = pd.DataFrame.from_records(stp_records, columns=stp_records[0].keys()) 
#                             update_stp_settelment_vol(df, stp_records_df, stp_records) 
#                             update_stp_consumption_vol(stp_records_df, min_date, max_date)
                   
#                     else:                        
#                         update_non_stp_consumption_settelment_vol(df, min_date, max_date)
                    
#                     # stp_records_df, stp_records = incoming_stp_records(df,min_date, max_date)

#                     # if not stp_records_df.empty:                       
#                     #     update_stp_settelment_vol(df, stp_records_df, stp_records) 
#                     #     update_stp_consumption_vol(stp_records_df, min_date, max_date)  

#                     # else:
#                     #     update_non_stp_consumption_settelment_vol(df, min_date, max_date)

# def insert_erp_invoice(zip_obj, separator):

#     full_df = pd.DataFrame()

#     for zf in zip_obj.namelist() :
#         if zf.endswith('.csv'): 

#             input_df = reader_csv(zip_obj, zf, separator)
            
#             if(full_df.empty):           
#                 full_df = input_df
#             else:           
#                 full_df = full_df.append(input_df, ignore_index=True)
        
#     erp_inv_df = full_df[['number','date','event','correction_note']].copy()
#     erp_inv_df.drop_duplicates(subset = ['number','correction_note','event','date'], keep = 'first', inplace = True)
#     erp_inv_df.reset_index(inplace = True, drop = True)
    
#     erp_inv_df['date'] =erp_inv_df['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
   
#     erp_inv_df['composite_key'] = erp_inv_df['correction_note'].apply(str) + erp_inv_df['event'].apply(str) + erp_inv_df['number'].apply(str) + erp_inv_df['date']

#     print(f'erp_inv_df to DB \n{erp_inv_df}')
    
#     # update_or_insert(erp_inv_df, ErpInvoice.__table__.name)

#     return erp_inv_df

# def insert_mrus(zip_obj, separator, password = None, enc = None):

#     erp_invoice_df =  pd.read_sql(ErpInvoice.query.statement, db.session.bind)   
#     tech_tbl = pd.DataFrame()
#     distr_tbl = pd.DataFrame()
   
#     for zf in zip_obj.namelist() :
#         if zf.endswith('.csv'): 
#             print(f'file name:{zf}\n')
#             input_df = reader_csv(zip_obj, zf, separator, password, enc)

#             tech_point = get_tech_point(input_df, erp_invoice_df)
#             distrib_point = get_distrib_point(input_df, erp_invoice_df)

#             try:
#                 if((distr_tbl.empty) & (distrib_point is not None)):           
#                     distr_tbl = distrib_point

#                 elif distrib_point is not None:           
#                     distr_tbl = distr_tbl.append(distrib_point, ignore_index=True)

#                 else:
#                     print(f'empty distribution point')

#             except Exception as e:
#                 print(f'distribution is None {e}')

#             try:
#                 if((tech_tbl.empty) & (tech_point is not None)):
#                     tech_tbl = tech_point  
                              
#                 elif tech_point is not None:           
#                     tech_tbl = tech_tbl.append(tech_point, ignore_index=True) 

#                 else:
#                     print(f'empty tech point')

#             except Exception as e:
#                 print(f'tech is None {e}')


#     if distr_tbl.empty:
#         print(f'distrib table is empty')   

#     else:
#         try:
#             have_all_itns_meta(distr_tbl['itn'].values)
#             print(f'distrib_tbl to DB {distr_tbl}')
#             update_or_insert(distr_tbl, Distribution.__table__.name)
#         except Exception as e:
#             print(f'Exception from writing distribution to DB, with message: {e}')

#     if tech_tbl.empty:
#         print(f'tech table is empty') 

#     else:
#         try:       
#             have_all_itns_meta(tech_tbl['itn'].values)
#             print(f'tech_tbl to DB {tech_tbl}')
#             update_or_insert(tech_tbl, Tech.__table__.name)
#         except Exception as e:
#             print(f'Exception from writing tech to DB, with message: {e}')    

# def reader_csv(zip_obj, file_name, separator,password = None, enc = None):
        
#     try:
#         df = pd.read_csv(zip_obj.open(file_name),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)
#     # except:
#     #     df = pd.read_csv(zip_obj.open(text_file.filename,pwd=bytes(password, enc)),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)
#     #     for text_file in zip_obj.infolist() if text_file.filename.endswith('.csv')



#     # # print('in reader_csv', file = sys.stdout)
#     # try:
#         # df = pd.read_csv(zip_obj.open(file_name),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)
    
#         # print(f'in except {file_name} -----> {PASSWORD}')
#         # df = pd.read_csv(zip_obj.open(file_name, 'r', pwd=bytes(PASSWORD, ENCODING)),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)
    
    
#         df.columns = df.columns.str.replace('"','')

#         df = replace_char(df,'"','')
#         df = replace_char(df,',','.')

#         col_names = ['erp_code','1','6','content','subscriber_number','place_number','7','customer_number','8','9','itn','electric_meter_number','start_date',
#                      'end_date','4','scale_number','scale_code','scale_type','time_zone','new_readings','old_readings','readings_difference','constant','correction','storno',
#                      'total_amount','tariff','calc_amount','price','value','correction_note','event']
#         df.columns = col_names
#         cols_to_drop = ['1','4','6','7','8','9','erp_code']
#         df = df.drop(cols_to_drop, axis = 1)


#         df['start_date'] = pd.to_datetime(df['start_date'], format = '%d.%m.%Y')
#         df['end_date'] = pd.to_datetime(df['end_date'], format = '%d.%m.%Y')

#         df['calc_amount'].apply(Decimal)
#         df['price'].apply(Decimal)
#         df['value'].apply(Decimal)

#         df['new_readings'].apply(Decimal)
#         df['old_readings'].apply(Decimal)
#         df['readings_difference'].apply(Decimal)
#         df['storno'].apply(Decimal)
#         df['total_amount'].apply(Decimal)




#         inv_data = get_invoice_data(zip_obj, file_name)
#         df['number'] = pd.Series(inv_data[0], index=df.index)
#         df['date'] = pd.Series(inv_data[1], index=df.index)
# #         df['is_correction'] = not df['invoice_correction'].isnull().all()

#         df = df.fillna(0)
#     except Exception as e: 
#         print(e)
#     else:
#         # df.to_excel(f'{file_name}.xlsx')
#         return df