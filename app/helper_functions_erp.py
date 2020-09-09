import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash
from app.models import *    
from app.helper_functions import update_or_insert
import collections
from zipfile import ZipFile
from io import BytesIO



def replace_char(df, bad_char, good_char):
    df = df.applymap(lambda x: x.replace(bad_char,good_char) if(isinstance(x,str) and (bad_char in x)) else x)
    return df

def get_invoice_data(zip_obj, file_name):  

    # print('in get_invoice_data', file = sys.stdout)
    df = pd.read_fwf(zip_obj.open(file_name), encoding="cp1251", engine='python',nrows=0)
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
                      
                                    

def reader_csv(zip_obj, file_name, separator):
    # print('in reader_csv', file = sys.stdout)
    try:
        df = pd.read_csv(zip_obj.open(file_name),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)
    
    
        df.columns = df.columns.str.replace('"','')

        df = replace_char(df,'"','')
        df = replace_char(df,',','.')

        col_names = ['erp_code','1','6','content','subscriber_number','place_number','7','customer_number','8','9','itn','electric_meter_number','start_date',
                     'end_date','4','scale_number','scale_code','scale_type','time_zone','new_readings','old_readings','readings_difference','constant','correction','storno',
                     'total_amount','tariff','calc_amount','price','value','correction_note','event']
        df.columns = col_names
        cols_to_drop = ['1','4','6','7','8','9','erp_code']
        df = df.drop(cols_to_drop, axis = 1)


        df['start_date'] = pd.to_datetime(df['start_date'], format = '%d.%m.%Y')
        df['end_date'] = pd.to_datetime(df['end_date'], format = '%d.%m.%Y')

        df['calc_amount'].apply(Decimal)
        df['price'].apply(Decimal)
        df['value'].apply(Decimal)

        df['new_readings'].apply(Decimal)
        df['old_readings'].apply(Decimal)
        df['readings_difference'].apply(Decimal)
        df['storno'].apply(Decimal)
        df['total_amount'].apply(Decimal)




        inv_data = get_invoice_data(zip_obj, file_name)
        df['number'] = pd.Series(inv_data[0], index=df.index)
        df['date'] = pd.Series(inv_data[1], index=df.index)
#         df['is_correction'] = not df['invoice_correction'].isnull().all()

        df = df.fillna(0)
    except Exception as e: 
        print(e)
    else:
        return df
    

def get_tech_point(df, erp_invoice = None):   
    
    try:

        df = df[df['content'] == 'Техническа част']
        cols_to_drop = ['content','tariff','calc_amount','price','value','event','correction_note']    
        df = df.drop(cols_to_drop, axis = 1)
        df.drop_duplicates(subset=['itn', 'start_date','new_readings','total_amount'],keep='first',inplace = True) 
        if(erp_invoice is not None):
            erp_invoice = erp_invoice[erp_invoice['event'] == '']
            df = df.merge(erp_invoice, on = 'number', how = 'left')
            df.drop(columns = ['date_x', 'date_y', 'event', 'correction_note', 'fk'], inplace = True)
            df.rename(columns = {'id':'erp_invoice_id'}, inplace = True)
    except Exception as e: 
        print(e)
    else:
        return df


def get_distrib_point(df, erp_invoice_df = None):   

    try:
    
        df = df[df['content'] == 'Разпределение']

        cols_to_drop = ['content','subscriber_number','place_number','customer_number','electric_meter_number','scale_number','scale_code','scale_type','time_zone','new_readings',
                        'old_readings','readings_difference','constant','correction','storno','total_amount']
        df = df.drop(cols_to_drop, axis = 1)

        df.drop_duplicates(subset=['itn', 'start_date','end_date','price','value'],keep='first',inplace = True)
        if(erp_invoice_df is not None):
        
            df['fk'] = df['correction_note'].apply(str) + df['event'].apply(str) + df['number'].apply(str) + df['date'].apply(str)
            
            df = df.merge(erp_invoice_df, on = 'fk', how = 'left')
            # print(df, file = sys.stdout)
            df.drop(columns = ['correction_note_y','event_x', 'number_x', 'date_x', 'fk','number_y', 'date_y', 'event_y','correction_note_x'], inplace = True)
            df.rename(columns = {'id':'erp_invoice_id'}, inplace = True)
            

    except Exception as e: 
        print(e)
    else:
        return df


def insert_to_db(paths):
    erp_invoice = get_all(session, erp_invoice)
    
    for path in paths:        
        print(path)
        input_df = reader_csv(path,'";"')
        tech_point = get_tech_point(input_df, erp_invoice)
        distrib_point = get_distrib_point(input_df, erp_invoice)
        try:
            update_or_insert(engine, tech_point, 'tech')
            update_or_insert(engine, distrib_point, 'distribution')
        except Exception as e: 
            print(e)

            

def insert_to_df(zip_obj, separator):
    erp_invoice_df =  pd.read_sql(ErpInvoice.query.statement, db.session.bind)
    
    erp_invoice_df['fk'] = erp_invoice_df['correction_note'].apply(str) + erp_invoice_df['event'].apply(str) + erp_invoice_df['number'].apply(str) + erp_invoice_df['date'].apply(str)
    tech_tbl = pd.DataFrame()
    distr_tbl = pd.DataFrame()
    # print(erp_invoice_df['fk'], file = sys.stdout)
    
    for zf in zip_obj.namelist() :
        if zf.endswith('.csv'): 

            input_df = reader_csv(zip_obj, zf, separator)

            tech_point = get_tech_point(input_df, erp_invoice_df)
            distrib_point = get_distrib_point(input_df, erp_invoice_df)
            if(distr_tbl.empty):           
                distr_tbl = distrib_point
            else:           
                distr_tbl = distr_tbl.append(distrib_point, ignore_index=True)
            if(tech_tbl.empty):
                tech_tbl = tech_point            
            else:
                tech_tbl = tech_tbl.append(tech_point, ignore_index=True) 

    # update_or_insert(distr_tbl, Distribution.__table__.name)

    # distr_tbl['start_date'] = distr_tbl['start_date'].astype(str)  
    # distr_tbl['end_date'] = distr_tbl['end_date'].astype(str)    
    # bulk_list = distr_tbl.to_dict(orient='records')  
    # print(distr_tbl, file = sys.stdout) 
    # db.session.bulk_insert_mappings(Distribution, bulk_list)


    # update_or_insert(tech_tbl, Tech.__table__.name)

    # tech_tbl['start_date'] = tech_tbl['start_date'].astype(str)  
    # tech_tbl['end_date'] = tech_tbl['end_date'].astype(str)
    # bulk_list = tech_tbl.to_dict(orient='records')   
    # db.session.bulk_insert_mappings(Tech, bulk_list)

    # db.session.commit()       
    return distr_tbl, tech_tbl

def insert_erp_invoice(zip_obj, separator):

    full_df = pd.DataFrame()
    # print(f' in insert {type(full_df)}', file = sys.stdout)
    for zf in zip_obj.namelist() :
        if zf.endswith('.csv'): 

            input_df = reader_csv(zip_obj, zf, separator)
            
            if(full_df.empty):           
                full_df = input_df
            else:           
                full_df = full_df.append(input_df, ignore_index=True)
        
    erp_inv_df = full_df[['number','date','event','correction_note']].copy()
    erp_inv_df.drop_duplicates(subset = ['number','correction_note','event','date'], keep = 'first', inplace = True)
    erp_inv_df.reset_index(inplace = True, drop = True)
    print(f'from insert table name : {ErpInvoice.__table__.name}', file = sys.stdout)
    update_or_insert(erp_inv_df, ErpInvoice.__table__.name)
    # erp_inv_df['date'] = erp_inv_df['date'].astype(str)
    # bulk_list = erp_inv_df.to_dict(orient='records')   
    
    # db.session.bulk_insert_mappings(ErpInvoice, bulk_list)
    # db.session.commit()
    
    return erp_inv_df

    #################### Hourly ###########################

def update_reported_volume(df, table_name):

    all_data = pd.read_sql(ItnSchedule.query.filter(ItnSchedule.itn.in_(list(set(df['itn'])))).statement, db.session.bind)
    # print(all_data.shape, file = sys.stdout)
    all_data.drop(columns = 'reported_vol', inplace = True)
    all_data.dropna(inplace = True)
    updated_data = pd.merge(df, all_data, on = ['itn','utc'], how = 'inner')
    # print(df.head,file = sys.stdout)    
    update_or_insert(updated_data, table_name)

def create_db_df_eepro_evn(df, ITN):      

    df = df.fillna(0)
    df = df.iloc[3:]
    df['1'] = df['1'].apply(lambda x: x.replace('.','/'))
    is_manufacturer = True if df['3'].mean() != 0 else False
    df_for_db= pd.DataFrame(columns=['ITN_Id','Utc','Reported_Volume'])    
    df_for_db['Utc'] = pd.to_datetime(df['1'], format = '%d/%m/%Y %H:%M')
    df_for_db['ITN_Id'] = ITN
    df_for_db['Reported_Volume'] = df['3'].astype(float) if is_manufacturer else df['2'].astype(float)  
    df_for_db.set_index('Utc', inplace = True)
    df_for_db.index = df_for_db.index.shift(periods=-1, freq='h').tz_localize('EET', ambiguous='infer').tz_convert('UTC').tz_convert(None)  
    df_for_db.reset_index(inplace = True)

    return df_for_db

def fill_db_from_excel_cez(zip_obj): 

    ordered_dict = order_files_by_date(zip_obj)
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.xlsx'):
            print(file_name, file = sys.stdout) 
            df = pd.read_excel(zip_obj.read(file_name))
            # print(df, file = sys.stdout) 

            df = df.fillna(0)
            df.drop(['DD.MM.YYYY hh:mm','Име на Клиент, ЕСО:','Сетълмент период:'], axis=1, inplace = True)
            
            df_cols = df.columns[1:]
            df_cols = [x.replace('.','/') if(isinstance(x,str) and ('.' in x)) else x for x in df_cols]
            s_date = df_cols[0] if isinstance(df_cols[0], dt.date) else dt.datetime.strptime(df_cols[0], '%d/%m/%Y %H:%M')
            e_date = df_cols[-1] if isinstance(df_cols[-1], dt.date) else dt.datetime.strptime(df_cols[0], '%d/%m/%Y %H:%M')
            
            time_series = pd.date_range(start = s_date - dt.timedelta(hours =1), end = e_date - dt.timedelta(hours =1), tz = 'EET', freq='h')
            df.columns = time_series.insert(0,df.columns[0])
            
            df = pd.melt(df, id_vars=['Уникален Идентификационен Номер:'], var_name = ['utc'], value_name = 'reported_vol')
            df.rename(columns={'Уникален Идентификационен Номер:': 'itn'}, inplace = True)
            df.set_index(pd.DatetimeIndex(df['utc']), inplace = True)
            df.drop(columns= 'utc', inplace = True)
            
            try:
                df.index = df.index.tz_convert('UTC').tz_convert(None)
            except Exception as e:
                print(f'Exception from cez hourly loading: {e}')

            else:
                if(not df.empty):
                    df.reset_index(inplace = True)
                    update_reported_volume(df, ItnSchedule.__table__.name)
                    # all_data = pd.read_sql(ItnSchedule.query.filter(ItnSchedule.itn.in_(list(set(df['itn'])))).statement, db.session.bind)
                    # # print(all_data.shape, file = sys.stdout)
                    # all_data.drop(columns = 'reported_vol', inplace = True)
                    # all_data.dropna(inplace = True)
                    # updated_data = pd.merge(df, all_data, on = ['itn','utc'], how = 'inner')
                    # # print(df.head,file = sys.stdout)
                    
                    # update_or_insert(updated_data, ItnSchedule.__table__.name)
                    
                
    #  df = pd.read_excel(zip_obj.read(file_name))           
def fill_db_from_excel_e_pro(zip_obj):

    ordered_dict = order_files_by_date(zip_obj)
    # print(ordered_dict, file = sys.stdout)
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
            print(file_name, file = sys.stdout)
            
            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)
         
            dfs = {text_file.filename: pd.read_excel(inner_zip.read(text_file.filename))
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.xlsx')}
            # print(dfs, file = sys.stdout)

            for key in dfs.keys():
                try:
                    df = dfs[key]
                    df.columns = df.columns.str.strip()
                    ClientName = [x for x in df.columns if(x.find('Unnamed:') == -1)][0]
                    ITN = df.iloc[:1][ClientName].values[0].split(': ')[1]            
                    df = df.rename(columns={ClientName:'1','Unnamed: 1':'2', 'Unnamed: 2':'3'})          

                    df_for_db = create_db_df_eepro_evn(df, ITN)
                    if(not df_for_db.empty):
                        # print('ITN is: ', ITN)
                        print(df_for_db, file = sys.stdout)
                        # update_or_insert(engine,df_for_db, SCHEDULE_TABLE)
                    else:
                        print('Values in file ', key, ' was only 0 !')
                except Exception as e: 
                    print('File ', key, ' was NOT proceeded !')
                    print (str(e))
                
                
def fill_db_from_excel_evn(zip_obj):
    
    PASSWORD = '8yc#*3-Q5ADt'
    ENCODING = 'utf-8'

    ordered_dict = order_files_by_date(zip_obj)
    # print(ordered_dict, file = sys.stdout)
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
            print(file_name, file = sys.stdout)

            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)
         
            dfs_dict = {text_file.filename: pd.read_excel(inner_zip.read(text_file.filename,pwd=bytes(PASSWORD, ENCODING)))
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.xlsx')}
            # print(dfs, file = sys.stdout)   
            for key in dfs_dict.keys():
                try:
                    df = dfs_dict[key]
                    df.columns = df.columns.str.strip()
                    ITN = df.iloc[:1].values[0][0] 
                    df = df.rename(columns={'Гранд Енерджи Дистрибюшън ЕООД':'1','Unnamed: 1':'2', 'Unnamed: 2':'3'})
                    df_for_db = create_db_df_eepro_evn(df, ITN)
                    if(not df_for_db.empty): 
                        print(df_for_db, file = sys.stdout)                  
                        # update_or_insert(engine, df_for_db, SCHEDULE_TABLE)
                    else:
                        print('Values in file ', key, ' was only 0 !')
                except Exception as e:
                    print('File ', key, ' was NOT proceeded !')
                    print (str(e))

                
def get_df_eso_direct(path_csv):
#     PATH = r'C:\WORK\GED\input\chavdar\06_2020\данни ЕСО\2020 06\преки\АГРОПОЛИХИМ.csv'
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
                    update_or_insert(engine, df, SCHEDULE_TABLE)
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
    return ordered_dict


