import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash
from app.models import *    
from app.helper_functions import update_or_insert, stringifyer
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
        # df.to_excel(f'{file_name}.xlsx')
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

        cols_to_drop = ['content','subscriber_number','place_number','customer_number','electric_meter_number','scale_number','scale_code','scale_type','time_zone','new_readings',
                        'old_readings','readings_difference','constant','correction','storno','total_amount']
        df = df.drop(cols_to_drop, axis = 1)

        df.drop_duplicates(subset=['itn', 'start_date','end_date','price','value'],keep='first',inplace = True)
        # print(f'from get_distrib_point before if  ------> df : \n{df}')
        if(erp_invoice_df is not None):
        
            df['composite_key'] = df['correction_note'].apply(str) + df['event'].apply(str) + df['number'].apply(str) + df['date'].apply(str)
            
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


# def insert_to_db(paths):
#     erp_invoice = get_all(session, erp_invoice)
    
#     for path in paths:        
#         print(path)
#         input_df = reader_csv(path,'";"')
#         tech_point = get_tech_point(input_df, erp_invoice)
#         distrib_point = get_distrib_point(input_df, erp_invoice)
#         try:
#             update_or_insert(engine, tech_point, 'tech')
#             update_or_insert(engine, distrib_point, 'distribution')
#         except Exception as e: 
#             print(e)

            

def insert_mrus(zip_obj, separator):

    erp_invoice_df =  pd.read_sql(ErpInvoice.query.statement, db.session.bind)   
    tech_tbl = pd.DataFrame()
    distr_tbl = pd.DataFrame()
   
    for zf in zip_obj.namelist() :
        if zf.endswith('.csv'): 
            print(f'file name:{zf}\n')
            input_df = reader_csv(zip_obj, zf, separator)

            tech_point = get_tech_point(input_df, erp_invoice_df)
            distrib_point = get_distrib_point(input_df, erp_invoice_df)

            try:
                if(distr_tbl.empty):           
                    distr_tbl = distrib_point
                else:           
                    distr_tbl = distr_tbl.append(distrib_point, ignore_index=True)
            except Exception as e:
                print(f'distribution is None {e}')
            try:
                if(tech_tbl.empty):
                    tech_tbl = tech_point            
                else:
                    tech_tbl = tech_tbl.append(tech_point, ignore_index=True) 
            except Exception as e:
                print(f'tech is None {e}')
         
    try:
        have_all_itns_meta(distr_tbl['itn'].values)
        update_or_insert(distr_tbl, Distribution.__table__.name)
    except Exception as e:
        print(f'Exception from writing distribution to DB, with message: {e}')

    try:       
        have_all_itns_meta(tech_tbl['itn'].values)
        update_or_insert(tech_tbl, Tech.__table__.name)
    except Exception as e:
        print(f'Exception from writing distribution to DB, with message: {e}')    
  
    
def insert_erp_invoice(zip_obj, separator):

    full_df = pd.DataFrame()

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
    
    erp_inv_df['date'] =erp_inv_df['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
   
    erp_inv_df['composite_key'] = erp_inv_df['correction_note'].apply(str) + erp_inv_df['event'].apply(str) + erp_inv_df['number'].apply(str) + erp_inv_df['date']
    
    update_or_insert(erp_inv_df, ErpInvoice.__table__.name)

    return erp_inv_df

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


def update_reported_volume(df, table_name):
    
    print(f'########################## ENTERING UPDATE REPORTED VOLUME #############################')
    # update_df = df[['itn','utc','reported_vol']]

    
    
    # have_all_itns_meta(df['itn'])
    

    stringifyer(df)
    bulk_update_list = df.to_dict(orient='records')
    
    db.session.bulk_update_mappings(ItnSchedule, bulk_update_list)
    db.session.commit()
    # print(f'len of bulk list {len(bulk_update_list)}')
    # print(f'len of df {df.shape[0]}')


    # # print(f'from update_reported_volume ----> INPUT_DATA\n {df.head}')  
    # print(f'shape of input df : {df.shape}')    
    # s_date = str(min(df['utc']))
    # e_date = str(max(df['utc']))
    # records = (
    #     db.session.query(ItnSchedule.itn, ItnSchedule.utc, ItnSchedule.reported_vol)
    #     .filter(ItnSchedule.itn.in_(list(set(df['itn']))))
    #     .filter(ItnSchedule.utc >= s_date, ItnSchedule.utc <= e_date)
    #     .all())

    # all_data = pd.DataFrame.from_records(records, columns = records[0].keys())
    # all_data = all_data[['itn','utc','reported_vol']]
    # m = df.merge(all_data, on=['itn','utc'], how='outer', suffixes=['', '_'], indicator=True)
    # null_mask = m.isnull().any(axis=1)
    # m = m[null_mask]
    # print(f'from {s_date} ----- {e_date}')
    # print(f' M is {m}')
    # m.to_excel('m.xlsx')
    # all_data.dropna(inplace = True)
    # updated_data = pd.merge(df, all_data, on = ['itn','utc'], how = 'left')
    # print(f'len of updated_data {updated_data.shape[0]}')
    # print(f'min {min(all_data.utc)}')
    # print(f'max {max(all_data.utc)}')
    # print(f'min {min(df.utc)}')
    # print(f'max {max(df.utc)}')
    # print(f'min {min(all_data.utc)}')
    # print(f'max {max(all_data.utc)}')
    
   
    # print(f' M is {m}')
    # updated_data = pd.merge(df, all_data, on = ['itn','utc'], how = 'inner')
    # update_or_insert(updated_data, table_name)
    
    # # all_data = pd.read_sql(ItnSchedule.query.statement, db.session.bind) 
    # # df_parts =[df[i : i + SPLIT_SIZE] for i in range(0, df.shape[0], SPLIT_SIZE)]
    # # count = 1
    # # for part_df in df_parts:

    # #     print(f'from UPDARE VOLUME - READED DATA FROM DB ----> all_data from db\n {all_data}')
    # #     # all_data.drop(columns = 'reported_vol', inplace = True)
       
    # #     updated_data = pd.merge(part_df, all_data, on = ['itn','utc'], how = 'inner')
    # #     update_or_insert(updated_data, table_name)
    # #     print(f'from update loop i = {count}') 
    # #     count += 1   
    print(f'########################## Finiiiiiiish UPDATE REPORTED VOLUME #############################')
    

def create_db_df_eepro_evn(df, ITN):      
    try:
        df = df.fillna(0)
        df = df.iloc[3:]
        # print(f'entering create_db_df_eepro_evn df :\n -------> {df}')
        df['1'] = df['1'].apply(lambda x: x.replace('.','/'))
        
        is_manufacturer = True if df['3'].mean() != 0 else False
        df_for_db= pd.DataFrame(columns=['itn','utc','reported_vol']) 
        
        df_for_db['utc'] = pd.to_datetime(df['1'], format = '%d/%m/%Y %H:%M')
        df_for_db['itn'] = ITN
        
        df_for_db['reported_vol'] = df['3'].astype(float) if is_manufacturer else df['2'].astype(float)  
        df_for_db.set_index('utc', inplace = True)
        df_for_db.index = df_for_db.index.shift(periods=-1, freq='h').tz_localize('EET', ambiguous='infer').tz_convert('UTC').tz_convert(None)  

        df_for_db.reset_index(inplace = True)
    except Exception as e:
        print(f'Exception from create_db_df_eepro_evn: {e}')

    print(f'from create_db_df_eepro_evn df_for_db ITN======{ITN}\n ---->{df_for_db}')   
    return df_for_db

def insert_settlment_cez(zip_obj, itn_meta_df): 

    ordered_dict = order_files_by_date(zip_obj)
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.xlsx'):
            print(f'From insert_settlment_cez: file name is:  {file_name}') 
            df = pd.read_excel(zip_obj.read(file_name))  
            merged_df = pd.merge(df, itn_meta_df, left_on = df.columns[1], right_on = 'itn', how = 'left')             
            df = merged_df[(merged_df['code'] == 'DIRECT') | (merged_df['code'] == 'UNDIRECT')]

            if df.empty:
                print(f'thera are only stp itn - skipping!')
                continue

            df.drop(columns = ['code', 'itn'], inplace = True)
            print(f'from insert_settlment_cez redacted DF: \n{df}')
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
                    # print(f'From insert_settlment_cez ---> %%%%%%%%%%%%%%%%%%%df is:\n {df}') 
                    update_reported_volume(df, ItnSchedule.__table__.name)
            
def insert_settlment_e_pro(zip_obj, itn_meta_df):

    ordered_dict = order_files_by_date(zip_obj)
    
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
            print(file_name, file = sys.stdout)
            
            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)
         
            dfs = {text_file.filename: pd.read_excel(inner_zip.read(text_file.filename))
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.xlsx')}
            
            for key in dfs.keys():
                try:
                    df = dfs[key]
                    df.columns = df.columns.str.strip()
                    ClientName = [x for x in df.columns if(x.find('Unnamed:') == -1)][0]
                    ITN = df.iloc[:1][ClientName].values[0].split(': ')[1]   
                    if itn_meta_df[itn_meta_df['itn'] == ITN]['code'].values[0] in ['DIRECT, UNDIRECT']:
                        continue
                    
                    df = df.rename(columns={ClientName:'1','Unnamed: 1':'2', 'Unnamed: 2':'3'})          
                   
                    df_for_db = create_db_df_eepro_evn(df, ITN)
                    
                    if(not df_for_db.empty):
                        update_reported_volume(df_for_db, ItnSchedule.__table__.name)
                    else:
                        print('Values in file ', key, ' was only 0 !')
                except Exception as e: 
                    print(f'File {key} was NOT proceeded .Exception message: {e}!')
                    
                
                
def insert_settlment_evn(zip_obj):
    
    # PASSWORD = '8yc#*3-Q5ADt'
    PASSWORD = '79+Kg+*rLA7P'
    ENCODING = 'utf-8'

    ordered_dict = order_files_by_date(zip_obj)
    print(ordered_dict, file = sys.stdout)
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
            print(file_name, file = sys.stdout)

            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)
         
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
                    # print(f'FROM INSERT EVN {df_for_db}')
                    if(not df_for_db.empty):  
                       
                        curr_sub_measuring_type = SubContract.query.filter(SubContract.itn == ITN,\
                            SubContract.start_date <= df_for_db.iloc[0].utc.to_pydatetime(),\
                            SubContract.end_date >= df_for_db.iloc[0].utc.to_pydatetime()).first().measuring_type.code
                        
                        # if curr_sub_measuring_type in ['DIRECT', 'UNDIRECT']:
                                           
                        # update_reported_volume(df_for_db, ItnSchedule.__table__.name)
                    else:
                        print('Values in file ', key, ' was only 0 !')
                except Exception as e:
                    print(f'File {key} was NOT proceeded .Exception message: {e}!')
                
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
    return ordered_dict


