import sys, pytz, datetime as dt

import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash
from app.models import *    
from app.helpers.helper_functions import update_or_insert, stringifyer, convert_date_to_utc, get_subcontracts_by_itn_and_utc_dates, generate_provisional_subcontract
import collections
from zipfile import ZipFile
from io import BytesIO

from sqlalchemy.exc import ProgrammingError


def replace_char(df, bad_char, good_char):
    df = df.applymap(lambda x: x.replace(bad_char,good_char) if(isinstance(x,str) and (bad_char in x)) else x)
    return df
                      
def get_invoice_data(df, file_name):  

    df = df.columns.str.extract(r'(\d+)[^0-9]+([\d.]+)').dropna()
    
    if df.empty:       
        raw_data = file_name.rsplit('_',1)[1].rsplit('.',1)
        match = re.search(r'\d+', raw_data[0])
        date_time_obj = dt.datetime.strptime(match.group(0), '%Y%m%d')       
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
    # print(f'erp_inv_df \n {erp_inv_df}')
    erp_inv_df['composite_key'] = erp_inv_df['correction_note'].apply(str) + erp_inv_df['event'].apply(str) + erp_inv_df['number'].apply(str) + erp_inv_df['date']
    erp_inv_df = erp_inv_df.fillna('')
    update_or_insert(erp_inv_df, ErpInvoice.__table__.name)

    return erp_inv_df

def reader_csv(df, file_name, erp_name, is_temp = False):   

        
        df.columns = df.columns.str.replace('"','')        

        df = replace_char(df,'"','')
        df = replace_char(df,',','.')

        col_names = ['erp_code','1','6','content','subscriber_number','10','7','customer_number','8','9','itn','electric_meter_number','start_date',
                     'end_date','4','scale_number','scale_code','scale_type','time_zone','new_readings','old_readings','readings_difference','constant','correction','storno',
                     'total_amount','tariff','calc_amount','price','value','correction_note','event']
        df.columns = col_names

        if not is_temp:
            itn_list = get_list_all_itn_in_db_by_erp(erp_name)
            df = df[df['itn'].isin(itn_list)]

        cols_to_drop = ['1','4','6','7','8','9','10','erp_code']
        df = df.drop(cols_to_drop, axis = 1)
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)        
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
        # print(f'FROM READER CSV DF ----------------------->{df}')
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
        print(f'{e}  \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}')
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

# def create_point_df(point_df, point):

#     try:
#         if((point_df.empty) & (point is not None)):           
#             point_df = point

#         elif point is not None:           
#             point_df = point_df.append(point, ignore_index=True)

#         else:
#             print(f'empty {point} ')

#     except Exception as e:
#         print(f'Exception from create_point_df: {point} is None {e} \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}')
    

def insert_mrus(raw_df, file_name, erp_name):  
            
    input_df = reader_csv(raw_df, file_name, erp_name)
    input_temp_df = reader_csv(raw_df, file_name, erp_name, True)
    
    input_df['date'] = input_df['date'].apply(lambda x: convert_date_to_utc('EET', x))
    input_temp_df['date'] = input_temp_df['date'].apply(lambda x: convert_date_to_utc('EET', x))
    # print(f'input df insert erp invoice \n{input_df}')
    insert_erp_invoice(input_df)      


    erp_invoice_df =  pd.read_sql(ErpInvoice.query.statement, db.session.bind)   

    # print(f'BBBBBBBBBBBBBBBB {erp_invoice_df}')
    tech_tbl = pd.DataFrame()
    distr_tbl = pd.DataFrame()
    distr_temp_tbl = pd.DataFrame()

    tech_point = get_tech_point(input_df, erp_invoice_df)    
    distrib_point = get_distrib_point(input_df, erp_invoice_df)
    distrib_point_temp = get_distrib_point(input_temp_df)
    
    # create_point_df(tech_tbl, tech_point)
    # create_point_df(distr_tbl, distrib_point)
    # create_point_df(distr_temp_tbl,distrib_point_temp)
    try:
        if((distr_temp_tbl.empty) & (distrib_point_temp is not None)):           
            distr_temp_tbl = distrib_point_temp

        elif distrib_point_temp is not None:           
            distr_temp_tbl = distr_temp_tbl.append(distrib_point_temp, ignore_index=True)

        else:
            print(f'empty distrib_point_temp ')

    except Exception as e:
        print(f'distrib_point_temp is None {e} \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}')

    try:
        if((distr_tbl.empty) & (distrib_point is not None)):           
            distr_tbl = distrib_point

        elif distrib_point is not None:           
            distr_tbl = distr_tbl.append(distrib_point, ignore_index=True)

        else:
            print(f'empty distribution point')

    except Exception as e:
        print(f'distribution is None {e} \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}')

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
            # have_all_itns_meta(distr_tbl['itn'].values)
            distr_tbl = distr_tbl.replace(np.nan,0)
            # print(f'distrib_tbl to DB \n{distr_tbl.head()} \n{distr_tbl.columns}')           
            update_or_insert(distr_tbl, Distribution.__table__.name)
        except Exception as e:
            print(f'Exception from writing distribution to DB, with message: {e} \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}')

    if tech_tbl.empty:
        print(f'tech table is empty') 

    else:
        try:       
            # have_all_itns_meta(tech_tbl['itn'].values)
            # print(f'tech_tbl to DB \n{tech_tbl}')
            tech_tbl = tech_tbl.replace(np.nan,0)
            update_or_insert(tech_tbl, Tech.__table__.name)
        except Exception as e:
            print(f'Exception from writing tech to DB, with message: {e} \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}') 
    
    if distr_temp_tbl.empty:
        print(f'distr_temp_tbl table is empty')

    else:
        try:             
            distr_temp_tbl = distr_temp_tbl.replace(np.nan,0)
            # print(f'distr_temp_tbl to DB \n{distr_temp_tbl}\n{distr_temp_tbl.shape}')
            update_or_insert(distr_temp_tbl, DistributionTemp.__table__.name)
            
        except Exception as e:
            print(f'Exception from writing distr_temp_tbl to DB, with message: {e} \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}')


def insert_settlment_cez(zip_obj,separator): 

    # PASSWORD = 'XY3R9TAPAB4BZEKDTU9L'
    PASSWORD = 'SRE9N7TXUUQ56ZHCEXB7'
    ENCODING = 'utf-8'
    ERP = 'CEZ'

    ordered_dict = order_files_by_size(zip_obj)
    db_stp_records = 0
    incoming_points = []
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
          
            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)

            dfs_csv_dict = {text_file.filename: pd.read_csv(inner_zip.open(text_file.filename,pwd=bytes(PASSWORD, ENCODING)),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.csv')}
           
            for key in dfs_csv_dict.keys():
                try:
                    df = dfs_csv_dict[key]

                except Exception as e:
                    print(f'File {key} was NOT proceeded .Exception message: {e}! \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}') 

                else:
                    insert_mrus(df, key, ERP)            

        elif file_name.endswith('.xlsx'):
            
            try:
                df = pd.read_excel(zip_obj.read(file_name))
                initial_rows_count = df.shape[0]               
                
                df.drop(['DD.MM.YYYY hh:mm','Име на Клиент, ЕСО:','Сетълмент период:'], axis=1, inplace = True)
                
                df_cols = df.columns[1:]                
                df_cols = [x.replace('.','/') if(isinstance(x,str) and ('.' in x)) else x for x in df_cols]
                
                s_date = df_cols[0] if isinstance(df_cols[0], dt.date) else dt.datetime.strptime(df_cols[0], '%d/%m/%Y %H:%M')
                e_date = df_cols[-1] if isinstance(df_cols[-1], dt.date) else dt.datetime.strptime(df_cols[-1], '%d/%m/%Y %H:%M')
                
                time_series = pd.date_range(start = s_date - dt.timedelta(hours = 1), end = e_date - dt.timedelta(hours =1), tz = 'EET', freq='h')
               
                df.columns = time_series.insert(0,df.columns[0])
                
                df = pd.melt(df, id_vars=['Уникален Идентификационен Номер:'], var_name = ['utc'], value_name = 'consumption_vol')
                df.rename(columns={'Уникален Идентификационен Номер:': 'itn'}, inplace = True)
                df.set_index(pd.DatetimeIndex(df['utc']), inplace = True)
                df.drop(columns= 'utc', inplace = True)

            except Exception as e:
                print(f'File {file_name} was NOT proceeded .Exception message: {e}! \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}')  

            else: 
                try:
                    df.index = df.index.tz_convert('UTC').tz_convert(None)

                except Exception as e:
                    print(f'Exception from cez hourly loading: {e} \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}')

                else:
                    if(not df.empty):
                        df.reset_index(inplace = True)
                        incoming_itns = df['itn'].tolist() 
                        incoming_points += incoming_itns                      
                        
                        min_date = min(df['utc']).to_pydatetime()                    
                        max_date = max(df['utc']).to_pydatetime() 

                        incoming_stp_records = get_incoming_stp_records(df,min_date, max_date)

                        if (len(incoming_stp_records) > 0) & (len(incoming_stp_records) >= (initial_rows_count // 2)):
                            
                            if db_stp_records == 0:
                                db_stp_records = get_stp_from_db('CEZ', min_date, max_date)                              
                                # get_missing_points(incoming_stp_records, db_stp_records)
       
                            # get_extra_points(incoming_stp_records, db_stp_records)

                            stp_records_df = pd.DataFrame.from_records(incoming_stp_records, columns=incoming_stp_records[0].keys()) 
                                             
                            update_stp_settelment_vol(df, stp_records_df, incoming_stp_records) 
                            update_stp_consumption_vol(stp_records_df, min_date, max_date)  

                        else:
                            db_non_stp_records = get_non_stp_from_db('CEZ', min_date, max_date)
                            incoming_non_stp_records = get_incoming_non_stp_records(df,min_date, max_date)
                            # get_missing_points(incoming_non_stp_records, db_non_stp_records)
                            # get_extra_points(incoming_non_stp_records, db_non_stp_records)

                            update_non_stp_consumption_settelment_vol(df, min_date, max_date) 

    get_missing_extra_points_by_erp(ERP, incoming_points)


def insert_settlment_e_pro(zip_obj, separator):

    ERP = 'E-PRO'
    ordered_dict = order_files_by_date(zip_obj)
    incomming_points = []
    # incoming_stp_itns_list = []
    for date_created, file_name in ordered_dict.items():
        if file_name.endswith('.zip'):
            
            # print(file_name, file = sys.stdout)
            
            inner_zfiledata = BytesIO(zip_obj.read(file_name))
            inner_zip =  ZipFile(inner_zfiledata)
         
            dfs = {text_file.filename: pd.read_excel(inner_zip.read(text_file.filename))
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.xlsx')}
            
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
                # print(f'E PRO csv reading {file_name}')
                df = pd.read_csv(zip_obj.open(file_name),sep=separator,  encoding="cp1251", engine='python',skiprows = 1)

            except Exception as e:
                print(f'File {file_name} was NOT proceeded .Exception message: {e}! \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}') 

            else:
                insert_mrus(df, file_name, ERP) 
                # print(f'from e pro csv reading ----- >\n{df}')

        elif file_name == '021CIN03.xlsx':
            
            incoming_stp_itns_list = proceed_e_pro_stp_excel_file(zip_obj, file_name)
            # print(f'incoming_stp_itns_list\n{incoming_stp_itns_list}')
            
            # print(f'incomming_points\n{incomming_points}')
            

    incomming_points = [x[0] for x in incomming_points if len(x) > 0]
    incomming_points += incoming_stp_itns_list
    get_missing_extra_points_by_erp(ERP, incomming_points)



    # print(f'FROM NON stp E-Pro')
    
    # incomming_points = [x[0] for x in incomming_points if len(x) > 0]
    # # print(f'INCOMING POINTS -----------------------> \n{incomming_points}')
    # db_non_stp_records = get_non_stp_from_db('E-PRO', min_date, max_date)
    # get_missing_points(incomming_points, db_non_stp_records)
    # get_extra_points(incomming_points, db_non_stp_records)

def insert_settlment_evn(zip_obj,separator):
    
    PASSWORD = '8yc#*3-Q5ADt'
    # PASSWORD = '79+Kg+*rLA7P'
    ENCODING = 'utf-8'
    ERP = 'EVN'
    
    ordered_dict = order_files_by_size(zip_obj)
    distribution_stp_records = 0
    distribution_non_stp_records = 0
    incoming_non_stp_records = []
    incomming_points = []
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
                    insert_mrus(df, key, ERP)
            
            dfs_dict = {text_file.filename: pd.read_excel(inner_zip.read(text_file.filename,pwd=bytes(PASSWORD, ENCODING)))
            for text_file in inner_zip.infolist() if text_file.filename.endswith('.xlsx')}

            for key in dfs_dict.keys():
                try:
                    df = dfs_dict[key]
                    df.columns = df.columns.str.strip()
                    ITN = df.iloc[:1].values[0][0] 
                    
                    df = df.rename(columns={'Гранд Енерджи Дистрибюшън ЕООД':'1','Unnamed: 1':'2', 'Unnamed: 2':'3'})
                    # df = df.rename(columns={'Юропиан Трейд Оф Енерджи АД':'1','Unnamed: 1':'2', 'Unnamed: 2':'3'})
                    df_for_db = create_db_df_eepro_evn(df, ITN) 
                    # print(f'df for db \n{df_for_db}')                   
                    if(not df_for_db.empty):  
                        min_date = min(df_for_db['utc']).to_pydatetime()                    
                        max_date = max(df_for_db['utc']).to_pydatetime()
                        # print(f'min max date {min_date} --- {max_date} ---- {df_for_db.iloc[0].itn}')                         
                        
                        if distribution_stp_records == 0:

                            db_stp_records = get_stp_from_db(ERP, min_date, max_date)                            
                            
                            db_non_stp_records = get_non_stp_from_db(ERP, min_date, max_date)
                           
                            # min_date_inv = min_date.replace(min_date.year, min_date.month, 11,0,0,0)
                            # max_date_inv = max_date.replace(max_date.year, max_date.month + 1, 10,23,0,0) 

                            invoice_start_date = min_date + dt.timedelta(hours = (10 * 24 + 1))
                            invoice_end_date = max_date + dt.timedelta(hours = (10 * 24))

                            # print(f'invoice_start_date invoice_end_date date {invoice_start_date} --- {invoice_end_date} ---- {df_for_db.iloc[0].itn}') 

                            distribution_stp_records = get_distribution_stp_records(ERP,min_date,max_date)

                            stp_records_df = pd.DataFrame.from_records(distribution_stp_records, columns=distribution_stp_records[0].keys())
                            
                            update_stp_consumption_vol(stp_records_df, min_date, max_date, True) 

                            distribution_non_stp_records = get_distribution_non_stp(ERP,invoice_start_date, invoice_end_date)
                            # print(f'stp records df \n{stp_records_df}')

                        if(not df_for_db.empty):
                            incoming_non_stp_records.append(list(zip(set(df_for_db.itn), )))               

                            applicable_subcontracts = get_subcontracts_by_itn_and_utc_dates(df_for_db.iloc[0]['itn'], min_date, max_date)

                            for subcontarct in applicable_subcontracts:
                                print(f'$$$$$$$$$$$$$$$ applicable_subcontracts  $$$$$$$$$$$$$$$$\n{subcontarct}')
                                partial_df = df_for_db[((df_for_db.utc >= subcontarct.start_date) & (df_for_db.utc <= subcontarct.end_date))].copy()
                                print(f'$$$$$$$$$$$$$$$ applicable_subcontracts -- partial_df $$$$$$$$$$$$$$$$\n{partial_df}')
                                # incoming_non_stp_records.append(list(zip(set(partial_df.itn), )))
                                if len(incomming_points) == 0:
                                    incomming_points = partial_df.itn
                                else:
                                    [y for x in [incomming_points, partial_df.itn] for y in x]
                                    # incomming_points += partial_df.itn
                                update_non_stp_consumption_settelment_vol(partial_df, subcontarct.start_date, subcontarct.end_date)
                   
                    else:
                        print('Values in file ', key, ' was only 0 !')
        
                except Exception as e:
                    print(f'Exception from EVN xlsx upload. File {key} was NOT proceeded .Exception message: {e}! \n  {df_for_db} \n Exception at row --->{print(sys.exc_info()[2].tb_lineno)}') 

    

    incoming_non_stp_records = [x[0] for x in incoming_non_stp_records if len(x) > 0]
    [y for x in [incomming_points, incoming_non_stp_records] for y in x]
    # incomming_points += incoming_non_stp_records
    get_missing_extra_points_by_erp(ERP, incomming_points)

    # get_missing_points(distribution_stp_records, db_stp_records)  
    # get_extra_points(distribution_stp_records, db_stp_records)
    # get_missing_points(incoming_non_stp_records, db_non_stp_records)  
    # get_extra_points(incoming_non_stp_records, db_non_stp_records)

def get_missing_points(incoming_records, db_records):

    incoming_records = set([x[0] for x in incoming_records])
    db_records = set([x[0] for x in db_records])
    res = db_records - incoming_records
    print(f'This itn points are in the database but not came data for them from ERP files ---> {res}')

    return res

def get_extra_points(incoming_records, db_records):

    incoming_records = set([x[0] for x in incoming_records])
    db_records = set([x[0] for x in db_records])
    res = incoming_records - db_records
    print(f'This itn points are NOT in the database but came EXTRA data for them from ERP files ---> {res}')

    return res

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
            # .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
            .filter( ~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
            .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ErpInvoice.date >= start_date, ErpInvoice.date <= end_date)
            .filter(Erp.name == erp_name)
            .distinct()
            .all()
    )
    
    end = time.time()
    # print(f'Time elapsed for get_distribution_non_stp : {end - start}  !')
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
            .filter( ~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ErpInvoice.date >= start_date, ErpInvoice.date <= end_date)
            .filter(Erp.name == erp_name)
            .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)
            .distinct(SubContract.itn)
            .all()
    )
    
    end = time.time()
    # print(f'Time elapsed for get_distribution_stp : {end - start}  !')
    return  distribution_stp_records

def get_incoming_stp_records(input_df,min_date, max_date):   
    start = time.time()
    stp_records = (
        db.session 
            .query(SubContract.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id')) 
            .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
            .join(MeasuringType) 
            .filter(SubContract.itn.in_(input_df.itn)) 
            .filter( ~((SubContract.start_date > max_date) | (SubContract.end_date < min_date)))
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date) 
            .distinct(SubContract.itn)
            .all()
    )
    
    end = time.time()
    # print(f'Time elapsed for create stp_records : {end - start}  !')
    return  stp_records

def get_incoming_non_stp_records(input_df,min_date, max_date):   
    start = time.time()
    stp_records = (
        db.session 
            .query(SubContract.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id')) 
            .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
            .join(MeasuringType) 
            .filter(SubContract.itn.in_(input_df.itn)) 
            .filter( ~((SubContract.start_date > max_date) | (SubContract.end_date < min_date)))
            .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date) 
            .distinct(SubContract.itn)
            .all()
    )
    
    end = time.time()
    # print(f'Time elapsed for create stp_records : {end - start}  !')
    return  stp_records


def resolve_poins_colision(input_df, min_date, max_date):
    
    colision_records = (
        db.session 
            .query(SubContract.itn, ItnSchedule.tariff_id, MeasuringType.id.label('measuring_id')) 
            .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
            .join(MeasuringType) 
            .filter(SubContract.itn.in_(input_df.itn)) 
            .filter( ~((SubContract.start_date > max_date) | (SubContract.end_date < min_date)))
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT')))             
            .filter(ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date) 
            .distinct(SubContract.itn)
            .all()
    )
    if len(colision_records) ==  0:
        return []

    invoice_start_date = min_date + dt.timedelta(hours = (10 * 24 + 1))   
    invoice_end_date = max_date + dt.timedelta(hours = (10 * 24))            
          
    colision_point_non_stp_df = pd.DataFrame.from_records(colision_records, columns=colision_records[0].keys())

    colision_points = [x[0] for x in colision_records]
    print(f'Colision points {colision_points}')

    for colision_point in colision_points:
        
        temp_input_df = input_df[(input_df['itn'] == colision_point) & (~pd.isnull(input_df['consumption_vol']))]       

        if colision_point_non_stp_df.empty:
            continue
        curr_subcontract = (SubContract.query.filter(SubContract.itn == temp_input_df.iloc[0]['itn']) 
                                                        .filter( ~((SubContract.start_date > max_date) | (SubContract.end_date < min_date)))
                                                        .first())

      
        stp_consumption_aproximation(input_df, colision_point, min_date, max_date)

        temp_input_df = temp_input_df.merge(colision_point_non_stp_df, on = 'itn', how = 'left')
        temp_input_df['settelment_vol'] = temp_input_df['consumption_vol']
        
        temp_input_df.drop(columns = 'measuring_id', inplace = True)         
        removed_nan_df = temp_input_df.fillna(0).copy()
        # removed_nan_df = removed_nan_df.replace(to_replace = 'nan',value = '0')
        # print(f'removed_nan_df \n{removed_nan_df}')
        update_reported_volume(removed_nan_df, ItnSchedule.__table__.name)
        generate_provisional_subcontract(temp_input_df, curr_subcontract)

    return colision_points

def stp_consumption_aproximation(input_df, colision_point, min_date, max_date):

    
    non_stp_input_df = input_df[(input_df['itn'] == colision_point) & (~pd.isnull(input_df['consumption_vol']))]    

    invoice_start_date = min_date + dt.timedelta(hours = (10 * 24 + 1))
    invoice_end_date = max_date + dt.timedelta(hours = (10 * 24))    
    
    stp_last_date = dt.datetime.strptime(str(non_stp_input_df.iloc[0]['utc']), "%Y-%m-%d %H:%M:%S") - dt.timedelta(hours = 1)
    
    print(f'stp_last_date --- > {stp_last_date}')
    total_consumption_record = (
        db.session
            .query(Distribution.itn.label('itn'), 
                func.sum(Distribution.calc_amount).label('total_consumption')) 
            
            .join(ErpInvoice, ErpInvoice.id == Distribution.erp_invoice_id)   
            .filter(Distribution.itn == colision_point)      
            .filter(Distribution.tariff.in_(['Достъп','Пренос през електропреносната мрежа', 'Разпределение'])) 
            .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date) 
            .group_by(Distribution.itn)
            .first()
    )
    total_consumption = total_consumption_record.total_consumption 

    if len(total_consumption_record) > 0:                  
        consumption_per_hour = total_consumption / Decimal(get_hours_between_dates(stp_last_date, max_date))

        interpolated_monthly_consumption = Decimal(get_hours_between_dates(min_date, max_date)) * consumption_per_hour

        stp_records = (
            db.session
                .query(ItnSchedule.itn,
                    ItnSchedule.utc,
                    ItnSchedule.forecast_vol,
                    ItnSchedule.consumption_vol,
                    ItnSchedule.price,
                    ItnSchedule.settelment_vol,
                    ItnSchedule.tariff_id)                
                .filter(ItnSchedule.itn == colision_point, ItnSchedule.utc >= min_date, ItnSchedule.utc <= max_date)                 
                .all()
        )

        if len(stp_records)> 0:
            stp_input_df = pd.DataFrame.from_records(stp_records, columns=stp_records[0].keys()) 
            stp_input_df = stp_input_df[(stp_input_df['utc'] >= min_date) & (stp_input_df['utc'] <= stp_last_date)]
            stp_input_df['consumption_vol'] = (stp_input_df['consumption_vol'] / total_consumption) * interpolated_monthly_consumption
            update_reported_volume(stp_input_df, ItnSchedule.__table__.name)
            # print(f'total_consumption_df -----------> \n{stp_input_df}')
        
        

def get_hours_between_dates(start_date, end_date):

    diff = end_date - start_date
    days, seconds = diff.days, diff.seconds
    hours = days * 24 + seconds // 3600
    return hours 



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

        update_reported_volume(non_stp_df, ItnSchedule.__table__.name)

def update_stp_settelment_vol(input_df, stp_records_df, stp_records):
                         
    stp_df = input_df[input_df['itn'].isin([x[0] for x in stp_records])]
    stp_df = stp_df.merge(stp_records_df, on = 'itn', how = 'left') 
    stp_df.rename(columns = {'consumption_vol':'settelment_vol'}, inplace = True)  
    stp_df.drop(columns = 'measuring_id', inplace = True)  
    stp_df = stp_df.fillna(0)
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
        # print(f'@@@@@@@@@@@@@@@@@@@ TOTAL CONSUMPTION DF @@@@@@@@@@@@@@@@@@ \n {total_consumption_df}')
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
            # print(f'dates ---> {invoice_start_date}   {invoice_end_date}')
            # print(f'@@@@@@@@@@@@@@@@@@@ UPDATE DF @@@@@@@@@@@@@@@@@@ \n {stp_df}')
            stp_df = stp_df.fillna(0)
            update_reported_volume(stp_df, ItnSchedule.__table__.name)
            

    #     else:
    #         print(f'stp coefs records is 0')
    # else:
    #     print(f'stp total consumption is 0')


                    
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
        a = []
        return a

    else:
        summary_df.rename(columns = {'Уникален номер':'itn'}, inplace = True)
        # stp_records = incoming_stp_records(summary_df, s_date, e_date)
        incoming_stp_records = get_incoming_stp_records(summary_df,s_date, e_date)
                        
        if len(incoming_stp_records) > 0:  
            # print(f'FROM stp E-Pro')                      
            stp_records_df = pd.DataFrame.from_records(incoming_stp_records, columns=incoming_stp_records[0].keys()) 
            db_stp_records = get_stp_from_db('E-PRO', s_date, e_date)                              
            # get_missing_points(incoming_stp_records, db_stp_records)
            # get_extra_points(incoming_stp_records, db_stp_records)
            # a = summary_df[summary_df.itn == '32Z4800110120529']
            # print(f'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ stp_records_df  \n{a}')

            update_stp_settelment_vol(summary_df, stp_records_df, incoming_stp_records)             
            update_stp_consumption_vol(stp_records_df, s_date, e_date) 
        
        return summary_df['itn'].tolist()               


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
            # .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
            .filter( ~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(Erp.name == erp_name)
            .distinct(SubContract.itn)
            .all()
    )
    
    end = time.time()
    # print(f'Time elapsed for get_stp_from_db : {end - start}  !')
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
            # .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
            .filter( ~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
            .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
            .filter(Erp.name == erp_name)
            .distinct(SubContract.itn)
            .all()
    )
    
    end = time.time()
    # print(f'Time elapsed for get_non_stp_from_db : {end - start}  !')
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
            # print(f'inner ZIP ---> {inner_zip.infolist()[0]}')
            if inner_zip.infolist()[0].filename.endswith('.csv'):
                csv_dict[k] = v
            else:
                non_csv_dict[k] = v

        csv_dict.update(non_csv_dict)





    # for k, v in csv_dict.items(): print(f' key: {k} ----> value {v}', file = sys.stdout)
    for k, v in csv_dict.items(): print(f' key: {k} ----> value {v}', file = sys.stdout)
    return csv_dict

def update_reported_volume(df, table_name):
    
    # print(f'########################## ENTERING UPDATE REPORTED VOLUME #############################')
    start = time.time()

    stringifyer(df)
    bulk_update_list = df.to_dict(orient='records')    
    db.session.bulk_update_mappings(ItnSchedule, bulk_update_list)
    db.session.commit()
    end = time.time()
    # print(f'Time elapsed for bulk update is : {end - start} for {df.shape[0]} rows and {df.shape[1]} columns !') 
   
    # print(f'########################## Finiiiiiiish UPDATE REPORTED VOLUME #############################')

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

def get_list_all_itn_in_db_by_erp(erp):

    itn_records = db.session.query(ItnMeta.itn).join(Erp).filter(Erp.name == erp).distinct(ItnMeta.itn).all()
    itn_list = [x[0] for x in itn_records]
    return itn_list
    
def get_missing_extra_points_by_erp(erp, incoming_itns):

    db_itns = get_list_all_itn_in_db_by_erp(erp)
    db_itn_set = set(db_itns)
    incoming_itns = set(incoming_itns)
    missing = list(db_itn_set - incoming_itns)
    print(f'This itn points are in the database but not came data for them from ERP: {erp} files ---> {missing}')
    extra = list(incoming_itns - db_itn_set)
    print(f'This itn points are NOT in the database but came data for them from ERP: {erp} files ---> {extra}')




    




