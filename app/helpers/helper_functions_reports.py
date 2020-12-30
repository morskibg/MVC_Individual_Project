import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import *
from flask import  flash
from app.models import *  
from app import app
from app.helpers.helper_functions import (convert_date_to_utc,)                                 

from app.helpers.helper_functions_queries import (                                         
                                        get_grid_services_tech_records,                          
                                        get_grid_services_distrib_records,                                        
                                        get_stp_itn_by_inv_group_for_period_sub,
                                        get_stp_consumption_for_period_sub,
                                        get_non_stp_itn_by_inv_group_for_period_sub,
                                        get_non_stp_consumption_for_period_sub,
                                        get_itn_with_grid_services_sub,
                                        get_grid_services_sub,
                                        get_summary_records,
                                        is_spot_inv_group,
                                        get_summary_records_spot,
                                        get_contractors_names_and_411,
                                        get_stp_itn_by_inv_group_for_period_spot_sub,
                                        get_non_stp_itn_by_inv_group_for_period_spot_sub,
                                        get_all_stp_itn_by_inv_group_for_period_sub,
                                        get_spot_itns,
                                        get_spot_fin_results,
                                        get_tariff_limits,
                                        get_time_zone,
                                        get_list_inv_groups_by_contract,
                                        get_erp_consumption_records_by_grid,
                                        get_erp_money_records_by_grid,
                                        get_total_consumption_by_grid ,
                                        get_total_money_by_grid  )

from app.helpers.helper_function_excel_writer import (generate_ref_excel, generate_integra_file)



def create_utc_dates(inv_group_name, start_date, end_date):


    if isinstance(start_date, str):
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')

    if isinstance(end_date, str):
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')

    # start_date = dt.datetime.strptime(local_start_date, '%Y-%m-%d') if is  
    # end_date = dt.datetime.strptime(local_end_date, '%Y-%m-%d') 
    print(f'{start_date} - {end_date}')
    time_zone = get_time_zone(inv_group_name, start_date, end_date)
    if time_zone is None:
        return None,None,None,None
    # print(f'{inv_group_name} ---- {time_zone}')

    invoice_start_date = start_date + dt.timedelta(hours = (10 * 24 + 1))        
    invoice_start_date = convert_date_to_utc(time_zone, invoice_start_date)
    
    invoice_end_date = end_date + dt.timedelta(hours = (10 * 24))            
    invoice_end_date = convert_date_to_utc(time_zone, invoice_end_date)

    start_date = convert_date_to_utc(time_zone, start_date)
    end_date = convert_date_to_utc(time_zone, end_date) + dt.timedelta(hours = 23)

    return start_date, end_date, invoice_start_date, invoice_end_date

def get_grid_services(inv_group_name, start_date, end_date, invoice_start_date, invoice_end_date):    

    grid_itns = get_itn_with_grid_services_sub(inv_group_name,start_date, end_date)

    grid_services_sub = get_grid_services_sub(grid_itns, invoice_start_date, invoice_end_date) 

    grid_services_tech_records = get_grid_services_tech_records(grid_itns, invoice_start_date, invoice_end_date)
    grid_services_distrib_records = get_grid_services_distrib_records(grid_itns, invoice_start_date, invoice_end_date)             

    grid_services_df = pd.DataFrame()
    if (len(grid_services_tech_records) == 0) :
        grid_services_df = pd.DataFrame(columns=['Абонат №', 'А д р е с', 'Име на клиент', 'ЕГН/ЕИК',
                                                'Идентификационен код', 'Електромер №', 'Отчетен период от',
                                                'Отчетен период до', 'Брой дни', 'Номер скала', 'Код скала',
                                                'Часова зона', 'Показания  ново', 'Показания старо', 'Разлика (квтч)',
                                                'Константа', 'Корекция (квтч)', 'Приспаднати (квтч)',
                                                'Общо количество (квтч)', 'Тарифа/Услуга', 'Количество (кВтч/кВАрч)',
                                                'Единична цена (лв./кВт/ден)/ (лв./кВтч)', 'Стойност (лв)',
                                                'Корекция към фактура', 'Основание за издаване'])
    else:    
        grid_services_tech_records_df = pd.DataFrame.from_records(grid_services_tech_records, columns = grid_services_tech_records[0].keys())
        grid_services_distrib_records_df = pd.DataFrame.from_records(grid_services_distrib_records, columns = grid_services_distrib_records[0].keys())
        grid_services_df = pd.concat([grid_services_tech_records_df,grid_services_distrib_records_df])
        grid_services_df = grid_services_df.sort_values(by='Идентификационен код', ascending=False, ignore_index=True)
        grid_services_df['Брой дни'] = grid_services_df['Брой дни'].apply(lambda x: x + dt.timedelta(days = 1))

    return grid_services_sub, grid_services_df


def get_weighted_price(inv_group_names, start_date, end_date):   

    spot_itns_sub = get_spot_itns(inv_group_names, start_date, end_date) 
    fin_res = get_spot_fin_results(spot_itns_sub, start_date, end_date)
    # print(f'fin res {fin_res}')
    if len(fin_res) ==  0:
        return fin_res

    fin_res_df = pd.DataFrame.from_records(fin_res, columns = fin_res[0].keys())
    print(f'res df {fin_res_df}')
    limits = get_tariff_limits(spot_itns_sub, start_date, end_date)
    if len(limits[0]) > 2:
        print(f'Wrong lower, upper limits count for this invoicing group: {inv_group_names} for period :{start_date} - {end_date}')
        return Decimal('0')
    else:
        lower_limit = Decimal(str(limits[0][0]))
        upper_limit = Decimal(str(limits[0][1]))
        # print(f'{lower_limit} --- {upper_limit} ')
        weighted_price = Decimal(fin_res_df['fin_res'].sum()) / Decimal(fin_res_df['total_consumption'].sum())
       

        if weighted_price.compare(lower_limit) == -1:            
            weighted_price = lower_limit

        elif ((weighted_price.compare(upper_limit) == 1) & (upper_limit.compare(Decimal('0')) != 0)):            
            weighted_price = upper_limit

        fin_res_df['Сума за енергия'] = fin_res_df['total_consumption'].apply(lambda x: Decimal(x) * weighted_price)
        # weighted_price = weighted_price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)        
        # return weighted_price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)
        print(f'weighted price = {weighted_price}')
        return weighted_price

def get_summary_df_non_spot(inv_group_name, start_date, end_date, invoice_start_date, invoice_end_date):
        
        grid_services_sub, grid_services_df = get_grid_services(inv_group_name, start_date, end_date, invoice_start_date, invoice_end_date)        
            
        ###################### create stp records ##############################################################       

        stp_non_spot_itns = get_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date)   

        stp_consumption_for_period_sub = get_stp_consumption_for_period_sub(stp_non_spot_itns, invoice_start_date, invoice_end_date)                

        summary_stp = get_summary_records(stp_consumption_for_period_sub, grid_services_sub, stp_non_spot_itns, start_date, end_date)
       
        # ###################### create non stp records ##############################################################
        non_stp_itns = get_non_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date)   
               
        non_stp_spot_consumption_for_period_sub = get_non_stp_consumption_for_period_sub(non_stp_itns, start_date, end_date)
        
        summary_non_stp = get_summary_records(non_stp_spot_consumption_for_period_sub, grid_services_sub, non_stp_itns, start_date, end_date)
        
        return summary_stp, summary_non_stp, grid_services_df
        
def get_summary_spot_df(inv_group_names, start_date, end_date, invoice_start_date, invoice_end_date, weighted_price):  

    if weighted_price is None:
        weighted_price = get_weighted_price(inv_group_names, start_date, end_date)  

    grid_services_sub, grid_services_df = get_grid_services(inv_group_names[0], start_date, end_date, invoice_start_date, invoice_end_date)    
    
    ###################### create stp records ##############################################################
   
    stp_spot_itns = get_stp_itn_by_inv_group_for_period_spot_sub(inv_group_names[0], start_date, end_date)   

    stp_consumption_for_period_sub = get_stp_consumption_for_period_sub(stp_spot_itns, invoice_start_date, invoice_end_date)                

    summary_records_stp_spot = get_summary_records_spot(stp_consumption_for_period_sub, grid_services_sub, stp_spot_itns, start_date, end_date)
    
    ###################### create non stp records ##############################################################
    non_stp_itns = get_non_stp_itn_by_inv_group_for_period_spot_sub(inv_group_names[0], start_date, end_date)   
            
    non_stp_spot_consumption_for_period_sub = get_non_stp_consumption_for_period_sub(non_stp_itns, start_date, end_date)
    
    summary_non_stp_spot = get_summary_records_spot(non_stp_spot_consumption_for_period_sub, grid_services_sub, non_stp_itns, start_date, end_date)    
    
    return summary_records_stp_spot, summary_non_stp_spot, grid_services_df, weighted_price
    
def appned_df(df, temp_df):
    if df.empty:
        df = temp_df
    else:
        df = df.append(temp_df, ignore_index=True)
    return df

# def create_excel_files(summary_stp, summary_non_stp, grid_services_df, start_date, end_date, invoice_start_date, invoice_end_date, invoice_ref_path = None, inetgra_src_path, weighted_price = None):
def create_excel_files(summary_stp, summary_non_stp, grid_services_df, start_date, end_date, invoice_start_date, invoice_end_date,  weighted_price = None):    
    df = pd.DataFrame()
    inv_group_str = None
    if len(summary_stp) != 0:
        try:
            temp_df = pd.DataFrame.from_records(summary_stp, columns = summary_stp[0].keys())                
                
        except Exception as e:
            print(f'Unable to proceed stp data for invoicing group {summary_stp[0][7]} - {summary_stp[0][8]}. Message is: {e}')

        else:
            inv_group_str = f'{summary_stp[0][7]} - {summary_stp[0][8]}'
            df = appned_df(df, temp_df)      
                
    if len(summary_non_stp) > 0:
        try:                     
            temp_df = pd.DataFrame.from_records(summary_non_stp, columns = summary_non_stp[0].keys())             

        except Exception as e:
            print(f'Unable to proceed data for invoicing group {summary_non_stp[0][7]} - {summary_non_stp[0][8]}. Message is: {e}')

        else:
            if inv_group_str is None:
                inv_group_str = f'{summary_non_stp[0][7]} - {summary_non_stp[0][8]}'
            df = appned_df(df, temp_df)        

    if df.empty:
        print(f'There is not any non spot itn in this invoicing group  ')
    else:
        df = df.drop_duplicates(subset='Обект (ИТН №)', keep = 'first')     
        df.insert(loc=0, column = '№', value = [x for x in range(1,df.shape[0] + 1)])  
        if weighted_price is not None:
            df['Сума за енергия'] = df['Потребление (kWh)'] * weighted_price
        
        ref_file_name = generate_ref_excel(df, grid_services_df, invoice_start_date, invoice_end_date, start_date, end_date)
        
        integra_df = df[df['make_invoice']]
        
        if integra_df.empty:
            print(f'{inv_group_str} doesn\'t create integra file !')
        else:
            generate_integra_file(integra_df, start_date, end_date, ref_file_name)
       

def create_report_from_grid(invoice_start_date, invoice_end_date):

    operators = ['CEZ','EVN','E-PRO']
    
    rows_list = []
    for erp in operators:

        erp_consumption_records = get_erp_consumption_records_by_grid(erp, invoice_start_date, invoice_end_date)              
        erp_money_records = get_erp_money_records_by_grid(erp, invoice_start_date, invoice_end_date)
        # print(f'{erp}\n{erp_consumption_records}\n{erp_money_records}') 
        try:       
            data_dict = {'Erp':erp, 'Consumption':erp_consumption_records[0][1], 'Value': erp_money_records[0][1] }
        except:
            print(f'exception from {erp}')
        else:
            rows_list.append(data_dict)                

    total_consumption_by_grid = get_total_consumption_by_grid(invoice_start_date, invoice_end_date)   
    total_sum_records = get_total_money_by_grid(invoice_start_date, invoice_end_date)     
    data_dict = {'Erp':'Total', 'Consumption':total_consumption_by_grid[0][0], 'Value': total_sum_records[0][0] }
    rows_list.append(data_dict)  
    report_df = pd.DataFrame(rows_list)
    date = invoice_start_date.month
    report_df.to_excel(f'app/static/reports/grid_report_for_month-{date}___{dt.date.today()}.xlsx')
    return report_df

def create_full_ref_for_all_itn(files):

    path = os.path.join(app.root_path, app.config['INV_REFS_PATH'])
    summary_df = pd.DataFrame()
    for curr_file in files:
        invoice_group_name = curr_file.split('_')
        invoice_group_name = f'{invoice_group_name[2]}_{invoice_group_name[3]}'
        try:           
            raw_df = pd.read_excel(os.path.join(path , curr_file))
            
        except:
            print(f'There is not file with name {curr_file} in {path}') 
        else:
            client = raw_df.iloc[5][raw_df.columns[0]].split(':')[1].strip()            
            columns = [x for x in raw_df.iloc[26] if str(x) != 'nan']
            data_df = raw_df.loc[28:]
            data_df = data_df.dropna(axis=1, how='all')
            data_df.columns = columns
            print(f'{columns}')
            data_df = data_df.assign(contragent = client)        
            data_df.loc[:,'invoice_group_name'] = invoice_group_name
            
            if summary_df.empty:
                summary_df = data_df
            else:
                summary_df= summary_df.append(data_df, ignore_index = True)
            print(f'{summary_df}')
            break
    power_sum = summary_df['Потребление (kWh)'].sum()
    money_sum = summary_df['Сума за енергия'].sum()
    zko_sum = summary_df['Задължение към обществото'].sum()
    grid_sum = summary_df['Мрежови услуги (лв.)'].sum()
    akciz_sum = summary_df['Акциз'].sum()
    total_sum = summary_df['Обща сума (без ДДС)'].sum()
    
    summary_df.loc[-1] = ['Total', '', '', power_sum, money_sum, zko_sum, grid_sum, akciz_sum, total_sum,'','']  # adding a row
    summary_df.index = summary_df.index + 1  # shifting index
    summary_df = summary_df.sort_index()  # sorting by index
    return summary_df

def log_writer(path, df, unique_columns):
    
    if df.empty:
        print(f'Empty dataframe is passed ! Nothing to record !')
    else:
        try:
            curr_file = pd.read_excel(path)
        except:
            print(f'There is not such a file: {path}. Creating !')
            df = df.sort_values(by=['erp','category','input_type','inv_description'], ascending=True)
            df.to_excel(path, index=False)
        else:
            
            if set(df.columns).issubset(set(curr_file.columns)):
                # print(f'in file append DF\n {df}')
                
                curr_file = curr_file.append(df, ignore_index = True)
                curr_file.drop_duplicates(subset = unique_columns , keep = 'last', inplace = True)
                curr_file = curr_file.sort_values(by=['erp','category','inv_description'], ascending=True)
                print(f'in file append curr_file\n {curr_file}')
                curr_file.to_excel(path, index=False)
            else:
                print(f'Columns mismatch ! \nPointed file has: {curr_file.columns}. Data to add has: {df.columns} columns !\nAborting !')

def create_inv_refs_by_inv_groups(inv_groups, start_date, end_date, weighted_price):
    counter = 0
    result_df = None
    # invoice_ref_path = inetgra_src_path = None
    for inv_group_name in inv_groups:
                
        # print(f'{inv_group_name}')
        start_date_utc, end_date_utc, invoice_start_date, invoice_end_date = create_utc_dates(inv_group_name, start_date, end_date)
        print(f'{inv_group_name}-{end_date_utc}-{invoice_start_date}-{invoice_end_date}')

        ibex_last_valid_date = (db.session.query(IbexData.utc, IbexData.price).filter(IbexData.price == 0).order_by(IbexData.utc).first()[0])

        if ibex_last_valid_date < dt.datetime.strptime(end_date, '%Y-%m-%d'):
            update_ibex_data(start_date, end_date)
            update_schedule_prices(start_date, end_date)

        if start_date_utc is None:
            print(f'There is not data for {inv_group_name}, for period {start_date} - {end_date}')
            continue

        is_spot = is_spot_inv_group([inv_group_name], start_date_utc, end_date_utc)
        
        if is_spot:
            counter += 1
            
            summary_stp, summary_non_stp, grid_services_df, weighted_price= get_summary_spot_df([inv_group_name], start_date_utc, end_date_utc, invoice_start_date, invoice_end_date, weighted_price)
            create_excel_files(summary_stp, summary_non_stp, grid_services_df, start_date_utc, end_date_utc, invoice_start_date, invoice_end_date, weighted_price)
            
        else:
            counter += 1
            
            summary_stp, summary_non_stp, grid_services_df= get_summary_df_non_spot([inv_group_name], start_date_utc, end_date_utc, invoice_start_date, invoice_end_date)
            create_excel_files(summary_stp, summary_non_stp, grid_services_df, start_date_utc, end_date_utc, invoice_start_date, invoice_end_date)

    return counter