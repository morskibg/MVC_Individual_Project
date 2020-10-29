import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
# from decimal import Decimal, ROUND_HALF_UP, compare
from decimal import *
from flask import  flash
from app.models import *  

from app.helper_functions import (convert_date_to_utc,)
                                 

from app.helper_functions_queries import (                                         
                                        get_grid_services_tech_records,                          
                                        get_grid_services_distrib_records,                                        
                                        get_stp_itn_by_inv_group_for_period_sub,
                                        get_stp_consumption_for_period_sub,
                                        get_non_stp_itn_by_inv_group_for_period_sub,
                                        get_non_stp_consumption_for_period_sub,
                                        get_itn_with_grid_services_sub,
                                        get_grid_services_sub,
                                        get_summary_records,
                                        # get_summary_records_non_stp,
                                        get_summary_records_spot,
                                        get_contractors_names_and_411,
                                        get_stp_itn_by_inv_group_for_period_spot_sub,
                                        get_non_stp_itn_by_inv_group_for_period_spot_sub,
                                        get_all_stp_itn_by_inv_group_for_period_sub,
                                        get_spot_itns,
                                        get_spot_fin_results,
                                        get_tariff_limits,
                                        get_time_zone,
                                        get_list_inv_groups_by_contract
                                        
                                        
)

from app.helper_function_excel_writer import (generate_excel,)



def create_utc_dates(inv_group_name, local_start_date, local_end_date):

    start_date = dt.datetime.strptime(local_start_date, '%Y-%m-%d')  
    end_date = dt.datetime.strptime(local_end_date, '%Y-%m-%d') 
    
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

    return grid_services_sub, grid_services_df

# def get_weighted_price(inv_group_name, start_date, end_date):

#     spot_itns_sub = get_spot_itns(inv_group_name, start_date, end_date)
#     res = get_spot_fin_results(spot_itns_sub, start_date, end_date)
#     res_df = pd.DataFrame.from_records(res, columns = res[0].keys())
#     limits = get_tariff_limits(inv_group_name, start_date, end_date)
#     if len(limits[0]) > 2:
#         print(f'Wrong lower, upper limits count for this invoicing group: {inv_group_name} for period :{start_date} - {end_date}')
#         return Decimal('0')
#     else:
#         lower_limit = Decimal(str(limits[0][0]))
#         upper_limit = Decimal(str(limits[0][1]))
#         # print(f'{lower_limit} --- {upper_limit} ')
#         weighted_price = Decimal(res_df['fin_res'].sum()) / Decimal(res_df['total_consumption'].sum())
       

#         if weighted_price.compare(lower_limit) == -1:            
#             weighted_price = lower_limit

#         elif ((weighted_price.compare(upper_limit) == 1) & (upper_limit.compare(Decimal('0')) != 0)):            
#             weighted_price = upper_limit

#         res_df['Сума за енергия'] = res_df['total_consumption'].apply(lambda x: Decimal(x) * weighted_price)
#         # weighted_price = weighted_price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)        
#         # return weighted_price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)
#         return weighted_price

def get_weighted_price(inv_group_names, start_date, end_date, internal_id = None):   

    
    if internal_id is not None:   
        inv_group_names = get_list_inv_groups_by_contract(internal_id, start_date, end_date)

    spot_itns_sub = get_spot_itns(inv_group_names, start_date, end_date) 
    fin_res = get_spot_fin_results(spot_itns_sub, start_date, end_date)
    if len(fin_res) ==  0:
        return fin_res

    res_df = pd.DataFrame.from_records(fin_res, columns = res[0].keys())
    # print(f'res df {res_df}')
    limits = get_tariff_limits(spot_itns_sub, start_date, end_date)
    if len(limits[0]) > 2:
        print(f'Wrong lower, upper limits count for this invoicing group: {inv_group_name} for period :{start_date} - {end_date}')
        return Decimal('0')
    else:
        lower_limit = Decimal(str(limits[0][0]))
        upper_limit = Decimal(str(limits[0][1]))
        # print(f'{lower_limit} --- {upper_limit} ')
        weighted_price = Decimal(res_df['fin_res'].sum()) / Decimal(res_df['total_consumption'].sum())
       

        if weighted_price.compare(lower_limit) == -1:            
            weighted_price = lower_limit

        elif ((weighted_price.compare(upper_limit) == 1) & (upper_limit.compare(Decimal('0')) != 0)):            
            weighted_price = upper_limit

        res_df['Сума за енергия'] = res_df['total_consumption'].apply(lambda x: Decimal(x) * weighted_price)
        # weighted_price = weighted_price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)        
        # return weighted_price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)
        # print(f'weighted price = {weighted_price}')
        return weighted_price

def get_summary_df_non_spot(inv_group_name, start_date, end_date, invoice_start_date, invoice_end_date):
        
        grid_services_sub, grid_services_df = get_grid_services(inv_group_name, start_date, end_date, invoice_start_date, invoice_end_date)        
            
        ###################### create stp records ##############################################################       

        stp_non_spot_itns = get_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date)   

        stp_consumption_for_period_sub = get_stp_consumption_for_period_sub(stp_non_spot_itns, invoice_start_date, invoice_end_date)                

        summary_stp = get_summary_records(stp_consumption_for_period_sub, grid_services_sub, stp_non_spot_itns, start_date, end_date)
        # print(f'from get summmary --- summary_stp \n {summary_stp}')
        # ###################### create non stp records ##############################################################
        non_stp_itns = get_non_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date)   
               
        non_stp_spot_consumption_for_period_sub = get_non_stp_consumption_for_period_sub(non_stp_itns, start_date, end_date)
        
        summary_non_stp = get_summary_records(non_stp_spot_consumption_for_period_sub, grid_services_sub, non_stp_itns, start_date, end_date)
        # print(f'from get summmary --- summary_non_stp \n {summary_non_stp}')
        # print(f'ssssssssssssssssssssssssssssssssssssssssssssss \n{summary_non_stp}')
        #############################################################################################################
        
        df = pd.DataFrame()
        if len(summary_stp) != 0:
            try:
                temp_df = pd.DataFrame.from_records(summary_stp, columns = summary_stp[0].keys())                
                 
            except Exception as e:
                print(f'Unable to create grid service dataframe for invoicing group {form.invoicing_group.data[0].name} for period {start_date} - {end_date}. Message is: {e}')

            else:
                if df.empty:
                    df = temp_df
                else:
                    df = df.append(temp_df, ignore_index=True) 
        try: 
            if len(summary_non_stp) > 0:           
                temp_df = pd.DataFrame.from_records(summary_non_stp, columns = summary_non_stp[0].keys())  
                    
                # print(f'from Non STP shape = {temp_df.shape[0]}')
                if df.empty:
                    df = temp_df
                else:
                    df = df.append(temp_df, ignore_index=True) 

        except Exception as e:
            print(f'Unable to proceed data for invoicing group {form.invoicing_group.data[0].name} for period {start_date} - {end_date}. Message is: {e}')

        else:
            df = df.drop_duplicates(subset='Обект (ИТН №)', keep = 'first')  
        
        df.insert(loc=0, column = '№', value = [x for x in range(1,df.shape[0] + 1)])  
        if df.empty:
            print(f'There is not any non spot itn in this invoicing group : {inv_group_name} ')
        else:
        # # df.to_excel('temp/burgas.xlsx')  
            print(f'{df}')    
            generate_excel(df, grid_services_df, invoice_start_date, invoice_end_date, start_date, end_date)
       

def get_summary_spot_df(inv_group_names, start_date, end_date, invoice_start_date, invoice_end_date):    

    grid_services_sub, grid_services_df = get_grid_services(inv_group_names[0], start_date, end_date, invoice_start_date, invoice_end_date)   
   
    weighted_price = get_weighted_price(inv_group_names, start_date, end_date)
    ###################### create stp records ##############################################################
   
    stp_spot_itns = get_stp_itn_by_inv_group_for_period_spot_sub(inv_group_names[0], start_date, end_date)   

    stp_consumption_for_period_sub = get_stp_consumption_for_period_sub(stp_spot_itns, invoice_start_date, invoice_end_date)                

    summary_records_stp_spot = get_summary_records_spot(stp_consumption_for_period_sub, grid_services_sub, stp_spot_itns, start_date, end_date)
    
    ###################### create non stp records ##############################################################
    non_stp_itns = get_non_stp_itn_by_inv_group_for_period_spot_sub(inv_group_names[0], start_date, end_date)   
            
    non_stp_spot_consumption_for_period_sub = get_non_stp_consumption_for_period_sub(non_stp_itns, start_date, end_date)
    
    summary_non_stp_spot = get_summary_records_spot(non_stp_spot_consumption_for_period_sub, grid_services_sub, non_stp_itns, start_date, end_date)
    
    #############################################################################################################
    
    df = pd.DataFrame()
    if len(summary_records_stp_spot) != 0:
        try:
            temp_df = pd.DataFrame.from_records(summary_records_stp_spot, columns = summary_records_stp_spot[0].keys())                
                
        except Exception as e:
            print(f'Unable to create grid service dataframe for invoicing group {form.invoicing_group.data[0].name} for period {start_date} - {end_date}. Message is: {e}')

        else:
            if df.empty:
                df = temp_df
            else:
                df = df.append(temp_df, ignore_index=True) 
    try: 
        if len(summary_non_stp_spot) > 0:           
            temp_df = pd.DataFrame.from_records(summary_non_stp_spot, columns = summary_non_stp_spot[0].keys())  
                
            # print(f'from Non STP shape = {temp_df.shape[0]}')
            if df.empty:
                df = temp_df
            else:
                df = df.append(temp_df, ignore_index=True) 

    except Exception as e:
        print(f'Unable to proceed data for invoicing group {form.invoicing_group.data[0].name} for period {start_date} - {end_date}. Message is: {e}')

    else:
        df = df.drop_duplicates(subset='Обект (ИТН №)', keep = 'first')  
    
      
    if df.empty:
        print(f'There is not any non spot itn in this invoicing group : {inv_group_names[0]} .Probably missing data from erp for {[x[0] for x in db.session.query(stp_spot_itns).all()]}')
    else:
    # # df.to_excel('temp/burgas.xlsx')
        df.insert(loc=0, column = '№', value = [x for x in range(1,df.shape[0] + 1)])
        df['Сума за енергия'] = df['Потребление (kWh)'] * weighted_price
        # df['Сума за енергия'] = df['Сума за енергия'].apply(lambda x: x.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)) 
          
        generate_excel(df, grid_services_df, invoice_start_date, invoice_end_date, start_date, end_date)
       



def create_report_from_grid(invoice_start_date, invoice_end_date):

    operators = ['CEZ','EVN','E-PRO']
    
    rows_list = []
    for erp in operators:

        erp_consumption_records = get_erp_consumption_records_by_grid(erp, invoice_start_date, invoice_end_date)              
        erp_money_records = get_erp_money_records_by_grid(erp, invoice_start_date, invoice_end_date)        
        data_dict = {'Erp':erp, 'Consumption':erp_consumption_records[0][1], 'Value': erp_money_records[0][1] }
        rows_list.append(data_dict)                

    total_consumption_by_grid = get_total_consumption_by_grid(invoice_start_date, invoice_end_date)   
    total_sum_records = get_total_money_by_grid(invoice_start_date, invoice_end_date)     
    data_dict = {'Erp':'Total', 'Consumption':total_consumption_by_grid[0][0], 'Value': total_sum_records[0][0] }
    rows_list.append(data_dict)  
    report_df = pd.DataFrame(rows_list)
    date = invoice_start_date.month
    # report_df.to_excel(f'app/static/reports/grid_report_for_month-{date}___{dt.date.today()}.xlsx')
    return report_df