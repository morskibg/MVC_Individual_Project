import sys, pytz, datetime as dt
import calendar
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
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
                                        get_summary_records_non_stp,
                                        get_contractors_names_and_411,
                                        get_summary_records_aggr
                                        
)

GOODES_CODE = {'Сума за енергия':'304-1', 'Мрежови услуги (лв.)':'498-56','Задължение към обществото':'459-2','Акциз':'456-1'}
PRICES = {'304-1':'price', '498-56':'Мрежови услуги (лв.)','459-2':'zko','456-1':'akciz'}

# def get_price_for_each_type(good_type,electricity, zko, akciz, grid):

#     if good_type == '304-1':
#         return electricity
#     elif good_type = ''

def generate_num_and_name(first_digit, num_411, inv_group, name):
    
    prefix = num_411.rsplit('-',1)[1]    
    suffix = inv_group.rsplit('_',1)[1]
    prefix_zeroes = (5-len(prefix)) * '0'
    suffix_zeroes = (3-len(suffix)) * '0'
    num_str = f'{first_digit}{prefix_zeroes}{prefix}{suffix_zeroes}{suffix}'
    name_str =  name + ' ' + str(suffix) if suffix != '0' else name
            
    return (num_str, name_str)



def create_invoicing_reference(inv_groups, start_date, end_date):

    final_df = pd.DataFrame()

    for inv_group in inv_groups:

        curr_contract = db.session.query(Contract).join(Contractor).join(SubContract).join(InvoiceGroup).filter(InvoiceGroup.name == inv_group.name).filter(SubContract.start_date <= start_date, SubContract.end_date > start_date).first()
        # to do --- if is there more than one subcontract  ?!?!?!?!

        time_zone = TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code
        
        invoice_start_date = start_date + dt.timedelta(hours = (10 * 24 + 1))        
        invoice_start_date = convert_date_to_utc(time_zone, invoice_start_date)

        invoice_end_date = end_date + dt.timedelta(hours = (10 * 24))            
        invoice_end_date = convert_date_to_utc(time_zone, invoice_end_date)     

        grid_itns = get_itn_with_grid_services_sub(inv_group.name,start_date, end_date)

        grid_services_sub = get_grid_services_sub(grid_itns, invoice_start_date, invoice_end_date) 
        
        ###################### create stp records ##############################################################
        stp_itns = get_stp_itn_by_inv_group_for_period_sub(inv_group.name, start_date, end_date)

        stp_consumption_for_period_sub = get_stp_consumption_for_period_sub(stp_itns, invoice_start_date, invoice_end_date)        

        summary_stp = get_summary_records(stp_consumption_for_period_sub, grid_services_sub, stp_itns, start_date, end_date)

        ###################### create non stp records ##############################################################
        non_stp_itns = get_non_stp_itn_by_inv_group_for_period_sub(inv_group.name, start_date, end_date)

        non_stp_consumption_for_period_sub = get_non_stp_consumption_for_period_sub(non_stp_itns, start_date, end_date)

        summary_non_stp = get_summary_records(non_stp_consumption_for_period_sub, grid_services_sub, non_stp_itns, start_date, end_date)
        #############################################################################################################
        df = pd.DataFrame()
        if len(summary_stp) != 0:
            try:
                temp_df = pd.DataFrame.from_records(summary_stp, columns = summary_stp[0].keys())                

            except Exception as e:
                print(f'Unable to create grid service dataframe for invoicing group {form.invoicing_group.data.name} for period {start_date} - {end_date}. Message is: {e}')

            else:
                if df.empty:
                    df = temp_df
                else:
                    df = df.append(temp_df, ignore_index=True) 
        try: 
            if len(summary_non_stp) > 0:           
                temp_df = pd.DataFrame.from_records(summary_non_stp, columns = summary_non_stp[0].keys())  

                print(f'from Non STP shape = {temp_df.shape[0]}')
                if df.empty:
                    df = temp_df
                else:
                    df = df.append(temp_df, ignore_index=True) 

        except Exception as e:
            print(f'Unable to proceed data for invoicing group {form.invoicing_group.data.name} for period {start_date} - {end_date}. Message is: {e}')

        else:
            df = df.drop_duplicates(subset='Обект (ИТН №)', keep = 'first')  

         
        df['price'] = (df['Сума за енергия'].sum()) / (df['Потребление (kWh)'].sum())

        for_invoice_df = df[['Потребление (kWh)','Сума за енергия','Мрежови услуги (лв.)','Задължение към обществото','Акциз']].sum()

        for_invoice_df = for_invoice_df.to_frame().T   
        for_invoice_df['inv_group']=df.iloc[0]['invoice_group_name'] 
        for_invoice_df['Получател']=df.iloc[0]['contractor_name']    
        for_invoice_df['сметка 411']=df['invoice_group_name'].iloc[0].split('_')[0]

        last_month_date = end_date.replace(day = calendar.monthrange(end_date.year, end_date.month)[1])
        for_invoice_df['Дата на издаване'] = last_month_date.strftime('%d/%m/%Y')
        for_invoice_df['Падеж'] = (dt.date.today() + pd.offsets.BDay(curr_contract.maturity_interval)).strftime('%d/%m/%Y')
        reason_date_str = last_month_date.strftime('%m.%Y') 
        for_invoice_df['Основание'] = f' за м.{reason_date_str}г.'

        epay_code, epay_name =  generate_num_and_name(1, for_invoice_df.iloc[0]['сметка 411'], for_invoice_df.iloc[0]['inv_group'],for_invoice_df.iloc[0]['Получател'])
        for_invoice_df['easy_pay_num'] = epay_code
        for_invoice_df['easy_pay_name'] = epay_name
        for_invoice_df = pd.melt(for_invoice_df, id_vars=['Потребление (kWh)', 'inv_group','Получател','сметка 411','Дата на издаване','Падеж','Основание','easy_pay_num','easy_pay_name'],var_name = 'Код на стоката',value_name = 'Стойност без ДДС')
        for_invoice_df['Стойност без ДДС'] = for_invoice_df['Стойност без ДДС'].apply(lambda x: round(Decimal(x) ,2))
        for_invoice_df['Код на стоката'] = for_invoice_df['Код на стоката'].apply(lambda x: GOODES_CODE[x])
        for_invoice_df['Основание'] = for_invoice_df.apply(lambda x: x['Основание'] if x['Код на стоката'] == '304-1' else '', axis = 1)
        for_invoice_df['Количество'] = for_invoice_df.apply(lambda x: x['Потребление (kWh)']if x['Код на стоката'] != '498-56' else 1, axis = 1)
        for_invoice_df['Цена без ДДС'] = for_invoice_df['Код на стоката'].apply(lambda x: df.iloc[0][PRICES[x]] * 1000)
        for_invoice_df['номер на фактура'] = ''
        for_invoice_df['Дименсия на количество'] = ''
        for_invoice_df['ЕИК'] = ''
        for_invoice_df['Валутен курс'] = 1
        for_invoice_df['ТИП на сделката по ДДС'] = 256
        for_invoice_df['ДДС %'] = 20
        for_invoice_df['ДДС'] = for_invoice_df['Стойност без ДДС'].apply(lambda x: round(Decimal(x) * Decimal('0.2') ,2))
        for_invoice_df['Крайна сума'] = for_invoice_df['Стойност без ДДС'].apply(lambda x: round(x * Decimal('1.2') ,2))
        for_invoice_df['email'] = curr_contract.contractor.email
        for_invoice_df['Код на валутата'] = 'лв'

        date_str = last_month_date.strftime('%Y-%m')
        file_name =f'{date_str}_{df.iloc[0].invoice_group_description}%{df.iloc[0].invoice_group_name}%invoice_reference.xlsx' 
        for_invoice_df['file_name'] = file_name
        # for_invoice_df = for_invoice_df[['Дата на издаване','Падеж', 'Основание','Код на стоката', 'Количество', 'Дименсия на количество', 
        #                                 'Цена без ДДС', 'Код на валутата', 'Валутен курс','Стойност без ДДС','ТИП на сделката по ДДС','ДДС %',
        #                                 'ДДС','Крайна сума', 'inv_group', 'email', 'file_name','easy_pay_num', 'easy_pay_name']]

        for_invoice_df = for_invoice_df[['Получател','сметка 411','ЕИК','номер на фактура','Дата на издаване','Падеж', 'Основание','Код на стоката', 'Количество', 'Дименсия на количество', 
                                        'Цена без ДДС', 'Код на валутата', 'Валутен курс','Стойност без ДДС','ТИП на сделката по ДДС','ДДС %',
                                        'ДДС','Крайна сума', 'inv_group', 'email', 'file_name','easy_pay_num', 'easy_pay_name']]
        if final_df.empty:
            for_invoice_df.insert(loc=0, column = '№ по ред', value = 1 )
            final_df = for_invoice_df
        else:
            next_num = final_df.tail(1)['№ по ред'].values[0] + 1  
            for_invoice_df.insert(loc=0, column = '№ по ред', value = next_num )       
            
            final_df = final_df.append(for_invoice_df, ignore_index=True)



    return final_df
    



# def generate_for_invoicing_file(inv_group, invoice_start_date, invoice_end_date):
    
#     COLUMNS = ['№ по ред', 'Получател', 'сметка 411', 'ЕИК', 'номер на фактура',
#                 'Дата на издаване', 'Падеж', 'Основание', 'Код на стоката',
#                 'Количество', 'Дименсия на количество', 'Цена без ДДС',
#                 'Код на валутата', 'Валутен курс', 'Стойност без ДДС',
#                 'ТИП на сделката по ДДС', 'ДДС %', 'ДДС', 'Крайна сума', 'inv_group',
#                 'email', 'file_name', 'easy_pay_num', 'easy_pay_name']
                
#     df = pd.DataFrame(columns = COLUMNS)



# def last_day_of_month(date_value):
#     return date_value.replace(day = monthrange(date_value.year, date_value.month)[1])
 
# given_date = datetime.today().date()
# print("\nGiven date:", given_date, " --> Last day of month:", last_day_of_month(given_date))
