import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash
from app.models import *  

from app.helper_functions_queries import (get_total_consumption_by_grid,
                                    get_total_money_by_grid,
                                    get_erp_consumption_records_by_grid,
                                    get_erp_money_records_by_grid
)

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