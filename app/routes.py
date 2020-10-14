import os
import xlrd
import time,re
import sys, pytz, datetime as dt
import pandas as pd
from flask import render_template, flash, redirect, url_for, request
from sqlalchemy import extract, or_
from app import app
from app.forms import (
    LoginForm, RegistrationForm, NewContractForm, AddItnForm, AddInvGroupForm, ErpForm,
    UploadInvGroupsForm, UploadContractsForm, UploadItnsForm, CreateSubForm, TestForm,
    UploadInitialForm)
from flask_login import current_user, login_user, logout_user, login_required
from app.models import *

from werkzeug.urls import url_parse
# from app import dbgrid_services_distrib_records,

from werkzeug.utils import secure_filename
# from app.helper_functions_erp import ( get_distribution_stp,)
from app.helper_functions import (get_contract_by_internal_id,
                                 convert_date_to_utc,
                                 convert_date_from_utc,
                                 validate_ciryllic,
                                 set_contarct_dates,
                                 get_address,
                                 get_invoicing_group,
                                 create_itn_meta,
                                 generate_utc_time_series,
                                 generate_subcontract_from_file,
                                 get_erp_id_by_name,
                                 get_subcontracts_by_itn_and_utc_dates,
                                 check_and_load_hourly_schedule,
                                 upload_remaining_forecat_schedule,
                                 has_overlaping_subcontracts,
                                 apply_collision_function,
                                 upload_forecasted_schedule_to_temp_db,                                 
                                 update_or_insert,
                                 validate_forecasting_df,
                                 generate_forecast_schedule,
                                 validate_subcontracts_dates,
                                 validate_input_df ,
                                 ROUND_HALF_UP,                                
                                 stringifyer,
                                 get_tariff_offset,
                                 create_tariff,
                                 
                                 
                                

)

from app.helper_function_excel_writer import (generate_excel,)


from app.helper_functions_queries import (                                         
                                        #  get_grid_services_tech_records,                                         
                                        #  get_single_tariff_consumption_records_sub,
                                        #  get_grid_services_distrib_records, 
                                        #  get_grid_service_sub_query,                                       
                                        #  get_summary_records_with_grid_services,
                                        #  get_summary_records_without_grid_services,
                                        #  get_time_zone,
                                         get_itn_by_inv_group_for_period_sub,
                                        #  get_single_tariff_consumption_records_sub,
                                        #  get_inv_group_itn_sub_query,
                                        get_stp_itn_by_inv_group_for_period_sub,
                                        get_stp_consumption_for_period_sub,
                                        get_non_stp_itn_by_inv_group_for_period_sub,
                                        get_non_stp_consumption_for_period_sub,
                                        get_itn_with_grid_services_sub,
)

from zipfile import ZipFile
from app.helper_functions_erp import (reader_csv, insert_erp_invoice,insert_mrus,
                                      insert_settlment_cez, insert_settlment_e_pro,
                                      insert_settlment_evn,
                                      
)

MONEY_ROUND = 9


@app.route('/erp', methods=['GET', 'POST'])
@login_required
def erp():
    
    form = ErpForm()
    if form.validate_on_submit():
        separator = '";"'
        # metas = db.session.query(ItnMeta.itn,MeasuringType.code).join(SubContract,SubContract.itn == ItnMeta.itn).join(MeasuringType).all()
        # itn_meta_df = pd.DataFrame.from_records(metas, columns = metas[0].keys()) 
        
        if request.files.get('file_cez').filename != '':
            start = time.time()
            erp_zip = ZipFile(request.files.get('file_cez'))    
            # insert_erp_invoice(erp_zip, separator)               
            # insert_mrus(erp_zip,separator)    
            insert_settlment_cez(erp_zip,separator)    
            end = time.time()
            print(f'Time elapsed for cez monthly update is : {end - start}')    
            flash('Data from CEZ uploaded successfully','success')

        if request.files.get('file_epro').filename != '':

            start = time.time()
            erp_zip = ZipFile(request.files.get('file_epro'))
            # insert_erp_invoice(erp_zip, separator)           
            # insert_mrus(erp_zip,separator)  
            insert_settlment_e_pro(erp_zip, separator) 
            end = time.time()
            
            print(f'Time elapsed for e-pro monthly update is : {end - start}') 
                    
            flash('Data from E PRO uploaded successfully','success')

        if request.files.get('file_evn').filename != '':

            erp_zip = ZipFile(request.files.get('file_evn'))
            # insert_erp_invoice(erp_zip, separator)           
            insert_settlment_evn(erp_zip, separator)
            # insert_mrus(erp_zip,separator)           
            flash('Data from EVN uploaded successfully','success')
            


    return render_template('erp.html', title='ERP Upload', form=form)

@app.route('/test', methods=['GET', 'POST'])
@login_required
def test():
    MONEY_ROUND = 2
    ENERGY_ROUND = 3
    form = TestForm()
    # form = CreateSubForm()
    if form.validate_on_submit(): 

        time_zone = 'EET'
        start_date = convert_date_to_utc(time_zone, form.start_date.data)
        end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)

        invoice_start_date = dt.datetime.strptime (form.start_date.data,"%Y-%m-%d")
        invoice_start_date = invoice_start_date + dt.timedelta(hours = (10 * 24 + 1))        
        invoice_start_date = convert_date_to_utc(time_zone, invoice_start_date)

        invoice_end_date = dt.datetime.strptime (form.end_date.data,"%Y-%m-%d") 
        invoice_end_date = invoice_end_date + dt.timedelta(hours = (10 * 24))            
        invoice_end_date = convert_date_to_utc(time_zone, invoice_end_date) 


        # itns = get_non_stp_itn_by_inv_group_for_period_sub(form.invoicing_group.data.name, start_date, end_date)
        # print(f'start_date --> {start_date}   end_date --> {end_date} ')
        # a = get_non_stp_consumption_for_period_sub(itns, start_date, end_date)
        # # print(f'{form.invoicing_group.data.name}')
        # df =  pd.DataFrame.from_records(a, columns = a[0].keys())
        # print(f'FROM TEST appl subs df \n{df}')

        itns = get_itn_with_grid_services_sub(form.invoicing_group.data.name, start_date, end_date)
        print(f'FROM TEST appl subs df \n{itns}')

        return render_template('test.html', title='Test', form=form)

        
       
        # ################################# Initial Ibex  ###################################### 
        
        # invoice_start_date = pd.to_datetime('01/07/2020', format = '%d/%m/%Y')
        # invoice_end_date = pd.to_datetime('31/12/2021', format = '%d/%m/%Y')
        # time_series = pd.date_range(start = invoice_start_date, end = invoice_end_date , freq='h', tz = 'EET')
        # forecast_df = pd.DataFrame(time_series, columns = ['utc'])
        # forecast_df['forecast_price'] = 0
        # forecast_df['volume'] = 0
        # forecast_df['price'] = 0
        # forecast_df.set_index('utc', inplace = True)
        
        # forecast_df.index = forecast_df.index.tz_convert('UTC').tz_localize(None)
        # forecast_df.reset_index(inplace = True)
        # forecast_df = forecast_df[['utc','price', 'forecast_price', 'volume']]
        # stringifyer(forecast_df)
        # bulk_update_list = forecast_df.to_dict(orient='records')
    
        # db.session.bulk_insert_mappings(IbexData, bulk_update_list)
        # db.session.commit()
        # #######################################################################

        # ################################# Upload to Ibex  ###################################### 
        # print(f' IN IBEX')
        # invoice_start_date = pd.to_datetime('01/07/2020', format = '%d/%m/%Y')
        # invoice_end_date = pd.to_datetime('28/09/2021', format = '%d/%m/%Y')
        # invoice_start_date = form.start_date.data
        # invoice_end_date = form.end_date.data
        # ibex_df = IbexData.download_from_ibex_web_page(invoice_start_date, invoice_end_date)
        # stringifyer(ibex_df)
        # bulk_update_list = ibex_df.to_dict(orient='records')
        # print(f' IN IBEX {bulk_update_list}')
        # db.session.bulk_update_mappings(IbexData, bulk_update_list)
        # db.session.commit()
        
        # ########################### v1 ############################################
        # start = time.time()
        # erp_itn_sub = ItnMeta.query.join(Erp).filter(Erp.name == form.erp.data.name).subquery()
        # invoice_end_date = convert_date_to_utc('EET', form.end_date.data) + dt.timedelta(hours = 23)

        # reported_vol_sum = (ItnSchedule.query.with_entities(func.sum(ItnSchedule.consumption_vol))             
        #     .filter( ItnSchedule.utc >= convert_date_to_utc('EET',form.start_date.data), ItnSchedule.utc <= invoice_end_date) 
        #     .join(erp_itn_sub, erp_itn_sub.c.itn == ItnSchedule.itn)             
        #     .all())
        # end = time.time()

        # flash(f'From join with subquery, elapsed time:{end - start}. Calculated sum for erp:{form.erp.data.name} is :{reported_vol_sum}. Period is from: {form.start_date.data} to:{invoice_end_date}', 'success')
        # ##########################################################################

        # ########################### v2 ############################################
        # start = time.time()
        # invoice_end_date = convert_date_to_utc('EET', form.end_date.data) + dt.timedelta(hours = 23)

        # reported_vol_by_erp_all = ItnSchedule.query.with_entities(func.sum(ItnSchedule.consumption_vol)) \
        #     .join(ItnMeta) \
        #     .join(SubContract) \
        #     .join(Erp) \
        #     .join(InvoiceGroup) \
        #     .filter( ItnSchedule.utc >= convert_date_to_utc('EET',form.start_date.data), ItnSchedule.utc <= invoice_end_date, Erp.name == form.erp.data.name) \
        #     .all()
        # end = time.time()
        # flash(f'From multiple joins, elapsed time:{end - start}. Calculated sum for erp:{form.erp.data.name} is :{reported_vol_by_erp_all}. Period is from: {form.start_date.data} to:{invoice_end_date}', 'info')

        # ########################### by invoicing groups ############################################
        # start = time.time()
        # inv_group_itn_sub = ItnMeta.query.join(SubContract).join(InvoiceGroup).filter(InvoiceGroup.name == form.invoicing_group.data.name).subquery()
        
        # time_zone = TimeZone.query.join(Contract).join(SubContract).join(inv_group_itn_sub, inv_group_itn_sub.c.itn == SubContract.itn).first().code
        # invoice_start_date = convert_date_to_utc(time_zone, form.start_date.data)
        # invoice_end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)

        # reported_vol_sum = (ItnSchedule.query.with_entities(func.sum(ItnSchedule.consumption_vol))             
        #     .filter( ItnSchedule.utc >= invoice_start_date, ItnSchedule.utc <= invoice_end_date) 
        #     .join(inv_group_itn_sub, inv_group_itn_sub.c.itn == ItnSchedule.itn)             
        #     .all())[0][0].quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
        # end = time.time()

        # flash(f'From by invoicing groups, elapsed time:{end - start}. Calculated sum for erp:{form.erp.data.name} is :{reported_vol_sum} kWh. Period is from: {invoice_start_date} to:{invoice_end_date} time zone {time_zone}', 'success')
        # ##########################################################################  

        # ########################### by invoicing groups and itn ############################################
        # start = time.time()
        # inv_group_itn_sub = ItnMeta.query.join(SubContract).join(InvoiceGroup).filter(InvoiceGroup.name == form.invoicing_group.data.name).subquery()

        # time_zone = TimeZone.query.join(Contract).join(SubContract).join(inv_group_itn_sub, inv_group_itn_sub.c.itn == SubContract.itn).first().code
        # invoice_start_date = convert_date_to_utc(time_zone, form.start_date.data)
        # invoice_end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)

        # reported_vol_sum = (ItnSchedule.query.with_entities(func.sum(ItnSchedule.consumption_vol))             
        #     .filter( ItnSchedule.utc >= invoice_start_date, ItnSchedule.utc <= invoice_end_date) 
        #     .join(inv_group_itn_sub, inv_group_itn_sub.c.itn == ItnSchedule.itn)  
        #     .group_by(ItnSchedule.itn)           
        #     .all())
        # end = time.time()

        # flash(f'reported_vol_sum {reported_vol_sum} kWh')
        # inv_group_itn = ItnMeta.query.join(SubContract).join(InvoiceGroup).filter(InvoiceGroup.name == form.invoicing_group.data.name).all()
        # flash(f'inv_group_itn {inv_group_itn}')
        # ####################### DELETE ALL ###################################################  
        # metas = ItnMeta.query.all()
        # for meta in metas:
        #     print(f'itn {meta.itn}  deleted !')
        #     meta.delete()
        # db.session.commit()
        # print(f'all itn deleted')
        # ########################################################################## 

        ######################### DELETE BY INv GROUP ################################################ 

        # metas = ItnMeta.query.join(SubContract,SubContract.itn == ItnMeta.itn).join(InvoiceGroup).filter(InvoiceGroup.name == form.invoicing_group.data.name ).all()
        # for meta in metas:
        #     print(f'itn {meta.itn} from inv group : {form.invoicing_group.data.name}  deleted !')
        #     meta.delete()
        # db.session.commit()
        # print(f'all itn from inv group {form.invoicing_group.data.name} deleted')
        
        # itns = get_itn_by_inv_group_for_period_sub(form.invoicing_group.data.name, start_date, end_date)
        # a = get_single_tariff_consumption_records_sub(itns, invoice_start_date, invoice_end_date)
        # # print(f'{form.invoicing_group.data.name}')
        # df =  pd.DataFrame.from_records(a, columns = a[0].keys())
        # print(f'FROM TEST appl subs df \n{df}')
        # return render_template('test.html', title='Test', form=form)
        # ########################################################################## 

        # ################################# DELETE CONTRACT  ###################################### 
        # contract_to_del = Contract.query.filter(Contract.internal_id == 'ТК-36').first()
        # # sub_to_del = SubContract.query.join(Contract).filter(Contract.internal_id == 'ТК-36').first()
        # print(f'contract to delete {contract_to_del} \n subcontract_to_delete : ')
        # contract_to_del.delete()

        # contracts = Contract.query.all()
        # for c in contracts:
        #     c.delete()





        # ########################################################################## 
        # time_zone = 'EET'
        # start_date = convert_date_to_utc(time_zone, form.start_date.data)
        # end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)
        # stp_records = (
        # db.session 
        #     .query(SubContract.itn) 
        #     .join(ItnSchedule, ItnSchedule.itn == SubContract.itn) 
        #     .join(MeasuringType) 
        #     .join(ItnMeta, ItnMeta.itn == SubContract.itn)
        #     .join(Erp)
        #     .filter(SubContract.start_date <= start_date, SubContract.end_date >= end_date) 
        #     .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT'))) 
        #     .filter(Erp.name == form.erp.data.name)
        #     .distinct()
        #     .all()
        # )

        # # print(f'all stp ---> {len(stp_records)}')
        # db_uniqute_stp = set([x[0] for x in stp_records])
        # # print(f'unique db stp {db_uniqute_stp}')
        

        
        ########################### tariff from itn schedule sum by invoicing group ############################################
        # start = time.time()

        # inv_group_itn_sub_query = (
        #     db.session.query(
        #         ItnMeta.itn.label('sub_itn'),
        #         SubContract.zko.label('zko'),
        #         SubContract.akciz.label('akciz'),                
        #         Contractor.name.label('Contractor'),
        #     )
        #     .join(SubContract)
        #     .join(InvoiceGroup)
        #     .join(Contractor)
        #     .filter(InvoiceGroup.name == form.invoicing_group.data.name)
        #     .subquery()
        # )
        
        # time_zone = TimeZone.query.join(Contract).join(SubContract).join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == SubContract.itn).first().code
        
        # invoice_start_date = dt.datetime.strptime (form.start_date.data,"%Y-%m-%d")
        # invoice_start_date = invoice_start_date + dt.timedelta(hours = (10 * 24 + 1))        
        # invoice_start_date = convert_date_to_utc(time_zone, invoice_start_date)

        # invoice_end_date = dt.datetime.strptime (form.end_date.data,"%Y-%m-%d") 
        # invoice_end_date = invoice_end_date + dt.timedelta(hours = (10 * 24))            
        # invoice_end_date = convert_date_to_utc(time_zone, invoice_end_date) 

        # period_start_date = convert_date_to_utc(time_zone, form.start_date.data)
        # period_end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)  
        # power_e_date_local = dt.datetime.strptime(form.end_date.data, "%Y-%m-%d") + dt.timedelta(hours = 23) 
        
        # # # filters = (
        # # #     func.convert_tz(ItnSchedule.utc,'UTC',time_zone) >= form.start_date.data, func.convert_tz(ItnSchedule.utc,'UTC',time_zone) <= power_e_date_local,
        # # #     Transaction.amount < 100,
        # # # )
        # # # db.session.query(Transaction).filter(*filters)

       
        # itn_t = 'BG5521900615200000000000002009103'
        # print(f'{get_tariff_offset(form.start_date.data,time_zone)}')

        # records = (
        #     db.session.query(
        #         ItnSchedule.itn, func.sum(ItnSchedule.consumption_vol) )
        #         .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)
        #         # .filter(extract('hour', ItnSchedule.utc) > 4,  extract('hour', ItnSchedule.utc) <= 20)
        #         .filter(ItnSchedule.itn == itn_t) 
        #         .group_by(ItnSchedule.itn)   
        #         .all()
        # )

        # records_day = (
        #     db.session.query(
        #         ItnSchedule.itn, func.sum(ItnSchedule.consumption_vol) )
        #         .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)
        #         .filter(extract('hour', ItnSchedule.utc) > 4,  extract('hour', ItnSchedule.utc) <= 20)
        #         .filter(ItnSchedule.itn == itn_t) 
        #         .group_by(ItnSchedule.itn)   
        #         .all()
        # )

        # records_night = (
        #     db.session.query(
        #         ItnSchedule.itn, func.sum(ItnSchedule.consumption_vol) )
        #         .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)
        #         .filter(or_(extract('hour', ItnSchedule.utc) <= 4,  extract('hour', ItnSchedule.utc) > 20))
        #         .filter(ItnSchedule.itn == itn_t) 
        #         .group_by(ItnSchedule.itn)   
        #         .all()
        # )
        # print(f'{records}')
        # records_eet = (
        #     db.session.query(
        #         ItnSchedule.itn, func.convert_tz(ItnSchedule.utc,'UTC',time_zone).label('eet'), ItnSchedule.consumption_vol )
        #         .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)
        #         # .filter(or_(extract('hour', ItnSchedule.utc) <= 5,  extract('hour', ItnSchedule.utc) > 21))
        #         .filter(ItnSchedule.itn == itn_t) 
                
        #         .all()
        # )

        # temp_df = pd.DataFrame.from_records(records_eet, columns = records_eet[0].keys())
        # records = (
        #     db.session.query(
        #         ItnSchedule.itn, func.sum(ItnSchedule.consumption_vol) )
        #         .filter(func.convert_tz(ItnSchedule.utc,'UTC',time_zone) >= form.start_date.data, func.convert_tz(ItnSchedule.utc,'UTC',time_zone) <= power_e_date_local)
        #         .filter((extract('hour',func.convert_tz(ItnSchedule.utc,'UTC',time_zone)) > tariff_start_hour_by_date(func.convert_tz(ItnSchedule.utc,'UTC',time_zone))), (extract('hour',func.convert_tz(ItnSchedule.utc,'UTC',time_zone)) <= 10))
        #         .filter(ItnSchedule.itn == 'BG5521900615200000000000002009103') 
        #         .group_by(ItnSchedule.itn)   
        #         .all()
        # )
        

        # .filter((extract('month',func.convert_tz(ITN_SCHEDULE.Utc,'UTC','EET')) == inv_month) & (extract('year',func.convert_tz(ITN_SCHEDULE.Utc,'UTC','EET')) == inv_year))
        # print(f'{temp_df}')
        # temp_df.to_excel('BG5521900615200000000000002009103.xlsx')
        # print(f'$$$$$$$$$$$$$$$ --- {records} ----{records_day} ---- {records_night} $$$$$$$$$$$$$$$ \n {period_start_date} --- {period_end_date}')

        ################################# multi TARIF ##############################################
        # start = time.time()

        # inv_group_itn_sub_query = (
        #     db.session.query(
        #         ItnMeta.itn.label('sub_itn'),
        #         SubContract.zko.label('zko'),
        #         SubContract.akciz.label('akciz'),                
        #         Contractor.name.label('Contractor'),
        #     )
        #     .join(SubContract)
        #     .join(InvoiceGroup)
        #     .join(Contractor)
        #     .filter(InvoiceGroup.name == form.invoicing_group.data.name)
        #     .subquery()
        # )
        
        # time_zone = TimeZone.query.join(Contract).join(SubContract).join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == SubContract.itn).first().code
        
        # invoice_start_date = dt.datetime.strptime (form.start_date.data,"%Y-%m-%d")
        # invoice_start_date = invoice_start_date + dt.timedelta(hours = (10 * 24 + 1))        
        # invoice_start_date = convert_date_to_utc(time_zone, invoice_start_date)

        # invoice_end_date = dt.datetime.strptime (form.end_date.data,"%Y-%m-%d") 
        # invoice_end_date = invoice_end_date + dt.timedelta(hours = (10 * 24))            
        # invoice_end_date = convert_date_to_utc(time_zone, invoice_end_date) 

        # period_start_date = convert_date_to_utc(time_zone, form.start_date.data)
        # period_end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)  
        # power_e_date_local = dt.datetime.strptime(form.end_date.data, "%Y-%m-%d") + dt.timedelta(hours = 23) 

        # grid_service_sub_query = (
        #     db.session.query(
        #         Distribution.itn.label('itn_id'),
        #         func.round(func.sum(Distribution.value), MONEY_ROUND).label('grid_services')                
        #     )
        #     .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == Distribution.itn)
        #     .join(ErpInvoice,ErpInvoice.id == Distribution.erp_invoice_id)
        #     .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)
        #     .group_by(Distribution.itn)
        #     .subquery()
        # )         
        
        # day_tariff_consumption_records_sub = (
        #     db.session
        #         .query(Tech.itn.label('itn_day'), func.sum(Tech.readings_difference).label('day_tariff_consumption')) 
        #         .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == Tech.itn)           
        #         .join(ErpInvoice, ErpInvoice.id == Tech.erp_invoice_id)                      
        #         .filter(Tech.scale_code.in_(['1.8.2','Д'])) 
        #         .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)  
        #         .group_by(Tech.itn)                  
        #         .subquery()
        # )       

        # # df =  pd.DataFrame.from_records(day_tariff_consumption_records_sub, columns = day_tariff_consumption_records_sub[0].keys())
        # # df = df[df['itn_day']=="BG5521900616200000000000002010152"]
        # # print(f'{df}')

        # night_tariff_consumption_records_sub = (
        #     db.session
        #         .query(Tech.itn.label('itn_night'), func.sum(Tech.readings_difference).label('night_tariff_consumption')) 
        #         .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == Tech.itn)           
        #         .join(ErpInvoice, ErpInvoice.id == Tech.erp_invoice_id)                      
        #         .filter(Tech.scale_code.in_(['1.8.1','Н'])) 
        #         .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)  
        #         .group_by(Tech.itn)                  
        #         .subquery()
        # )

        # peak_tariff_consumption_records_sub = (
        #     db.session
        #         .query(Tech.itn.label('itn_peak'), func.sum(Tech.readings_difference).label('peak_tariff_consumption')) 
        #         .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == Tech.itn)           
        #         .join(ErpInvoice, ErpInvoice.id == Tech.erp_invoice_id)                      
        #         .filter(Tech.scale_code.in_(['1.8.3'])) 
        #         .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)  
        #         .group_by(Tech.itn)                  
        #         .subquery()
        # )

        # single_tariff_consumption_records_sub = (
        #     db.session.query(
        #         ItnSchedule.itn.label('itn_single'),
        #         func.round(func.sum(ItnSchedule.consumption_vol), ENERGY_ROUND).label('single_tariff_consumption'),
        #         ) 
        #         .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == ItnSchedule.itn)           
        #         .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)       
        #         .group_by(ItnSchedule.itn)                  
        #         .subquery()
        # )

        # summary_records_with_grid_services = (
        #     db.session.query(
        #         ItnSchedule.itn.label('Обект (ИТН №)'),
        #         grid_service_sub_query.c.grid_services.label('Мрежови услуги (лв.)'),
        #         AddressMurs.name.label('Адрес'),
        #         single_tariff_consumption_records_sub.c.single_tariff_consumption,
        #         day_tariff_consumption_records_sub.c.day_tariff_consumption,
        #         Tariff.price_day,
        #         night_tariff_consumption_records_sub.c.night_tariff_consumption,
        #         Tariff.price_night,
        #         peak_tariff_consumption_records_sub.c.peak_tariff_consumption,
        #         Tariff.price_peak,
        #         inv_group_itn_sub_query.c.zko,
        #         inv_group_itn_sub_query.c.akciz,
        #         Tariff.name,                
        #         func.round(night_tariff_consumption_records_sub.c.night_tariff_consumption * Tariff.price_night, MONEY_ROUND).label('Сума за нощна енергия'),
        #         func.round(peak_tariff_consumption_records_sub.c.peak_tariff_consumption * Tariff.price_peak, MONEY_ROUND).label('Сума за пикова енергия'),
        #         inv_group_itn_sub_query.c.Contractor
        #         )
        #         .outerjoin(single_tariff_consumption_records_sub, single_tariff_consumption_records_sub.c.itn_single == ItnSchedule.itn)
        #         .outerjoin(peak_tariff_consumption_records_sub, peak_tariff_consumption_records_sub.c.itn_peak == ItnSchedule.itn)
        #         .outerjoin(night_tariff_consumption_records_sub, night_tariff_consumption_records_sub.c.itn_night == ItnSchedule.itn)
        #         .outerjoin(day_tariff_consumption_records_sub, day_tariff_consumption_records_sub.c.itn_day == ItnSchedule.itn)
        #         .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == ItnSchedule.itn) 
        #         .join(grid_service_sub_query, grid_service_sub_query.c.itn_id == ItnSchedule.itn)                        
        #         .join(ItnMeta, ItnMeta.itn == ItnSchedule.itn)                        
        #         .outerjoin(AddressMurs,AddressMurs.id == ItnMeta.address_id) 
        #         .join(Tariff, Tariff.id == ItnSchedule.tariff_id)                       
        #         .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)                 
        #         .group_by(ItnSchedule.itn, inv_group_itn_sub_query.c.zko, inv_group_itn_sub_query.c.akciz, Tariff.price_day, 
        #                 Tariff.price_night, Tariff.price_peak, day_tariff_consumption_records_sub.c.day_tariff_consumption, Tariff.name,
        #                 night_tariff_consumption_records_sub.c.night_tariff_consumption, peak_tariff_consumption_records_sub.c.peak_tariff_consumption)                        
        #         .all()
        # )
        # df = pd.DataFrame()
        # if len(summary_records_with_grid_services) != 0:
        #     try:
        #         temp_df = pd.DataFrame.from_records(summary_records_with_grid_services, columns = summary_records_with_grid_services[0].keys())
        #         print(f'from with shape = {df.shape[0]}')

        #     except Exception as e:
        #         print(f'Unable to create grid service dataframe for invoicing group {form.invoicing_group.data.name} for period {period_start_date} - {period_end_date}. Message is: {e}')

        #     else:
        #         if df.empty:
        #             df = temp_df
        #         else:
        #             df = df.append(temp_df, ignore_index=True)       
        
        # summary_records_without_grid_services = (
        #     db.session.query(
        #         ItnSchedule.itn.label('Обект (ИТН №)'),                
        #         AddressMurs.name.label('Адрес'),
        #         single_tariff_consumption_records_sub.c.single_tariff_consumption,
        #         day_tariff_consumption_records_sub.c.day_tariff_consumption,
        #         Tariff.price_day,
        #         night_tariff_consumption_records_sub.c.night_tariff_consumption,
        #         Tariff.price_night,
        #         peak_tariff_consumption_records_sub.c.peak_tariff_consumption,
        #         Tariff.price_peak,
        #         inv_group_itn_sub_query.c.zko,
        #         inv_group_itn_sub_query.c.akciz,
        #         Tariff.name,
        #         func.round(night_tariff_consumption_records_sub.c.night_tariff_consumption * Tariff.price_night, MONEY_ROUND).label('Сума за нощна енергия'),
        #         func.round(peak_tariff_consumption_records_sub.c.peak_tariff_consumption * Tariff.price_peak, MONEY_ROUND).label('Сума за пикова енергия'),               
        #         inv_group_itn_sub_query.c.Contractor
        #         )
        #         .outerjoin(single_tariff_consumption_records_sub, single_tariff_consumption_records_sub.c.itn_single == ItnSchedule.itn)
        #         .outerjoin(peak_tariff_consumption_records_sub, peak_tariff_consumption_records_sub.c.itn_peak == ItnSchedule.itn)
        #         .outerjoin(night_tariff_consumption_records_sub, night_tariff_consumption_records_sub.c.itn_night == ItnSchedule.itn)
        #         .outerjoin(day_tariff_consumption_records_sub, day_tariff_consumption_records_sub.c.itn_day == ItnSchedule.itn)
        #         .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == ItnSchedule.itn)                                        
        #         .join(ItnMeta, ItnMeta.itn == ItnSchedule.itn) 
        #         .join(SubContract, SubContract.itn == ItnSchedule.itn)                       
        #         .outerjoin(AddressMurs,AddressMurs.id == ItnMeta.address_id)
        #         .filter(SubContract.start_date <= period_start_date, SubContract.end_date >= period_end_date, SubContract.has_grid_services == False)
        #         .join(Tariff, Tariff.id == ItnSchedule.tariff_id)                      
        #         .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)  
        #         .group_by(ItnSchedule.itn, inv_group_itn_sub_query.c.zko, inv_group_itn_sub_query.c.akciz, Tariff.price_day, 
        #                 Tariff.price_night, Tariff.price_peak, day_tariff_consumption_records_sub.c.day_tariff_consumption,Tariff.name,
        #                 night_tariff_consumption_records_sub.c.night_tariff_consumption, peak_tariff_consumption_records_sub.c.peak_tariff_consumption)                       
        #         .all()
        # )
        
        # try: 
        #     if len(summary_records_without_grid_services) > 0:           
        #         temp_df = pd.DataFrame.from_records(summary_records_without_grid_services, columns = summary_records_without_grid_services[0].keys())

        #         temp_df.insert(loc=1, column = 'Мрежови услуги (лв.)', value = 0)                 
        #         print(f'from WITHOUT shape = {temp_df.shape[0]}')
        #         if df.empty:
        #             df = temp_df
        #         else:
        #             df = df.append(temp_df, ignore_index=True)  

            
        # except Exception as e:
        #     print(f'Unable to proceed data for invoicing group {form.invoicing_group.data.name} for period {period_start_date} - {period_end_date}. Message is: {e}')

        # else:
        #     df = df.drop_duplicates(subset='Обект (ИТН №)', keep = 'last')
        #     df = df.fillna(0)
            
        #     df.insert(loc=0, column = '№', value = [x for x in range(1,df.shape[0] + 1)])
        #     df['Потребление общо (kWh)'] = df.apply(lambda x: x['day_tariff_consumption'] + x['night_tariff_consumption'] + x['peak_tariff_consumption'] if x['name'] != 'single_tariff' else x['single_tariff_consumption'], axis = 1)
        #     df['day_tariff_consumption'] = df.apply(lambda x: x['day_tariff_consumption'] if (pd.isnull(x['peak_tariff_consumption']) & (x['name'] != 'peak_tariff')) else  x['peak_tariff_consumption'] + x['day_tariff_consumption'] , axis = 1)
            
        #     df['single_tariff_consumption'] = df.apply(lambda x: 0  if x['name'] != 'single_tariff' else x['single_tariff_consumption'], axis = 1)
            
        #     df.rename(columns = {'day_tariff_consumption':'Потребление - дневна (kWh)','night_tariff_consumption':'Потребление - нощна (kWh)','single_tariff_consumption':'Потребление - еднотарифно (kWh)'}, inplace = True)
            

        #     df['price_day'] = df['price_day'].apply(Decimal)
        #     df['price_night'] = df['price_night'].apply(Decimal)
        #     df['price_peak'] = df['price_peak'].apply(Decimal)
        #     df['zko'] = df['zko'].apply(Decimal)
        #     df['akciz'] = df['akciz'].apply(Decimal)
        #     df['Потребление общо (kWh)'] = df['Потребление общо (kWh)'].apply(Decimal)

        #     df['Нощна - пари'] = df['Потребление - нощна (kWh)'] * df['price_night']
        #     df['Дневна - пари'] = df['Потребление - дневна (kWh)'] * df['price_day']
        #     df['Еднотарифно - пари'] = df['Потребление - еднотарифно (kWh)'] * df['price_day']
        #     df['акциз- пари'] = Decimal(df['Потребление общо (kWh)'].sum()) * df['akciz']
        #     df['зко- пари'] = Decimal(df['Потребление общо (kWh)'].sum()) * df['zko']
        #     df['пари общо'] = df.iloc[0]['акциз- пари'] + df['Еднотарифно - пари'].sum().quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) + df['Дневна - пари'].sum().quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) + \
        #                     df['Нощна - пари'].sum().quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) + df.iloc[0]['зко- пари'].quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)  + \
        #                     df['Мрежови услуги (лв.)'].sum().quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        #     df['Дневна - пари общо'] = df['Дневна - пари'].sum()
        #     df['Дневна - консумация общо'] = df['Потребление - дневна (kWh)'].sum()
        #     df.to_excel(f'{form.invoicing_group.data.description}_{9}.xlsx')
        #     print(f'{df}')
       

       ################################# single TARIF ##############################################
    #     start = time.time()
        
    #     time_zone = TimeZone.query.join(Contract).join(SubContract).join(InvoiceGroup).filter(InvoiceGroup.name == form.invoicing_group.data.name).first().code
        
    #     invoice_start_date = dt.datetime.strptime (form.start_date.data,"%Y-%m-%d")
    #     invoice_start_date = invoice_start_date + dt.timedelta(hours = (10 * 24 + 1))        
    #     invoice_start_date = convert_date_to_utc(time_zone, invoice_start_date)

    #     invoice_end_date = dt.datetime.strptime (form.end_date.data,"%Y-%m-%d") 
    #     invoice_end_date = invoice_end_date + dt.timedelta(hours = (10 * 24))            
    #     invoice_end_date = convert_date_to_utc(time_zone, invoice_end_date) 

    #     period_start_date = convert_date_to_utc(time_zone, form.start_date.data)
    #     period_end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)  
    #     # power_e_date_local = dt.datetime.strptime(form.end_date.data, "%Y-%m-%d") + dt.timedelta(hours = 23) 
        
        
    #     inv_group_itn_sub_query = get_inv_group_itn_sub_query(form.invoicing_group.data.name, 
    #                                                             period_start_date, 
    #                                                             period_end_date)

    #     single_tariff_consumption_records_sub = get_single_tariff_consumption_records_sub(inv_group_itn_sub_query, 
    #                                                                                         period_start_date, 
    #                                                                                         period_end_date)

        
    #     grid_services_tech_records = get_grid_services_tech_records(inv_group_itn_sub_query, invoice_start_date, invoice_end_date)
    #     grid_services_distrib_records = get_grid_services_distrib_records(inv_group_itn_sub_query, invoice_start_date, invoice_end_date)

        

    #     grid_services_df = pd.DataFrame()
    #     if (len(grid_services_tech_records) == 0) :
    #         grid_services_df = pd.DataFrame(columns=['Абонат №', 'А д р е с', 'Име на клиент', 'ЕГН/ЕИК',
    #                                                 'Идентификационен код', 'Електромер №', 'Отчетен период от',
    #                                                 'Отчетен период до', 'Брой дни', 'Номер скала', 'Код скала',
    #                                                 'Часова зона', 'Показания  ново', 'Показания старо', 'Разлика (квтч)',
    #                                                 'Константа', 'Корекция (квтч)', 'Приспаднати (квтч)',
    #                                                 'Общо количество (квтч)', 'Тарифа/Услуга', 'Количество (кВтч/кВАрч)',
    #                                                 'Единична цена (лв./кВт/ден)/ (лв./кВтч)', 'Стойност (лв)',
    #                                                 'Корекция към фактура', 'Основание за издаване'])
    #     else:    
    #         grid_services_tech_records_df = pd.DataFrame.from_records(grid_services_tech_records, columns = grid_services_tech_records[0].keys())
    #         grid_services_distrib_records_df = pd.DataFrame.from_records(grid_services_distrib_records, columns = grid_services_distrib_records[0].keys())
    #         grid_services_df = pd.concat([grid_services_tech_records_df,grid_services_distrib_records_df])
    #         grid_services_df = grid_services_df.sort_values(by='Идентификационен код', ascending=False, ignore_index=True)
            



    #     grid_service_sub_query = get_grid_service_sub_query(inv_group_itn_sub_query, invoice_start_date, invoice_end_date)
    #     single_tariff_consumption_records_sub = get_single_tariff_consumption_records_sub(inv_group_itn_sub_query, 
    #                                                                                         period_start_date, 
    #                                                                                         period_end_date)

    #     summary_records_with_grid_services = get_summary_records_with_grid_services(inv_group_itn_sub_query,
    #                                                                                 single_tariff_consumption_records_sub, 
    #                                                                                 grid_service_sub_query, 
    #                                                                                 period_start_date, 
    #                                                                                 period_end_date)



    #     summary_records_without_grid_services = get_summary_records_without_grid_services(inv_group_itn_sub_query,
    #                                                                                         single_tariff_consumption_records_sub, 
    #                                                                                         grid_service_sub_query, 
    #                                                                                         period_start_date, 
    #                                                                                         period_end_date)
        
    #     df = pd.DataFrame()
    #     if len(summary_records_with_grid_services) != 0:
    #         try:
    #             temp_df = pd.DataFrame.from_records(summary_records_with_grid_services, columns = summary_records_with_grid_services[0].keys())
    #             print(f'from with shape = {df.shape[0]}')

    #         except Exception as e:
    #             print(f'Unable to create grid service dataframe for invoicing group {form.invoicing_group.data.name} for period {period_start_date} - {period_end_date}. Message is: {e}')

    #         else:
    #             if df.empty:
    #                 df = temp_df
    #             else:
    #                 df = df.append(temp_df, ignore_index=True) 
    #     try: 
    #         if len(summary_records_without_grid_services) > 0:           
    #             temp_df = pd.DataFrame.from_records(summary_records_without_grid_services, columns = summary_records_without_grid_services[0].keys())

    #             temp_df.insert(loc=1, column = 'Мрежови услуги (лв.)', value = 0)                 
    #             print(f'from WITHOUT shape = {temp_df.shape[0]}')
    #             if df.empty:
    #                 df = temp_df
    #             else:
    #                 df = df.append(temp_df, ignore_index=True)  

            
    #     except Exception as e:
    #         print(f'Unable to proceed data for invoicing group {form.invoicing_group.data.name} for period {period_start_date} - {period_end_date}. Message is: {e}')

    #     else:
    #         df = df.drop_duplicates(subset='Обект (ИТН №)', keep = 'last')  

    #     print(f'{df.head()}')
    #     # df = df.apply(pd.to_numeric, errors='ignore')
        
    #     df.insert(loc=0, column = '№', value = [x for x in range(1,df.shape[0] + 1)])
    #     # df.to_excel(f'temp/nzok.xlsx')
    #     generate_excel(df, grid_services_df, invoice_start_date, invoice_end_date, period_start_date, period_end_date, time_zone)

    return render_template('test.html', title='Test', form=form)
    

@app.route('/upload_initial_data', methods=['GET', 'POST'])
@login_required
def upload_initial_data():
    
    form = UploadInitialForm()
    if form.validate_on_submit():
        if request.files.get('file_contractors').filename != '':
            
            raw_contractor_df =  pd.read_csv(request.files.get('file_contractors'), encoding = "windows-1251",header=None)
            raw_contractor_df.drop(columns = [0,1,2,3,4,5,6,7,8,9,10], inplace = True)
            
            start_idx = raw_contractor_df[raw_contractor_df[11] == '411'].index[0] 
            end_idx = raw_contractor_df[raw_contractor_df[11] == '421'].index[0] 
            raw_contractor_df = raw_contractor_df[start_idx:end_idx]
            raw_contractor_df.iloc[0] = raw_contractor_df.iloc[0].shift(-1)
            raw_contractor_df.drop(columns = [20,21], inplace = True)
            raw_contractor_df.columns = ['Name','EIC','Vat_Number','Phone','E_Mail','City','Comptroller','Address','Acc_411']
            raw_contractor_df['Address'] = raw_contractor_df['City'] + ',' + raw_contractor_df['Address']
            raw_contractor_df.drop(columns = ['City'], inplace = True)
            raw_contractor_df['Head_Address'] = np.nan
            raw_contractor_df = raw_contractor_df[['Name', 'EIC','Address','Vat_Number','E_Mail','Acc_411']]
            raw_contractor_df.rename(columns = {'Name':'name', 'EIC':'eic','Address':'address','Vat_Number':'vat_number','E_Mail':'email','Acc_411':'acc_411'}, inplace = True)
   
            update_or_insert(raw_contractor_df, Contractor.__table__.name)

        if request.files.get('file_measuring').filename != '':
            measuring_df = pd.read_excel(request.files.get('file_measuring'))
            update_or_insert(measuring_df, MeasuringType.__table__.name)

        if request.files.get('file_erp').filename != '':
            erp_df =  pd.read_excel(request.files.get('file_erp'))
            update_or_insert(erp_df, Erp.__table__.name)

        if request.files.get('file_stp').filename != '':

            START_DATE = '01/01/2020 00:00:00'
            END_DATE = '31/12/2020 23:00:00'
            FORMAT = '%d/%m/%Y %H:%M:%S'

            df = pd.read_excel(request.files.get('file_stp'),sheet_name='full')
            sDate = pd.to_datetime(START_DATE,format = FORMAT)
            eDate = pd.to_datetime(END_DATE,format = FORMAT)
            timeseries = pd.date_range(start=sDate, end=eDate, tz='EET', freq='h')
            Utc = timeseries.tz_convert('UTC').tz_localize(None) 
            df['utc'] = Utc
            df_m = pd.melt(df, id_vars =['utc'], value_vars =['CEZ_B1', 'CEZ_B2', 'CEZ_B3', 'CEZ_B4', 'CEZ_B5', 'CEZ_H1', 'CEZ_H2',
                'CEZ_S1', 'EPRO_B01', 'EPRO_B02', 'EPRO_B03', 'EPRO_B04', 'EPRO_H0',
                'EPRO_H1', 'EPRO_S0', 'EVN_G0', 'EVN_G1', 'EVN_G2', 'EVN_G3', 'EVN_G4',
                'EVN_H0', 'EVN_H1', 'EVN_H2', 'EVN_BD000'],var_name='code') 
            
            df_measure_code = pd.read_sql(MeasuringType.query.statement, db.session.bind)
            df_l = df_m.merge(df_measure_code, on=['code'])
            df_l.rename(columns = {'id':'measuring_type_id','Utc':'utc'}, inplace = True)
            df_l.drop(columns = ['code'], inplace = True)
            df_l = df_l[['utc','measuring_type_id','value']]
            
            update_or_insert(df_l, StpCoeffs.__table__.name)

        if request.files.get('file_inv_group').filename != '':
            if request.files.get('file_inv_group').filename == 'contractors_inv_groups_itn_old.xlsx':
                df = pd.read_excel(request.files.get('file_inv_group'))
                df.drop_duplicates(subset = 'Invoice_Group_Name', keep = 'first', inplace = True)
                df['contractor_id'] = df['Contractor_Name'].apply(lambda x: Contractor.query.filter(Contractor.name == x).first().id if Contractor.query.filter(Contractor.name == x).first() is not None else -1)
                df['name'] = df['Invoice_Group_Name'].apply(lambda x: x[:29])
                df['description'] = df['Invoice_Group_Name']
                # df.rename(columns = {'Invoice_Group_Name':'name'}, inplace = True)
                df = df[['name', 'contractor_id', 'description']]
                update_or_insert(df, InvoiceGroup.__table__.name)
            else:
                flash('Empty','danger')

        if request.files.get('file_hum_contractors').filename != '':
            df = pd.read_excel(request.files.get('file_hum_contractors'),skiprows=1)
            
            flash(df.columns)
            children_df = df[df['parent_name'] != 0]
            df['parent_id'] = np.nan
            df = df[['parent_id', 'name', 'eic', 'address','vat_number','email','acc_411']]
            update_or_insert(df, Contractor.__table__.name)

            
            db_df = pd.read_sql(Contractor.query.statement, db.session.bind)
            db_df = db_df[['id','name']]
            db_df.rename(columns = {'id':'parent_id', 'name': 'parent_name'}, inplace = True)
            children_df = children_df.merge(db_df, on = 'parent_name', how = 'left')
            df = children_df[['parent_id', 'name', 'eic', 'address','vat_number','email','acc_411']]
           
            update_or_insert(df, Contractor.__table__.name)

        if request.files.get('file_hum_contracts').filename != '':

            df = pd.read_excel(request.files.get('file_hum_contracts'),skiprows=0)            

            tks = df['internal_id'].apply(lambda x: validate_ciryllic(x))            
            
            all_cyr = tks.all()
            
            
            if not all_cyr:
                flash('There is tk in latin, aborting', 'danger')
                return redirect(url_for('upload_initial_data'))
            
            df = df.fillna(0)
            df['time_zone_id'] = df['time_zone'].apply(lambda x: TimeZone.query.filter(TimeZone.code == x).first().id )            

            
            df['signing_date'] = df.apply(lambda x: convert_date_to_utc(TimeZone.query.filter(TimeZone.id == x['time_zone_id']).first().code, x['signing_date']), axis = 1)
            df['start_date'] = df.apply(lambda x: convert_date_to_utc(TimeZone.query.filter(TimeZone.id == x['time_zone_id']).first().code, x['start_date']), axis = 1)
            df['end_date'] = df.apply(lambda x: convert_date_to_utc(TimeZone.query.filter(TimeZone.id == x['time_zone_id']).first().code, x['end_date']) + dt.timedelta(hours = 23), axis = 1)
            
            # df['end_date'] = df['end_date'] + dt.timedelta(hours = 23)
            df['duration_in_days'] = df.apply(lambda x: (x['end_date'] - x['start_date']).days + 1, axis = 1)
            
            
            
            df['automatic_renewal_interval'] = 0
            df['subject'] = ''
            df['parent_id'] = 0
            invoicing_dict = {'до 12-то число, следващ месеца на доставката':42,'на 10 дни':10,'на 15 дни':15,'последно число':31,'конкретна дата':-1}
            df['invoicing_interval'] = df['invoicing_interval'].apply(lambda x: invoicing_dict[x.strip()] if(invoicing_dict.get(str(x).strip())) else 0 )
            contract_type_dict = {'OTC':'End_User','ОП':'Procurement'}

            df['contract_type'] = df['contract_type'].apply(lambda x: contract_type_dict[x.strip()] if(contract_type_dict.get(str(x).strip())) else 0 )
            df['contract_type_id'] = df['contract_type'].apply(lambda x: ContractType.query.filter(ContractType.name == x).first().id if ContractType.query.filter(ContractType.name == x).first() is not None else x)

            work_day_dict = {'календарни дни':0, 'работни дни':1}
            df['is_work_days'] = df['is_work_day'].apply(lambda x: work_day_dict[x.strip()] if(work_day_dict.get(str(x).strip())) else 0 )

            df['contractor_id'] = df['contractor'].apply(lambda x: Contractor.query.filter(Contractor.name == x).first().id if Contractor.query.filter(Contractor.name == x).first() is not None else x)
            df = df[['internal_id','contractor_id','subject','parent_id','time_zone_id','signing_date','start_date','end_date','duration_in_days', \
                                                                    'invoicing_interval','maturity_interval','contract_type_id','is_work_days','automatic_renewal_interval','collateral_warranty','notes']]

            update_or_insert(df, Contract.__table__.name)
            flash('upload was successiful','info')
            # t_format = '%Y-%m-%dT%H:%M'
            
        
        if request.files.get('file_hum_itn').filename != '':

            STP_MAP_DICT = {
                'B01':'EPRO_B01','B02':'EPRO_B02','B03':'EPRO_B03','B04':'EPRO_B04','H01':'EPRO_H01','H02':'EPRO_H02','S01':'EPRO_S01','BD000':'EVN_BD000','G0':'EVN_G0','G1':'EVN_G1','G2':'EVN_G2',
                'G3':'EVN_G3','G4':'EVN_G4', 'H0':'EVN_H0','H1':'EVN_H1','H2':'EVN_H2','B1':'CEZ_B1','B2':'CEZ_B2','B3':'CEZ_B3','B4':'CEZ_B4','B5':'CEZ_B5','H1':'CEZ_H1','H2':'CEZ_H2','S1':'CEZ_S1'    
            }

            df = pd.read_excel(request.files.get('file_hum_itn'), sheet_name=None)  
            df = validate_input_df(df['data'])
            # df['price'] = df['price'].apply(lambda x: Decimal(str(x)) / Decimal('1000'))
            df['zko'] = df['zko'].apply(lambda x: Decimal(str(x)) / Decimal('1000'))
            df['akciz'] = df['akciz'].apply(lambda x: Decimal(str(x)) / Decimal('1000')) 
            df['forecast_montly_consumption'] = df['forecast_montly_consumption'].apply(lambda x: Decimal(str(x)) * Decimal('1000'))
            
            df['measuring_type'] = df['measuring_type'].apply(lambda x: STP_MAP_DICT.get(x) if STP_MAP_DICT.get(x) is not None else x) 
            df.rename(columns = {'invoice_group_name':'invoice_group'}, inplace = True)
            arr = []
            for index,row in df.iterrows():
                #print(f'in rows --------------->>{row.internal_id}')
                curr_contract = get_contract_by_internal_id(row['internal_id'])
                #print(f'From upload itns: current contract ----> {curr_contract}', file = sys.stdout)
                if curr_contract is None :
                    flash(f'Itn: {row.itn} does\'t have an contract ! Skipping !','danger')
                    print(f'Itn: {row.itn} does\'t have an contract ! Skipping !')
                    continue
                if curr_contract.start_date is None:
                    set_contarct_dates(curr_contract, row['activation_date'])
                curr_itn_meta = create_itn_meta(row)                    
                if curr_itn_meta is None:

                    flash(f'Itn: {row.itn} already exist ! Skipping !','info')
                    print(f'Itn: {row.itn} already exist ! Skipping !')
                    continue
                else:
                    
                    curr_sub_contr = generate_subcontract_from_file(row, curr_contract, df, curr_itn_meta)
                    if curr_sub_contr is not None:
                        #print(f'currrrr sub contr--->{curr_sub_contr} %%%%% curr_meta ----->{curr_itn_meta}')
                        curr_itn_meta.save()
                        curr_sub_contr.save()
                       
                        flash(f'Subcontract {curr_sub_contr} was created !','info')
                        print(f'Subcontract {curr_sub_contr} was created !','info')

                    else:
                        flash(f'Itn: {row.itn} faled to create subcontract ! Skipping !')
                        continue   

                    flash(f'Itn: {row.itn} was uploaded successifuly !','success') 
                    print(f'Itn: {row.itn} was uploaded successifuly !')        
        

        
        if request.files.get('file_hum_inv_groups').filename != '':  
            df = pd.read_excel(request.files.get('file_hum_inv_groups'), sheet_name=None, usecols='E,F,T')
            df = validate_input_df(df['data'])
            df.rename(columns = {'invoice_group_name':'name', 'invoice_group_description':'description'},inplace = True)
            df['contractor_id'] = df['911-9-4'].apply(lambda x: Contractor.query.filter(Contractor.acc_411 == x).first().id \
                                             if Contractor.query.filter(Contractor.acc_411 == x).first() is not None else 0)
            df = df[['name','contractor_id','description']]
            update_or_insert(df, InvoiceGroup.__table__.name)
            flash('upload was successiful','info')


            




        
    return render_template('upload_initial_data.html', title='Test', form=form)




@app.route('/')
@app.route('/index')
@login_required
def index():
    
    return render_template("index.html", title='Home Page')

@app.route('/login', methods=['GET', 'POST'])
def login():
	
    if current_user.is_authenticated:	   
        # #print('is_authenticated', file=sys.stdout)
        return redirect(url_for('index'))

    form = LoginForm()  

    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password')
            return redirect(url_for('login'))
        login_user(user, remember=form.remember_me.data)
        next_page = request.args.get('next')
        if not next_page or url_parse(next_page).netloc != '':
            next_page = url_for('index')
        return redirect(next_page)
    return render_template('login.html', title='Sign In', form=form)	
    

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User(username=form.username.data, email=form.email.data)
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()
        flash('Congratulations, you are now a registered user!')
        return redirect(url_for('login'))
    return render_template('register.html', title='Register', form=form)

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('index'))

@app.route('/add_contract', methods=['GET', 'POST'])
@login_required
def add_contract():

    form = NewContractForm() 
    form.contractor_id.choices = [(c.id, c.name) for c in Contractor.query.order_by(Contractor.name)]
    form.parent_contract_internal_id.choices = [(c.id, c.internal_id) for c in Contract.query.order_by(Contract.internal_id)]
    form.parent_contract_internal_id.choices.insert(0,(0,'None'))
    form.contract_type_id.choices = [(c.id, c.name) for c in ContractType.query.order_by(ContractType.id)]
    if form.validate_on_submit():

        signing_date_utc =  convert_date_to_utc("Europe/Sofia",form.signing_date.data)
        start_date_utc =  convert_date_to_utc("Europe/Sofia",form.start_date.data) if form.start_date.data != '' else None
        end_date_utc =  convert_date_to_utc("Europe/Sofia",form.end_date.data) + dt.timedelta(hours = 23) if form.end_date.data != '' else None
  
        current_conract = Contract(internal_id = form.internal_id.data, contractor_id = form.contractor_id.data, subject = form.subject.data, \
                        parent_id = form.parent_contract_internal_id.data, \
                        time_zone_id = form.time_zone.data.id, \
                        signing_date = signing_date_utc, \
                        start_date = start_date_utc , \
                        end_date = end_date_utc, \
                        duration_in_days = form.duration_in_days.data, \
                        invoicing_interval = form.invoicing_interval.data, maturity_interval = form.maturity_interval.data, \
                        contract_type_id = form.contract_type_id.data, is_work_days = form.is_work_days.data, \
                        automatic_renewal_interval = form.automatic_renewal_interval.data, collateral_warranty = form.collateral_warranty.data, \
                        notes = form.notes.data)
        # #print(f' %%%%%%%%%%%%%%%%%%%%    {form.time_zone.data.id}',file=sys.stdout)

        current_conract.save()  

        # , price = round(form.price.data, MONEY_ROUND)   has_balancing = form.has_balancing.data           
        # db.session.add(current_conract)    
        # db.session.commit()            
        # #print(current_conract,file=sys.stdout)
        # #print(f'{form.internal_id.data}, {form.contractor_id.data}, {round(form.price.data, MONEY_ROUND)},\
        # {form.subject.data},{form.parent_contract_internal_id.data},{form.signing_date.data},{form.start_date.data},{form.end_date.data},\
        # {form.duration_in_days.data},{form.invoicing_interval.data},{form.maturity_interval.data},{form.contract_type_id.data},\
        # {form.automatic_renewal_interval.data},{form.collateral_warranty.data},{form.notes.data},{form.is_work_days.data},{form.has_balancing.data}',file=sys.stdout)
    return render_template('add_contract.html', title='Add New Contract', form=form)

    #,start_date='2020-02-01',end_date='2020-12-31',price=125,invoicing_interval=31,maturity_interval=5,\
    #   contract_type_id=ContractType.query.filter_by(name = 'Procurement').first().id,is_work_days = 0,has_balancing = 1

@app.route('/add_itn', methods=['GET', 'POST'])
@login_required
def add_itn():

    form = AddItnForm()
    tz = "Europe/Sofia"
    form.measuring_type_id.choices = [(c.id, c.code) for c in MeasuringType.query.order_by(MeasuringType.id)]
    form.internal_id.choices = [(c.id, (f'{c.internal_id}, {c.contractor.name}, {convert_date_from_utc(tz,c.signing_date,False).date()}')) for c in Contract.query.order_by(Contract.internal_id)]                              
    form.erp_id.choices = [(c.id, c.name) for c in Erp.query.order_by(Erp.id)]
    form.grid_voltage.choices = [(x, x) for x in ['HV', 'MV', 'LV']]
    form.virtual_parent_id.choices = [(c.itn, c.itn) for c in ItnMeta.query.filter(ItnMeta.is_virtual == True).order_by(ItnMeta.itn)]
    form.virtual_parent_id.choices.insert(0,(None,None))
    form.invoice_group_name.choices = [(c.id, c.name) for c in InvoiceGroup.query.order_by(InvoiceGroup.name)]
    
    if form.validate_on_submit():
        
        # activation_date_utc =  convert_date_to_utc("Europe/Sofia",form.activation_date.data)
        
        #1. ADDRESS
        form_addr = form.address.data.lower() if form.address.data.lower() != '' else 'none'
        curr_address = AddressMurs.query.filter(AddressMurs.name == form_addr).first()
        if(curr_address is None):            
            curr_address = AddressMurs(name = form_addr)
                 
        
        #2. ITN META
        curr_itn_meta = ItnMeta.query.filter(ItnMeta.itn == form.itn.data).first()
        if curr_itn_meta is  None:
            curr_itn_meta = ItnMeta(itn = form.itn.data, 
            description = form.description.data, 
            grid_voltage = form.grid_voltage.data, 
            address = curr_address, 
            erp_id = form.erp_id.data, 
            is_virtual = form.is_virtual.data, 
            virtual_parent_itn = form.virtual_parent_id.data)
        
        else:
            flash('Such an ITN already exist !','info')
            return redirect(url_for('add_itn'))
            # curr_itn_meta.update({'description': request.form['description'],'grid_voltage': request.form['grid_voltage'],
            #                       'address': curr_address,'erp_id': request.form['erp_id'],'is_virtual':request.form['is_virtual'],
            #                       'virtual_parent_itn': request.form['virtual_parent_id']})
            # flash(f'ITN <{form.itn.data}> was successifuly updated !','success')
        
        #3. INVOICING GROUP
        curr_inv_group = InvoiceGroup.query.filter(InvoiceGroup.id == form.invoice_group_name.data).first()       
        
        #4 SUB CONTRACT
        curr_contract = Contract.query.filter(Contract.id == form.internal_id.data).first()
        
        if curr_contract is None:
            flash(f'Strange - no such an contract with id :{form.internal_id.data}!','danger')
        else:
            if curr_contract.start_date is None:
                set_contarct_dates(curr_contract, form.activation_date.data)

            curr_measuring_type = MeasuringType.query.filter(MeasuringType.id == form.measuring_type_id.data).first()
           
            forecasted_vol = None
            if request.files.get('file_').filename != '' and curr_measuring_type.code in ['DIRECT','UNDIRECT']:
                df = pd.read_excel(request.files.get('file_'), sheet_name=None)
                if set(df[form.itn.data].columns).issubset(['date', 'forecasted_volume']):
                    # forecasted_vol = Decimal(str(df[form.itn.data]['forecasted_volume'].sum()))
                    forecasted_vol = upload_forecasted_schedule_to_temp_db(df[form.itn.data], form.itn.data, round(form.price.data, MONEY_ROUND))
                    # g.forcasted_schedule = df[form.itn.data]
            else:
                if form.forecast_vol.data is None:
                    flash('No forcasted volume provided or measuring type mismatch.','danger')
                    return redirect(url_for('add_itn'))
                else:
                    forecasted_vol = Decimal(str(form.forecast_vol.data))
            
            curr_sub_contract = SubContract(itn = form.itn.data, \
                                    contract_id = curr_contract.id, \
                                    object_name = '',\
                                    # price = round(form.price.data, MONEY_ROUND), \
                                    invoice_group_id = curr_inv_group.id, \
                                    measuring_type_id = curr_measuring_type.id, \
                                    start_date = convert_date_to_utc("Europe/Sofia", form.activation_date.data),\
                                    end_date = curr_contract.end_date.replace(tzinfo=None), \
                                    zko = form.zko.data, \
                                    akciz = form.akciz.data,
                                    has_grid_services = form.has_grid_services.data, \
                                    has_spot_price = form.has_spot_price.data, \
                                    has_balancing = form.has_balancing.data, \
                                    forecast_vol = forecasted_vol)
            db_sub_contract = SubContract.query.filter((SubContract.itn == curr_sub_contract.itn) 
                                                        & (SubContract.start_date == curr_sub_contract.start_date) 
                                                        & (SubContract.end_date == curr_sub_contract.end_date)).first()
            if db_sub_contract is not None:
                flash('Duplicate sub contract','error')
            else:
                curr_sub_contract.save()  
                flash(f'Subcontract <{form.itn.data}> was successifuly created !','success')

    return render_template('add_itn.html', title='Add ITN', form=form)

@app.route('/add_invoicing_group', methods=['GET', 'POST'])
@login_required
def add_invoicing_group():

    form = AddInvGroupForm()
    tz = "Europe/Sofia"
    form.internal_id.choices = [(c.id, (f'{c.internal_id}, {c.contractor.name}, {convert_date_from_utc(tz,c.signing_date,False).date()}')) for c in Contract.query.order_by(Contract.internal_id)]    

    if form.validate_on_submit():
        curr_contract = Contract.query.filter(Contract.id == form.internal_id.data).first()
        curr_inv_group = InvoiceGroup.query.filter(InvoiceGroup.name == form.invoice_group_name.data).first()
        if curr_inv_group is not None:
            flash('Such invoicing group already exist','error')
        else:
            curr_contractor_id = curr_contract.contractor.id                               
            curr_inv_group = InvoiceGroup(name = form.invoice_group_name.data, contractor_id = curr_contractor_id)
            curr_inv_group.save()
            return redirect(url_for('add_invoicing_group'))

    return render_template('add_invoicing_group.html', title='Add Invoicing Group', form=form)


@app.route('/upload_inv_groups', methods=['GET', 'POST'])
@login_required
def upload_invoicing_group():

    form = UploadInvGroupsForm()
    if form.validate_on_submit():
        
        df = pd.read_excel(request.files.get('file_'),usecols = 'B:C')
        if all(elem in list(df.columns)  for elem in ['Contractor_Name', 'Invoice_Group_Name']):
            df = df.drop_duplicates(subset = 'Invoice_Group_Name', keep = 'first')
            groups = [InvoiceGroup(name = x[1]['Invoice_Group_Name'], contractor_id = Contractor.query.filter(Contractor.name == x[1]['Contractor_Name']).first().id) for x in df.iterrows()]

            # start = time.time() 
            # db.session.add_all(groups)
            db.session.bulk_save_objects(groups)
            db.session.commit()
            
            # end = time.time()
            flash('Invoice groups successifully uploaded','success')
        else:
            flash('Upload failed','error')        

    return render_template('upload_inv_groups.html', title='Upload Invoicing Group', form=form)

@app.route('/upload_itns', methods=['GET', 'POST'])
@login_required
def upload_itns():
    # template_cols = ['itn', 'activation_date', 'internal_id', 'measuring_type', 'invoice_group_name', 'invoice_group_description', 'price', 'zko', 
    #                 'akciz', 'has_grid_services', 'has_spot_price', 'erp','grid_voltage', 'address', 'description', 'is_virtual',
    #                 'virtual_parent_itn', 'forecast_montly_consumption','has_balancing', 'acc_411']
    template_cols = ['itn', 'activation_date', 'internal_id', 'measuring_type', 'invoice_group_name', 'invoice_group_description',  'zko', 
                    'akciz', 'has_grid_services', 'has_spot_price', 'erp','grid_voltage', 'address', 'description', 'is_virtual',
                    'virtual_parent_itn', 'forecast_montly_consumption','has_balancing', 'acc_411']
    form = UploadItnsForm()
    if form.validate_on_submit():
        df = pd.read_excel(request.files.get('file_'), sheet_name=None)
        if set(df['data'].columns).issubset(template_cols):
            
            # df['data']['price'] = df['data']['price'].apply(lambda x: Decimal(str(x)) / Decimal('1000'))
            df['data']['zko'] = df['data']['zko'].apply(lambda x: Decimal(str(x)) / Decimal('1000'))
            df['data']['akciz'] = df['data']['akciz'].apply(lambda x: Decimal(str(x)) / Decimal('1000'))

            df['data']['forecast_montly_consumption'] = df['data']['forecast_montly_consumption'].apply(lambda x: Decimal(str(x)) * Decimal('1000'))
            df['data'].rename(columns = {'invoice_group_name':'invoice_group'}, inplace = True)
            arr = []
            for index,row in df['data'].iterrows():
                
                curr_contract = get_contract_by_internal_id(row['internal_id'])
                #print(f'From upload itns: current contract ----> {curr_contract}', file = sys.stdout)
                
                if curr_contract is None :
                    flash(f'Itn: {row.itn} does\'t have an contract ! Skipping !')
                    continue
                if curr_contract.start_date is None:
                    set_contarct_dates(curr_contract, row['activation_date'])
                
                curr_itn_meta = create_itn_meta(row)                    
                if curr_itn_meta is None:

                    flash(f'Itn: {row.itn} already exist ! Skipping !','info')
                    continue
                else:
                    curr_sub_contr = generate_subcontract_from_file(row, curr_contract, df, curr_itn_meta)
                    if curr_sub_contr is not None:
                        curr_sub_contr.save()
                        
                        flash(f'Sucontract {curr_sub_contr} was created !','info')
                    else:
                        flash(f'Itn: {row.itn} faled to create subcontract ! Skipping !')
                        continue   

                    flash(f'Itn: {row.itn} was uploaded successifuly !','success')        
        else:
            flash(f'Upload failed from mismatched columns: {set(df.get("data").columns).difference(set(template_cols))}','danger') 

    return render_template('upload_itns.html', title='Upload ITNs', form=form)


@app.route('/upload_contracts/<start>/<end>/', methods=['GET', 'POST'])
@login_required
def upload_contracts(start,end):

    
    form = UploadContractsForm()
    if form.validate_on_submit():
        
        df = pd.read_excel(request.files.get('file_'),usecols = 'D:M,O:R')
        
        df = df.fillna(0)
        df = df[int(start):int(end)]
        flash(df.columns)
        if set(df.columns).issubset(['parent_id', 'internal_id', 'contractor', 'sign_date', 'start_date',
                                    'end_date', 'invoicing_interval', 'maturity_interval', 'contract_type',
                                    'is_work_day', 'automatic_renewal_interval', 'collateral_warranty',
                                    'notes','time_zone']):

            tks = df['internal_id'].apply(lambda x: validate_ciryllic(x))            
            parent_tks = df['parent_id'].apply(lambda x: validate_ciryllic(x) if x != 0 else True)
            all_cyr = tks.all()
            all_cyr_parent = parent_tks.all()
            
            if not (all_cyr & all_cyr_parent):
                flash('There is tk in latin, aborting', 'danger')
                return redirect(url_for('upload_contracts'))

            df = df.fillna(0)
            df['parent_id_initial_zero'] = 0
            df['end_date'] = df['end_date'] + dt.timedelta(hours = 23)
            df['duration_in_days'] = df.apply(lambda x: (x['end_date'] - x['start_date']).days + 1, axis = 1)
            df['time_zone'] = df['time_zone'].apply(lambda x: TimeZone.query.filter(TimeZone.code == x).first() if TimeZone.query.filter(TimeZone.code == x).first() is not None else x)
            
            renewal_dict = {'удължава се автоматично с още 12 м. ако никоя от страните не заяви писмено неговото прекратяване':12,'Подновява се автоматично за 1 година , ако никоя от страните не възрази писмено за прекратяването му поне 15 дни преди изтичането му':12,
                        'удължава се автоматично с още 6 м. ако никоя от страните не заяви писмено неговото прекратяване':6,'удължава се автоматично за 3 м. ако никоя от страните не заяви писмено неговото прекратяване с допълнително споразумение.':3,'За срок от една година. Подновява се с ДС / не се изготвя справка към ф-ра':12,
                        'За срок от една година. Подновява се с ДС / не се изготвя справка към ф-ра':12}
            df['automatic_renewal_interval'] = df['notes'].apply(lambda x: renewal_dict[x.strip()] if(renewal_dict.get(str(x).strip())) else 0 )

            invoicing_dict = {'до 12-то число, следващ месеца на доставката':42,'на 10 дни':10,'на 15 дни':15,'последно число':31,'конкретна дата':-1}
            df['invoicing_interval'] = df['invoicing_interval'].apply(lambda x: invoicing_dict[x.strip()] if(invoicing_dict.get(str(x).strip())) else 0 )
            contract_type_dict = {'OTC':'End_User','ОП':'Procurement'}

            df['contract_type'] = df['contract_type'].apply(lambda x: contract_type_dict[x.strip()] if(contract_type_dict.get(str(x).strip())) else 0 )
            df['contract_type'] = df['contract_type'].apply(lambda x: ContractType.query.filter(ContractType.name == x).first().id if ContractType.query.filter(ContractType.name == x).first() is not None else x)

            work_day_dict = {'календарни дни':0, 'работни дни':1}
            df['is_work_day'] = df['is_work_day'].apply(lambda x: work_day_dict[x.strip()] if(work_day_dict.get(str(x).strip())) else 0 )
            t_format = '%Y-%m-%dT%H:%M'
            contracts = [Contract(internal_id = x[1]['internal_id'], contractor_id = Contractor.query.filter(Contractor.name == x[1]['contractor']).first().id, subject = 'None', parent_id =  x[1]['parent_id_initial_zero'], \
                        signing_date =  convert_date_to_utc(x[1]['time_zone'].code,x[1]['sign_date'].strftime(t_format),t_format)  , \
                        start_date = convert_date_to_utc(x[1]['time_zone'].code, x[1]['start_date'].strftime(t_format),t_format), \
                        end_date = convert_date_to_utc(x[1]['time_zone'].code, x[1]['end_date'].strftime(t_format),t_format) , \
                        duration_in_days = x[1]['duration_in_days'], invoicing_interval = x[1]['invoicing_interval'], maturity_interval = x[1]['maturity_interval'], \
                        contract_type_id = x[1]['contract_type'], is_work_days = x[1]['is_work_day'], automatic_renewal_interval = x[1]['automatic_renewal_interval'], \
                        collateral_warranty = x[1]['collateral_warranty'], notes =  x[1]['notes'],time_zone_id = x[1]['time_zone'].id) \
                        for x in df.iterrows()]
            # start = time.time() 
            # flash(contracts)
            # db.session.add_all(contracts)
            db.session.bulk_save_objects(contracts)
            db.session.commit()
            flash(f'Contracts from {start} to {end} successifully uploaded','success')
            
            has_parrent_df = df[df['parent_id'] != 0]
            
            for index, row in has_parrent_df.iterrows():
                child = Contract.query.filter(Contract.internal_id == row['internal_id']).first()
                
                child.update({'parent_id':Contract.query.filter(Contract.internal_id == row['parent_id']).first().id})
                flash(f'parent {Contract.query.filter(Contract.id == child.parent_id).first().internal_id} added to {child.internal_id}','success')
                
            
            # end = time.time()
            
            
        else:
            flash('Upload failed','danger')  
            flash(df.shape,'info')      

    return render_template('upload_contracts.html', title='Upload Invoicing Group', form=form)




@app.route('/create_subcontract', methods=['GET', 'POST'])
@login_required
def create_subcontract():
    
    form = CreateSubForm()
    form.tariff_name.choices =['single_tariff', 'double_tariff', 'triple_tariff','custom']

    if form.validate_on_submit():
        curr_contract = get_contract_by_internal_id(form.contract_data.data.internal_id)

        if curr_contract.start_date is None:
            set_contarct_dates(curr_contract, form.start_date.data)
        
        time_zone = TimeZone.query.filter(TimeZone.id == curr_contract.time_zone_id).first().code
        form_start_date_utc = convert_date_to_utc(time_zone, form.start_date.data)
        form_end_date_utc = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)

        form_start_date_utc, form_end_date_utc = validate_subcontracts_dates(form_start_date_utc, form_end_date_utc, curr_contract)

        if form_start_date_utc is None:
            flash('Wrong dates according the contract !','danger')
            return redirect(url_for('create_subcontract'))
       
        form_zko = round(Decimal(str(form.zko.data)) / Decimal('1000'), MONEY_ROUND)
        form_akciz = round(Decimal(str(form.akciz.data)) / Decimal('1000'), MONEY_ROUND)
        form_forecast_df = pd.read_excel(request.files.get('file_'), sheet_name=None) if request.files.get('file_').filename != '' else None
        form_forecasted_vol = form.forecast_vol.data
        df = pd.read_excel(request.files.get('file_'),sheet_name=None) if request.files.get('file_').filename != '' else None
        
        forecast_df = validate_forecasting_df(df, form.itn.data.itn) if df is not None else None        
        
        applicable_sub_contracts = get_subcontracts_by_itn_and_utc_dates(form.itn.data.itn, form_start_date_utc, form_end_date_utc)
       
        if has_overlaping_subcontracts(form.itn.data.itn, form_start_date_utc) and has_overlaping_subcontracts(form.itn.data.itn, form_end_date_utc):
            flash('overlaping', 'danger')

        else:               
            new_sub_contract = (SubContract(itn = form.itn.data.itn,
                                    contract_id = form.contract_data.data.id, 
                                    object_name = form.object_name.data,                                    
                                    invoice_group_id = form.invoice_group.data.id, 
                                    measuring_type_id = form.measuring_type.data.id, 
                                    start_date = form_start_date_utc,
                                    end_date =  form_end_date_utc, 
                                    zko = form_zko, 
                                    akciz = form_akciz, 
                                    has_grid_services = form.has_grid_services.data, 
                                    has_spot_price = form.has_spot_price.data, 
                                    has_balancing = form.has_balancing.data))                                    
            
            for curr_subcontract in applicable_sub_contracts:                                  
                apply_collision_function(new_sub_contract, curr_subcontract, form.measuring_type.data.code, form.itn.data.itn, \
                                        form.forecast_vol.data, form_forecast_df, form.start_date.data, curr_contract)

            form_day_price = form.single_tariff_price.data if form.day_tariff_price.data == 0 else form.day_tariff_price.data

            curr_tariff =  create_tariff(form.tariff_name.data, form_day_price, form.night_tariff_price.data, form.peak_tariff_price.data)

            generate_forecast_schedule(form.measuring_type.data, form.itn.data.itn, form_forecasted_vol, forecast_df, form_start_date_utc, curr_contract, curr_tariff, form_end_date_utc)
           
            new_sub_contract.save() 
            
           




        # #print(applicable_sub_contracts, file = sys.stdout)
        # #print(has_overlaping_subcontracts(form.itn.data.itn, form_start_date_utc))

        
        # if len(sub_contracts) > 1:
        #     flash(f'Error ! Overlaping subcontracts with itn {form.itn.data.itn} and local start date {form.start_date.data}','error')

        # elif len(sub_contracts) == 1:
        #     old_sub_calculated_end_date = form_start_date_utc - dt.timedelta(hours = 1)
        #     new_sub_calculated_end_date = form_end_date_utc + dt.timedelta(hours = 23)        
        #     old_sub_end_date = sub_contracts[0].end_date

        #     if form_start_date_utc == curr_contract.start_date:
        #         #print('in strat date == conract start date', file = sys.stdout)
        #         if form_end_date_utc == curr_contract.end_date:
        #             #print('reset subcontract', file = sys.stdout)
        #             ItnSchedule.query.filter((ItnSchedule.utc >= form_start_date_utc) & (ItnSchedule.utc <= form_end_date_utc) & (ItnSchedule.itn == form.itn.data.itn)).delete()
        #             SubContract.query.filter((SubContract.itn == form.itn.data.itn) & (SubContract.start_date >= form_start_date_utc) & (SubContract.end_date <= form_end_date_utc)).delete()
        #             db.session.commit()
        #         else:
        #             sub_contracts[0].update({'start_date':form_end_date_utc})
        #             flash('old_subcontract start date updated','info')

        #     else:
        #         if form_end_date_utc != old_sub_end_date:
        #             remaining_schedule = get_remaining_forecat_schedule(form.itn.data.itn, form_end_date_utc)
        #             old_schedule = ItnSchedule.query.filter(ItnSchedule.itn == form.itn.data.itn, ItnSchedule.utc <= old_sub_end_date).all()
        #         # calculated_old_forecasted_vol = 
        #         sub_contracts[0].update({'end_date':old_sub_calculated_end_date})
        #         flash('old_subcontract updated','info')

            # forecasted_vol = check_and_load_hourly_schedule(form)    
            # new_sub_contract = SubContract(itn = form.itn.data.itn,
            #                         contract_id = form.contract_data.data.id, \
            #                         object_name = form.object_name.data,\
            #                         price = form_price, \
            #                         invoice_group_id = form.invoice_group.data.id, \
            #                         measuring_type_id = form.measuring_type.data.id, \
            #                         start_date = form_start_date_utc,\
            #                         end_date =  form_end_date_utc, \
            #                         zko = form_zko, \
            #                         akciz = form_akciz, \
            #                         has_grid_services = form.has_grid_services.data, \
            #                         has_spot_price = form.has_spot_price.data, \
            #                         has_balancing = form.has_balancing.data, \
            #                         forecast_vol = forecasted_vol)

            # new_sub_contract.save()
            # if (form_end_date_utc != old_sub_end_date) & (form_start_date_utc != curr_contract.start_date) & (form_end_date_utc != curr_contract.end_date):
            #     flash(f'ended before old sub contract end date - create one more additional sub','info')
            #     # forecasted_vol = Decimal(str('999'))  
            #     forecasted_vol = check_and_load_hourly_schedule(form)              
            #     additional_sub_contract = SubContract(itn = sub_contracts[0].itn,
            #                         contract_id = sub_contracts[0].contract_id, \
            #                         object_name = sub_contracts[0].object_name,\
            #                         price = sub_contracts[0].price, \
            #                         invoice_group_id = sub_contracts[0].invoice_group_id, \
            #                         measuring_type_id = sub_contracts[0].measuring_type_id, \
            #                         start_date = form_end_date_utc + dt.timedelta(hours = 1) ,\
            #                         end_date =  old_sub_end_date, \
            #                         zko = sub_contracts[0].zko, \
            #                         akciz = sub_contracts[0].akciz, \
            #                         has_grid_services = sub_contracts[0].has_grid_services, \
            #                         has_spot_price = sub_contracts[0].has_spot_price, \
            #                         has_balancing = sub_contracts[0].has_balancing, \
            #                         forecast_vol = forecasted_vol)

            #     additional_sub_contract.save()               
       
            
            
            # sub_contracts[0].end_date = old_sub_calculated_end_date
                

                

            # flash(f'old_sub_calculated_end_date :{old_sub_calculated_end_date}', 'info')
            # flash(f'form.start_date: {convert_date_to_utc("Europe/Sofia",form.start_date.data)}', 'info')
            # flash(f'form_sub_end_date: {form_sub_end_date}', 'info')
            # flash(f'old_sub_end_date: {old_sub_end_date}', 'info')

        
        



    return render_template('create_subcontract.html', title='Create SubContract', form = form)






@app.route('/table', methods=['GET', 'POST'])
@login_required
def table():
    return render_template('table.html', title='Table')







# def __get_contract_by_nternal_id__(internal_id):
#     return Contract.query.filter(Contract.internal_id == internal_id).first()




    
