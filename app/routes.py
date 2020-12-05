import os
import glob as gl
import os.path
import xlrd
import time,re
import sys, pytz, datetime as dt
import pandas as pd
from zipfile import ZipFile
from flask import render_template, flash, redirect, url_for, request, send_file,send_from_directory
from sqlalchemy import extract, or_
from app import app
from app.forms import (
    LoginForm, RegistrationForm, NewContractForm, AddItnForm, AddInvGroupForm, ErpForm, AdditionalReports, RedactEmailForm,
    UploadInvGroupsForm, UploadContractsForm, UploadItnsForm, CreateSubForm, TestForm, MonthlyReportErpForm, PostForm,
    UploadInitialForm, IntegraForm, InvoiceForm, MonthlyReportForm, MailForm, MonthlyReportErpForm,MonthlyReportOptionsForm)
from flask_login import current_user, login_user, logout_user, login_required
from app.models import *

from werkzeug.urls import url_parse

from werkzeug.utils import secure_filename
import calendar



from app.helpers.helper_functions import (get_contract_by_internal_id,
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
                                 date_format_corector,
                                 get_excel_files,
                                 delete_excel_files,
                                 parse_integra_csv,
                                 create_df_from_integra_csv,
                                 get_files,
                                 update_ibex_data,
                                 update_schedule_prices,
                                 delete_files,
                                 apply_linked_collision_function,
                                 convert_datetime64_to_datetime,
                                 generate_tariff_hours
                                 )

# from app.helpers.helper_function_excel_writer import ( INV_REFS_PATH, INTEGRA_INDIVIDUAL_PATH, INTEGRA_FOR_UPLOAD_PATH, PDF_INVOICES_PATH, REPORTS_PATH)

from app.helpers.helper_functions_queries import (                                         
                                        get_contractors_names_and_411,
                                        is_spot_inv_group,
                                        get_all_inv_groups,
                                        get_time_zone,
                                        get_list_inv_groups_by_contract,
                                        has_ibex_real_data,
                                        get_inv_gr_id_single_erp,
                                        get_inv_gr_id_erp,
                                        get_inv_groups_by_internal_id_and_dates,
                                        get_subcontacts_by_internal_id_and_start_date,
                                        get_subcontracts_by_inv_gr_name_and_date,
                                        get_grid_itns_by_erp_for_period,
                                        get_non_grid_itns_by_erp_for_period,
                                        get_all_itns_by_erp_for_period,
                                        get_incomming_grid_itns, 
                                        get_incomming_non_grid_itns
)

from app.helpers.helper_functions_erp import (reader_csv, insert_erp_invoice,insert_mrus, get_distribution_stp_records,
                                      insert_settlment_cez, insert_settlment_e_pro, insert_settelment_eso,
                                      insert_settlment_evn, insert_settelment_nkji ,update_reported_volume,order_files_by_date,
                                      get_missing_extra_points_by_erp_for_period, create_report_by_itn,                     
                                      
)
from app.helpers.helper_functions_reports import (create_report_from_grid, get_summary_df_non_spot,
                                         get_summary_spot_df, get_weighted_price, create_utc_dates,
                                         get_weighted_price, create_excel_files, appned_df, create_full_ref_for_all_itn)

from app.helpers.invoice_writer import create_invoices
from app.email import (send_email)
from app.helpers.helper_functions_bgposhti import cash_receipt_generation, upload_file_generation

MEASURE_MAP_DICT = {
                'B01':'EPRO_B01','B02':'EPRO_B02','B03':'EPRO_B03','B04':'EPRO_B04','H01':'EPRO_H01','H02':'EPRO_H02','S01':'EPRO_S01','BD000':'EVN_BD000','G0':'EVN_G0','G1':'EVN_G1','G2':'EVN_G2',
                'G3':'EVN_G3','G4':'EVN_G4', 'H0':'EVN_H0','H1':'EVN_H1','H2':'EVN_H2','B1':'CEZ_B1','B2':'CEZ_B2','B3':'CEZ_B3','B4':'CEZ_B4','B5':'CEZ_B5','H1':'CEZ_H1','H2':'CEZ_H2','S1':'CEZ_S1',
                'DIRECT':'DIRECT','UNDIRECT':'UNDIRECT'    
            }
MONEY_ROUND = 9



@app.route('/additional_reports', methods=['GET', 'POST'])
def add_reports():
    form = AdditionalReports()

    if form.submit_delete.data:
            delete_files(os.path.join(app.root_path, app.config['REPORTS_PATH']), form.reports_files.data, 'xlsx', form.delete_all.data)
            return redirect(url_for('add_reports'))

    # form.ref_files.choices = sorted([(x,x) for x in get_files(INV_REFS_PATH,'xlsx')])
    # form.reports_files.choices = sorted([(x,x) for x in get_files(REPORTS_PATH,'xlsx')])
    form.ref_files.choices = sorted([(x,x) for x in get_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']),'xlsx')])
    form.reports_files.choices = sorted([(x,x) for x in get_files(os.path.join(app.root_path, app.config['REPORTS_PATH']),'xlsx')])
    
    if form.validate_on_submit():
        if form.submit.data:
            files = form.ref_files.data
            summary_df = create_full_ref_for_all_itn(files)
            now = dt.datetime.now()
            now_string = now.strftime("%Y-%m-%d %H:%M:%S")
            summary_df.to_excel(os.path.join(os.path.join(app.root_path, app.config['REPORTS_PATH']), f'{now_string}_full_report.xlsx'), index = False)
            return redirect(url_for('add_reports'))
       
        

    return render_template('quick_template_wider.html', title='Additional Reports', form=form, header = 'Additional Reports')

@app.route('/monthly_erp', methods=['GET', 'POST'])
def monthly_erp():
    form = MonthlyReportOptionsForm()
    if form.validate_on_submit():
        if form.submit.data:
            
            return redirect(url_for('monthly_report_by_erp', erp = form.erp.data, start_date = form.start_date.data, 
                            end_date = form.end_date.data, contract_type = form.contract_type.data, is_mixed = form.include_all.data,  **request.args))

    return render_template('quick_template.html', title='Monthly reports filter', form=form, header = 'Monthly report filters')

@app.route('/redact_email', methods=['GET', 'POST'])
@login_required
def redact_email():
    form = RedactEmailForm()
    
    if form.validate_on_submit():
        res = form.inv_goups_mails.data
        new_mail = form.new_mail.data
        mail_to_add = Mail.query.filter(Mail.name == new_mail).first()
        if mail_to_add is None:
            mail_to_add = Mail(name = new_mail)
            mail_to_add.save()
            print(f'adding new mail {mail_to_add}')
            mail_to_add = Mail.query.filter(Mail.name == new_mail).first()
        curr_inv_group = form.inv_goups_mails.data
        curr_inv_group.update({'email_id':mail_to_add.id})
        print(f'Invoicing group {curr_inv_group.name} will mail to {mail_to_add.name} !')
        return redirect(url_for('redact_email'))

    return render_template('quick_template_wider.html', title='Readcting Email', form=form, header = 'Redacting EMAILS')

@app.route('/bgpost', methods=['GET', 'POST'])
@login_required
def bgpost():
    form = PostForm()
    
    if form.validate_on_submit():
        if form.upload_csv.data:
            dtype_dict= {'ANUM': str, 'PHONE' : str, 'INVOICE' : str,'AMOUNT':float}
            df = pd.read_csv(request.files.get('file_easypay_csv'), sep=';', dtype = dtype_dict)
            upload_file_generation(df)

    return render_template('quick_template.html', title='BgPost', form=form, header = 'Create BgPost file')

@app.route('/test', methods=['GET', 'POST'])
@login_required
def test():
    form = TestForm()
    # summary_df = pd.DataFrame()
    # form = PostForm()
    if form.validate_on_submit():

        metas = ItnMeta.query.all()
        count = 1
        for itn in metas:
            itn.delete()
            print(f'{count} - {itn} deleted')
            count+=1
        
        # erp_invoice_df =  pd.read_sql(ErpInvoice.query.statement, db.session.bind) 
        # df = erp_invoice_df.drop_duplicates(subset=['composite_key'], keep = 'last')
        # print(f'{erp_invoice_df.shape}')
        # print(f'{df.shape}')
        # erp_name = 'E-PRO'
        # start_date = convert_date_to_utc('EET',form.start_date.data)   
        # end_date = convert_date_to_utc('EET',form.end_date.data) 
        # # start_date = convert_date_to_utc('EET','2020-10-01')   
        # # end_date = convert_date_to_utc('EET','2020-10-31') 
        # end_date = end_date + dt.timedelta(hours = 23)
        # invoice_start_date = start_date + dt.timedelta(hours = (10 * 24 + 1))
        # invoice_end_date = end_date + dt.timedelta(hours = (10 * 24))

        
        # get_missing_extra_points_by_erp_for_period(erp_name, start_date, end_date)

        # grid_db_itns = get_grid_itns_by_erp_for_period(erp_name, start_date, end_date)
        # non_grid_db_itns = get_non_grid_itns_by_erp_for_period(erp_name, start_date, end_date)
        # incomming_grid_itns = get_incomming_grid_itns(erp_name, start_date, end_date)
        # incomming_non_grid_itns = get_incomming_non_grid_itns(erp_name, start_date, end_date)
        # a = "32Z470001214089K" in non_grid_db_itns
        # print(a)
        # extra_non_grid = list(set(incomming_non_grid_itns) - set(non_grid_db_itns) - set(incomming_grid_itns))
        # extra = list(set(incomming_non_grid_itns) - set(non_grid_db_itns) - set(grid_db_itns))
        # print(f'{extra}')

        # # all_db_itns = get_all_itns_by_erp_for_period(ERP, start_date, end_date)
        # # print(f'{len(all_db_itns)}')
        # itn_records = (
        #     db.session
        #         .query(
        #             SubContract.itn                                
        #         )  
        #         .join(ItnMeta, ItnMeta.itn == SubContract.itn) 
        #         .join(Erp, Erp.id == ItnMeta.erp_id)                                
        #         .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))                        
        #         .distinct(SubContract.itn) 
        #         .all())
        # all_itns = [x[0] for x in itn_records]
        # print(f'{len(all_itns)}')

        # all_incomming_itn =(
        #     db.session.query(
        #         IncomingItn.itn
        #     )
        #     .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        #     .join(Erp, Erp.id == ItnMeta.erp_id)               
        #     .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date)
        #     .distinct(IncomingItn.itn) 
        #     .all()

        # )
        # all_incomming_itn = [x[0] for x in all_incomming_itn]
        # print(f'{len(all_incomming_itn)}')

        # missing = list(set(all_itns) - set(all_incomming_itn))
        # df = pd.DataFrame()
        # for itn in missing:
        #     rec = (db.session
        #         .query(ItnMeta.itn, Erp.name.label('erp'), Contract.internal_id, InvoiceGroup.description.label('inv_description'),
        #             InvoiceGroup.name.label('invoice_name'), SubContract.start_date.label('sub_start_date'), SubContract.end_date.label('sub_end_date'), MeasuringType.code.label('measuring_type'))
        #         .join(SubContract, SubContract.itn == ItnMeta.itn)
        #         .join(Contract, Contract.id == SubContract.contract_id)
        #         .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id)
        #         .join(MeasuringType, MeasuringType.id == SubContract.measuring_type_id)
        #         .join(Erp, Erp.id == ItnMeta.erp_id)
        #         .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
        #         .filter(ItnMeta.itn == itn).
        #         all()
        #     )
        
        #     temp_df = pd.DataFrame.from_records(rec, columns = rec[0].keys())
        #     if df.empty:
        #         df = temp_df
        #     else:
        #         df = df.append(temp_df, ignore_index=True)
        # df = df.sort_values(['erp','inv_description'])
        # df.to_excel('temp/all_missing_10.xlsx')
        # print(f'{df}')
        # # get_missing_extra_points_by_erp_for_period(ERP, start_date, end_date)
        # grid_db_itns = get_grid_itns_by_erp_for_period(ERP, start_date, end_date)
        # non_grid_db_itns = get_non_grid_itns_by_erp_for_period(ERP, start_date, end_date)

        # incomming_grid_itns = (
        #     db.session.query(
        #         IncomingItn.itn
        #     )
        #     .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        #     .join(Erp, Erp.id == ItnMeta.erp_id)  
        #     .filter(Erp.name == ERP) 
        #     .filter(IncomingItn.as_grid == 1)       
        #     .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date)
        #     .all()

        # )
        # incomming_grid_itns = [x[0] for x in incomming_grid_itns]
        # print(f'incomming_grid_itns from {ERP} -- {len(incomming_grid_itns)}')

        # incomming_non_grid_itns = (
        #     db.session.query(
        #         IncomingItn.itn
        #     )
        #     .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        #     .join(Erp, Erp.id == ItnMeta.erp_id)  
        #     .filter(Erp.name == ERP) 
        #     .filter(IncomingItn.as_grid == 0)       
        #     .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date)
        #     .all()

        # )
        # incomming_non_grid_itns = [x[0] for x in incomming_non_grid_itns]
        # print(f'incomming_non_grid_itns from {ERP} -- {len(incomming_non_grid_itns)}')

        # incomming_sett_itns = (
        #     db.session.query(
        #         IncomingItn.itn
        #     )
        #     .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        #     .join(Erp, Erp.id == ItnMeta.erp_id)  
        #     .filter(Erp.name == ERP) 
        #     .filter(IncomingItn.as_settelment == 1)       
        #     .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date)
        #     .all()

        # )
        # incomming_sett_itns = [x[0] for x in incomming_sett_itns]
        # print(f'incomming_sett_itns from {ERP} -- {len(incomming_sett_itns)}')
        # print(f'grid_db_itns {len(grid_db_itns)}')
        # print(f'non_grid_db_itns {len(non_grid_db_itns)}')


        # missing_grid = list(set(grid_db_itns) - set(incomming_grid_itns)) 
        # print(f'missing_grid---> {missing_grid}')
        # a = '32Z103003032365Y' in grid_db_itns
        # print(a)
        # [print(x) for x in missing_grid]
        # missing_grid_df = create_report_by_itn(missing_grid, start_date, end_date, ERP, True)
        # print(f'missing_df \n{missing_grid_df}')

        # missing_non_grid = list(set(non_grid_db_itns) - set(incomming_non_grid_itns))
        # missing_non_grid_df = create_report_by_itn(missing_non_grid, start_date, end_date, ERP, True)
        # print(f'missing_non_grid_df \n{missing_non_grid_df}')

        # all_incomming_itns= (
        #         db.session.query(
        #             IncomingItn.itn
        #         )
        #     .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        #     .join(Erp, Erp.id == ItnMeta.erp_id)  
        #     .filter(Erp.name == ERP)                    
        #     .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date)
        #     .all()
        # )
        # all_incomming_itns = [x[0] for x in all_incomming_itns]

        # all_db_itns= (            
        #     db.session.query(
        #             SubContract.itn                                
        #         )  
        #     .join(ItnMeta, ItnMeta.itn == SubContract.itn) 
        #     .join(Erp, Erp.id == ItnMeta.erp_id)
        #     .filter(Erp.name == ERP)                  
        #     .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))                          
        #     .distinct(SubContract.itn) 
        #     .all()
        # )
        # all_db_itns = [x[0] for x in all_db_itns]

        # missing_all_itns = list(set(all_db_itns) - set(all_incomming_itns))
        # missing_all_itns_df = create_report_by_itn(missing_all_itns, start_date, end_date, ERP, True)
        # print(f'missing_all_itns_df \n{missing_all_itns_df}')
        # print(f'{missing_all_itns}')
        # get_missing_extra_points_by_erp_for_period(ERP, start_date, end_date)
        # distribution_stp_records = get_distribution_stp_records(ERP,start_date,end_date)

        # stp_records_df = pd.DataFrame.from_records(distribution_stp_records, columns=distribution_stp_records[0].keys())
        # total_consumption_records = (
        #    db.session
        #     .query(Distribution.itn.label('itn'), 
        #         func.sum(Distribution.calc_amount).label('total_consumption')) 
            
        #     .join(ErpInvoice, ErpInvoice.id == Distribution.erp_invoice_id)   
        #     .filter(Distribution.itn.in_(stp_records_df['itn']))      
        #     .filter(Distribution.tariff.in_(['Достъп','Пренос през електропреносната мрежа', 'Разпределение'])) 
        #     .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date) 
        #     .group_by(Distribution.itn)
        #     .all()
        # )
        # # print(f'{total_consumption_records}')
        # total_consumption_df = pd.DataFrame.from_records(total_consumption_records, columns=total_consumption_records[0].keys())
        # total_consumption_df.to_excel('temp/total_consumption_df.xlsx')
        # stp_records_df.to_excel('temp/stp_records_df.xlsx')
        # # print(f'@@@@@@@@@@@@@@@@@@@ TOTAL CONSUMPTION DF @@@@@@@@@@@@@@@@@@ \n {total_consumption_df}')
        # total_consumption_df = total_consumption_df.merge(stp_records_df, on = 'itn', how = 'right')
        # total_consumption_df.to_excel('temp/total_consumption_df_2.xlsx')
        
        # missing_points = total_consumption_df[total_consumption_df['total_consumption'].isnull()]['itn']   
        
        # total_consumption_df['total_consumption'] = total_consumption_df['total_consumption'].apply(lambda x: Decimal('0') if pd.isnull(x) else x)
        # print(f'Missing point from input CSV files regard input settelment file \n{missing_points}')


        # r = db.session.query(Mail.id, Mail.name).all()
        # mail_df = pd.DataFrame.from_records(r, columns=r[0].keys())
        # mail_df['mask'] = mail_df['name'].apply(lambda x: str(x).find(',') != -1 )
        # res_df = mail_df[mail_df['mask']]
        # res_df['name'] = res_df['name'].apply(lambda x: str(x).replace(',',';'))
        # # bulk_update_list = res_df.to_dict(orient='records')  
        
        # # db.session.bulk_update_mappings(Mail, bulk_update_list)
        # # db.session.commit()
        # print(f'{res_df}')
        # curr_mails = db.session.query(Contractor.email).filter(Contractor.id == 1565).first()[0]
        # db_mails = db.session.query(Mail.id, Mail.name).filter(Mail.name == curr_mails).all()
        # mail_id = db.session.query(Mail.id).filter(Mail.name == curr_mails).first()[0]
        # df = pd.DataFrame.from_records(db_mails, columns=db_mails[0].keys())
        
        
        # df['mask'] = df['name'].apply(lambda x: str(x).find(a) != -1)
        # df= df[df['mask']]
        # print(f'{len(db_mails)}')
        # contracts_mails = (db.session
        #                 .query(InvoiceGroup.id,InvoiceGroup.name, InvoiceGroup.description, Contractor.acc_411)                                               
        #                 .join(SubContract, SubContract.invoice_group_id == InvoiceGroup.id)   
        #                 .join(Contractor, Contractor.id == InvoiceGroup.contractor_id)                      
        #                 # .filter( ~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
        #                 .distinct()
        #                 .all()
        # )
        # df_c = pd.DataFrame.from_records(contracts_mails, columns=contracts_mails[0].keys())
        # mails = Contractor.query.with_entities(Contractor.acc_411,Contractor.email.label('mail')).distinct().all()
        # df = pd.DataFrame.from_records(mails, columns=mails[0].keys())
        # df['mail'] = df['mail'].apply(lambda x: str(x).strip().lower())
        # df_1 = df.merge(df_c, on='acc_411')
        # db_mails = db.session.query(Mail.id.label('email_id'), Mail.name.label('mail')).all()
        # df_mails = pd.DataFrame.from_records(db_mails, columns=db_mails[0].keys())
        # df_1 = df_1.merge(df_mails, on='mail')
        # db_df = df_1[['id','email_id']]
        # bulk_update_list = db_df.to_dict(orient='records')  
        
        # db.session.bulk_update_mappings(InvoiceGroup, bulk_update_list)
        # db.session.commit()
        # a = df_1[df_1['mail'] == 'daci.r@abv.bg']
        # a = df_c[df_c['acc_411'] == '411-3-126']
        # print(f'{a}')


        # db.session.query(IncomingItn).delete()
        # db.session.commit()
        # s = IncomingItn.query.delete()
        # print(f'{s}')
        # delete_sch = IncomingItn.__table__.delete()
        
        # db.session.execute(delete_sch)
        # db.session.commit()
        # time_zone = 'EET'
        # start_date = convert_date_to_utc('EET',form.start_date.data)   
        # end_date = convert_date_to_utc('EET',form.end_date.data) 
        # end_date_f = end_date + dt.timedelta(hours = 23)
        # erp_name = 'E-PRO'
        # print(f'{start_date} --- {end_date_f}')
        # itn_records_in_db = (
        #     db.session
        #         .query(ItnMeta.itn)            
        #         .join(SubContract, SubContract.itn == ItnMeta.itn)  
        #         .join(Erp, Erp.id == ItnMeta.erp_id)  
        #         .filter(Erp.name == erp_name)        
        #         .filter(~((SubContract.start_date > end_date_f) | (SubContract.end_date < start_date)))
        #         .distinct(ItnMeta.itn)
        #         .all())
        # itn_in_db = [x[0] for x in itn_records_in_db]

        # itn_records_in_incoming = (
        #     db.session
        #         .query(IncomingItn.itn)   
        #         .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        #         .join(Erp, Erp.id == ItnMeta.erp_id)  
        #         .filter(Erp.name == erp_name)        
        #         .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date_f)
        #         .all())
            
        # incoming_itns = [x[0] for x in itn_records_in_incoming]
        # # print(f'{incoming_itns}')

        # db_itn_set = set(itn_in_db)
        # incoming_itns = set(incoming_itns)
        # missing = list(db_itn_set - incoming_itns)
        # print(f'This itn points are in the database but not came data for them from ERP: {erp_name} files ---> {missing}')
        # extra = list(incoming_itns - db_itn_set)
        # print(f'This itn points are NOT in the database but came data for them from ERP: {erp_name} files ---> {extra}')

        # missing_df = pd.DataFrame()
        # for itn in missing:
        #     rec = (db.session
        #         .query(ItnMeta.itn, Contract.internal_id, InvoiceGroup.description.label('inv_fescription'),
        #             InvoiceGroup.name.label('invoice_name'), SubContract.start_date.label('sub_start_date'), SubContract.end_date.label('sub_end_date'))
        #         .join(SubContract, SubContract.itn == ItnMeta.itn)
        #         .join(Contract, Contract.id == SubContract.contract_id)
        #         .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id)
        #         .filter(~((SubContract.start_date > end_date_f) | (SubContract.end_date < start_date)))
        #         .filter(ItnMeta.itn == itn).
        #         all()
        #     )
        #     temp_df = pd.DataFrame.from_records(rec, columns = rec[0].keys())
        #     temp_df['sub_start_date'] = temp_df['sub_start_date'].apply(lambda x: convert_date_from_utc('EET', x, True, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"))
        #     temp_df['sub_end_date'] = temp_df['sub_end_date'].apply(lambda x: convert_date_from_utc('EET', x, True, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"))
        #     if missing_df.empty:
        #         missing_df = temp_df
        #     else:
        #         missing_df = missing_df.append(temp_df, ignore_index=True)
        # print(f'{missing_df}')

        # start_date_f = convert_date_to_utc('EET',"2020-11-01")
        # end_date_f = convert_date_to_utc('EET',"2020-12-31")
        # end_date_f_ = end_date_f + dt.timedelta(hours = 23)
        # print(f'{start_date_f} --- {end_date_f} --- {end_date_f_}')

        # local_start_date = convert_date_from_utc(time_zone, start_date_f, False)    
        # local_end_date = convert_date_from_utc(time_zone, end_date_f, False)
        # local_start_date_2 =  local_end_date.replace(day = 1)
        # local_start_date_2 =  convert_date_from_utc(time_zone, local_start_date_2 , False)
        # print(f'{local_start_date} --- {local_end_date} --- {local_start_date_2}')


        # time_series = pd.date_range(start = local_start_date, end = local_end_date , freq='h', tz = time_zone)
        # forecast_df = pd.DataFrame(time_series, columns = ['utc'])
        # forecast_df['weekday'] = forecast_df['utc'].apply(lambda x: x.strftime('%A'))
        # forecast_df['hour'] = forecast_df['utc'].apply(lambda x: x.hour)


        # invoice_start_date = start_date + dt.timedelta(hours = (10 * 24 + 1))
        # invoice_end_date = end_date + dt.timedelta(hours = (10 * 24))
        
        # invoice_group = '411-3-135_1'
        # itn = '32Z4700032380151'
        # tarrif = Tariff.query.first()
        # # itn = 'BG5521900880000000000000002288570'
        
        # end_date_ = end_date + dt.timedelta(hours = 23)
        # rec = (
        #     db.session.query(ItnSchedule.itn, ItnSchedule.utc, ItnSchedule.forecast_vol, ItnSchedule.consumption_vol, ItnSchedule.settelment_vol)
        #     .filter(ItnSchedule.itn == itn, ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date_)
        #     .all()

        # )
        # df = pd.DataFrame.from_records(rec, columns=rec[0].keys())
        # df.set_index('utc', inplace = True)
        # df.index = df.index.tz_localize('UTC').tz_convert(time_zone)
        # df.reset_index(inplace = True)
        # df['weekday'] = df['utc'].apply(lambda x: x.strftime('%A'))
        # df['hour'] = df['utc'].apply(lambda x: x.hour)
        # df['consumption_vol']  = df['consumption_vol'].astype(float)
        # df = df[['weekday','hour','consumption_vol']].groupby(['weekday','hour']).mean()

        # merged_df = forecast_df.merge(df,on = ['weekday','hour'], how = 'right')
        # # merged_df.drop(columns = ['utc_y'], inplace = True)
        # # merged_df.rename(columns = {'utc_x':'utc'}, inplace = True)
        # merged_df.set_index('utc', inplace = True)
        # merged_df.index = merged_df.index.tz_convert('UTC').tz_localize(None)
        # merged_df.sort_index(inplace=True)
        # merged_df.to_excel('temp/merged.xlsx')
        # # df['weekday'] = df.index.map(lambda x: x.to_pydatetime().weekday()) 
        # # df['hour'] = df.index.map(lambda x: x.to_pydatetime().hour) 
        # # # df.reset_index(inplace = True)
        # # df['consumption_vol']  = df['consumption_vol'].astype(float)
        # # df['proba'] = df.index.map(lambda x: generate_tariff_hours(x, tarrif)) # x.name because use index
        # # df['proba2'] = df.index.map(lambda x: generate_tariff_hours(x, tarrif) * Decimal(str(df.loc[x]['consumption_vol']))) 
        # # averaged_df = df[['hour','consumption_vol','weekday']].groupby(['hour','weekday']).mean()
        # # df.apply(lambda x: print(f'{x.index}')) # x.name because use index
        # # a = convert_datetime64_to_datetime(df.head(1).utc.values[0])
        # # b = convert_datetime64_to_datetime(df.tail(1).utc.values[0])
        # print(f'{merged_df}')
        # rec = get_inv_groups_by_internal_id_and_dates('ТК45', start_date)
        # rec = get_subcontacts_by_internal_id_and_start_date('ТК45', start_date)
        
        # print(f'ddddddddddddddd --- > {start_date}')
        # print(f'{rec}')
        # inv_groups_itns = (
        #     SubContract
        #     .query
        #     .join(InvoiceGroup,InvoiceGroup.id == SubContract.invoice_group_id)
        #     .filter(InvoiceGroup.name == '411-3-135_1')
        #     .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
        #     .all()
        # )
        # linked_contract = Contract.query.filter(Contract.internal_id == 'ТК45').first()
        # linked_contract_start_date = linked_contract.start_date
        # linked_contract_end_date = linked_contract.end_date
        
        
        # inv_groups_itns_linked = (
        #     SubContract
        #     .query
        #     .join(InvoiceGroup,InvoiceGroup.id == SubContract.invoice_group_id)
        #     .filter(InvoiceGroup.name == '411-3-135_1')
        #     .filter((SubContract.start_date < linked_contract_end_date) & (SubContract.end_date >= linked_contract_end_date))
        #     .all()
        # )

        # print(f'ddddddddddddddd --- > {inv_groups_itns_linked}')

            
       

    return render_template('test.html', title='TEST', form=form)

@app.route('/mailing', methods=['GET', 'POST'])
@login_required
def mailing():
    form = MailForm()
    # # form.attachment_files.choices = sorted([(x[0],x[1]) for x in create_list_of_tuples(INV_REFS_PATH, PDF_INVOICES_PATH)])
    # if form.validate_on_submit():
    #     if form.submit.data:
    #         selected_invoices = form.attachment_files.data
    #         for inv in selected_invoices:
    #             contractor =  Contractor.query.filter(Contractor.id == inv.contractor_id).first()
    #             mails = contractor.email.split(';')
    #             mails = [x for x in mails]
    #             mails.append('t.kalaidjieva@grandenergy.net')
    #             ref_file_name = inv.ref_file_name 
    #             inv_file_name = str(inv.id)+ '.pdf'
    #             # file_data = [(PDF_INVOICES_PATH, inv_file_name), (INV_REFS_PATH, ref_file_name, inv_file_name)]
    #             if form.send_excel.data:
    #                 file_data = [(os.path.join(app.root_path, app.config['INV_REFS_PATH']), ref_file_name, inv_file_name)]
    #             elif form.send_pdf.data:
    #                 file_data = [(os.path.join(app.root_path, app.config['PDF_INVOICES_PATH']), inv_file_name)]
    #             else:
    #                 file_data = [(os.path.join(app.root_path, app.config['PDF_INVOICES_PATH']), inv_file_name), (os.path.join(app.root_path, app.config['INV_REFS_PATH']), ref_file_name, inv_file_name)]
                
    #             send_email(mails, file_data, form.subject.data)   
    # 
    if form.validate_on_submit():
        if form.submit.data:
            selected_invoices = form.attachment_files.data
            for inv in selected_invoices:
                tokens = inv.ref_file_name.split('-')
                
                inv_group_name = db.session.query(InvoiceGroup.name).filter(InvoiceGroup.id == inv.invoice_group_id).first()[0]
                raw_mails =  db.session.query(Mail.name).join(InvoiceGroup, InvoiceGroup.email_id == Mail.id).filter(InvoiceGroup.name == inv_group_name).first()[0]
                
                mails =[x for x in raw_mails.split(';')]
                print(f'{mails}')
                mails.append('openmarket@grandenergy.net')
                ref_file_name = inv.ref_file_name 
                inv_file_name = str(inv.id)+ '.pdf'
                # file_data = [(PDF_INVOICES_PATH, inv_file_name), (INV_REFS_PATH, ref_file_name, inv_file_name)]
                if form.send_excel.data:
                    file_data = [(os.path.join(app.root_path, app.config['INV_REFS_PATH']), ref_file_name, inv_file_name)]
                elif form.send_pdf.data:
                    file_data = [(os.path.join(app.root_path, app.config['PDF_INVOICES_PATH']), inv_file_name)]
                else:
                    file_data = [(os.path.join(app.root_path, app.config['PDF_INVOICES_PATH']), inv_file_name), (os.path.join(app.root_path, app.config['INV_REFS_PATH']), ref_file_name, inv_file_name)]
                
                send_email(mails, file_data, form.subject.data)       
    

    return render_template('mail.html', title='TEST', form=form)

@app.route('/create_invoice', methods=['GET', 'POST'])
@login_required
def create_invoice():
    
    form = InvoiceForm()
    
    if form.create_invoice.data:

            full_path = os.path.join(os.path.join(app.root_path, app.config['TEMP_INVOICE_PATH']),app.config['TEMP_INVOICE_NAME'])
            dtype_dict= {'BULSTAT': str, 'TaxNum' : str, 'DocNumber' : str}    
            raw_df = pd.read_excel(full_path, dtype = dtype_dict)
            print(f'raw_df {raw_df}')
            redacted_df = raw_df[raw_df['DocNumber'].isin([str(x) for x in form.invoicing_list.data])]
            
            create_invoices(redacted_df)            
            return redirect(url_for('create_invoice'))   

    if form.validate_on_submit():         
        if form.upload_csv.data:
            try:
                invoice_df = create_df_from_integra_csv(request.files.get('file_integra_csv'))
                
            except:
                flash(f'Wrong or no csv file is choosen. Abort !','danger')
            else:
                full_path = os.path.join(os.path.join(app.root_path, app.config['TEMP_INVOICE_PATH']),app.config['TEMP_INVOICE_NAME'])
                # delete_files(os.path.join(app.root_path, app.config['TEMP_INVOICE_PATH']), app.config['TEMP_INVOICE_NAME'], 'xlsx', True)
                invoice_df.to_excel(full_path)    
                            
                form.invoicing_list.choices = parse_integra_csv(invoice_df)      

    return render_template('create_invoice.html', title='Invoice Creation', form=form)


@app.route('/erp', methods=['GET', 'POST'])
@login_required
def erp():
    
    form = ErpForm()
    if form.validate_on_submit():
        if form.delete_incoming_table.data:
            db.session.query(IncomingItn).delete()
            db.session.commit()
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

        if request.files.get('file_nkji').filename != '':
            nkji_zip = ZipFile(request.files.get('file_nkji'))             
            insert_settelment_nkji(nkji_zip)  
            flash('Data from NKJI uploaded successfully','success')

        if request.files.get('file_eso').filename != '':
            eso_zip = ZipFile(request.files.get('file_eso'))
            insert_settelment_eso(eso_zip)  
            flash('Data from ESO uploaded successfully','success')
        
    return render_template('erp.html', title='ERP Upload', form=form)

@app.route('/monthly_report', methods=['GET', 'POST'])
@login_required
def monthly_report():
    form = MonthlyReportForm()
    form.ref_files.choices = sorted([(x,x) for x in get_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']),'xlsx')])
    if form.validate_on_submit():
        
        # contractors = (db.session
        #     .query(Contract.id, Contractor.name, Contractor.vat_number, Contractor.address, Contractor.acc_411)
        #         .join(Contractor,Contractor.id == Contract.contractor_id)
        #         .join(ContractType,ContractType.id == Contract.contract_type_id)
        #         .filter(ContractType.name == "Mass_Market")
        #         .distinct(Contractor.name)
        #         .all())
        # temp_df = pd.DataFrame.from_records(contractors, columns = contractors[0].keys())  
        # temp_df.to_excel('temp/contractors.xlsx')   

    

        start = time.time()

        if form.submit_delete.data:
            delete_excel_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']), form.ref_files.data, form.delete_all.data)
            return redirect(url_for('monthly_report'))
           

        elif form.submit.data:
            weighted_price = None

            if form.by_contract.data:            
                time_zone = TimeZone.query.join(Contract, Contract.time_zone_id == TimeZone.id).filter(Contract.internal_id == form.contracts.data.internal_id).first().code
                start_date = convert_date_to_utc(time_zone, form.start_date.data)
                end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)
                inv_groups = get_list_inv_groups_by_contract(form.contracts.data.internal_id, start_date, end_date)
                weighted_price = get_weighted_price(inv_groups, start_date, end_date)
                # print(f'weighted_price -- {weighted_price}')
            else:            
                inv_groups = get_all_inv_groups() if form.bulk_creation.data else [x.name for x in form.invoicing_group.data]   

            result_df = None

            invoice_ref_path = inetgra_src_path = None

            for inv_group_name in inv_groups:
                # print(f'{inv_group_name}')
                start_date, end_date, invoice_start_date, invoice_end_date = create_utc_dates(inv_group_name, form.start_date.data, form.end_date.data)

                ibex_last_valid_date = (db.session.query(IbexData.utc, IbexData.price).filter(IbexData.price == 0).order_by(IbexData.utc).first()[0])

                if ibex_last_valid_date < end_date:
                    update_ibex_data(form.start_date.data, form.end_date.data)
                    update_schedule_prices(start_date, end_date)

                if start_date is None:
                    print(f'There is not data for {inv_group_name}, for period {form.start_date.data} - {form.end_date.data}')
                    continue

                is_spot = is_spot_inv_group([inv_group_name], start_date, end_date)
                
                if is_spot:
                    # print(f'in spot {weighted_price}')
                    summary_stp, summary_non_stp, grid_services_df, weighted_price= get_summary_spot_df([inv_group_name], start_date, end_date, invoice_start_date, invoice_end_date, weighted_price)
                    create_excel_files(summary_stp, summary_non_stp, grid_services_df, start_date, end_date, invoice_start_date, invoice_end_date, invoice_ref_path, inetgra_src_path, weighted_price)
                    
                else:
                    summary_stp, summary_non_stp, grid_services_df= get_summary_df_non_spot([inv_group_name], start_date, end_date, invoice_start_date, invoice_end_date)
                    create_excel_files(summary_stp, summary_non_stp, grid_services_df, start_date, end_date, invoice_start_date, invoice_end_date, invoice_ref_path, inetgra_src_path)
            return redirect(url_for('monthly_report'))     
                
        end = time.time()
        print(f'Time elapsed for generate excel file(s) : {end - start}  !')

    return render_template('monthly_report.html', title='Monthly Report', form=form)

@app.route('/create_excel_for_integra', methods=['GET', 'POST'])
@login_required
def create_integra_excel():
    
    form = IntegraForm()
    form.integra_files.choices = sorted([(x,x) for x in get_files(os.path.join(app.root_path, app.config['INTEGRA_INDIVIDUAL_PATH']),'xlsx')])
    form.integra_upload_files.choices = sorted([(x,x) for x in get_files(os.path.join(app.root_path, app.config['INTEGRA_FOR_UPLOAD_PATH']),'xlsx')])
    if form.validate_on_submit(): 

        if form.delete_integra.data:
            delete_excel_files(os.path.join(app.root_path, app.config['INTEGRA_INDIVIDUAL_PATH']), form.integra_files.data , form.delete_all.data)

            return redirect(url_for('create_integra_excel'))             
            
        elif form.submit.data:

            concated_df = pd.DataFrame()
            files_to_concat = []
            if not form.concatenate_all.data:
                files_to_concat = sorted(form.integra_files.data)
            else:
                for root, dirs, files in os.walk(os.path.join(app.root_path, app.config['INTEGRA_INDIVIDUAL_PATH'])):            
                    for filename in files:
                        if filename.endswith('.xlsx') & (filename.find('~') == -1):
                            files_to_concat.append(filename)
                files_to_concat = sorted(files_to_concat)

            for filename in files_to_concat:
                curr_df = pd.read_excel(os.path.join(os.path.join(app.root_path, app.config['INTEGRA_INDIVIDUAL_PATH']), filename))
                if concated_df.empty:
                    concated_df = curr_df
                else:
                    las_num = concated_df.iloc[-1]['№ по ред']
                    curr_df['№ по ред']  = las_num + 1
                    concated_df = concated_df.append(curr_df,ignore_index=True)      

            concated_df.to_excel(os.path.join(os.path.join(app.root_path, app.config['INTEGRA_FOR_UPLOAD_PATH']),form.file_name.data), index = False)
            return redirect(url_for('create_integra_excel'))

        elif form.delete_upload_integra.data:
            
            delete_excel_files(os.path.join(app.root_path, app.config['INTEGRA_FOR_UPLOAD_PATH']), form.integra_upload_files.data, form.delete_all_upload.data)
            return redirect(url_for('create_integra_excel')) 

        

    return render_template('create_excel_for_integra.html', title='Integra file', form=form)
     
@app.route('/upload_initial_data', methods=['GET', 'POST'])
@login_required
def upload_initial_data():
    
    form = UploadInitialForm()
    if form.validate_on_submit():
        if request.files.get('file_contractors').filename != '':
            print(f'in contractors')
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
            raw_contractor_df = raw_contractor_df.replace( ':','-', regex=True)
            raw_contractor_df = raw_contractor_df.replace( ',',';', regex=True)
           
            update_or_insert(raw_contractor_df, Contractor.__table__.name)

        if request.files.get('file_measuring').filename != '':
            measuring_df = pd.read_excel(request.files.get('file_measuring'))
            update_or_insert(measuring_df, MeasuringType.__table__.name)

        if request.files.get('file_erp').filename != '':
            erp_df =  pd.read_excel(request.files.get('file_erp'))
            update_or_insert(erp_df, Erp.__table__.name)

        if request.files.get('file_stp').filename != '':
            print(f'in upload stp coeffs')

            START_DATE = '01/01/2020 00:00:00'
            END_DATE = '31/12/2020 23:00:00'
            FORMAT = '%d/%m/%Y %H:%M:%S'
            df = pd.read_excel(request.files.get('file_stp'),sheet_name='full')

            # START_DATE = '01/01/2021 00:00:00'
            # END_DATE = '31/12/2021 23:00:00'
            # FORMAT = '%d/%m/%Y %H:%M:%S'
            # df = pd.read_excel(request.files.get('file_stp'),sheet_name='full_with_dates_2021')

            sDate = pd.to_datetime(START_DATE,format = FORMAT)
            eDate = pd.to_datetime(END_DATE,format = FORMAT)
            timeseries = pd.date_range(start=sDate, end=eDate, tz='EET', freq='h')
            Utc = timeseries.tz_convert('UTC').tz_localize(None) 
            df['utc'] = Utc
            df_m = pd.melt(df, id_vars =['utc'], value_vars =['CEZ_B1', 'CEZ_B2', 'CEZ_B3', 'CEZ_B4', 'CEZ_B5', 'CEZ_H1', 'CEZ_H2',
                'CEZ_S1', 'EPRO_B01', 'EPRO_B02', 'EPRO_B03', 'EPRO_B04', 'EPRO_H01',
                'EPRO_H02', 'EPRO_S01', 'EVN_G0', 'EVN_G1', 'EVN_G2', 'EVN_G3', 'EVN_G4',
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
            
        #######################################################################################################################################################
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
        

        #######################################################################################################################################################
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
    

@app.route('/register_gyz', methods=['GET', 'POST'])
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
    template_cols = ['itn', 'activation_date', 'internal_id', 'measuring_type',
                    'invoice_group_name', 'invoice_group_description', 'zko', 'akciz',
                    'has_grid_services', 'has_spot_price', 'erp', 'grid_voltage', 'address',
                    'description', 'is_virtual', 'virtual_parent_itn',
                    'forecast_montly_consumption', 'has_balancing', '411-3', 'time_zone',
                    'tariff_name', 'price_day', 'price_night','make_invoice','lower_limit','upper_limit']
    form = UploadItnsForm()
    if form.validate_on_submit():
        df = pd.read_excel(request.files.get('file_'), sheet_name=None)

        if df.get('data') is None:
            flash(f'Upload failed from missing data spreadsheet in excel file', 'danger')
            return redirect(url_for('upload_itns'))

        if set(df['data'].columns).issubset(template_cols):
            
            input_df = validate_input_df(df['data'])            
            input_df['zko'] = input_df['zko'].apply(lambda x: Decimal(str(x)) / Decimal('1000'))
            input_df['akciz'] = input_df['akciz'].apply(lambda x: Decimal(str(x)) / Decimal('1000'))

            input_df['forecast_montly_consumption'] = input_df['forecast_montly_consumption'].apply(lambda x: Decimal(str(x)) * Decimal('1000'))
            # input_df.rename(columns = {'invoice_group_name':'invoice_group'}, inplace = True)
            contractors = get_contractors_names_and_411()
            contractors_df = pd.DataFrame.from_records(contractors, columns = contractors[0].keys())
            input_df = input_df.merge(contractors_df, on = '411-3', how = 'left' )
            input_df['measuring_type'] = input_df['measuring_type'].apply(lambda x: MEASURE_MAP_DICT.get(x) if MEASURE_MAP_DICT.get(x) is not None else np.nan)
            
            itns_for_deletion = input_df[pd.isnull(input_df['measuring_type'])].itn.values
            if len(itns_for_deletion) > 0:
                print(f'Folowing itn will not be inserted because of wrong measuring type ! \n{itns_for_deletion}')
                input_df = input_df.dropna()

            for index,row in input_df.iterrows():
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
        else:
            input_set = set(df['data'].columns)
            expected_set = set(template_cols)
            mismatched = list(expected_set - input_set)
            print(f'Columns from input file mismatch {mismatched}')                 
           
    return render_template('upload_itns.html', title='Upload ITNs', form=form)


@app.route('/upload_contracts', methods=['GET', 'POST'])
@login_required
def upload_contracts():

    
    form = UploadContractsForm()
    if form.validate_on_submit():
        print(f'in upload_contracts')
        df = pd.read_excel(request.files.get('file_'))

        # tks = list(df.internal_id)
        # contracts = Contract.query.filter(Contract.internal_id.in_(tks)).all()
        # contract_type_dict = {'OTC':'End_User','ОП':'Procurement','Mass_Market':'Mass_Market'}

        # # df['contract_type'] = df['contract_type'].apply(lambda x: contract_type_dict[x.strip()] if(contract_type_dict.get(str(x).strip())) else 0 )
        # df['contract_type'] = df['contract_type'].apply(lambda x: ContractType.query.filter(ContractType.name == x).first().id if ContractType.query.filter(ContractType.name == x).first() is not None else x)
        # for c in contracts:
        #     curr_idx = c.contract_type_id
        #     print(f'curr contract type {curr_idx}')
        #     input_idx = df[df['internal_id'] == c.internal_id].contract_type.values[0]
        #     print(f'must be contract type {input_idx}')
        #     c.update({'contract_type_id':int(input_idx)})
        #     # c.update({'contract_type_id':3})
        
        template_cols = ['411-3', 'parent_contract_internal_id', 'internal_id', 'contractor',
                                    'signing_date', 'start_date', 'end_date', 'duration',
                                    'invoicing_interval', 'maturity_interval', 'contract_type',
                                    'is_work_day', 'automatic_renewal_interval', 'collateral_warranty',
                                    'notes', 'time_zone','parent_contractor_411']
        
        if set(df.columns).issubset(template_cols):

            tks = df['internal_id'].apply(lambda x: validate_ciryllic(x))            
            parent_tks = df['parent_contract_internal_id'].apply(lambda x: validate_ciryllic(x) if x != 0 else True)
            # print(f'{parent_tks}')    
            all_cyr = tks.all()
            all_cyr_parent = parent_tks.all()
            # print(f'all_cyr ---> {all_cyr}')
            # print(f'all_cyr_parent ---> {all_cyr_parent}')
            if not (all_cyr & all_cyr_parent):
                flash('There is tk in latin, aborting', 'danger')
                return redirect(url_for('upload_contracts'))

            
            df = df.fillna(0)

            contractors = get_contractors_names_and_411()
            contractors_df = pd.DataFrame.from_records(contractors, columns = contractors[0].keys())
            df = df.merge(contractors_df, on = '411-3', how = 'left' )            
            
            df['parent_id_initial_zero'] = 0
            df['end_date'] = df['end_date'] + dt.timedelta(hours = 23)
            df['duration_in_days'] = df.apply(lambda x: (x['end_date'] - x['start_date']).days, axis = 1)
            df['time_zone'] = df['time_zone'].apply(lambda x: TimeZone.query.filter(TimeZone.code == x).first() if TimeZone.query.filter(TimeZone.code == x).first() is not None else x)
            
            renewal_dict = {'удължава се автоматично с още 12 м. ако никоя от страните не заяви писмено неговото прекратяване':12,
                            'Подновява се автоматично за 1 година , ако никоя от страните не възрази писмено за прекратяването му поне 15 дни преди изтичането му':12,
                            'удължава се автоматично с още 6 м. ако никоя от страните не заяви писмено неговото прекратяване':6,
                            'удължава се автоматично за 3 м. ако никоя от страните не заяви писмено неговото прекратяване с допълнително споразумение.':3,
                            'За срок от една година. Подновява се с ДС / не се изготвя справка към ф-ра':12,
                            'За срок от една година. Подновява се с ДС.':12,
                            'с неустойка, удължава се с доп. спораз-е': -1}
                            
            df['automatic_renewal_interval'] = df['notes'].apply(lambda x: renewal_dict[x.strip()] if(renewal_dict.get(str(x).strip())) else 0 )

            invoicing_dict = {'до 12-то число, следващ месеца на доставката':42,'на 10 дни':10,'на 15 дни':15,'последно число':31,'конкретна дата':-1}
            df['invoicing_interval'] = df['invoicing_interval'].apply(lambda x: invoicing_dict[x.strip()] if(invoicing_dict.get(str(x).strip())) else 0 )
            contract_type_dict = {'OTC':'End_User','ОП':'Procurement','Mass_Market':'Mass_Market'}

            # df['contract_type'] = df['contract_type'].apply(lambda x: contract_type_dict[x.strip()] if(contract_type_dict.get(str(x).strip())) else 0 )
            df['contract_type'] = df['contract_type'].apply(lambda x: ContractType.query.filter(ContractType.name == x).first().id if ContractType.query.filter(ContractType.name == x).first() is not None else x)

            work_day_dict = {'календарни дни':0, 'работни дни':1}
            df['is_work_day'] = df['is_work_day'].apply(lambda x: work_day_dict[x.strip()] if(work_day_dict.get(str(x).strip())) else 0 )
            t_format = '%Y-%m-%dT%H:%M'
            contracts = []
            for index,row in df.iterrows():
                #print(f'in rows --------------->>{row.internal_id}')
                curr_contract = get_contract_by_internal_id(row['internal_id'])
                if curr_contract is not None :
                    internal_id = row['internal_id']
                    flash(f'There is a contract with this internal id {internal_id} ! Skipping !','danger')
                    print(f'There is a contract with this internal id {internal_id} ! Skipping !')
                    continue
                else:
                    curr_contract = (
                        Contract(internal_id = row['internal_id'], contractor_id = row['contractor_id'], subject = 'None', parent_id =  row['parent_id_initial_zero'],                                
                                signing_date =  convert_date_to_utc(row['time_zone'].code,row['signing_date'].strftime(t_format),t_format),
                                start_date = convert_date_to_utc(row['time_zone'].code, row['start_date'].strftime(t_format),t_format), 
                                end_date = convert_date_to_utc(row['time_zone'].code, row['end_date'].strftime(t_format),t_format), 
                                duration_in_days = row['duration_in_days'], invoicing_interval = row['invoicing_interval'], maturity_interval = row['maturity_interval'], 
                                contract_type_id = row['contract_type'], is_work_days = row['is_work_day'], automatic_renewal_interval = row['automatic_renewal_interval'], 
                                collateral_warranty = row['collateral_warranty'], notes =  row['notes'],time_zone_id = row['time_zone'].id) 
                    )
                    contracts.append(curr_contract)

            nan_df = df[df.isna().any(axis=1)]
            if not nan_df.empty:
                print(f'THERE IS CONTRACT WITH WRONG DATA ! ABORTING ! \n{nan_df}')
            else:
                db.session.bulk_save_objects(contracts)
                db.session.commit()                
                
                has_parent_contract_df = df[df['parent_contract_internal_id'] != 'none']
                
                for index, row in has_parent_contract_df.iterrows():
                    child_contract = Contract.query.filter(Contract.internal_id == row['internal_id']).first()                    
                    child_contract.update({'parent_id':Contract.query.filter(Contract.internal_id == row['parent_contract_internal_id']).first().id})
                    flash(f'Parent contract {Contract.query.filter(Contract.id == child_contract.parent_id).first().internal_id} added to {child_contract.internal_id}','success')

                has_parent_contractor_df = df[df['parent_contractor_411'] != 'none']

                for index, row in has_parent_contractor_df.iterrows():
                    parent_contractor = Contractor.query.filter(Contractor.acc_411 == row['parent_contractor_411']).first()
                    child_contractor = Contractor.query.filter(Contractor.acc_411 == row['411-3']).first()
                    child_contractor.update({'parent_id':parent_contractor.id})
                    flash(f'Parent contractor {parent_contractor.name} added to {child_contractor.name}','success')           
        else:
            input_set = set(df['data'].columns)
            expected_set = set(template_cols)
            mismatched = list(expected_set - input_set)
            print(f'Columns from input file mismatch {mismatched}')               
            
    
             

    return render_template('upload_contracts.html', title='Upload Contracts', form=form)


@app.route('/upload_linked_contracts', methods=['GET', 'POST'])
@login_required
def upload_linked_contracts():

    
    form = UploadContractsForm()
    if form.validate_on_submit():
        print(f'in upload_linked_contracts')
        df = pd.read_excel(request.files.get('file_'))
        # print(f'{df.columns}')
        template_cols = ['411-3', 'linked_contract_internal_id', 'internal_id', 'contractor',
                        'signing_date', 'start_date', 'end_date', 'duration',
                        'invoicing_interval', 'maturity_interval', 'contract_type',
                        'is_work_day', 'automatic_renewal_interval', 'collateral_warranty',
                        'notes', 'time_zone','parent_contractor_411','invoice_group_name','invoice_group_description',
                        'zko','akciz','has_grid_services','has_balancing','has_spot_price','time_zone','tariff_name','price_day','price_night','make_invoice','lower_limit','upper_limit']

        if set(df.columns).issubset(template_cols):


            tks = df['internal_id'].apply(lambda x: validate_ciryllic(x))            
            parent_tks = df['linked_contract_internal_id'].apply(lambda x: validate_ciryllic(x) if x != 0 else True)
            # print(f'{parent_tks}')    
            all_cyr = tks.all()
            all_cyr_parent = parent_tks.all()
            # print(f'all_cyr ---> {all_cyr}')
            # print(f'all_cyr_parent ---> {all_cyr_parent}')
            if not (all_cyr & all_cyr_parent):
                flash('There is tk in latin, aborting', 'danger')
                return redirect(url_for('upload_contracts'))

            
            df = df.fillna(0)

            contractors = get_contractors_names_and_411()
            contractors_df = pd.DataFrame.from_records(contractors, columns = contractors[0].keys())
            df = df.merge(contractors_df, on = '411-3', how = 'left' )            
            
            df['parent_id_initial_zero'] = 0
            df['end_date'] = df['end_date'] + dt.timedelta(hours = 23)
            df['duration_in_days'] = df.apply(lambda x: (x['end_date'] - x['start_date']).days, axis = 1)
            df['time_zone'] = df['time_zone'].apply(lambda x: TimeZone.query.filter(TimeZone.code == x).first() if TimeZone.query.filter(TimeZone.code == x).first() is not None else x)
            
            renewal_dict = {'удължава се автоматично с още 12 м. ако никоя от страните не заяви писмено неговото прекратяване':12,
                            'Подновява се автоматично за 1 година , ако никоя от страните не възрази писмено за прекратяването му поне 15 дни преди изтичането му':12,
                            'удължава се автоматично с още 6 м. ако никоя от страните не заяви писмено неговото прекратяване':6,
                            'удължава се автоматично за 3 м. ако никоя от страните не заяви писмено неговото прекратяване с допълнително споразумение.':3,
                            'За срок от една година. Подновява се с ДС / не се изготвя справка към ф-ра':12,
                            'За срок от една година. Подновява се с ДС.':12,
                            'с неустойка, удължава се с доп. спораз-е': -1}
                            
            df['automatic_renewal_interval'] = df['notes'].apply(lambda x: renewal_dict[x.strip()] if(renewal_dict.get(str(x).strip())) else 0 )

            invoicing_dict = {'до 12-то число, следващ месеца на доставката':42,'на 10 дни':10,'на 15 дни':15,'последно число':31,'конкретна дата':-1}
            df['invoicing_interval'] = df['invoicing_interval'].apply(lambda x: invoicing_dict[x.strip()] if(invoicing_dict.get(str(x).strip())) else 0 )
            contract_type_dict = {'OTC':'End_User','ОП':'Procurement','Mass_Market':'Mass_Market'}

            # df['contract_type'] = df['contract_type'].apply(lambda x: contract_type_dict[x.strip()] if(contract_type_dict.get(str(x).strip())) else 0 )
            df['contract_type'] = df['contract_type'].apply(lambda x: ContractType.query.filter(ContractType.name == x).first().id if ContractType.query.filter(ContractType.name == x).first() is not None else x)

            work_day_dict = {'календарни дни':0, 'работни дни':1}
            df['is_work_day'] = df['is_work_day'].apply(lambda x: work_day_dict[x.strip()] if(work_day_dict.get(str(x).strip())) else 0 )
            t_format = '%Y-%m-%dT%H:%M'
            contracts = []
            # print(f'proceeded df \n{df}')
            for index,row in df.iterrows():
                #print(f'in rows --------------->>{row.internal_id}')
                curr_contract = get_contract_by_internal_id(row['internal_id'])
                # a = row['internal_id']
                # print(f'curr_contract \n{curr_contract} -- {a}')
                zko = round(Decimal(str(row['zko'])) / Decimal('1000'), MONEY_ROUND)
                akciz = round(Decimal(str(row['akciz'])) / Decimal('1000'), MONEY_ROUND)                
                start_date = convert_date_to_utc(row['time_zone'].code, row['start_date'].strftime(t_format),t_format) 
                end_date = convert_date_to_utc(row['time_zone'].code, row['end_date'].strftime(t_format),t_format)
                if curr_contract is not None :
                    internal_id = row['internal_id']
                    flash(f'There is a contract with this internal id {internal_id} ! Skipping !','danger')
                    print(f'There is a contract with this internal id {internal_id} ! Skipping !')
                    continue
                else:
                    parent_contract = get_contract_by_internal_id(row['linked_contract_internal_id'])
                    # a = row['linked_contract_internal_id']
                    # print(f'parent_linked_contract \n{parent_linked_contract.internal_id} -- {a}')

                    new_linked_contract = (
                        Contract(internal_id = row['internal_id'], contractor_id = row['contractor_id'], subject = 'None', parent_id =  row['parent_id_initial_zero'],                                
                                signing_date =  convert_date_to_utc(row['time_zone'].code,row['signing_date'].strftime(t_format),t_format),
                                start_date = start_date, 
                                end_date = end_date, 
                                duration_in_days = row['duration_in_days'], invoicing_interval = row['invoicing_interval'], maturity_interval = row['maturity_interval'], 
                                contract_type_id = row['contract_type'], is_work_days = row['is_work_day'], automatic_renewal_interval = row['automatic_renewal_interval'], 
                                collateral_warranty = row['collateral_warranty'], notes =  row['notes'],time_zone_id = row['time_zone'].id) 
                    )
                    new_linked_contract.save() #!!!!!
                    print(f'saved new linked contract --- > {new_linked_contract}')
                    apply_linked_collision_function(parent_contract, new_linked_contract, row['invoice_group_name'], row['invoice_group_description'], zko, akciz, row['has_grid_services'], row['has_spot_price'],
                                                                                row['has_balancing'], row['tariff_name'], row['price_day'],	row['price_night'], row['make_invoice'], row['lower_limit'], row['upper_limit']) 
                    # subcontracts = get_subcontracts_by_inv_gr_name_and_date(row['invoice_group_name'], start_date, end_date)
                    # print(f'subcontracts --- > {subcontracts}')
                    # contracts.append(new_linked_contract)
                    

            nan_df = df[df.isna().any(axis=1)]
            if not nan_df.empty:
                print(f'THERE IS CONTRACT WITH WRONG DATA ! ABORTING ! \n{nan_df}')
            else:
                print(f'in save')
                # db.session.bulk_save_objects(contracts)
                # db.session.commit()                
                
                # has_parent_contract_df = df[df['parent_contract_internal_id'] != 'none']
                
                # for index, row in has_parent_contract_df.iterrows():
                #     child_contract = Contract.query.filter(Contract.internal_id == row['internal_id']).first()                    
                #     child_contract.update({'parent_id':Contract.query.filter(Contract.internal_id == row['parent_contract_internal_id']).first().id})
                #     flash(f'Parent contract {Contract.query.filter(Contract.id == child_contract.parent_id).first().internal_id} added to {child_contract.internal_id}','success')

                # has_parent_contractor_df = df[df['parent_contractor_411'] != 'none']

                # for index, row in has_parent_contractor_df.iterrows():
                #     parent_contractor = Contractor.query.filter(Contractor.acc_411 == row['parent_contractor_411']).first()
                #     child_contractor = Contractor.query.filter(Contractor.acc_411 == row['411-3']).first()
                #     child_contractor.update({'parent_id':parent_contractor.id})
                #     flash(f'Parent contractor {parent_contractor.name} added to {child_contractor.name}','success')         

            
        else:
                input_set = set(df.columns)
                expected_set = set(template_cols)
                mismatched = list(expected_set - input_set)
                print(f'Columns from input file mismatch {mismatched}')
    return render_template('quick_template.html', title='Upload Linked Contracts', form=form, header = 'Upload ANNEX')

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
            flash(f'Wrong dates according the contract {curr_contract.internal_id}!','danger')
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


@app.route('/monthly_report/<erp>/<contract_type>/<start_date>/<end_date>/<is_mixed>', methods=['GET', 'POST'])
@login_required
def monthly_report_by_erp( erp, start_date, end_date, contract_type, is_mixed):
    form = MonthlyReportErpForm()
    form.ref_files.choices = sorted([(x,x) for x in get_excel_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']))])
    filtered_records = get_inv_gr_id_single_erp(erp, contract_type, start_date, end_date, is_mixed)
    # form.invoicing_group.choices = [ (x[0],f'{x[0]} - {x[1]} ') for x in db.session.query(InvoiceGroup.name, InvoiceGroup.description).join(Contractor).join(SubContract).join(Contract)
    #                     .join(ItnMeta, ItnMeta.itn == SubContract.itn).join(Erp).join(ContractType, ContractType.id == Contract.contract_type_id)
    #                     .filter(Erp.name == erp)
    #                     .filter(SubContract.start_date <= start_date, SubContract.end_date > start_date)
    #                     .filter(ContractType.name == contract_type).order_by(Contractor.name)
    #                     .all()]

    form.invoicing_group.choices = sorted(list(set([ (x[0],f'{x[0]} - {x[1]} ') for x in filtered_records])),key = lambda y: y[1].split(' - ')[1])
    # form.contractor.choices = 
                 
    # form.contracts.choices =  [ (x,x) for x in Contract.query.join(Contractor).order_by(Contractor.name).all() ]   
    
    form.contracts.choices = sorted(list(set([(x[3],f'{x[2]}') for x in filtered_records] )) ,key = lambda y: y[1].split(' - ')[1]) 

    if form.validate_on_submit():        
        
        start = time.time()  
        if form.submit_delete.data:
            delete_excel_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']), form.ref_files.data, form.delete_all.data)
            return redirect(url_for('monthly_report_by_erp', erp = erp,start_date = start_date, end_date = end_date, contract_type = contract_type, is_mixed = is_mixed, **request.args))
           

        elif form.submit.data:
            counter = 0
            weighted_price = None

            if form.by_contract.data:   
                print(f'{form.contracts.data}')         
                time_zone = TimeZone.query.join(Contract, Contract.time_zone_id == TimeZone.id).filter(Contract.internal_id == form.contracts.data).first().code
                start_date_ = convert_date_to_utc(time_zone, start_date)
                end_date_ = convert_date_to_utc(time_zone, end_date) + dt.timedelta(hours = 23)
                inv_groups = get_list_inv_groups_by_contract(form.contracts.data, start_date_, end_date_)
                weighted_price = get_weighted_price(inv_groups, start_date_, end_date_)
                # print(f'weighted_price -- {weighted_price}')
            else:            
                inv_groups = [x[0] for x in form.invoicing_group.choices]  if form.bulk_creation.data else [x for x in form.invoicing_group.data]   
                
            result_df = None

            invoice_ref_path = inetgra_src_path = None

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
                    create_excel_files(summary_stp, summary_non_stp, grid_services_df, start_date_utc, end_date_utc, invoice_start_date, invoice_end_date, invoice_ref_path, inetgra_src_path, weighted_price)
                    
                else:
                    counter += 1
                   
                    summary_stp, summary_non_stp, grid_services_df= get_summary_df_non_spot([inv_group_name], start_date_utc, end_date_utc, invoice_start_date, invoice_end_date)
                    create_excel_files(summary_stp, summary_non_stp, grid_services_df, start_date_utc, end_date_utc, invoice_start_date, invoice_end_date, invoice_ref_path, inetgra_src_path)

            flash(f'{counter} invoice references was created !','info')
            return redirect(url_for('monthly_report_by_erp', erp = erp,start_date = start_date, end_date = end_date, contract_type = contract_type, is_mixed = is_mixed, **request.args))     
                
        end = time.time()
        print(f'Time elapsed for generate excel file(s) : {end - start}  !')

    return render_template('quick_template_wider.html', title=f'Monthly Report {erp}', form=form, header = f'Monthly Report {erp} - {contract_type} <br> Period: {start_date} / {end_date}<br> Included mixed groups - {is_mixed}')

    
