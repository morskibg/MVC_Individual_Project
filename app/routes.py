import os
import glob as gl
import os.path
import xlrd
import time,re
import sys, pytz, datetime as dt
import pandas as pd
from zipfile import ZipFile
from flask import render_template, flash, redirect, url_for, request, send_file,send_from_directory, jsonify
from sqlalchemy import extract, or_
from sqlalchemy.orm import aliased
from app import app
from app.forms import (
    LoginForm, RegistrationForm, NewContractForm, AddItnForm, AddInvGroupForm, ErpForm, AdditionalReports, RedactEmailForm, EmailsOptionsForm,
    UploadInvGroupsForm, UploadContractsForm, UploadItnsForm, CreateSubForm, TestForm, MonthlyReportErpForm, PostForm, RedactContractForm,
    UploadInitialForm, IntegraForm, InvoiceForm, MonthlyReportForm, MailForm, MonthlyReportErpForm,MonthlyReportOptionsForm, ContarctDataForm,
    ItnCosumptionDeletion, ModifyInvoiceForm, RedactContractorForm, ContarctorDataForm, ModifyForm, ModifyInvGroupForm, ModifyItn, ModifySubcontractEntryForm)
from flask_login import current_user, login_user, logout_user, login_required

from app.models import *

from werkzeug.urls import url_parse

from werkzeug.utils import secure_filename 
from werkzeug.datastructures import MultiDict
import calendar

from app.helpers.helper_function_excel_writer import generate_num_and_name

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
from app.helpers.helper_functions_reports import (create_report_from_grid, get_summary_df_non_spot, create_inv_refs_by_inv_groups,
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

@app.context_processor
def inject_is_test_base():

    base_name = app.config['SQLALCHEMY_DATABASE_URI']
    # base_name = base_name.split('/')[3]
    # base_name = base_name.split('?')[0]
    is_test_base = False if base_name.find('Ged_EU_v1') != -1 else True

    return dict(is_test_base = is_test_base)

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

    return render_template('quick_template.html', title='Monthly reports filter', form=form, header = 'Monthly report filters', need_dt_picker = True)

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
    need_dt_picker = True
    if form.validate_on_submit():
        
        # metas = ItnMeta.query.all()
        # count = 1
        # for itn in metas:
        #     itn.delete()
        #     print(f'{count} - {itn} deleted')
        #     count+=1

        # rec = (
        #     db.session.query(
        #         InvoiceGroup.name, InvoiceGroup.description, Mail.name, Contractor.name, Contractor.acc_411
        #     )
        #     .join(Mail, Mail.id == InvoiceGroup.email_id)
        #     .join(Contractor, Contractor.id == InvoiceGroup.contractor_id)
        #     .all()

        # )
        # mail_df = pd.DataFrame.from_records(rec, columns=rec[0].keys())
        # print(f'{mail_df}')
        # mail_df.to_excel('mails_last.xlsx')
        # erp_invoice_df =  pd.read_sql(ErpInvoice.query.statement, db.session.bind) 
        # df = erp_invoice_df.drop_duplicates(subset=['composite_key'], keep = 'last')
        # print(f'{erp_invoice_df.shape}')
        # print(f'{df.shape}')
        erp_name = 'CEZ'
        start_date = convert_date_to_utc('EET',form.start_date.data)   
        end_date = convert_date_to_utc('EET',form.end_date.data) 
        
        # start_date = convert_date_to_utc('EET','2020-10-01')   
        # end_date = convert_date_to_utc('EET','2020-10-31') 
        end_date = end_date + dt.timedelta(hours = 23)
        invoice_start_date = start_date + dt.timedelta(hours = (10 * 24 + 1))
        invoice_end_date = end_date + dt.timedelta(hours = (10 * 24))

        

        
        # try:
        #     while(1):
        #         print(f'{end_date}')
        #         schedule = (
        #             db.session.query(ItnSchedule.consumption_vol, ItnSchedule.settelment_vol)
        #             .filter(ItnSchedule.itn == '32Z103001108231F', ItnSchedule.utc == end_date)
        #             .first()
        #         )
        #         print(f'{schedule}')
        #         if schedule is not None and schedule[0] != -1:
        #             print(f'breaking - {end_date}')
        #             break
        #         else:   
        #             month = end_date.month
        #             prev_month = month - 1 if month != 1 else 12      
        #             # print(f'{prev_month} - {month} ')
        #             end_date = end_date.replace(day = calendar.monthrange(end_date.year, prev_month)[1], month = prev_month)
        # except:
        #     print('in exception')
        # print(f'{prev_month} - {last_prev_month_day} ')
                                                                  
        # if schedule is None:
            

        # contract_id = 550
       
        # contr = Contract.query.get(contract_id)
        # # print(f'{contr}')
        

        # varlist = ['id','start_date','internal_id','sub_contracts']
        # contract_schema = ContractSchema()
        # # output = contract_schema.dump(contr)
        # print(f' printing \n{contract_schema.dump(contr)}')


        



        # inv_name_part = form.contract_tk.data

        # pattern = re.compile(r"^411-[\d]{1,3}-[\d]{1,5}_[\d]{1,3}$")
        # result = pattern.match(inv_name_part)
        # if result is None:
        #     print(f'no matching') 
        # else:
        #     print(f'matching') 

        # inv_groups = (
        #     db.session.query(
        #         InvoiceGroup.name
        #     )
        #     .filter(InvoiceGroup.name.like("%" + inv_name_part + "%"))
        #     .order_by(InvoiceGroup.name)
        #     .all()
        # )
        # suffix = inv_groups[-1][0].split('_')[1]
        # # inv_groups = [x[0] for x in inv_groups]
        # # contract = Contract.query.filter(Contract.internal_id == 'ТК706').first()
        # print(f'dddd - {suffix}')

        # contract_id = 550
       
        # groups = (
        #     db.session.query
        #         (InvoiceGroup.name.label('invoice_group'), InvoiceGroup.description.label('invoice_group_description'), Mail.name.label('email'))
        #     .join(Mail,Mail.id == InvoiceGroup.email_id)            
        #     .join(Contract, Contract.contractor_id == InvoiceGroup.contractor_id)
        #     .filter(Contract.id == contract_id)
        #     .all()
        # )    
        # groups_arr = []
        # for group in groups:
        #     group_obj = {}
        #     group_obj['invoice_group'] =  group[0]  
        #     group_obj['invoice_group_description'] =  group[1]
        #     group_obj['email'] =  group[2]
        #     groups_arr.append(group_obj)
        # return jsonify({'groups':groups_arr})

        # df1 = pd.DataFrame.from_records(groups, columns = groups[0].keys())
        # res = df1.to_json(orient='records',force_ascii=False)
        # print(f'{jsonify(groups_arr)}')

        # print(f'{parent_contractor}')
        # rec = (
        #     db.session.query(
        #          Contract.internal_id, Contractor.name, Contractor.acc_411,Contract.end_date
        #     )
        #     .join(Contractor, Contractor.id == Contract.contractor_id)
        #     .filter(Contract.end_date == end_date)
        #     .all()
        # )

        rec = (
            db.session.query( 
                SubContract.itn,               
                Contractor.name.label('contractor_name'),func.sum(ItnSchedule.consumption_vol).label('total_consumption'),
                Contractor.acc_411, Contractor.eic.label('bulstat'),
                Tariff.price_day,Contract.end_date
            )
            
            .join(Contract,Contract.id == SubContract.contract_id)
            .join(Contractor,Contractor.id == Contract.contractor_id)           
            .join(ContractType, ContractType.id == Contract.contract_type_id)            
            .join(ItnSchedule,ItnSchedule.itn == SubContract.itn)
            .join(Tariff,Tariff.id == ItnSchedule.tariff_id)
            .filter(ItnSchedule.utc >= '2020-09-30 21:00:00', ItnSchedule.utc <= '2020-11-30 21:00:00')
            .filter(Contract.end_date > '2021-03-31 21:00:00')
            # .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))     
            .filter(ContractType.name == 'Procurement')
            .filter(ItnSchedule.consumption_vol > 0)
            # .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
            # .limit(5)
            .group_by(Contractor.name,Contractor.acc_411,Contractor.eic,Tariff.price_day,Contract.end_date)
            .all()
        )
        temp_df = pd.DataFrame.from_records(rec, columns = rec[0].keys())
        temp_df.to_excel('temp/zop_after_03_2021_.xlsx')
        # print(f'{temp_df}')
        # rec = (
        #     db.session.query(
        #         SubContract.itn, MeasuringType.code.label('measuring_type'), Erp.name.label('erp'),
        #         Contractor.name.label('contractor_name'), Contractor.acc_411, Contractor.eic.label('bulstat'),
        #         ContractType.name.label('contract_type'),Contract.end_date
        #     )
        #     .join(Contract,Contract.id == SubContract.contract_id)
        #     .join(Contractor,Contractor.id == Contract.contractor_id)
        #     .join(ItnMeta, ItnMeta.itn == SubContract.itn)
        #     .join(Erp, Erp.id == ItnMeta.erp_id)
        #     .join(MeasuringType, MeasuringType.id == SubContract.measuring_type_id)
        #     .join(ContractType, ContractType.id == Contract.contract_type_id)
        #     .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id)           
        #     .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))     
        #     .filter(ContractType.name == 'Procurement')
        #     # .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
        #     .limit(5)
        #     .all()
        # )
        print(f'{len(rec)}')
        ##############################################################################################
        # alias_for_parent_contractor = aliased(Contractor)
        # rec = (
        #     db.session.query(
        #         Contractor.id.label('contractor_id'),Contractor.acc_411.label('411-3'),Contract.internal_id.label('internal_id'),Contractor.name.label('contractor'),
        #         Contract.signing_date, Contract.start_date, Contract.end_date, Contract.duration_in_days.label('duration'),
        #         Contract.invoicing_interval, Contract.maturity_interval, ContractType.name, Contract.is_work_days.label('is_work_day'),
        #         Contract.automatic_renewal_interval, Contract.collateral_warranty, Contract.notes, TimeZone.code, alias_for_parent_contractor.acc_411.label('parent_contractor_411'),
        #         InvoiceGroup.name.label('invoice_group_name'), InvoiceGroup.description.label('invoice_group_description'), SubContract.zko,
        #         SubContract.akciz, SubContract.has_grid_services, SubContract.has_spot_price, SubContract.has_balancing, Tariff.name.label('tariff_name'),Tariff.price_day, Tariff.price_night,
        #         SubContract.make_invoice, Tariff.lower_limit, Tariff.upper_limit 
        #     )
        #     .join(Contract,Contract.contractor_id == Contractor.id)
        #     .outerjoin(alias_for_parent_contractor,alias_for_parent_contractor.id == Contractor.parent_id)
        #     .join(TimeZone,TimeZone.id == Contract.time_zone_id)
        #     .join(SubContract,SubContract.contract_id == Contract.id)
        #     # .join(ItnMeta, ItnMeta.itn == SubContract.itn)
        #     # .join(Erp, Erp.id == ItnMeta.erp_id)
        #     # .join(MeasuringType, MeasuringType.id == SubContract.measuring_type_id)
        #     .join(ContractType, ContractType.id == Contract.contract_type_id)
        #     .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id)
        #     .join(ItnSchedule,ItnSchedule.itn == SubContract.itn)
        #     .join(Tariff,Tariff.id == ItnSchedule.tariff_id)
        #     .filter(ItnSchedule.utc == end_date)
        #     .filter(SubContract.end_date == end_date)
        #     .distinct(InvoiceGroup.name)
        #     .order_by(Contractor.id)
        #     # .filter(ContractType.name == 'Mass_Market')
        #     # .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
        #     .all()
        # )
        # temp_df = pd.DataFrame.from_records(rec, columns = rec[0].keys())
        # temp_df.to_excel('temp/linked_template_12_2020.xlsx')
        # print(f'{temp_df}')
        ########################################################################################################
        # inv_groups = db.session.query(InvoiceGroup.name).all()
        # inv_groups = [x[0] for x in inv_groups]
        # a = '411-3-441_1' in inv_groups
        # print(f'{a}')
        # s =  SubContract.query.filter(SubContract.contract_id == 749).all()
        # a = [x.itn for x in s]
        # print(f'{a}')
        # all_itns = (
        #     db.session.query(
        #         SubContract.itn
        #     )            
        #     .join(ItnSchedule, ItnSchedule.itn == SubContract.itn)
        #     .filter(SubContract.start_date < "2020-11-30 22:00:00",SubContract.end_date >= "2020-11-30 22:00:00")            
        #     .distinct(SubContract.itn)
        #     .all()
        # )
        # all_itns = [x[0] for x in all_itns]

        # rec = (
        #     db.session.query(
        #         SubContract.itn
        #     )            
        #     .join(ItnSchedule, ItnSchedule.itn == SubContract.itn)
        #     .filter(SubContract.start_date < "2020-11-30 22:00:00",SubContract.end_date >= "2020-11-30 22:00:00")
        #     .filter(ItnSchedule.utc == SubContract.end_date)
        #     .distinct(SubContract.itn)
        #     .all()
        # )
        # correct_itns = [x[0] for x in rec]
        # diff = list(set(all_itns) - set(correct_itns))
        # # sched = (
        # #     db.session.query(
        # #         ItnSchedule.itn
        # #     )
        # #     .filter(ItnSchedule.itn.in_(itns))
        # #     .filter(ItnSchedule.utc >= "2020-11-30 22:00:00")
        # #     .distinct(ItnSchedule.itn)
        # #     .all()
        # # )
        # # correct = [x[0] for x in sched]
        # print(f'total {diff}')
        # print(f'correct {len(correct)}')
        # get_missing_extra_points_by_erp_for_period(erp_name, start_date, end_date)

        # grid_db_itns = get_grid_itns_by_erp_for_period(erp_name, start_date, end_date)
        # # non_grid_db_itns = get_non_grid_itns_by_erp_for_period(erp_name, start_date, end_date)
        # incomming_grid_itns = get_incomming_grid_itns(erp_name, start_date, end_date)
        # a = list(set(grid_db_itns) - set(incomming_grid_itns))
        # print(f'{a}')


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

            
       

    return render_template('test.html', title='TEST', form=form, need_dt_picker=need_dt_picker)

@app.route('/mailing', methods=['GET', 'POST'])
@login_required
def mailing():
    form = MailForm()    
    if form.validate_on_submit():
        if form.submit.data:
            selected_invoices = form.attachment_files.data
            for inv in selected_invoices:
                tokens = inv.ref_file_name.split('-')
                
                inv_group_name = db.session.query(InvoiceGroup.name).filter(InvoiceGroup.id == inv.invoice_group_id).first()[0]
                raw_mails =  db.session.query(Mail.name).join(InvoiceGroup, InvoiceGroup.email_id == Mail.id).filter(InvoiceGroup.name == inv_group_name).first()[0]
                
                mails =[x for x in raw_mails.split(';')]
                print(f'{mails}')
                if form.include_open_market.data:
                    mails.append('openmarket@grandenergy.net')
                else:
                    print(f'openmarket not included')
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
    # return render_template('test_redacting_emails.html', title='TEST', form=form)

@app.route('/create_invoice', methods=['GET', 'POST'])
@login_required
def create_invoice():
    
    form = InvoiceForm()

    if form.modify_invoice.data:
        # print(f'{form.invoicing_list.data[0]}')
        
        return redirect(url_for('modify_invoice', invoice_num = form.invoicing_list.data[0], **request.args))
    
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

    # return render_template('create_invoice.html', title='Invoice Creation', form=form)
    return render_template('quick_template_wider.html', title='Invoice Creation', form=form)

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



@app.route('/monthly_report', defaults={'key_word': None}, methods=['GET', 'POST'])
@app.route('/monthly_report/<key_word>', methods=['GET', 'POST'])
@login_required
def monthly_report(key_word = None):
    
    if request.method == "GET":
        form_dict = {}
        if key_word is not None:
            form_dict['search'] = key_word
        # else:
        #     form_dict['search'] = key_word = 'ТК'
        
        form = MonthlyReportForm(formdata=MultiDict(form_dict))
        form.ref_files.choices = sorted([(x,x) for x in get_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']),'xlsx')])
        
        return render_template('monthly_report.html', title='Monthly Report', form=form, need_dt_picker = True)
        
    else:
        form = MonthlyReportForm()
        form.ref_files.choices = sorted([(x,x) for x in get_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']),'xlsx')])
        key_word = form.search.data if form.search.data != '' else 'none'
        if form.validate_on_submit(): 
            if form.submit_delete.data:

                delete_excel_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']), form.ref_files.data, form.delete_all.data)
                return redirect(url_for('monthly_report', key_word = key_word, **request.args)) 
                
            elif form.submit.data:
                counter = 0
                weighted_price = None
                if len(form.contracts.data) >0:
                    for curr_contract in form.contracts.data: 
                        print(f'CURR CONTRACT - {curr_contract.internal_id}')         
                        time_zone = TimeZone.query.join(Contract, Contract.time_zone_id == TimeZone.id).filter(Contract.internal_id == curr_contract.internal_id).first().code
                        start_date = convert_date_to_utc(time_zone, form.start_date.data)
                        end_date = convert_date_to_utc(time_zone, form.end_date.data) + dt.timedelta(hours = 23)
                        inv_groups = get_list_inv_groups_by_contract(curr_contract.internal_id, start_date, end_date)
                        print(f'start_date -- {start_date}')
                        print(f'end_date -- {end_date}')
                        print(f'inv_groups -- {inv_groups}')
                        weighted_price = get_weighted_price(inv_groups, start_date, end_date)
                        print(f'weighted_price -- {weighted_price}')
                        counter += create_inv_refs_by_inv_groups(inv_groups, form.start_date.data, form.end_date.data, weighted_price)
                        
                    flash(f'{counter} invoice references was created !','info')    
                else:                
                    # inv_groups = get_all_inv_groups() if form.bulk_creation.data else [x.name for x in form.invoicing_group.data]      
                    inv_groups = [x.name for x in form.invoicing_group.data]            
                    counter = create_inv_refs_by_inv_groups(inv_groups, form.start_date.data, form.end_date.data, weighted_price)   
                    flash(f'{counter} invoice references was created !','info')

                return redirect(url_for('monthly_report', key_word = key_word, **request.args))     
                
        end = time.time()
        print(f'Time elapsed for generate excel file(s) : {end - start}  !')
        
        return render_template('monthly_report.html', title='Monthly Report', form=form, need_dt_picker = True, key_word = key_word)

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
            raw_contractor_df = raw_contractor_df.replace( '"','', regex=True)
            raw_contractor_df = raw_contractor_df.replace( "'",'', regex=True)
           
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
            acc_411 = form.invoice_group_name.data
            acc_411 = acc_411.split('_')[0]
            curr_contractor_id = Contractor.query.filter(Contractor.acc_411 == acc_411).first().id    
            # mails = form.invoice_group_emails.data 
            new_mail = form.invoice_group_emails.data
            mail_to_add = Mail.query.filter(Mail.name == new_mail).first()
            if mail_to_add is None:
                mail_to_add = Mail(name = new_mail)
                mail_to_add.save()
                print(f'adding new mail {mail_to_add}')
                mail_to_add = Mail.query.filter(Mail.name == new_mail).first()                         
            curr_inv_group = InvoiceGroup(name = form.invoice_group_name.data, contractor_id = curr_contractor_id, description = form.invoice_group_description.data, email_id = mail_to_add.id)
            print(f'{curr_inv_group}')
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
                if row['internal_id'] == row['linked_contract_internal_id']:
                    new_c = row['internal_id']                    
                    flash(f'Linked contracts have same internal id - skipping !: {new_c}','danger')
                    print(f'Linked contracts have same internal id - skipping !: {new_c}')
                    continue
                #print(f'in rows --------------->>{row.internal_id}')
                curr_contract = get_contract_by_internal_id(row['internal_id'])
                # a = row['internal_id']
                # print(f'curr_contract \n{curr_contract} -- {a}')
                zko = round(Decimal(str(row['zko'])) / Decimal('1000'), MONEY_ROUND)
                akciz = round(Decimal(str(row['akciz'])) / Decimal('1000'), MONEY_ROUND)                
                start_date = convert_date_to_utc(row['time_zone'].code, row['start_date'].strftime(t_format),t_format) 
                end_date = convert_date_to_utc(row['time_zone'].code, row['end_date'].strftime(t_format),t_format)
                parent_contract = get_contract_by_internal_id(row['linked_contract_internal_id'])

                if curr_contract is not None :
                    internal_id = row['internal_id']
                    # flash(f'There is a contract with this internal id {internal_id} ! Skipping !','danger')
                    print(f'There is a contract with this internal id {internal_id} ! Proceeding itn !')
                    # continue
                    apply_linked_collision_function(parent_contract, curr_contract, row['invoice_group_name'], row['invoice_group_description'], zko, akciz, row['has_grid_services'], row['has_spot_price'],
                                                                                row['has_balancing'], row['tariff_name'], row['price_day'],	row['price_night'], row['make_invoice'], row['lower_limit'], row['upper_limit'])
                else:
                    
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
                    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
                print(f'Linked {df.linked_contract_internal_id} added !!!!')
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
        
        forecast_df = validate_forecasting_df(df, form.itn.data) if df is not None else None        
        # curr_meta = ItnMeta.query.filter(ItnMeta.itn == )
        applicable_sub_contracts = get_subcontracts_by_itn_and_utc_dates(form.itn.data, form_start_date_utc, form_end_date_utc)
        print(f'applicable_sub_contracts {applicable_sub_contracts}')
        print(f'has_spot_price {form.has_spot_price.data}')
        if has_overlaping_subcontracts(form.itn.data, form_start_date_utc) and has_overlaping_subcontracts(form.itn.data, form_end_date_utc):
            flash('overlaping', 'danger')

        else:               
            new_sub_contract = (SubContract(itn = form.itn.data,
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
                                    has_balancing = form.has_balancing.data,
                                    make_invoice = form.make_invoice.data) )                                   
            
            for curr_subcontract in applicable_sub_contracts:                                  
                apply_collision_function(new_sub_contract, curr_subcontract, form.measuring_type.data.code, form.itn.data, \
                                        form.forecast_vol.data, form_forecast_df, form.start_date.data, curr_contract)

            form_day_price = form.single_tariff_price.data if form.day_tariff_price.data == 0 else form.day_tariff_price.data

            curr_tariff =  create_tariff(form.tariff_name.data, form_day_price, form.night_tariff_price.data, form.peak_tariff_price.data)

            generate_forecast_schedule(form.measuring_type.data, form.itn.data, form_forecasted_vol, forecast_df, form_start_date_utc, curr_contract, curr_tariff, form.has_spot_price.data, form_end_date_utc)
           
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

@app.route('/modify_contractor', methods=['GET', 'POST'])
@login_required
def modify_contractor():
    
    form = RedactContractorForm()
    if form.validate_on_submit():
        try:
            contractor_id = form.contractors.data[0].id
        except:
            flash('Choose contractor first, then pres ENTER or doubleclicked on it or press the button!.','danger')
        else:
            return redirect(url_for('modify_selected_contractor', contractor_id = contractor_id, **request.args)) 
    # else:
    #     flash('Choose contractor first','danger')
    return render_template('modify_contractor.html', title='Modify Contractor', form=form, header = 'Modifying CONTRACTOR')

@app.route('/table', methods=['GET', 'POST'])
@login_required
def table():
    return render_template('table.html', title='Table')

@app.route('/modify_contract/<contract_id>/<key_word>', methods=['GET', 'POST'])
@login_required
def modify_selected_contract(contract_id,key_word):

    curr_contract = Contract.query.filter(Contract.id == contract_id).first()           
    print(f'load current contract -- {curr_contract} - {curr_contract.contract_type_id}')
    if curr_contract is None:
            flash(f'There isn\'t an contract with id: {contract_id} ! Aborting !','danger')
            return redirect(url_for('modify_contract'))

    curr_contractor = Contractor.query.join(Contract, Contract.contractor_id == Contractor.id).filter(Contract.id == contract_id).first()

    # if curr_contract.parent_id != 0:
    parent_contract = Contract.query.filter(Contract.id == curr_contract.parent_id).first() if curr_contract.parent_id != 0 else None

    if request.method == "GET":
        print(f'in get')
        # curr_contractor = Contractor.query.join(Contract, Contract.contractor_id == Contractor.id).filter(Contract.id == contract_id).first()
        contr_names = [(x.id, x.name) for x in Contractor.query.all()]  
        # contr_names = sorted(contr_names, key = lambda x: x[1])
             
        contr_names.remove((curr_contractor.id, curr_contractor.name))
        contr_names.insert(0,(curr_contractor.id, curr_contractor.name))

        contracts = Contract.query.filter(Contract.id != contract_id).all()
        # 
        parent_contracts = sorted([(x.internal_id, f'{x.internal_id} - {Contractor.query.join(Contract,Contract.contractor_id == Contractor.id).filter(Contract.id == x.id).first().name}') 
                                                                                                                    for x in contracts], key = lambda x: x[1].split(' - ')[1]) 
        # sorted(parent_contracts, key = lambda y: y[1].split(' - ')[1])  
        parent_contracts.insert(0,('none','none - none'))
        if curr_contract.parent_id != 0:
            # parent_contract = Contract.query.filter(Contract.id == curr_contract.parent_id).first()
            parent_contractor = Contractor.query.join(Contract,Contract.contractor_id == Contractor.id).filter(Contract.internal_id == parent_contract.internal_id).first()
            parent_contracts.remove((parent_contract.internal_id, f'{parent_contract.internal_id} - {parent_contractor.name}'))
            parent_contracts.insert(0,(parent_contract.internal_id, f'{parent_contract.internal_id} - {parent_contractor.name}'))

        curr_contract_type = ContractType.query.join(Contract, Contract.contract_type_id == ContractType.id).filter(Contract.id == curr_contract.id).first()
        contract_types = [(x.name, x.name) for x in ContractType.query.all()]
        contract_types.remove((curr_contract_type.name, curr_contract_type.name))
        
        contract_types.insert(0,(curr_contract_type.name, curr_contract_type.name))

        form_dict = {}
        form_dict['internal_id'] = curr_contract.internal_id
        form_dict['subject'] = curr_contract.subject
        form_dict['end_date'] = curr_contract.end_date

        form = ContarctDataForm(formdata=MultiDict(form_dict))
        form.contractor.choices = contr_names  
        form.parent_contract.choices = parent_contracts        #  sorted(parent_contracts, key = lambda y: y[1].split(' - ')[1])     
        form.contract_type.choices = contract_types
        # par_names.insert(0,('none', 'none'))
        # names_tup =  [x for x in par_names if x[0] == curr_contractor.name][0]   

        subcontracts = SubContract.query.filter(SubContract.contract_id == contract_id).all()
        subcontracts = [x for x in subcontracts]
        form.subs.choices = [(x.itn,f'{x.itn} - {x.start_date} - {x.end_date}') for x in subcontracts]
    else:   
        print(f'in post')
        form = ContarctDataForm()
        form.contractor.choices = [(form.contractor.data, form.contractor.data)]
        
        form.parent_contract.choices = [(form.parent_contract.data, form.parent_contract.data)]
        form.contract_type.choices = [(form.contract_type.data, form.contract_type.data)]

        subcontracts = SubContract.query.filter(SubContract.itn.in_(form.subs.data)).all()
        subcontracts = [x for x in subcontracts]
        form.subs.choices = [(x.itn,f'{x.itn} - {x.start_date} - {x.end_date}') for x in subcontracts]
        print(f'form.parent_contract {form.parent_contract.data}')
        if form.validate_on_submit():            
            print(f'in submit')
            if form.delete_subs.data or form.delete_contract.data:
                selected = form.subs.data if not form.delete_contract.data else [x.itn for x in SubContract.query.filter(SubContract.contract_id == contract_id).all()]
                print(f'selected {selected}')
                for s in selected:
                    curr_sub = SubContract.query.filter(SubContract.itn == s, SubContract.contract_id == contract_id).first()
                    delete_sch = ItnSchedule.__table__.delete().where((ItnSchedule.itn == curr_sub.itn) & (ItnSchedule.utc >= curr_sub.start_date) & (ItnSchedule.utc <= curr_sub.end_date))
                    db.session.execute(delete_sch)
                    print(f'Schedule with itn: {curr_sub.itn}, from {curr_sub.start_date} to {curr_sub.end_date} is deleted.')                   
                    print(f'Subcontract {curr_sub} will be deleted ')
                    curr_sub.delete()

                if form.delete_contract.data:                
                    print(f'Contract {curr_contract} will be deleted ')
                    curr_contract.delete()
            else:
                update_dict = {}
                end_date = convert_date_to_utc('EET',form.end_date.data)
                end_date = end_date + dt.timedelta(hours = 23) 
                
                if end_date < curr_contract.end_date:
                    print(f'terminating to {end_date}')
                    curr_contract.update({'end_date':end_date})
                    subs = SubContract.query.filter(SubContract.contract_id == contract_id).all()    
                    for sub in subs:
                        sub.update({'end_date':end_date})

                curr_contract_type = ContractType.query.filter(ContractType.id == curr_contract.contract_type_id).first()
                
                if form.contract_type.data != curr_contract_type.name:
                    print(f'Update contract type from: {curr_contract_type.name} to {form.contract_type.data}')
                    new_type_id = ContractType.query.filter(ContractType.name == form.contract_type.data).first().id
                    update_dict['contract_type_id'] = new_type_id
                    # curr_contract.update({'contract_type_id':new_type_id})                    

                if form.subject.data != 'none': 
                    update_dict['subject'] = form.subject.data        
                    
                if form.contractor.data != curr_contractor.id:
                    update_dict['contractor_id'] = form.contractor.data

                curr_parent_contract_internal_id = parent_contract.internal_id if parent_contract is not None else None
                
                if form.parent_contract.data != curr_parent_contract_internal_id:
                    update_dict['parent_id'] = Contract.query.filter(Contract.internal_id == form.parent_contract.data).first().id 
                    # print(f'{form.parent_contract.data}')
                print(f'{update_dict}')
                curr_contract.update(update_dict)      
        else:
            # print(f'{form.errors}')
            # print(f'{form.subs.choices}')
            return render_template('ask_confirm.html', title=f'Modify Contract', form=form, header = f'Modify Contract {curr_contract.internal_id} - {curr_contractor.name}', need_dt_picker = True)
        return redirect(url_for('modify',key_word = key_word))


    return render_template('ask_confirm.html', title=f'Modify Contract', form=form, header = f'Modify Contract {curr_contract.internal_id} - {curr_contractor.name}', need_dt_picker = True)

@app.route('/modify_contractor/<contractor_id>', methods=['GET', 'POST'])
@login_required
def modify_selected_contractor(contractor_id):

    curr_contractor = Contractor.query.filter(Contractor.id == contractor_id).first()
    if curr_contractor is None:
        flash(f'There isn\'t contractor with id: {contractor_id} ! Aborting !','danger')
        return redirect(url_for('modify_contractor'))
    
    if request.method == "GET":   

        form_dict = {}
        form_dict['name'] = curr_contractor.name
        form_dict['eic'] = curr_contractor.eic
        form_dict['address'] = curr_contractor.address
        form_dict['vat_number'] = curr_contractor.vat_number
        form_dict['email'] = curr_contractor.email
        form_dict['acc_411'] = curr_contractor.acc_411
        form = ContarctorDataForm(formdata=MultiDict(form_dict))

        par_names = [(x.name, x.name) for x in Contractor.query.all()]          
        par_names.insert(0,('none', 'none'))
        names_tup =  [x for x in par_names if x[0] == curr_contractor.name][0]    
        parent_contractor_id = curr_contractor.parent_id
        
        if parent_contractor_id is not None:
        
            parent_contractor_name = Contractor.query.filter(Contractor.id == parent_contractor_id).first().name        
            par_names.remove((parent_contractor_name, parent_contractor_name))
            par_names.insert(0,(parent_contractor_name, parent_contractor_name))

        form.parent_contractor.choices = par_names 


    else:
        form = ContarctorDataForm()
        form.parent_contractor.choices = [(form.parent_contractor.data,form.parent_contractor.data)]
        
        if form.validate_on_submit(): 
            
            for_modificaion_contractor = Contractor.query.filter(Contractor.id == form.acc_411.data).first()
            if for_modificaion_contractor is None:
                flash(f'There isn\'t contractor with acc_411: {form.acc_411.data}! Aborting !','danger')
                return redirect(url_for('modify_contractor'))
            parent = Contractor.query.filter(Contractor.name == form.parent_contractor.data).first()
            parent_id = parent.id if parent is not None else parent
            for_modificaion_contractor.update({'parent_id':parent_id,'name':form.name.data, 'eic':form.eic.data,
                                                'address':form.address.data, 'vat_number':form.vat_number.data,'email': form.email.data})         
            
        return redirect(url_for('modify_contractor'))
    
    contr_name = curr_contractor.name if curr_contractor is not None else 'None'
    return render_template('ask_confirm.html', title=f'Modify Contractor', form=form, header = f'Modify Contractor {contr_name}')
    
    
@app.route('/monthly_report/<erp>/<contract_type>/<start_date>/<end_date>/<is_mixed>', methods=['GET', 'POST'])
@login_required
def monthly_report_by_erp( erp, start_date, end_date, contract_type, is_mixed):
    form = MonthlyReportErpForm()
    form.ref_files.choices = sorted([(x,x) for x in get_excel_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']))])
    filtered_records = get_inv_gr_id_single_erp(erp, contract_type, start_date, end_date, is_mixed)

    form.invoicing_group.choices = sorted(list(set([ (x[0],f'{x[0]} - {x[1]} ') for x in filtered_records])),key = lambda y: y[1].split(' - ')[1])

    
    form.contracts.choices = sorted(list(set([(x[3],f'{x[2]}') for x in filtered_records] )) ,key = lambda y: y[1].split(' - ')[1]) 

    if form.validate_on_submit():        
        
        start = time.time()  
        if form.submit_delete.data:
            delete_excel_files(os.path.join(app.root_path, app.config['INV_REFS_PATH']), form.ref_files.data, form.delete_all.data)
            return redirect(url_for('monthly_report_by_erp', erp = erp,start_date = start_date, end_date = end_date, contract_type = contract_type, is_mixed = is_mixed, **request.args))
           

        elif form.submit.data:
            counter = 0
            weighted_price = None
 
            if len(form.contracts.data) >0:
                for curr_contract in form.contracts.data: 
                    print(f'CURR CONTRACT - {curr_contract}')         
                    time_zone = TimeZone.query.join(Contract, Contract.time_zone_id == TimeZone.id).filter(Contract.internal_id == curr_contract).first().code
                    start_date_utc = convert_date_to_utc(time_zone, start_date)
                    end_date_utc = convert_date_to_utc(time_zone, end_date) + dt.timedelta(hours = 23)
                    inv_groups = get_list_inv_groups_by_contract(curr_contract, start_date_utc, end_date_utc)
                    weighted_price = get_weighted_price(inv_groups, start_date_utc, end_date_utc)
                    print(f'weighted_price -- {weighted_price}')
                    counter += create_inv_refs_by_inv_groups(inv_groups, start_date, end_date, weighted_price)
                flash(f'{counter} invoice references was created !','info')    
            else:            
                inv_groups = [x[0] for x in form.invoicing_group.choices]  if form.bulk_creation.data else [x for x in form.invoicing_group.data]
                counter = create_inv_refs_by_inv_groups(inv_groups, start_date, end_date, weighted_price)   
                flash(f'{counter} invoice references was created !','info')
            
            return redirect(url_for('monthly_report_by_erp', erp = erp,start_date = start_date, end_date = end_date, contract_type = contract_type, is_mixed = is_mixed, **request.args))     
                
        end = time.time()
        print(f'Time elapsed for generate excel file(s) : {end - start}  !')

    return render_template('quick_template_wider.html', title=f'Monthly Report {erp}', form=form, header = f'Monthly Report {erp} - {contract_type} <br> Period: {start_date} / {end_date}<br> Included mixed groups - {is_mixed}')

@app.route('/delete_itn_consumptions', methods=['GET', 'POST'])
@login_required
def delete_itn_consumptions():

    form = ItnCosumptionDeletion()
    if form.validate_on_submit(): 
        curr_date_utc = convert_date_to_utc('EET',form.start_date.data)
        schedule_df = pd.read_sql(ItnSchedule.query.filter(ItnSchedule.itn == form.itn.data, ItnSchedule.utc >= curr_date_utc).statement, db.session.bind) 
        schedule_df['consumption_vol'] = 0
        schedule_df['settelment_vol'] = 0
        stringifyer(schedule_df)
        bulk_update_list = schedule_df.to_dict(orient='records')    
        db.session.bulk_update_mappings(ItnSchedule, bulk_update_list)
        db.session.commit()

    return render_template('quick_template.html', title='Deleting ITN consumption', form=form, header = 'Deleting ITN consumption')   


@app.route('/modify_email', methods=['GET', 'POST'])
@login_required
def modify_email():
    form = RedactEmailForm()
    
    if form.validate_on_submit():
        res = form.inv_goups_mails.data[0]
        new_mail = form.new_mail.data
        mail_to_add = Mail.query.filter(Mail.name == new_mail).first()
        if mail_to_add is None:
            mail_to_add = Mail(name = new_mail)
            mail_to_add.save()
            print(f'adding new mail {mail_to_add}')
            mail_to_add = Mail.query.filter(Mail.name == new_mail).first()
        curr_inv_group = form.inv_goups_mails.data[0]
        curr_inv_group.update({'email_id':mail_to_add.id})
        print(f'Invoicing group {curr_inv_group.name} will mail to {mail_to_add.name} !')
        flash(f'Invoicing group {curr_inv_group.name} will mail to {mail_to_add.name} !','success')
        return redirect(url_for('modify_email'))

    return render_template('modify_email.html', title='Readcting Email', form=form, header = 'Redacting EMAILS')

@app.route('/modify_invoice/<invoice_num>', methods=['GET', 'POST'])
@login_required
def modify_invoice(invoice_num):

    full_path = os.path.join(os.path.join(app.root_path, app.config['TEMP_INVOICE_PATH']),app.config['TEMP_INVOICE_NAME'])
    dtype_dict= {'BULSTAT': str, 'TaxNum' : str, 'DocNumber' : str}    
    raw_df = pd.read_excel(full_path, dtype = dtype_dict)    
    selected_df = raw_df[raw_df['DocNumber'] == invoice_num].copy()  
    if selected_df.empty:
        print(f'No invoice with num {invoice_num}.') 
        flash(f'No invoice with num {invoice_num}! Abort !','danger')
        return redirect(url_for('create_invoice'))     
    
    if request.method == "GET":
        invoice_number  = selected_df.iloc[0]['DocNumber']

        curr_contractor = Contractor.query.filter(Contractor.acc_411 == selected_df.iloc[0]['fullcode']).first()
        if curr_contractor is None:
            name = selected_df.iloc[0]['FirmName']
            acc = selected_df.iloc[0]['fullcode']
            print(f'Warning! There isn\'t such a contractor: {name} with acc_411 :{acc} in the database !')
            flash(f'Warning! There isn\'t such a contractor: {name} with acc_411 :{acc} in the database ! Abort !','danger')
            return redirect(url_for('create_invoice')) 

        db_invoice = Invoice.query.filter(Invoice.id == invoice_number).first()
        if db_invoice is None:        
            print(f'Warning! There isn\'t such an invoice with number: {invoice_number} in the database !')

        inv_dict = {}

        inv_dict['invoice_num'] = invoice_number
        inv_dict['contractor_name'] = selected_df.iloc[0]['FirmName']
        inv_dict['bulstat'] = curr_contractor.eic if curr_contractor is not None else selected_df.iloc[0]['BULSTAT']
        inv_dict['vat_number'] = curr_contractor.vat_number if curr_contractor is not None else selected_df.iloc[0]['TaxNum']
        inv_dict['address'] = curr_contractor.address if curr_contractor is not None else selected_df.iloc[0]['Address']

        inv_dict['electricity_qty']  = selected_df[selected_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)']['Quantity'].values[0]
        inv_dict['electricity_price']  = selected_df[selected_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)']['PriceLev'].values[0]
        inv_dict['electricity_sum']  = selected_df[selected_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)']['ItemSuma'].values[0]
        inv_dict['zko_price']  = selected_df[selected_df['StockName'] == 'ЦЕНА ЗАДЪЛЖЕНИЕ КЪМ ОБЩЕСТВОТО СЪГЛАСНО ЧЛ.100 АЛ.4 ОТ ЗЕ И ЧЛ.31 ОТ']['PriceLev'].values[0]
        inv_dict['zko_sum']  = selected_df[selected_df['StockName'] == 'ЦЕНА ЗАДЪЛЖЕНИЕ КЪМ ОБЩЕСТВОТО СЪГЛАСНО ЧЛ.100 АЛ.4 ОТ ЗЕ И ЧЛ.31 ОТ']['ItemSuma'].values[0]
        inv_dict['akciz_price']  = selected_df[selected_df['StockName'] == 'НАЧИСЛЕН АКЦИЗ']['PriceLev'].values[0]
        inv_dict['akciz_sum']  = selected_df[selected_df['StockName'] == 'НАЧИСЛЕН АКЦИЗ']['ItemSuma'].values[0]
        inv_dict['grid_sum']  = selected_df[selected_df['StockName'] == 'ПРЕНОС И ДОСТЪП ДО ЕЛ.МРЕЖАТА']['ItemSuma'].values[0]
        inv_dict['sum_neto']  = selected_df[selected_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)']['DocSuma'].values[0]
        inv_dict['sum_vat']  = selected_df[selected_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)']['DocVatSuma'].values[0]
        inv_dict['sum_total']  = selected_df[selected_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)']['DocAllSuma'].values[0]
        inv_dict['pay_date']  = selected_df[selected_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)']['PayDate'].values[0]
        inv_dict['excel_ref_name']  = selected_df[selected_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)']['RepFileName'].values[0]   

        form = ModifyInvoiceForm(formdata=MultiDict(inv_dict))
    else:
        form = ModifyInvoiceForm()

    if form.validate_on_submit():
        
        to_modify_df = raw_df[raw_df['DocNumber'] == form.invoice_num.data].copy()

        to_modify_df['FirmName'] = form.contractor_name.data
        to_modify_df['bulstat'] = form.bulstat.data
        to_modify_df['vat_number'] = form.vat_number.data
        to_modify_df['address'] = form.address.data

        to_modify_df.loc[to_modify_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)','Quantity']= form.electricity_qty.data
        to_modify_df.loc[to_modify_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)','PriceLev']= form.electricity_price.data
        to_modify_df.loc[to_modify_df['StockName'] == 'ELECTRICITY - (ЕЛЕКТРИЧЕСКА ЕНЕРГИЯ)','ItemSuma']= form.electricity_sum.data
        to_modify_df.loc[to_modify_df['StockName'] == 'ЦЕНА ЗАДЪЛЖЕНИЕ КЪМ ОБЩЕСТВОТО СЪГЛАСНО ЧЛ.100 АЛ.4 ОТ ЗЕ И ЧЛ.31 ОТ','PriceLev']= form.zko_price.data
        to_modify_df.loc[to_modify_df['StockName'] == 'ЦЕНА ЗАДЪЛЖЕНИЕ КЪМ ОБЩЕСТВОТО СЪГЛАСНО ЧЛ.100 АЛ.4 ОТ ЗЕ И ЧЛ.31 ОТ','ItemSuma']= form.zko_sum.data
        to_modify_df.loc[to_modify_df['StockName'] == 'НАЧИСЛЕН АКЦИЗ','PriceLev']= form.akciz_price.data
        to_modify_df.loc[to_modify_df['StockName'] == 'НАЧИСЛЕН АКЦИЗ','ItemSuma']= form.akciz_sum.data
        to_modify_df.loc[to_modify_df['StockName'] == 'ПРЕНОС И ДОСТЪП ДО ЕЛ.МРЕЖАТА','ItemSuma']= form.grid_sum.data
        to_modify_df['DocSuma']= form.sum_neto.data
        to_modify_df['DocVatSuma']= form.sum_vat.data
        to_modify_df['DocAllSuma']= form.sum_total.data
        to_modify_df['PayDate']= form.pay_date.data
        to_modify_df['RepFileName']= form.excel_ref_name.data

        create_invoices(to_modify_df, is_modify = True)            
        return redirect(url_for('create_invoice'))

    return render_template('quick_template_wider.html', title='Readcting Invoice', form=form, header = 'Redacting INVOICES')


@app.route('/modify/<key_word>', methods=['GET', 'POST'])
# @app.route('/modify', defaults={'key_word': None}, methods=['GET', 'POST'])
@login_required
def modify(key_word):
    
    if request.method == "GET":
        form_dict = {}
        if key_word != 'none':
            form_dict['search'] = key_word   
        
        form = ModifyForm(formdata=MultiDict(form_dict))       
        return render_template('modify.html', title='Readcting', form=form, header = 'Redacting',key_word = key_word)
    else:
        form = ModifyForm()   
        key_word = form.search.data if form.search.data != '' else 'none'
        
        if form.validate_on_submit():
            if form.modify_contract.data:
                contract_id = form.contracts.data[0].id                
                return redirect(url_for('modify_selected_contract', contract_id = contract_id, key_word = key_word, **request.args))
        else:
            print(f'{form.errors}')       
        
        return render_template('modify.html', title='Readcting', form=form, header = 'Redacting', key_word = key_word)

@app.route('/modify_inv_group/<inv_name>/<key_word>', methods=['GET', 'POST'])
# @app.route('/modify_inv_group/<inv_name>', defaults={'key_word': None}, methods=['GET', 'POST'])
@login_required
def modify_inv_group(inv_name, key_word):
    print(f'in modify_inv_group -- {inv_name} - {key_word}')
    curr_contractor = Contractor.query.join(InvoiceGroup, InvoiceGroup.contractor_id == Contractor.id).filter(InvoiceGroup.name == inv_name).first()
    curr_invoice_group = InvoiceGroup.query.filter(InvoiceGroup.name == inv_name).first()
    mails = Mail.query.filter(Mail.id == curr_invoice_group.email_id).first()
    if request.method == "GET":
        print(f'in get')        
        contr_names = [(x.id, x.name) for x in Contractor.query.join(InvoiceGroup,InvoiceGroup.contractor_id == Contractor.id).all()]  
        contr_names = sorted(contr_names, key = lambda x: x[1])             
        contr_names.remove((curr_contractor.id, curr_contractor.name))
        contr_names.insert(0,(curr_contractor.id, curr_contractor.name))
        to_inv_names = [(x.id, x.name) for x in InvoiceGroup.query.filter(InvoiceGroup.contractor_id == curr_contractor.id).all()] 
        to_contracts = [(x.internal_id, f'{x.internal_id} - {x.start_date} - {x.end_date}') for x in Contract.query.join(Contractor,Contractor.id == Contract.contractor_id).filter(Contractor.id == curr_contractor.id).all()]  
        form_dict = {}
        form_dict['from_suffix'] = curr_invoice_group.name.split('_')[1]
        form_dict['from_group'] = curr_invoice_group.name
        form_dict['from_description'] = curr_invoice_group.description
        # form_dict['to_description'] = curr_invoice_group.description
        # form_dict['to_email'] = mails.name
        # form_dict['end_date'] = curr_contract.end_date

        form = ModifyInvGroupForm(formdata=MultiDict(form_dict))
        form.from_contractor.choices = contr_names 
        form.to_contractor.choices = contr_names 
        form.to_group.choices = to_inv_names
        form.to_contract.choices = to_contracts
    else:
        
        form = ModifyInvGroupForm()
        if form.validate_on_submit():
            for key, val in request.form.items():
                print(f'{key} --  {val}')
            print(f'{form.itns.data}')

            from_subcontracts = SubContract.query.join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id).filter(InvoiceGroup.name == form.from_group.data).filter(SubContract.itn.in_(form.itns.data)).all()
            from_invoicing_group = InvoiceGroup.query.filter(InvoiceGroup.name == form.from_group.data).first()
            new_mail = form.to_email.data
            mail_to_add = Mail.query.filter(Mail.name == new_mail).first()
            if mail_to_add is None:
                mail_to_add = Mail(name = new_mail)
                mail_to_add.save()
                print(f'adding new mail {mail_to_add}')
                mail_to_add = Mail.query.filter(Mail.name == new_mail).first()

            if(form.new_group.data):               
                
                to_contractor = Contractor.query.filter(Contractor.id == form.to_contractor.data).first()                                        
                to_invoicing_group = InvoiceGroup(name = form.new_group.data, contractor_id = to_contractor.id, description = form.to_description.data, email_id = mail_to_add.id)                
                to_invoicing_group.save()
                print(f'New invoicing group: {to_invoicing_group.name} was created !')
                to_invoicing_group = InvoiceGroup.query.filter(InvoiceGroup.name == form.new_group.data).first()

            else:                
                to_invoicing_group = InvoiceGroup.query.filter(InvoiceGroup.name == form.to_group.data).first()                 
                to_invoicing_group.update({'description':form.to_description.data,'email_id':mail_to_add.id})
                print(f'from_invoicing_group: {from_invoicing_group} to_invoicing_group: {to_invoicing_group}')          

            for sub in from_subcontracts:
                sub.update({'invoice_group_id':to_invoicing_group.id})

            remaining_subs = SubContract.query.filter(SubContract.invoice_group_id == from_invoicing_group.id).all()

            if len(remaining_subs) == 0:
                print(f'{from_invoicing_group.name} is orphaned ! Deleting !')
                from_invoicing_group.delete()                

            flash('success','info')
            return redirect(url_for('modify', key_word = key_word))
                
        else:
            print(f'{form.errors.items()}')

    return render_template('modify_inv_group.html', title='Readcting Inv Group', form=form, header = 'Redacting Invoicing Group')

@app.route('/modify_itn/<itn>/<key_word>', methods=['GET', 'POST'])
@login_required
def modify_itn(itn,key_word):

    # print('------ {0}'.format(request.form))
    if request.method == "GET":
        data = (
            db.session.query(
                ItnMeta.itn, AddressMurs.name, ItnMeta.description, ItnMeta.grid_voltage, Erp.name
            )
            .join(AddressMurs,AddressMurs.id == ItnMeta.address_id)
            .join(Erp,Erp.id == ItnMeta.erp_id)
            .filter(ItnMeta.itn == itn)
            .all()
        )
        
        form_dict = {}
        form_dict['itn'] = itn
        form_dict['itn_addr'] = data[0][1]
        form_dict['itn_descr'] = data[0][2]
        form_dict['grid_voltage'] = data[0][3]
        form_dict['erp'] = data[0][4]
        # form_dict['itn'] = itn
        form = ModifyItn(formdata=MultiDict(form_dict))
    else:
        form = ModifyItn()

        if form.validate_on_submit():

            curr_addr = AddressMurs.query.filter(AddressMurs.name == form.itn_addr.data).first()
            if curr_addr is None:   
                new_addr = AddressMurs(name = form.itn_addr.data)
                new_addr.save()
                curr_addr = AddressMurs.query.filter(AddressMurs.name == form.itn_addr.data).first()

            curr_meta = ItnMeta.query.get(form.itn.data)
            erp = Erp.query.filter(Erp.name == form.erp.data).first()
            curr_meta.update({'address_id': curr_addr.id,'description':form.itn_descr.data, 'grid_voltage':form.grid_voltage.data, 'erp_id':erp.id})
            flash('success','info')
            return redirect(url_for('modify', key_word = key_word))


            # addr = AddressMurs.query.join(ItnMeta, ItnMeta.address_id == AddressMurs.id).filter(ItnMeta.itn == form.itn.data).first()
            # addr.update({'name':form.itn_addr.data})
            # itn_meta = ItnMeta.query.filter(ItnMeta.itn == itn).first()
            # 
            # itn_meta.update({'description':form.itn_descr.data, 'grid_voltage':form.grid_voltage.data, 'erp_id':erp.id})
            # print('------ {0}'.format(request.form))
        
    return render_template('ask_confirm.html', title='Redacting Itn', form=form, header = 'Redacting ITN')

@app.route('/modify_subcontracts', methods=['GET', 'POST'])
@login_required
def modify_subcontracts():
    form = ModifySubcontractEntryForm()
    if form.validate_on_submit():
        # print(f'{form.subcontracts.data[0]}')
        tokens = form.subcontracts.data[0].split(' - ')
        print(f'{tokens}')
        subcontract = SubContract.query.filter(SubContract.itn == tokens[0], SubContract.start_date == tokens[1]).first()        
        subcontract.update({'has_grid_services': form.has_grid.data})
        
        print(f'{form.has_grid.data}')

    return render_template('modify_subcontracts.html', title='Redacting Subcontracts', form=form, header = 'Redacting SUBCONTRACTS', need_dt_picker = True)

@app.route('/_get_inv_groups/<contract_id>', methods=['GET', 'POST'])
@login_required
def _get_inv_groups(contract_id):
    
    is_id = str(contract_id).find('ТК') == -1 
      
    filters = (Contract.id == contract_id,) if is_id else (Contract.internal_id == contract_id,)
    
    groups = (
        db.session.query
            (InvoiceGroup.name.label('invoice_group'), InvoiceGroup.description.label('invoice_group_description'), Mail.name.label('email'))
        .join(Mail,Mail.id == InvoiceGroup.email_id)            
        .join(Contract, Contract.contractor_id == InvoiceGroup.contractor_id)
        .filter(*filters)
        .order_by(InvoiceGroup.name)
        .all()
    ) 
    
    groups_arr = []
    for group in groups:
        group_obj = {}
        group_obj['invoice_group'] =  group[0]  
        group_obj['invoice_group_description'] =  group[1]
        group_obj['email'] =  group[2]
        groups_arr.append(group_obj)

    return jsonify({'groups':groups_arr})

@app.route('/_get_contract/<tk>', methods=['GET', 'POST'])
@login_required
def _get_contract(tk = ''):
    print(f'{tk}')
    if len(tk) <= 2:
        print(f'in return')
        return jsonify({'groups':[]})
    
    contract = (
        db.session.query(
            Contract.internal_id, Contractor.name, Contract.start_date, Contract.end_date, Contract.id
        )
        .join(Contractor, Contractor.id == Contract.contractor_id)
        .filter(Contract.internal_id == tk)
        .first()
    )
    # print(f'from _get_contract contract --->{contract}')
    if contract is None:
        print(f'in second return')
        return jsonify({'groups':[]})
    
    contract_arr = [{'internal_id':contract[0],'contractor_name':contract[1], 'start_date':convert_date_from_utc('EET',contract[2]) , 'end_date':convert_date_from_utc('EET',contract[3]), 'contract_id':contract[4]}]
    # print(f'contract_arr {contract_arr}')
    
    return jsonify({'contracts':contract_arr})

@app.route('/_get_contracts/<contractor_id>', methods=['GET', 'POST'])
@login_required
def _get_contracts(contractor_id):
    
    contracts = (
        db.session.query(
            Contract.internal_id, Contractor.name, Contract.start_date, Contract.end_date, Contract.id
        )
        .join(Contractor, Contractor.id == Contract.contractor_id)
        .filter( Contractor.id == contractor_id)
        .all()
    )
    contract_arr = []
    for contract in contracts:
        contract_obj = {}
        contract_obj['internal_id'] = contract[0]
        contract_obj['contractor_name'] = contract[1]
        contract_obj['start_date'] = convert_date_from_utc('EET',contract[2])
        contract_obj['end_date'] = convert_date_from_utc('EET',contract[3])
        contract_obj['contract_id'] = contract[4]
        contract_arr.append(contract_obj)    
    return jsonify({'contracts':contract_arr})

@app.route('/_get_itns/<name>', methods=['GET', 'POST'])
@login_required
def _get_itns(name):
    itns = (
        db.session.query(
            SubContract.itn, InvoiceGroup.name, MeasuringType.code, AddressMurs.name, SubContract.has_grid_services
        )
        .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id)        
        .join(MeasuringType,MeasuringType.id == SubContract.measuring_type_id)
        .join(ItnMeta,ItnMeta.itn == SubContract.itn)
        .join(AddressMurs,AddressMurs.id == ItnMeta.address_id)
        .filter(InvoiceGroup.name == name)
        .order_by(MeasuringType.code, SubContract.itn)
        .all()
    )
    itns_arr = []
    for itn in itns:
        itn_obj = {}
        itn_obj['itn'] = itn[0]
        itn_obj['invoice_group'] = itn[1]
        itn_obj['type'] = itn[2]
        itn_obj['address'] = itn[3]
        itn_obj['has_grid_services'] = itn[4]
        itns_arr.append(itn_obj)
    return jsonify({'itns':itns_arr})

@app.route('/_get_contractor/<contractor_id>', methods=['GET', 'POST'])
@login_required
def _get_contractor(contractor_id):
    contractor = Contractor.query.filter(Contractor.id == contractor_id).first()
    
    print(f'contractor {contractor}')
    contarctor_arr = [{'name':contractor.name, 'acc_411':contractor.acc_411}]
    return jsonify({'contractor':contarctor_arr})

# @app.route('/_get_first_empty_inv_group/', defaults={'inv_name_part': None}, methods=['GET', 'POST'])
@app.route('/_get_first_empty_inv_group/<inv_name_part>', methods=['GET', 'POST'])
@login_required
def _get_first_empty_inv_group(inv_name_part):
    try:
        if inv_name_part.find('_') == -1:
            return jsonify({'groups':[]})
    except:
         return jsonify({'groups':[]})
    else:
        inv_groups = (
            db.session.query(
                InvoiceGroup.name
            )
            .filter(InvoiceGroup.name.like("%" + inv_name_part + "%"))        
            .all()
        )    
        try: 
            suffix = [int(x[0].split('_')[1]) for x in inv_groups]
            suffix = str(sorted(suffix)[-1] + 1)        
            # suffix = str(suffix)            
            tokens = inv_groups[-1][0].split('_')
        except:
            return jsonify({'groups':[]})
        else:        
            res = f'{tokens[0]}_{suffix}'        
            return jsonify({'groups':[{'name':res}]})

@app.route('/_get_inv_group_data/<inv_id>', methods=['GET', 'POST'])
@login_required
def _get_inv_group_data(inv_id):

    is_id = str(inv_id).find('-') == -1 
      
    filters = (InvoiceGroup.id == inv_id,) if is_id else (InvoiceGroup.name == inv_id,)
    group = {
        db.session.query(
            InvoiceGroup.description, Mail.name.label('email')
        )
        .join(Mail,Mail.id == InvoiceGroup.email_id)
        .filter(*filters)
        .first()
    }    
    group = list(group)    
    if group[0] is None:        
        return jsonify({'groups':[]})    
    else:
        return jsonify({'groups':[{'description':group[0][0],'email':group[0][1]}]})

@app.route('/_get_itn_data/<itn>', methods=['GET', 'POST'])
@login_required
def _get_itn_data(itn):
    data = (
        db.session.query(
            ItnMeta.itn, AddressMurs.name, ItnMeta.description, ItnMeta.grid_voltage, Erp.name
        )
        .join(AddressMurs,AddressMurs.id == ItnMeta.address_id)
        .join(Erp,Erp.id == ItnMeta.erp_id)
        .filter(ItnMeta.itn == itn)
        .all()
    )
    if len(data) == 0:
        return jsonify({'itns':[]})
    return jsonify({'itns':[{'itn':data[0][0],'address':data[0][1], 'description':data[0][2], 'grid_voltage':data[0][3], 'erp':data[0][4]}]})


@app.route('/_get_subcontracts/<start_date>/<end_date>/<filter_arg>/<is_id>', methods=['GET', 'POST'])
@login_required
def _get_subcontracts(start_date, end_date, filter_arg, is_id): 


    is_id = True if is_id != '0' else False
    filters = (Contract.id == filter_arg,) if is_id else (SubContract.itn == filter_arg,)
    print(f'filters\n{is_id}')

    start_date = convert_date_to_utc('EET',start_date)   
    end_date = convert_date_to_utc('EET',end_date)
    end_date = end_date + dt.timedelta(hours = 23)
    subs = SubContract.query.join(Contract,Contract.id == SubContract.contract_id).filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))).filter(*filters).all()    
    
    sub_schema = SubContractSchema()
    try:
        return jsonify(sub_schema.dump(subs, many = True))
    except:
        return jsonify([])

