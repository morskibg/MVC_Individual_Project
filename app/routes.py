import os
import xlrd
import time,re
import sys, pytz, datetime as dt
import pandas as pd
from flask import render_template, flash, redirect, url_for, request
from app import app
from app.forms import (
    LoginForm, RegistrationForm, NewContractForm, AddItnForm, AddInvGroupForm,
    UploadInvGroupsForm, UploadContractsForm, UploadItnsForm)
from flask_login import current_user, login_user, logout_user, login_required
from app.models import *
 
from werkzeug.urls import url_parse
from app import db

from werkzeug.utils import secure_filename
from app.helper_functions import (get_contract_by_internal_id,
                                 convert_date_to_utc,
                                 validate_ciryllic,
                                 set_contarct_dates,

)




MONEY_ROUND = 2




@app.route('/')
@app.route('/index')
@login_required
def index():
    
    return render_template("index.html", title='Home Page')

@app.route('/login', methods=['GET', 'POST'])
def login():
	
    if current_user.is_authenticated:	   
        print('is_authenticated', file=sys.stdout)
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
        start_date_utc =  convert_date_to_utc("Europe/Sofia",form.start_date.data)
        end_date_utc =  convert_date_to_utc("Europe/Sofia",form.end_date.data) + dt.timedelta(hours = 23)

        print(convert_date_to_utc("Europe/Sofia",form.signing_date.data),file=sys.stdout)    
        current_conract = Contract(internal_id = form.internal_id.data, contractor_id = form.contractor_id.data, subject = form.subject.data, \
                        parent_id = form.parent_contract_internal_id.data, \
                        signing_date = signing_date_utc, \
                        start_date = start_date_utc , \
                        end_date = end_date_utc, \
                        duration_in_days = form.duration_in_days.data, \
                        invoicing_interval = form.invoicing_interval.data, maturity_interval = form.maturity_interval.data, \
                        contract_type_id = form.contract_type_id.data, is_work_days = form.is_work_days.data, \
                        automatic_renewal_interval = form.automatic_renewal_interval.data, collateral_warranty = form.collateral_warranty.data, \
                        notes = form.notes.data)
        print(current_conract,file=sys.stdout)
        current_conract.save()  
        # , price = round(form.price.data, MONEY_ROUND)   has_balancing = form.has_balancing.data           
        # db.session.add(current_conract)    
        # db.session.commit()            
        # print(current_conract,file=sys.stdout)
        # print(f'{form.internal_id.data}, {form.contractor_id.data}, {round(form.price.data, MONEY_ROUND)},\
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

    form.measuring_type_id.choices = [(c.id, c.code) for c in MeasuringType.query.order_by(MeasuringType.id)]
    form.internal_id.choices = [(c.id, (f'{c.internal_id}, {c.contractor.name}, {c.signing_date.date()}')) for c in Contract.query.order_by(Contract.internal_id)]                              
    form.erp_id.choices = [(c.id, c.name) for c in Erp.query.order_by(Erp.id)]
    form.grid_voltage.choices = [(x, x) for x in ['HV', 'MV', 'LV']]
    form.virtual_parent_id.choices = [(c.itn, c.itn) for c in ItnMeta.query.filter(ItnMeta.is_virtual == True).order_by(ItnMeta.itn)]
    form.virtual_parent_id.choices.insert(0,(None,None))
    form.invoice_group_name.choices = [(c.id, c.name) for c in InvoiceGroup.query.order_by(InvoiceGroup.name)]
    
    if form.validate_on_submit():
        
        activation_date_utc =  convert_date_to_utc("Europe/Sofia",form.activation_date.data)
        
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
            curr_itn_meta.update({'description': request.form['description'],'grid_voltage': request.form['grid_voltage'],
                                  'address': curr_address,'erp_id': request.form['erp_id'],'virtual_parent_itn': request.form['virtual_parent_id']})
            flash(f'ITN <{form.itn.data}> was successifuly updated !','success')
        
        #3. INVOICING GROUP
        curr_inv_group = InvoiceGroup.query.filter(InvoiceGroup.id == form.invoice_group_name.data).first()       
        
        #4 SUB CONTRACT
        curr_contract = Contract.query.filter(Contract.id == form.internal_id.data).first()
        curr_measuring_type = MeasuringType.query.filter(MeasuringType.id == form.measuring_type_id.data).first()
        
        curr_sub_contract = SubContract(itn = form.itn.data, \
                                contract_id = curr_contract.id, \
                                object_name = '',\
                                price = round(form.price.data, MONEY_ROUND), \
                                invoice_group_id = curr_inv_group.id, \
                                measuring_type_id = curr_measuring_type.id, \
                                start_date = curr_contract.start_date,\
                                end_date = curr_contract.end_date, \
                                zko = form.zko.data, \
                                akciz = form.akciz.data,
                                has_grid_services = form.has_grid_services.data, \
                                has_spot_price = form.has_spot_price.data, \
                                has_balancing = form.has_balancing.data)
        db_sub_contract = SubContract.query.filter((SubContract.itn == curr_sub_contract.itn) 
                                                    & (SubContract.start_date == curr_sub_contract.start_date) 
                                                    & (SubContract.end_date == curr_sub_contract.end_date)).first()
        if db_sub_contract is not None:
            flash('Duplicate sub contract','error')
        else:
            curr_sub_contract.save()  
            flash(f'ITN <{form.itn.data}> was successifuly created !','success')

    return render_template('add_itn.html', title='Add ITN', form=form)

@app.route('/add_invoicing_group', methods=['GET', 'POST'])
@login_required
def add_invoicing_group():

    form = AddInvGroupForm()
    form.internal_id.choices = [(c.id, (f'{c.internal_id}, {c.contractor.name}, {c.signing_date.date()}')) for c in Contract.query.order_by(Contract.internal_id)]    

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

    
    form = UploadItnsForm()
    if form.validate_on_submit():
        df = pd.read_excel(request.files.get('file_'))
        if all(elem in list(df.columns)  for elem in ['itn', 'activation_date', 'internal_id', 'measuring_type', 'invoice_group', 'price', 'zko', 
                                                      'akciz', 'has_grid_services', 'has_spot_price', 'erp', 'address', 'description', 'is_virtual', 'virtual_parent_itn', 'forecast_montly_consumption']):
            arr = []
            for index,row in df.iterrows():
                
                curr_contract = get_contract_by_internal_id(row['internal_id'])
                if curr_contract is None :
                    flash(f'Itn: {row.itn} has not got an contract !')
                elif curr_contract.start_date is None:
                    set_contarct_dates(curr_contract, row['activation_date'])
                else:
                    
                    flash('OK','success')


                   
                        
      
            
        else:
            flash('Upload failed','danger') 


    return render_template('upload_itns.html', title='Upload ITNs', form=form)


@app.route('/upload_contracts/<start>/<end>/', methods=['GET', 'POST'])
@login_required
def upload_contracts(start,end):

    
    form = UploadContractsForm()
    if form.validate_on_submit():
        
        df = pd.read_excel(request.files.get('file_'),usecols = 'D:M,O:Q')
        
        df = df.fillna(0)
        df = df[int(start):int(end)]
        if all(elem in list(df.columns)  for elem in ['parent_id', 'internal_id', 'contractor', 'sign_date', 'start_date',
                                                    'end_date', 'invoicing_interval', 'maturity_interval', 'contract_type',
                                                    'is_work_day', 'automatic_renewal_interval', 'collateral_warranty',
                                                    'notes']):

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
            df['duration_in_days'] = df.apply(lambda x: (x['end_date'] - x['start_date']).days, axis = 1)

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
                        signing_date =  convert_date_to_utc("Europe/Sofia",x[1]['sign_date'].strftime(t_format),t_format)  , \
                        start_date = convert_date_to_utc("Europe/Sofia", x[1]['start_date'].strftime(t_format),t_format), end_date = convert_date_to_utc("Europe/Sofia", x[1]['end_date'].strftime(t_format),t_format) , duration_in_days = x[1]['duration_in_days'], invoicing_interval = x[1]['invoicing_interval'], maturity_interval = x[1]['maturity_interval'], \
                        contract_type_id = x[1]['contract_type'], is_work_days = x[1]['is_work_day'], automatic_renewal_interval = x[1]['automatic_renewal_interval'], collateral_warranty = x[1]['collateral_warranty'], \
                        notes =  x[1]['notes']) for x in df.iterrows()]
            # start = time.time() 
            db.session.add_all(contracts)
            # db.session.bulk_save_objects(contracts)
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


@app.route('/table', methods=['GET', 'POST'])
@login_required
def table():
    return render_template('table.html', title='Table')







# def __get_contract_by_nternal_id__(internal_id):
#     return Contract.query.filter(Contract.internal_id == internal_id).first()




    