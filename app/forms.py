from flask_wtf import FlaskForm
from wtforms import (
    StringField, PasswordField, BooleanField, SubmitField, DateField, TextAreaField,IntegerField,
    SelectField, TextField, IntegerField, DecimalField, FileField, SelectMultipleField)
# from wtforms.fields.html5 import DateField
# from wtforms.fields import DateField
from wtforms.ext.sqlalchemy.fields import QuerySelectField, QuerySelectMultipleField
from wtforms.validators import ValidationError, DataRequired, Email, EqualTo, Length, Optional,NumberRange
from app.models import User, Contract, Contractor, MeasuringType, ItnMeta, InvoiceGroup, MeasuringType, TimeZone, Erp, Invoice,SubContract, Mail, ContractType
import re
import sys
import datetime as dt
import pandas as pd
import calendar

from app.helpers.helper_functions import convert_date_to_utc

class NonValidatingSelectMultipleField(SelectMultipleField):

    def pre_validate(self, form):
        pass

class NonValidatingSelectField(SelectField):

    def pre_validate(self, form):
        pass

class UploadItnsForm(FlaskForm):

    file_ = FileField('Browse')
    submit = SubmitField('Upload Itns')

class UploadContractsForm(FlaskForm):

    file_ = FileField('Browse')
    submit = SubmitField('Upload Contracts')

class UploadInvGroupsForm(FlaskForm):

    file_ = FileField('Browse')
    submit = SubmitField('Upload Invoice Group')


class AddInvGroupForm(FlaskForm):

    # date = StringField(id='start_datepicker', validators = [DataRequired()], default = dt.datetime.utcnow().replace(day = 1, month = int(dt.datetime.utcnow().month)-1 if dt.datetime.utcnow().month != 1 else 12))

    internal_id = SelectField('Contract Internal Number, Contractor, Signing Date', validators=[DataRequired()])
    invoice_group_name = StringField('Invoicing Group Name', validators=[DataRequired()]) 
    invoice_group_description = StringField('Invoicing Group Description', validators=[DataRequired()]) 
    invoice_group_emails = StringField('Invoicing Group Emails', validators=[DataRequired()])
    submit = SubmitField('Add New Invoice Group')
    

class AddItnForm(FlaskForm):
    itn = StringField('ITN', validators=[DataRequired()])
    activation_date = StringField(id='start_datepicker', validators = [DataRequired()])
    internal_id = SelectField('Contract Internal Number, Contractor, Signing Date', validators=[DataRequired()])
    measuring_type_id = SelectField('Measuring Type', validators=[DataRequired()])
    # measuring_type_id = QuerySelectField(query_factory = lambda: MeasuringType.query, allow_blank = False,get_label='code', validators=[DataRequired()])


    invoice_group_name = SelectField('Invoicing Group Name', validators=[DataRequired()])    
    price = DecimalField('Price',validators=[NumberRange(min = 0.01, max = 300),DataRequired()])
    zko = DecimalField('Zko',validators=[NumberRange(min = 0.01, max = 100),DataRequired()], default = 21.47)
    akciz = DecimalField('Akciz',validators=[NumberRange(min = 0.01, max = 100),DataRequired()],default = 2.00)    
    erp_id = SelectField('Erp', validators=[DataRequired()])
    grid_voltage = SelectField('Grid Voltage', validators=[DataRequired()], default = 'MV')
    address = StringField('Address', validators=[DataRequired()], default = 'None')
    description = StringField('Description', validators=[DataRequired()], default = 'None')    
    virtual_parent_id = SelectField('Virtual Parent ITN', validators=[Optional()])
    forecast_vol = DecimalField('Forecasted Monthly Consumption [MWh]',validators=[Optional()])
    file_ = FileField('Browse for hourly forcast schedule',validators=[Optional()])
    is_virtual = BooleanField('Is Virtual', default = False)
    has_grid_services = BooleanField('Include Grid Services', default = True)
    has_spot_price = BooleanField('Has Spot Price', default = False)
    has_balancing = BooleanField('Include Balancing Services', default = True)


    submit = SubmitField('Add ITN')

    def validate_itn(self, itn):
        if len(itn.data) > 33 | len(itn.data) < 16:
            raise ValidationError('Wrong ITN number of digits')




class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign In')

class RegistrationForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired()])
    password2 = PasswordField(
        'Repeat Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Register')

    def validate_username(self, username):
        user = User.query.filter_by(username=username.data).first()
        if user is not None:
            raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        user = User.query.filter_by(email=email.data).first()
        if user is not None:
            raise ValidationError('Please use a different email address.')

class NewContractForm(FlaskForm):
    
    internal_id = StringField('Internal Number', validators=[DataRequired()])
    contractor_id = SelectField('Contractor Name', validators=[DataRequired()])
    subject = TextField('Subject')
    parent_contract_internal_id = SelectField('Parent Contract Number')
    time_zone = QuerySelectField(query_factory = lambda: TimeZone.query.order_by(TimeZone.id), allow_blank = False, get_label='code', validators=[DataRequired()])
    signing_date = StringField(id='sign_datepicker', validators = [DataRequired()])
    start_date = StringField(id='start_datepicker', validators = [Optional()])
    end_date = StringField(id='end_datepicker', validators = [Optional()])
    duration_in_days = IntegerField('Duration of The Contract (in days)', validators=[NumberRange(min = 1, max = 3652)], default=365)    
    # price = DecimalField('Price',validators=[NumberRange(min = 0.01, max = 300)])
    invoicing_interval = IntegerField('Invoicing Interval (in days)', validators=[NumberRange(min = 1, max = 90)], default=31)
    maturity_interval = IntegerField('Maturity Interval (in days)', validators=[NumberRange(min = 1, max = 90)], default=5)
    contract_type_id =  SelectField('Contractor Type', validators=[DataRequired()])
    automatic_renewal_interval = IntegerField('Automatic Renewal Interval (in days)', validators=[NumberRange(min = 1, max = 900), Optional()], default=31)
    collateral_warranty = StringField('Collateral Warranty', validators=[Optional()])
    notes = TextField('Notes', validators=[Optional()])
    is_work_days = BooleanField('Working Days Only', default = False)
    # has_balancing = BooleanField('Include Balancing Services', default = True)
    
   
    submit = SubmitField('Add Contract')

    def validate_internal_id(self, internal_id):

        no_digit_internal_id = re.sub(r'[\d]', '', str(internal_id))
        if not bool(re.search('[\u0400-\u04FF]', no_digit_internal_id)):
            raise ValidationError('Use only cyrillic characters !')       

        contract_internal_id = Contract.query.filter_by(internal_id = internal_id.data).first()
        if contract_internal_id is not None:
            raise ValidationError('There is such a contract in the database !')


    def validate_contractor_id(self, contractor_id):
        contractor = Contractor.query.filter_by(id=contractor_id.data).first()
        
        if contractor is None:
            raise ValidationError('There is not such a contractor in the database !')

    def validate_subject(self, subject):
        if len(subject.data) > 128:
            raise ValidationError('Subject must be less than 128 characters')

    def validate_end_date(self, end_date):
        
        dt_end_obj = dt.datetime.strptime(end_date.data, '%Y-%m-%d')
        try:
            dt_start_obj = dt.datetime.strptime(self.start_date.data, '%Y-%m-%d')
            
        except:
            dt_start_obj = None
        else:

            if dt_start_obj > dt_end_obj:
                raise ValidationError('Start Date must be before End Date')            
            if (dt_start_obj + dt.timedelta(days = self.duration_in_days.data - 1) != dt_end_obj ):                
                raise ValidationError('Mismatch between Start Date, End Date and duration')

    def validate_collateral_warranty(self, collateral_warranty):

        if len(collateral_warranty.data) > 256:
            raise ValidationError('Collateral Warranty must be less than 256 characters')

    def validate_notes(self, notes):        
        if len(notes.data) > 512:
            raise ValidationError('Notes must be less than 512 characters')

class StpCoeffsForm(FlaskForm):

    start_date = StringField(id='start_datepicker', validators = [DataRequired()])
    end_date = StringField(id='end_datepicker', validators = [DataRequired()])
    file_ = FileField('Browse for stp coefficients file')

    submit = SubmitField('Add Stp Coeffs')

class CreateSubForm(FlaskForm):

    # itn = QuerySelectField(query_factory = lambda: ItnMeta.query, allow_blank = False,get_label='itn', validators=[DataRequired()])
    itn = StringField(id='itn', validators = [DataRequired()])
    contract_data = QuerySelectField(query_factory = lambda: Contract.query.join(Contractor, Contractor.id == Contract.contractor_id).order_by(Contractor.name), allow_blank = False,get_label=Contract.__str__  , validators=[DataRequired()])
    start_date = StringField(id='start_datepicker', validators = [DataRequired()])
    end_date = StringField(id='end_datepicker', validators = [DataRequired()])
    invoice_group = QuerySelectField(query_factory = lambda: InvoiceGroup.query, allow_blank = False,get_label=InvoiceGroup.__str__, validators=[DataRequired()])
    # price = DecimalField('Price',validators=[NumberRange(min = 0.01, max = 300),DataRequired()], default = 100)
    tariff_name = SelectField( 'Tariff Type',validators=[DataRequired()])
    single_tariff_price = DecimalField('Single Tariff Price',validators=[NumberRange(min = 0.01, max = 300),DataRequired()], default = 100)
    day_tariff_price = DecimalField('Day Tariff Price',validators=[NumberRange(min = 0.00, max = 300)], default = 0)
    night_tariff_price = DecimalField('Night Tariff Price',validators=[NumberRange(min = 0.00, max = 300)], default = 0)
    peak_tariff_price = DecimalField('Peak Tariff Price',validators=[NumberRange(min = 0.00, max = 300)], default = 0)
    object_name = StringField('Object Name', validators=[Optional()])
    measuring_type = QuerySelectField(query_factory = lambda: MeasuringType.query, allow_blank = False,get_label='code', validators=[DataRequired()])
    zko = DecimalField('Zko',validators=[NumberRange(min = 0.01, max = 100),DataRequired()], default = 21.47)
    akciz = DecimalField('Akciz',validators=[NumberRange(min = 0.01, max = 100),DataRequired()],default = 2.00)
    forecast_vol = DecimalField('Forecasted Monthly Consumption [MWh]',validators=[Optional()], default = 0)
    file_ = FileField('Browse for hourly forcast schedule',validators=[Optional()])
    
    has_grid_services = BooleanField('Include Grid Services', default = True)
    has_spot_price = BooleanField('Has Spot Price', default = False)
    has_balancing = BooleanField('Include Balancing Services', default = True)
    make_invoice = BooleanField('Make Invoice', default = True)

    submit = SubmitField('Create SubContract')

    def validate_end_date(self, end_date):

        dt_start_obj = dt.datetime.strptime(self.start_date.data, '%Y-%m-%d')
        dt_end_obj = dt.datetime.strptime(end_date.data, '%Y-%m-%d')
        if dt_start_obj > dt_end_obj:
                raise ValidationError('Start Date must be before End Date')


class EditSubForm(FlaskForm):

    contract_data = QuerySelectField(query_factory = lambda: Contract.query, allow_blank = False,get_label=Contract.__str__  , validators=[DataRequired()])
    itn = QuerySelectField(query_factory = lambda: ItnMeta.query, allow_blank = False,get_label='itn', validators=[DataRequired()])
    
    # start_date = StringField(id='start_datepicker', validators = [DataRequired()])
    # end_date = StringField(id='end_datepicker', validators = [DataRequired()])
    # invoice_group = QuerySelectField(query_factory = lambda: InvoiceGroup.query, allow_blank = False,get_label='name', validators=[DataRequired()])
    # price = DecimalField('Price',validators=[NumberRange(min = 0.01, max = 300),DataRequired()], default = 100)
    # object_name = StringField('Object Name', validators=[Optional()])
    # measuring_type = QuerySelectField(query_factory = lambda: MeasuringType.query, allow_blank = False,get_label='code', validators=[DataRequired()])
    # zko = DecimalField('Zko',validators=[NumberRange(min = 0.01, max = 100),DataRequired()], default = 21.47)
    # akciz = DecimalField('Akciz',validators=[NumberRange(min = 0.01, max = 100),DataRequired()],default = 2.00)
    # forecast_vol = DecimalField('Forecasted Monthly Consumption [MWh]',validators=[Optional()], default = 0)
    # file_ = FileField('Browse for hourly forcast schedule',validators=[Optional()])
    
    # has_grid_services = BooleanField('Include Grid Services', default = True)
    # has_spot_price = BooleanField('Has Spot Price', default = False)
    # has_balancing = BooleanField('Include Balancing Services', default = True)

    # submit = SubmitField('Create SubContract')

    # def validate_end_date(self, end_date):

    #     dt_start_obj = dt.datetime.strptime(self.start_date.data, '%Y-%m-%d')
    #     dt_end_obj = dt.datetime.strptime(end_date.data, '%Y-%m-%d')
    #     if dt_start_obj > dt_end_obj:
    #             raise ValidationError('Start Date must be before End Date')


class MonthlyReportForm(FlaskForm):
    
    search = StringField(id='search', validators = [Optional()])

    start_date = StringField(id='start_datepicker', validators = [DataRequired()], default = dt.datetime.utcnow().replace(year = int(dt.datetime.utcnow().year if int(dt.datetime.utcnow().month) != 1 
                                                    else int(dt.datetime.utcnow().year) - 1), day = 1, month = 12 if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))

    end_date = StringField(id='end_datepicker', validators = [DataRequired()],
                                                                            default = dt.datetime.utcnow().replace(year = int(dt.datetime.utcnow().year if int(dt.datetime.utcnow().month) != 1 
                                                                                else int(dt.datetime.utcnow().year) - 1), day = calendar.monthrange(dt.datetime.utcnow().year,
                                                                            int(dt.datetime.utcnow().month) if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1)[1],
                                                                            month = 12 if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))
                                                                            
    contracts = QuerySelectMultipleField(query_factory = lambda: Contract.query.join(Contractor).order_by(Contractor.name), 
        allow_blank = False,get_label=Contract.__str__, validators=[Optional()], render_kw={'size':15})
    # erp = QuerySelectField(query_factory = lambda: Erp.query, allow_blank = False,get_label='name', validators=[DataRequired()])
    # invoicing_group = QuerySelectField(query_factory = lambda: InvoiceGroup.query, allow_blank = False,get_label=InvoiceGroup.__str__, validators=[Optional()])
    # bulk_creation = BooleanField('Create invoice reference for all Invoice Groups', default = False)
    invoicing_group = QuerySelectMultipleField(query_factory = lambda: InvoiceGroup.query.join(Contractor).order_by(Contractor.name), allow_blank = False,get_label=InvoiceGroup.__str__, validators=[Optional()], render_kw={'size':15})
    # invoicing_group = QuerySelectMultipleField(query_factory = lambda: InvoiceGroup.query.join(Contractor).order_by(InvoiceGroup.name), allow_blank = False,get_label=InvoiceGroup.__str__, validators=[Optional()], render_kw={'size':15})
    ##by_inv_group = BooleanField('Create invoice reference by Invoice Group', default = True)
    
    ##by_contract = BooleanField('Create invoice reference by Contract', default = False)    
    
    submit = SubmitField('Create')
    ref_files = SelectMultipleField('Individual files',  validators=[Optional()], render_kw={'size':20})
    delete_all = BooleanField('Delete all source files', default = False)
    submit_delete = SubmitField('Delete files') 

    def validate_end_date(self, end_date):

        dt_start_obj = dt.datetime.strptime(self.start_date.data, '%Y-%m-%d')
        dt_end_obj = dt.datetime.strptime(end_date.data, '%Y-%m-%d')
        if dt_start_obj > dt_end_obj:
                raise ValidationError('Start Date must be before End Date')

class ErpForm(FlaskForm):
    file_cez = FileField('Browse for CEZ Zip File')
    file_epro = FileField('Browse for E_PRO Zip File')
    file_evn = FileField('Browse for EVN Zip File')
    file_nkji = FileField('Browse for NKJI Zip File')
    file_eso = FileField('Browse for ESO Zip File')
    delete_incoming_table = BooleanField('Delete incoming itns table', default = False)

    submit = SubmitField('Upload')



class UploadInitialForm(FlaskForm):

    
    file_erp = FileField('Browse for ERP File')
    file_measuring = FileField('Browse for Measuring Type File')
    file_contractors = FileField('Browse for Contracors CSV File')
    file_stp = FileField('Browse for Stp File')
    file_inv_group = FileField('Browse for Invoice Group File')
    file_hum_contractors = FileField('Browse for Humne Contractors File')
    file_hum_contracts = FileField('Browse for Humne Contracts File')
    file_hum_inv_groups = FileField('Browse for Humne Invoice Groups File')
    file_hum_itn = FileField('Browse for Humne Itn File')

    submit = SubmitField('Upload')


class IntegraForm(FlaskForm):
    
    delete_all = BooleanField('Delete all source files', default = False) 
    delete_integra = SubmitField('Delete Integra single files')
    integra_files = SelectMultipleField('Individual files',  validators=[Optional()], render_kw={'size':25})      
    concatenate_all = BooleanField('Concatenate all source files', default = False) 
    file_name = StringField( 'Integra file name',validators=[Optional()], default = format(dt.datetime.now(),'%d-%m-%Y %H:%M')+'.xlsx')
    submit = SubmitField('Create') 
    
       
    integra_upload_files = SelectMultipleField('Upload files',  validators=[Optional()], render_kw={'size':25})
    delete_all_upload = BooleanField('Delete all upload files', default = False) 
    delete_upload_integra = SubmitField('Delete Integra upload files') 

    # proba = SubmitField('Proba') 

class InvoiceForm(FlaskForm):

    file_integra_csv = FileField('Browse for Integra CSV File')
    upload_csv = SubmitField('Upload CSV')

    invoicing_list = SelectMultipleField('Select records for invoice creation',coerce=int, choices=[],  render_kw={'size':65})
    # invoicing_list = StringField('Select records for invoice creation',  validators=[Optional()], render_kw={'size':25})
    modify_invoice = SubmitField('Modify invoice')
    create_invoice = SubmitField('Create invoices')

class MailForm(FlaskForm):
    search = StringField(id='search', validators = [Optional()])
    from_number = StringField(id='from_number', validators = [Optional()])
    to_number = StringField(id='to_number', validators = [Optional()])
    send_all = BooleanField('Send all mails', default = False)
    subject = StringField(id='Subject', validators = [DataRequired()], default = 'From GED automated invoice sender')
    attachment_files = QuerySelectMultipleField(query_factory = lambda: Invoice.query.all(), allow_blank = False,get_label=Invoice.__str__, validators=[Optional()], render_kw={'size':45})  
    send_excel = BooleanField('Attach only excel file', default = False)
    send_pdf = BooleanField('Attach only pdf file', default = False)
    include_open_market = BooleanField('Include open market mail', default = True)
    submit = SubmitField('Send selected')

class TestForm(FlaskForm):
    start_date = StringField(id='start_datepicker', validators = [DataRequired()], default = dt.datetime.utcnow().replace(year = int(dt.datetime.utcnow().year if int(dt.datetime.utcnow().month) != 1 
                                                    else int(dt.datetime.utcnow().year) - 1), day = 1, month = 12 if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))

    end_date = StringField(id='end_datepicker', validators = [DataRequired()],
                                                                            default = dt.datetime.utcnow().replace(year = int(dt.datetime.utcnow().year if int(dt.datetime.utcnow().month) != 1 
                                                                                else int(dt.datetime.utcnow().year) - 1), day = calendar.monthrange(dt.datetime.utcnow().year,
                                                                            int(dt.datetime.utcnow().month) if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1)[1],
                                                                            month = 12 if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))

    contracts = QuerySelectMultipleField(id = 'contracts',query_factory = lambda: Contract.query.join(Contractor).order_by(Contractor.name).all(), allow_blank = False,get_label=Contract.__str__, validators=[Optional()], render_kw={'size':15})                                                                  
    # contracts = QuerySelectField(id = 'contracts',query_factory = lambda: Contract.query.join(Contractor).order_by(Contractor.name), allow_blank = False,get_label=Contract.__str__, validators=[DataRequired()], render_kw={'size':15})
    # modify_contract = SubmitField('Modify Contract')
    # # contracts = SelectField(id = 'contracts',choices = [], validators=[DataRequired()], render_kw={'size':15})
    # # send_all = BooleanField('Send all mails', default = False)
    # # attachment_files = QuerySelectMultipleField(query_factory = lambda: Invoice.query.all(), allow_blank = False,get_label=Invoice.__str__, validators=[Optional()], render_kw={'size':15})  
    # # input_f = StringField(id='input', validators = [Optional()], render_kw={"onchange": "test_proba()"})
    # invoice_groups = SelectMultipleField(id = 'invoice_groups',choices = [],validators=[Optional()], render_kw={'size':10})
    # modify_inv_group = SubmitField('Modify Invoicing Group')
    # itns = SelectMultipleField(id = 'itns',choices = [],validators=[Optional()])
    submit = SubmitField('Test', id='tt')

class ModifyForm(FlaskForm):
    # contract_tk =  StringField(id='contract_tk', validators=[Optional()], default = 'ТК') 
    # contract_search = StringField(id='contract_search', validators=[Optional()]) 
    search = StringField(id='search', validators = [Optional()])
    contracts = QuerySelectMultipleField(id = 'contracts',query_factory = lambda: Contract.query.join(Contractor).order_by(Contractor.name).all(), allow_blank = False,get_label=Contract.__str__, validators=[Optional()], render_kw={'size':15})
    modify_contract = SubmitField('Modify Contract',render_kw={'style': 'margin-bottom:30px ; font-size:150% ; width:400px'})
    invoice_groups = NonValidatingSelectMultipleField(id = 'invoice_groups',choices = [],validators=[Optional()], render_kw={'size':10})    
    modify_inv_group = SubmitField('Modify Invoicing Group',render_kw={'style': 'margin-bottom:30px ; font-size:150% ; width:400px', 'type':'button', 'onclick':'modifyInvGroup()'})
    itns = NonValidatingSelectMultipleField(id = 'itns',choices = [],validators=[Optional()])
    modify_itn = SubmitField('Modify ITN',render_kw={'style': 'margin-bottom:30px ; font-size:150% ; width:400px','type':'button', 'onclick':'modifyItn()'})

class ModifyInvGroupForm(FlaskForm):
    from_contractor = NonValidatingSelectField(id = 'from_contractor',choices = [],validators=[Optional()])
    from_suffix = IntegerField(id = 'from_suffix',validators=[NumberRange(min = 1, max = 999)])
    from_group = StringField(id = 'from_group')
    from_description = StringField(id = 'from_description')
    itns = NonValidatingSelectMultipleField(id = 'itns',choices = [],validators=[DataRequired()])
    to_contractor = NonValidatingSelectField(id = 'to_contractor',choices = [],validators=[Optional()])
    to_contract = NonValidatingSelectField(id = 'to_contract',choices = [],validators=[Optional()])
    # to_suffix = StringField(id = 'to_suffix',validators=[NumberRange(min = 1, max = 999)], default = 1)
    to_group = NonValidatingSelectField(id = 'to_group',choices = [])
    new_group = StringField(id = 'new_group',render_kw={'style':'display:none'},validators=[Optional()])
    to_description = StringField(id = 'to_description',validators=[Length(min = 1, max = 128, message='Select to_group first!')])
    to_email = StringField(id = 'to_email')

    def validate_new_group(self, new_group):        
        pattern = re.compile(r"^411-[\d]{1,3}-[\d]{1,5}_[\d]{1,3}$")
        result = pattern.match(new_group.data)
        print(f'new_group{new_group.data}')
        if result is None:
            raise ValidationError('Wrong invoicing group')
        to_contractor_ = Contractor.query.filter(Contractor.id == self.to_contractor.data).first()
        acc_411 = new_group.data.split('_')[0] 
        if to_contractor_.acc_411 != acc_411:
            raise ValidationError(f'Invoicing group:{new_group.data} does\'t belong to contractor: {to_contractor_.name} ! Гледай, бе шебек ! ')

    # def validate_to_description(self, to_description):

    #     if len(to_description.data) == 0:
    #         raise ValidationError('Select invoicing group!')    

class ModifyItn(FlaskForm):
    itn = StringField(id='Itn', validators = [Optional()], render_kw={'readonly': True})
    itn_addr = StringField('Address', validators=[Optional()])
    itn_descr = StringField('Description', validators=[Optional()])
    grid_voltage = SelectField(choices = ['HV','MV','LV'], validators = [DataRequired()])
    erp = SelectField(choices = ['CEZ','E-PRO','EVN'], validators = [DataRequired()])

class ModifySubcontractEntryForm(FlaskForm):
    search = StringField(id='search', validators = [Optional()])
    search_by_itn = StringField(id='search_by_itn', validators = [Optional()])

    start_date = StringField(id='start_datepicker', validators = [DataRequired()], default = dt.datetime.utcnow().replace(year = int(dt.datetime.utcnow().year if int(dt.datetime.utcnow().month) != 1 
                                                    else int(dt.datetime.utcnow().year) - 1), day = 1, month = 12 if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))

    end_date = StringField(id='end_datepicker', validators = [DataRequired()],
                                                                            default = dt.datetime.utcnow().replace(year = int(dt.datetime.utcnow().year if int(dt.datetime.utcnow().month) != 1 
                                                                                else int(dt.datetime.utcnow().year) - 1), day = calendar.monthrange(dt.datetime.utcnow().year,
                                                                            int(dt.datetime.utcnow().month) if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1)[1],
                                                                            month = 12 if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))

    contracts = QuerySelectMultipleField(id = 'contracts',query_factory = lambda: Contract.query.join(Contractor).order_by(Contractor.name).all(), allow_blank = False,get_label=Contract.__str__, validators=[Optional()], render_kw={'size':15})
    subcontracts = NonValidatingSelectMultipleField(id = 'subcontracts',choices = [],validators=[DataRequired()])
    # itns = NonValidatingSelectMultipleField(id = 'itns',choices = [],validators=[Optional()])
    has_grid = BooleanField('Has Grid')
    modify_subcontract = SubmitField('Modify Sub',render_kw={'style': 'margin-bottom:30px ; font-size:150% ; width:400px','type':'submit'})
    

class MonthlyReportErpForm(FlaskForm):
    
    bulk_creation = BooleanField('Create invoice reference for all Invoice Groups', default = False)
    
    invoicing_group = SelectMultipleField(choices = [], validators=[Optional()], render_kw={'size':15})
    
    by_inv_group = BooleanField('Create invoice reference by Invoice Group', default = True)
    contracts = SelectMultipleField(choices = [],validators=[Optional()], render_kw={'size':15})
    # contracts = QuerySelectField(query_factory = lambda: Contract.query.join(Contractor).order_by(Contractor.name), allow_blank = False,get_label=Contract.__str__, validators=[Optional()], render_kw={'size':15})
    by_contract = BooleanField('Create invoice reference by Contract', default = False)   
    submit = SubmitField('Create')
    ref_files = SelectMultipleField('Individual files',  validators=[Optional()], render_kw={'size':20})
    delete_all = BooleanField('Delete all source files', default = False)
    submit_delete = SubmitField('Delete files') 

    def validate_end_date(self, end_date):

        dt_start_obj = dt.datetime.strptime(self.start_date.data, '%Y-%m-%d')
        dt_end_obj = dt.datetime.strptime(end_date.data, '%Y-%m-%d')
        if dt_start_obj > dt_end_obj:
                raise ValidationError('Start Date must be before End Date')

class MonthlyReportOptionsForm(FlaskForm):
    start_date = StringField(id='start_datepicker', validators = [DataRequired()], default = dt.datetime.utcnow().replace(year = int(dt.datetime.utcnow().year if int(dt.datetime.utcnow().month) != 1 
                                                    else int(dt.datetime.utcnow().year) - 1), day = 1, month = 12 if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))

    end_date = StringField(id='end_datepicker', validators = [DataRequired()],
                                                                            default = dt.datetime.utcnow().replace(year = int(dt.datetime.utcnow().year if int(dt.datetime.utcnow().month) != 1 
                                                                                else int(dt.datetime.utcnow().year) - 1), day = calendar.monthrange(dt.datetime.utcnow().year,
                                                                            int(dt.datetime.utcnow().month) if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1)[1],
                                                                            month = 12 if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))

    contract_type = SelectField(choices = ['Mass_Market','End_User','Procurement','All'], validators = [DataRequired()])
    erp = SelectField(choices = ['CEZ','E-PRO','EVN'], validators = [DataRequired()])
    include_all = BooleanField('Include invoice groups with ITN from different ERP', default = False)
    
    submit = SubmitField('Apply filters')

class AdditionalReports(FlaskForm):

    start_date = StringField(id='start_datepicker', validators = [DataRequired()], default = dt.datetime.utcnow().replace(day = 1, month = int(dt.datetime.utcnow().month) if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))
    end_date = StringField(id='end_datepicker', validators = [DataRequired()], 
                                                                            default = dt.datetime.utcnow().replace(day = calendar.monthrange(dt.datetime.utcnow().year, 
                                                                            int(dt.datetime.utcnow().month) if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1)[1], 
                                                                            month = int(dt.datetime.utcnow().month) if int(dt.datetime.utcnow().month) == 1 else int(dt.datetime.utcnow().month) - 1))
    bulk_creation = BooleanField('Select all files', default = False)
    ref_files = SelectMultipleField('Individual files',  validators=[Optional()], render_kw={'size':20})
    submit = SubmitField('Generate Full Report')
    reports_files = SelectMultipleField('Report files',  validators=[Optional()], render_kw={'size':20})
    delete_all = BooleanField('Delete all report files', default = False)
    submit_delete = SubmitField('Delete files')

class PostForm(FlaskForm):

    file_easypay_csv = FileField('Browse for EasyPay CSV File')
    upload_csv = SubmitField('Upload CSV')

    # invoicing_list = SelectMultipleField('Select records for invoice creation',coerce=int, choices=[],  render_kw={'size':25})
    # # invoicing_list = StringField('Select records for invoice creation',  validators=[Optional()], render_kw={'size':25})
    # create_invoice = SubmitField('Create invoices')

# class RedactEmailForm(FlaskForm):   

#     # def __init__(self, *args, **kwargs):
#     #     super().__init__(*args, **kwargs)
#     #     self.contract_type_id = contract_type_id
        

#     # inv_goups_mails = QuerySelectField(query_factory = lambda: InvoiceGroup.query.join(Mail, Mail.id == InvoiceGroup.email_id).order_by(InvoiceGroup.description).all(), allow_blank = False,get_label=InvoiceGroup.__str__, validators=[Optional()], render_kw={'size':65})
#     # inv_goups_mails = QuerySelectField(query_factory = lambda: InvoiceGroup.query
#     #                                         .join(Mail, Mail.id == InvoiceGroup.email_id)
#     #                                         .join(Contractor,Contractor.id == InvoiceGroup.contractor_id)
#     #                                         .join(Contract,Contract.contractor_id == Contractor.id)
#     #                                         .join(ContractType, ContractType.id == Contract.contract_type_id)
#     #                                         .filter(ContractType.id == self.contract_type_id)
#     #                                         .order_by(InvoiceGroup.description)
#     #                                         .all(), allow_blank = False,get_label=InvoiceGroup.__rep_for_mails__, validators=[Optional()], render_kw={'size':65})
#     # inv_goups_mails = SelectField( 'Available contractors/emails',validators=[DataRequired()])
#     # inv_goups_mails = QuerySelectField('trans_id', validators=[DataRequired()], get_label='name')
#     new_mail = StringField(id='New Email', validators = [DataRequired()], default = '')

#     submit = SubmitField('Apply changes')

class RedactContractForm(FlaskForm):

    
    contracts = QuerySelectField(query_factory = lambda: Contract.query.join(Contractor).order_by(Contractor.name), allow_blank = False,get_label=Contract.__str__, validators=[Optional()], render_kw={'size':25})
    submit = SubmitField('Select contract')

class RedactContractorForm(FlaskForm):

    search = StringField(id='search', validators = [Optional()])
    contractors = QuerySelectMultipleField(query_factory = lambda: Contractor.query.order_by(Contractor.name), allow_blank = False,get_label=Contractor.__str__, validators=[Optional()], render_kw={'size':25})
    submit_btn = SubmitField('Select contractor')

class ContarctDataForm(FlaskForm):

    internal_id = StringField(id='internal_id', validators = [DataRequired()], render_kw={'readonly': True})
    contractor = SelectField(choices = [], coerce=int, validators = [Optional()])
    subject = TextAreaField(id='subject', validators = [Optional()])
    parent_contract = SelectField(choices = [], coerce=str, validators = [Optional()])
    end_date = StringField(id='end_datepicker', validators = [DataRequired()])
    contract_type = SelectField(choices = [], coerce=str, validators = [DataRequired()])
    delete_subs = BooleanField('Delete selected subcontracts', default = False)
    subs = SelectMultipleField(choices = [], coerce=str, validators=[Optional()], render_kw={'size':25})
    delete_contract = BooleanField('Delete contract', default = False)

    def validate_end_date(self, end_date):
        
        # dt_end_obj = dt.datetime.strptime(end_date.data, '%Y-%m-%d')
        curr_contract = Contract.query.filter(Contract.internal_id == self.internal_id.data).first()
        end_date = convert_date_to_utc('EET',end_date.data)
        end_date = end_date + dt.timedelta(hours = 23)
        print(f'validate_end_date {end_date} -- {curr_contract.end_date}')
        if end_date > curr_contract.end_date:
            print(f'in validation')
            raise ValidationError('Modified end date MUST be before current end date !')
    
    # submit_sub_del = SubmitField('Delete subcontracts')

class ContarctorDataForm(FlaskForm):

    parent_contractor = SelectField(choices = [], coerce=str, validators = [Optional()])
    # names = SelectField(choices = [], coerce=str, validators = [DataRequired()])
    name = StringField(id='name', validators = [DataRequired()])
    eic = StringField(id='eic', validators = [DataRequired()])
    address = StringField(id='address', validators = [DataRequired()])
    vat_number = StringField(id='vat_number', validators = [DataRequired()])
    email = StringField(id='email', validators = [DataRequired()])
    acc_411 = StringField(id='acc_411', render_kw={'readonly': True})
    
    # submit = SubmitField('Apply changes', render_kw={"onclick": "modify_contractor_on_click()"})
    # submit = SubmitField('Apply changes')

class EmailsOptionsForm(FlaskForm):
    
    contract_type = SelectField(choices = ['Mass_Market','End_User','Procurement','All'], validators = [DataRequired()])
    
    submit = SubmitField('Apply filters')

class ItnCosumptionDeletion(FlaskForm):

    start_date = StringField(id='start_datepicker', validators = [DataRequired()], default = dt.datetime.utcnow().replace(day = 1, month = int(dt.datetime.utcnow().month)-1 if dt.datetime.utcnow().month != 1 else 12))
    itn = StringField('ITN', validators=[DataRequired()])
    submit = SubmitField('Apply changes')



class RedactEmailForm(FlaskForm):   

    search = StringField(id='email_search', validators = [Optional()])
    inv_goups_mails = QuerySelectMultipleField(query_factory = lambda: InvoiceGroup.query.join(Mail, Mail.id == InvoiceGroup.email_id).order_by(InvoiceGroup.description).all(), allow_blank = False,get_label=InvoiceGroup.__str__, validators=[Optional()], render_kw={'size':25})
    new_mail = StringField(id='new_mail', validators = [DataRequired()], default = '')

    # submit = SubmitField('Apply changes')   

class ModifyInvoiceForm(FlaskForm):

    invoice_num = StringField(id='invoice_num', validators = [DataRequired()])
    contractor_name = StringField(id='contractor_name', validators = [DataRequired()])
    bulstat = StringField(id='bulstat', validators = [DataRequired()])
    vat_number = StringField(id='vat_number', validators = [DataRequired()])
    address = StringField(id='address', validators = [DataRequired()])

    electricity_qty = DecimalField('electricity_qty',validators=[DataRequired()])
    electricity_price = DecimalField('electricity_price',validators=[DataRequired()])
    electricity_sum = DecimalField('electricity_sum',validators=[DataRequired()])
    zko_price = DecimalField('zko_price',validators=[DataRequired()])
    zko_sum = DecimalField('zko_sum',validators=[DataRequired()])
    akciz_price = DecimalField('akciz_price',validators=[DataRequired()])
    akciz_sum = DecimalField('akciz_sum',validators=[DataRequired()])
    grid_sum = DecimalField('grid_sum',validators=[Optional()])

    sum_neto = DecimalField('sum_neto',validators=[DataRequired()])
    vat_percentage = DecimalField('vat_percentage',validators=[DataRequired()], default = 20)
    sum_vat = DecimalField('sum_vat',validators=[DataRequired()])
    sum_total = DecimalField('sum_total',validators=[DataRequired()])

    pay_date = StringField(id='pay_date', validators = [DataRequired()])
    excel_ref_name = StringField(id='excel_ref_name', validators = [DataRequired()])

    submit = SubmitField('Apply changes')




    

    
