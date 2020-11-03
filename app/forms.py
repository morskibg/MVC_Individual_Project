from flask_wtf import FlaskForm
from wtforms import (
    StringField, PasswordField, BooleanField, SubmitField, DateField,
    SelectField, TextField, IntegerField, DecimalField, FileField, SelectMultipleField)
# from wtforms.fields.html5 import DateField
# from wtforms.fields import DateField
from wtforms.ext.sqlalchemy.fields import QuerySelectField, QuerySelectMultipleField
from wtforms.validators import ValidationError, DataRequired, Email, EqualTo, Length, Optional,NumberRange
from app.models import User, Contract, Contractor, MeasuringType, ItnMeta, InvoiceGroup, MeasuringType, TimeZone, Erp
import re
import sys
import datetime as dt
import pandas as pd




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

    internal_id = SelectField('Contract Internal Number, Contractor, Signing Date', validators=[DataRequired()])
    invoice_group_name =   StringField('Invoicing Group Name', validators=[DataRequired()]) 
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

    itn = QuerySelectField(query_factory = lambda: ItnMeta.query, allow_blank = False,get_label='itn', validators=[DataRequired()])
    contract_data = QuerySelectField(query_factory = lambda: Contract.query, allow_blank = False,get_label=Contract.__str__  , validators=[DataRequired()])
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


class TestForm(FlaskForm):
    
    start_date = StringField(id='start_datepicker', validators = [DataRequired()])
    end_date = StringField(id='end_datepicker', validators = [DataRequired()])
    
    # erp = QuerySelectField(query_factory = lambda: Erp.query, allow_blank = False,get_label='name', validators=[DataRequired()])
    # invoicing_group = QuerySelectField(query_factory = lambda: InvoiceGroup.query, allow_blank = False,get_label=InvoiceGroup.__str__, validators=[Optional()])
    bulk_creation = BooleanField('Create invoice reference for all Invoice Groups', default = False)
    invoicing_group = QuerySelectMultipleField(query_factory = lambda: InvoiceGroup.query.join(Contractor).order_by(Contractor.name), allow_blank = False,get_label=InvoiceGroup.__str__, validators=[Optional()], render_kw={'size':15})
    
    by_inv_group = BooleanField('Create invoice reference by Invoice Group', default = True)
    contracts = QuerySelectField(query_factory = lambda: Contract.query.join(Contractor).order_by(Contractor.name), allow_blank = False,get_label=Contract.__str__, validators=[Optional()], render_kw={'size':15})
    by_contract = BooleanField('Create invoice reference by Contract', default = False)

    def validate_end_date(self, end_date):

        dt_start_obj = dt.datetime.strptime(self.start_date.data, '%Y-%m-%d')
        dt_end_obj = dt.datetime.strptime(end_date.data, '%Y-%m-%d')
        if dt_start_obj > dt_end_obj:
                raise ValidationError('Start Date must be before End Date')

    
    
    submit = SubmitField('Create')
    ref_files = SelectMultipleField('Individual files',  validators=[Optional()], render_kw={'size':20})
    delete_all = BooleanField('Delete all source files', default = False)
    submit_delete = SubmitField('Delete files') 

class ErpForm(FlaskForm):
    file_cez = FileField('Browse for CEZ Zip File')
    file_epro = FileField('Browse for E_PRO Zip File')
    file_evn = FileField('Browse for EVN Zip File')
    file_nkji = FileField('Browse for NKJI Zip File')

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

    proba = SubmitField('Proba') 

class InvoiceForm(FlaskForm):

    file_integra_csv = FileField('Browse for Integra CSV File')
    upload_csv = SubmitField('Upload CSV')

    invoicing_list = SelectMultipleField('Select records for invoice creation',coerce=int, choices=[],  render_kw={'size':25})
    # invoicing_list = StringField('Select records for invoice creation',  validators=[Optional()], render_kw={'size':25})
    create_invoice = SubmitField('Create invoices')



            
    

    
