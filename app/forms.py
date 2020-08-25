from flask_wtf import FlaskForm
from wtforms import (
    StringField, PasswordField, BooleanField, SubmitField, DateField,
    SelectField, TextField, IntegerField, DecimalField, FileField)
# from wtforms.fields.html5 import DateField
# from wtforms.fields import DateField
from wtforms.ext.sqlalchemy.fields import QuerySelectField
from wtforms.validators import ValidationError, DataRequired, Email, EqualTo, Length, Optional,NumberRange
from app.models import User, Contract, Contractor, MeasuringType
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
    signing_date = StringField(id='sign_datepicker', validators = [DataRequired()])
    start_date = StringField(id='start_datepicker', validators = [DataRequired()])
    end_date = StringField(id='end_datepicker', validators = [DataRequired()])
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
                raise ValidationError('Start date must be before End date')            
            if (dt_start_obj + dt.timedelta(days = self.duration_in_days.data - 1) != dt_end_obj ):                
                raise ValidationError('Mismatch between start date, end date and duration')

    def validate_collateral_warranty(self, collateral_warranty):

        if len(collateral_warranty.data) > 256:
            raise ValidationError('Collateral Warranty must be less than 256 characters')

    def validate_notes(self, notes):        
        if len(notes.data) > 512:
            raise ValidationError('Notes must be less than 512 characters')
    



       
            
    

    









# class NewContractForm(FlaskForm):
    
#     internal_number = StringField('InternalNumber', validators=[DataRequired()])
#     contractor_name = SelectField('ContractorName', validators=[DataRequired()])

#     def __init__(self, *args, **kwargs):
#         super(NewContractForm, self).__init__(*args, **kwargs)
#         self.contractor_name.choices = [(c.id, c.name) for c in Contractor.query.order_by(Contractor.name)]

#     submit = SubmitField('AddContract')

#     def validate_contractor(self, name):
#         contractor = Contractor.query.filter_by(name=name.data).first()
#         if contractor is None:
#             raise ValidationError('There is not such a contractor in database !')
