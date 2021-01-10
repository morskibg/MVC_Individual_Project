import sys
import datetime as dt
import pandas as pd
import numpy as np
from decimal import Decimal 
from flask import g, flash
from app import db, ma
from sqlalchemy.sql import func
import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin
from app import login
from app import app

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, auto_field, fields

import simplejson


class BaseModel(db.Model):
    __abstract__ = True
    def save(self):
        try:
            if self not in db.session:
                db.session.add(self)
            db.session.commit()
        except:
            flash(f'Error on saving {self} in database. Aborting !','danger')
            print(f'Error on saving {self} in database. Aborting !')
        else: 
            print(f'{self} commited to db')

    def update(self, data: dict):
        for field, value in data.items():
            setattr(self, field, value)
        try:
            self.save()
        except:
            flash(f'Error on updating {self} . Aborting !','danger')
            print(f'Error on updating {self} . Aborting !')
        else:
            print(f'updated successifuly')

    def delete(self):
        try:
            db.session.delete(self)
            db.session.commit()
        except:
            flash(f'Error on deleting {self} . Aborting !','danger')
            print(f'Error on deleting {self} . Aborting !')
        else:
            print(f'{self} delete successifuly')

class User(UserMixin,BaseModel):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True)
    email = db.Column(db.String(120), index=True, unique=True)
    password_hash = db.Column(db.String(128))

    def __repr__(self):
        return '<User {}>'.format(self.username)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

@login.user_loader
def load_user(id):
    return User.query.get(int(id))

class Contractor(BaseModel):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    parent_id = db.Column(db.Integer, nullable=True)
    name = db.Column(db.String(128), nullable = False)
    eic = db.Column(db.String(32), nullable = False)
    address = db.Column(db.String(128), nullable=True)
    vat_number = db.Column(db.String(32), nullable=True)
    email = db.Column(db.String(128), nullable=True)
    acc_411 = db.Column(db.String(16), unique=True, nullable = False)
    last_updated = db.Column(db.DateTime, default = dt.datetime.utcnow, onupdate=dt.datetime.utcnow)

    contracts = db.relationship("Contract", back_populates="contractor", lazy="dynamic")
    invoice_groups = db.relationship("InvoiceGroup", back_populates="contractor", lazy="dynamic")
    

    def __repr__(self):
        return '<Contractor {} - {}>'.format(self.name, self.acc_411)  

    def __str__(self):
        return f'{self.name} - {self.acc_411}'
    
class TimeZone(BaseModel):
    id = db.Column(db.SmallInteger, primary_key=True, autoincrement=True)
    code = db.Column(db.String(32), unique=True, nullable = False)

    contracts = db.relationship("Contract", back_populates="time_zone", lazy="dynamic")   

    def __repr__(self):
        return f'<Time Zone: {self.code}'

class Contract(BaseModel):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    internal_id = db.Column(db.String(32), unique=True, nullable = False)
    contractor_id = db.Column(db.Integer, db.ForeignKey('contractor.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    subject = db.Column(db.String(128), nullable=True)
    parent_id = db.Column(db.Integer, nullable=True)
    time_zone_id = db.Column(db.SmallInteger, db.ForeignKey('time_zone.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    signing_date = db.Column(db.DateTime, nullable=False)
    start_date = db.Column(db.DateTime, index=True, nullable=True)
    end_date = db.Column(db.DateTime, index=True, nullable=True)
    duration_in_days = db.Column(db.SmallInteger, nullable=False)    
    invoicing_interval = db.Column(db.SmallInteger,nullable=False)
    maturity_interval = db.Column(db.SmallInteger,nullable=False)
    contract_type_id = db.Column(db.SmallInteger, db.ForeignKey('contract_type.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    is_work_days = db.Column(db.Boolean, nullable=False)    
    automatic_renewal_interval = db.Column(db.SmallInteger, nullable=True)
    collateral_warranty = db.Column(db.String(256), nullable=True)
    notes = db.Column(db.String(512), nullable=True)
    last_updated = db.Column(db.DateTime, default = dt.datetime.utcnow, onupdate=dt.datetime.utcnow)
    invoicing_label = db.Column(db.String(32), unique=False, nullable = True)

    contractor = db.relationship('Contractor', back_populates = 'contracts')
    contract_type = db.relationship('ContractType', back_populates = 'contracts')
    sub_contracts = db.relationship("SubContract", back_populates="contract", lazy="dynamic",cascade="all, delete")
    time_zone = db.relationship("TimeZone", back_populates="contracts")

    def __inv_label_name__(self):
        return self.invoicing_label
    
    def __str__(self):
        from app.helpers.helper_functions import convert_date_from_utc
        start_date_eet = convert_date_from_utc('EET',self.start_date)
        end_date_eet = convert_date_from_utc('EET',self.end_date)
        return f'{self.internal_id} - {self.contractor.name} - {start_date_eet} - {end_date_eet}'

    def __repr__(self):
        return '<Contract :internal_id - {},  contractor_id - {}, {}, {}, {}, {}, {}, {}, {}, {}, {}>'\
        .format(self.internal_id, self.contractor_id, self.subject, self.parent_id, self.signing_date, 
        self.start_date, self.end_date, self.invoicing_interval, self.maturity_interval, 
        self.contract_type_id, self.is_work_days)   

class ContractType(BaseModel):
    id = db.Column(db.SmallInteger, primary_key=True, autoincrement=True)
    name = db.Column(db.String(16), nullable=False)

    contracts = db.relationship("Contract", back_populates="contract_type", lazy="dynamic")

    def __repr__(self):
        return '<ContractType : {}>'.format(self.name)

class ItnMeta(BaseModel):
    itn = db.Column(db.String(33), primary_key = True)
    description = db.Column(db.String(128), nullable = True)
    grid_voltage = db.Column(db.String(128), nullable=False)
    address_id = db.Column(db.SmallInteger, db.ForeignKey('address_murs.id', ondelete='CASCADE',onupdate = 'CASCADE'), nullable=False)
    erp_id = db.Column(db.SmallInteger, db.ForeignKey('erp.id', ondelete='CASCADE'), nullable=False)
    is_virtual = db.Column(db.Boolean, nullable = False, default = 0)
    virtual_parent_itn = db.Column(db.String(33),nullable = True)
    last_updated = db.Column(db.DateTime, default = dt.datetime.utcnow, onupdate=dt.datetime.utcnow)

    address = db.relationship('AddressMurs', back_populates = 'itns')
    erp = db.relationship('Erp', back_populates = 'itns')
    sub_contracts = db.relationship("SubContract", back_populates="meta", lazy="dynamic",cascade="all, delete")
    leaving_itns = db.relationship('LeavingItn', back_populates = 'meta', lazy="dynamic",cascade="all, delete")
    itn_schedules = db.relationship('ItnSchedule', back_populates = 'meta', lazy="dynamic",cascade="all, delete")
    

    def __repr__(self):
        return '<ItnMeta : {}, {}, {}, {}, {}, {}, {}>'.format(self.itn,self.description,  self.grid_voltage,self.address_id,self.erp_id,self.is_virtual,self.virtual_parent_itn)

class AddressMurs(BaseModel):
    id = db.Column(db.SmallInteger, primary_key=True, autoincrement=True)
    name = db.Column(db.String(128), nullable=False, unique = True)

    itns = db.relationship("ItnMeta", back_populates="address", lazy="dynamic")

    def __repr__(self):
        return '<id:{},name:{}>'.format(self.id, self.name)
        # return  {'id':self.id, 'name':self.name}

class Erp(BaseModel):
    id = db.Column(db.SmallInteger, primary_key=True, autoincrement=True)
    name = db.Column(db.String(8), nullable=False)
    code = db.Column(db.String(16), nullable=False)

    itns = db.relationship("ItnMeta", back_populates="erp", lazy="dynamic")    
    
    def __repr__(self):
        return '<ERP Name: {}, ERP Code: {}>'.format(self.name, self.code)

class ErpInvoice(BaseModel):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    number = db.Column(db.String(64), nullable=False, unique = False)  
    date = db.Column(db.DateTime, nullable=False)
    event = db.Column(db.String(128), nullable = True)
    correction_note = db.Column(db.String(64), nullable = True)
    composite_key = db.Column(db.String(256), nullable=False, unique = True)

    distributions = db.relationship("Distribution", back_populates="invoice", lazy="dynamic")
    tech = db.relationship("Tech", back_populates="invoice", lazy="dynamic")

    def __repr__(self):
        return '<ERP Invoice Date: {}, ERP Invoice Number: {}>'.format(self.date, self.number)

class Distribution(BaseModel):
    itn = db.Column(db.String(33), db.ForeignKey('itn_meta.itn', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True)
    start_date = db.Column(db.DateTime, primary_key = True)
    end_date = db.Column(db.DateTime, nullable=False)
    tariff= db.Column(db.String(256), nullable = False)
    calc_amount = db.Column(db.Numeric(12,6),nullable = False)
    price = db.Column(db.Numeric(10,6), primary_key = True)
    value = db.Column(db.Numeric(10,3), primary_key = True) 
    erp_invoice_id = db.Column(db.Integer, db.ForeignKey('erp_invoice.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False, primary_key = True)
    

    invoice = db.relationship("ErpInvoice", back_populates="distributions")

    def __repr__(self):
        return '<Distribution ITN: {}, Distribution tariff: {}, Distribution calc_amount: {}, Distribution value: {}, Distribution invoice_date: {}>'.format(self.itn, self.tariff, self.calc_amount,self.value, self.invoice.date)

class DistributionTemp(BaseModel):
    itn = db.Column(nullable=False)
    start_date = db.Column(db.DateTime, primary_key = True)
    end_date = db.Column(db.DateTime, nullable=False)
    tariff= db.Column(db.String(256), nullable = False)
    calc_amount = db.Column(db.Numeric(12,6),nullable = False)
    price = db.Column(db.Numeric(10,6), primary_key = True)
    value = db.Column(db.Numeric(10,3), primary_key = True) 
    correction_note = db.Column(db.String(64), primary_key = True)
    event = db.Column(db.String(128))
    number = db.Column(db.String(64))
    date = db.Column(db.DateTime, nullable=False)  

    
    def __repr__(self):
        return '<Distribution ITN: {}, Distribution tariff: {}, Distribution calc_amount: {}, Distribution value: {}, Distribution invoice_date: {}>'.format(self.itn, self.tariff, self.calc_amount,self.value, self.date)

class Tech(BaseModel):
    subscriber_number = db.Column(db.String(64),nullable = False)
    # place_number = db.Column(db.String(16),nullable = False)
    customer_number = db.Column(db.String(16),nullable = False)
    itn = db.Column(db.String(33), db.ForeignKey('itn_meta.itn', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True)
    electric_meter_number = db.Column(db.String(32),nullable = False)
    start_date = db.Column(db.DateTime, primary_key = True)
    end_date = db.Column(db.DateTime, nullable=False)
    scale_number = db.Column(db.SmallInteger,nullable = False)
    scale_code = db.Column(db.String(16), primary_key = True, nullable = False)
    scale_type = db.Column(db.String(8),nullable = False)
    time_zone = db.Column(db.String(8),nullable = False)
    new_readings = db.Column(db.Numeric(10,3), primary_key = True)
    old_readings = db.Column(db.Numeric(10,3),nullable = False)
    readings_difference = db.Column(db.Numeric(10,3),nullable = False)
    constant = db.Column(db.SmallInteger,nullable = False)
    correction = db.Column(db.Integer,primary_key = True)
    storno = db.Column(db.Numeric(10,3),nullable = False)
    total_amount = db.Column(db.Numeric(10,3),nullable = False)
    erp_invoice_id = db.Column(db.Integer, db.ForeignKey('erp_invoice.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False, primary_key = True)

    invoice = db.relationship("ErpInvoice", back_populates="tech")


    def __repr__(self):
        return '<Tech ITN: {}, Tech total_amount: {}, Tech invoice_date: {}>'.format(self.itn,  self.total_amount,self.invoice.number)

class MeasuringType(BaseModel):
    id = db.Column(db.SmallInteger, primary_key=True, autoincrement=True)
    code = db.Column(db.String(16), nullable=False)
    
    sub_contracts = db.relationship("SubContract", back_populates="measuring_type", lazy="dynamic")
    stp_coeffs = db.relationship("StpCoeffs", back_populates="measuring_type", lazy="dynamic")

    def __repr__(self):
        return f'<MeasuringType: id={self.id}_code={self.code}>'

    def __str__(self):
        return f'{self.code}'    

class ForecastType(BaseModel):
    id = db.Column(db.SmallInteger, primary_key=True, autoincrement=True)
    code = db.Column(db.String(16), nullable=False)
    
    sub_contracts = db.relationship("SubContract", back_populates="forecast_type", lazy="dynamic")
    forecast_coeffs = db.relationship("ForecastCoeffs", back_populates="forecast_type", lazy="dynamic")

    def __repr__(self):
        return f'<ForecastType: id={self.id}_code={self.code}>'
    def __str__(self):
        return f'{self.code}'

class InvoiceGroup(BaseModel):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(32), nullable=False, unique = True)
    contractor_id = db.Column(db.Integer, db.ForeignKey('contractor.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    description = db.Column(db.String(128), nullable=True, unique = False)
    email_id = db.Column(db.Integer, db.ForeignKey('mail.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=True)

    contractor = db.relationship('Contractor', back_populates = 'invoice_groups')
    sub_contracts = db.relationship("SubContract", back_populates="invoice_group", lazy="dynamic")

    def __rep_for_mails__(self):

        from app.helpers.helper_function_excel_writer import generate_num_and_name
        email = Mail.query.filter(Mail.id == self.email_id).first().name
        first_digit = app.config['EPAY_NUM_FIRST_DIGIT']
        curr_contractor = Contractor.query.filter(Contractor.id == self.contractor_id).first()
        acc_411 = curr_contractor.acc_411
        name = curr_contractor.name
        epay_num, epay_name = generate_num_and_name(first_digit, acc_411, self.name, name)
        return f'{epay_num} - {epay_name} - {self.name} - {email}'

    def __str__(self):

        email = Mail.query.filter(Mail.id == self.email_id).first().name
        return f'{self.name} - {self.description} - {email}'

    def __repr__(self):
        return '<InvoiceGroup:{}, {}, Contractor_id {}>'.format(self.id, self.name, self.contractor_id)

class SubContract(BaseModel):
    itn = db.Column(db.String(33), db.ForeignKey('itn_meta.itn', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True)
    contract_id = db.Column(db.Integer, db.ForeignKey('contract.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    object_name = db.Column(db.String(64), nullable = True)
    # price = db.Column(db.Numeric(8,7), nullable=False)
    invoice_group_id = db.Column(db.Integer, db.ForeignKey('invoice_group.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    measuring_type_id = db.Column(db.SmallInteger, db.ForeignKey('measuring_type.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    forecast_type_id = db.Column(db.SmallInteger, db.ForeignKey('forecast_type.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=True)
    start_date = db.Column(db.DateTime, primary_key = True)
    end_date = db.Column(db.DateTime, primary_key = True)    
    zko = db.Column(db.Numeric(8,7), nullable=False)
    akciz = db.Column(db.Numeric(8,7), nullable=False)
    has_grid_services = db.Column(db.Boolean, nullable = False, default = 0)
    has_spot_price = db.Column(db.Boolean, nullable = False, default = 0)
    has_balancing = db.Column(db.Boolean, nullable=False)
    # forecast_vol = db.Column(db.Numeric(15,3), nullable=False)
    make_invoice = db.Column(db.Boolean, nullable = False, default = 1)
    
    last_updated = db.Column(db.DateTime, default = dt.datetime.utcnow, onupdate = dt.datetime.utcnow)


    contract = db.relationship('Contract', back_populates = 'sub_contracts')   
    invoice_group = db.relationship('InvoiceGroup', back_populates = 'sub_contracts')
    measuring_type = db.relationship('MeasuringType', back_populates = 'sub_contracts')
    forecast_type = db.relationship('ForecastType', back_populates = 'sub_contracts')
    meta = db.relationship('ItnMeta', back_populates = 'sub_contracts')

    def __repr__(self):
        return (f'<{self.itn}, {self.contract_id}, {self.object_name}, \
        {self.invoice_group_id}, {self.measuring_type_id}, {self.start_date}, {self.end_date},  \
        {self.zko}, {self.akciz}, {self.has_grid_services}, {self.has_spot_price}, {self.has_balancing}>')

class StpCoeffs(BaseModel):
    utc = db.Column(db.DateTime, primary_key = True)
    measuring_type_id = db.Column(db.SmallInteger, db.ForeignKey('measuring_type.id', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True)
    value = db.Column(db.Numeric(9,7), nullable=False)

    measuring_type = db.relationship('MeasuringType', back_populates = 'stp_coeffs')
    

    def __repr__(self):
        return '<Utc: {}, StpCoeffs {}>'.format(self.utc, self.value)

class ForecastCoeffs(BaseModel):
    utc = db.Column(db.DateTime, primary_key = True)
    forecast_type_id = db.Column(db.SmallInteger, db.ForeignKey('forecast_type.id', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True)    
    value = db.Column(db.Numeric(9,7), nullable=False)   

    @hybrid_property
    def weekday(self):
        
        return self.utc.weekday()

    

    forecast_type = db.relationship('ForecastType', back_populates = 'forecast_coeffs')
    

    def __repr__(self):
        return '<Utc: {}, forecast_type_id {}, value {}, weekday {}>'.format(self.utc, self.forecast_type_id, self.value, self.weekday)

class Mail(BaseModel):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(32), nullable=False, unique = True)
    def __repr__(self):
        return f'{self.name}'

class ItnScheduleTemp(BaseModel):
    itn = db.Column(db.String(33), primary_key = True)
    utc = db.Column(db.DateTime, primary_key = True)
    forecast_vol = db.Column(db.Numeric(12,6), nullable=False)
    consumption_vol = db.Column(db.Numeric(12,6), nullable=False)
    settelment_vol = db.Column(db.Numeric(12,6), nullable=False, default = -1)
    price = db.Column(db.Numeric(8,7), nullable=False)
    tariff_id = db.Column(db.Integer,nullable=False, primary_key = True)
    def __repr__(self):
        return '<itn: {}, utc: {}, forecast_vol: {}, consumption_vol: {}, price: {}>'.format(self.itn, self.utc, self.forecast_vol, self.consumption_vol, self.price)
   
class ItnSchedule(BaseModel):
    itn = db.Column(db.String(33), db.ForeignKey('itn_meta.itn', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True)
    utc = db.Column(db.DateTime, primary_key = True)
    forecast_vol = db.Column(db.Numeric(12,6), nullable=False, default = 0)
    consumption_vol = db.Column(db.Numeric(12,6), nullable=False, default = -1)
    settelment_vol = db.Column(db.Numeric(12,6), nullable=False, default = -1)
    price = db.Column(db.Numeric(8,7), nullable=False)
    tariff_id = db.Column(db.Integer,db.ForeignKey('tariff.id', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True,nullable=False)

    meta = db.relationship('ItnMeta', back_populates = 'itn_schedules')
    
    
    def __repr__(self):
        return '<itn: {}, utc: {}, forecast_vol: {}, consumption_vol: {}, price: {}, tariff_id: {}>'.format(self.itn, self.utc, self.forecast_vol, self.consumption_vol, self.price, self.tariff_id)

    @staticmethod
    def generate_bulk_list(schedule_df):

        schedule_df['utc'] = schedule_df['utc'].astype(str)
        bulk_list =  schedule_df.to_dict(orient='records')
        return bulk_list

    @classmethod
    def autoinsert_new(cls, mapper, connection, target):        
        print(f'in autoinsert_new')
        sub_contract = target        
        schedule_df = pd.read_sql(ItnScheduleTemp.query \
            .filter(ItnScheduleTemp.itn == sub_contract.itn, ItnScheduleTemp.utc >= sub_contract.start_date, ItnScheduleTemp.utc <= sub_contract.end_date)\
            .statement, db.session.bind) 
        bulk_list = cls.generate_bulk_list(schedule_df)          
        db.session.bulk_insert_mappings(ItnSchedule, bulk_list)

    @classmethod
    def autoupdate_existing(cls, mapper, connection, target):

        state = db.inspect(target)
        changes = {}

        for attr in state.attrs:
            hist = attr.load_history()
            if not hist.has_changes():
                continue
            changes[attr.key] = hist.added

        print(f'from update callback \n{changes}')
        
        if changes.get('start_date') is not None:
            hist = sa.inspect(target).attrs.start_date.history            
            print(f'from update start_date {target.start_date} <----> {target.end_date}')
            print(f'hist_deleted ---> {hist.deleted}')
            delete_sch = ItnSchedule.__table__.delete().where((ItnSchedule.utc >= hist.deleted) & (ItnSchedule.utc < target.start_date) & (ItnSchedule.itn == target.itn))
            connection.execute(delete_sch)

        elif changes.get('end_date') is not None:
            hist = sa.inspect(target).attrs.end_date.history
            print(f'from update end_date')
            print(f'hist_deleted ---> {hist.deleted}')
            print(f'from update end_date {target.start_date} <----> {target.end_date}')
            delete_sch = ItnSchedule.__table__.delete().where((ItnSchedule.utc <= hist.deleted) & (ItnSchedule.utc > target.end_date) & (ItnSchedule.itn == target.itn))
            connection.execute(delete_sch)
        
        # print('deleted in shedule successiful', file = sys.stdout)
    
    # @classmethod
    # def autoupdate_from_contract(cls, mapper, connection, target):

    #     state = db.inspect(target)
    #     changes = {}

    #     for attr in state.attrs:
    #         hist = attr.load_history()
    #         if not hist.has_changes():
    #             continue
    #         changes[attr.key] = hist.added
        
    #     if changes.get('end_date') is not None:
    #         print(f'from autoupdate_from_contract callback \n{changes}')
    #         hist = sa.inspect(target).attrs.end_date.history
    #         date_before_update = hist.deleted[0]

    #         contract = target
    #         if contract.end_date < date_before_update:
    #             subs = SubContract.query.filter(SubContract.contract_id == contract.id).all()    
    #             for sub in subs:
    #                 sub.update({'end_date':contract.end_date})



    @classmethod
    def before_delete(cls, mapper, connection, target):
       
        delete_sch = ItnSchedule.__table__.delete().where((ItnSchedule.utc >= target.start_date) & (ItnSchedule.utc < target.end_date) & (ItnSchedule.itn == target.itn))
        connection.execute(delete_sch)

class LeavingItn(BaseModel):
    itn = db.Column(db.String(33), db.ForeignKey('itn_meta.itn', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True)
    date = db.Column(db.DateTime, primary_key = True)   
    meta = db.relationship("ItnMeta", back_populates="leaving_itns")


    def __repr__(self):
        return '<itn: {}, check-out date: {}>'.format(self.itn, self.date)

class IncomingItn(BaseModel):
    itn = db.Column(db.String(33), primary_key = True)
    as_settelment = db.Column(db.Boolean, nullable = False, default = 0)
    as_grid = db.Column(db.Boolean, nullable = False, default = 0)
    date = db.Column(db.DateTime, primary_key = True)

    def __repr__(self):
        return '<itn: {}, check-in date: {}>'.format(self.itn, self.date)

class IbexData(BaseModel):
    utc = db.Column(db.DateTime, primary_key = True)
    price = db.Column(db.Numeric(5,2), nullable=False, default = Decimal('0'))
    forecast_price = db.Column(db.Numeric(5,2), nullable=False, default = Decimal('0'))
    volume = db.Column(db.Numeric(8,2), nullable=False, default = Decimal('0'))

    def __repr__(self):
        return '<ibex: {}, price: {}, volume: {}>'.format(self.utc, self.price,  self.volume)

    @staticmethod
    def download_from_ibex_web_page(start_date, end_date):

        ibex_df = pd.DataFrame()
        start_date = pd.to_datetime(start_date,format='%Y-%m-%d') - dt.timedelta(hours = 24)
        end_date =  pd.to_datetime(end_date,format='%Y-%m-%d') + dt.timedelta(hours = 24)
        
        for day_offset in range(0, (end_date - start_date).days):
            date = start_date + dt.timedelta(days = day_offset)
            # print(f'date witth offset {date}')
            url = (r'http://www.ibex.bg/download-prices-volumes-data.php?date='
                        + str(date.year) + '-' # year
                        + str(date.month) + '-' # month
                        + str(date.day) + # day
                        '&lang=bg')
            print(f'From satic ibex s_date ={day_offset} ------- {url}')
            df = pd.read_excel(url, parse_dates=True, index_col='Date')
            range_date = pd.datetime(date.year, date.month, date.day)                         
            df.index = (
                pd.date_range(start=range_date, end=range_date + dt.timedelta(hours=23), freq='h', tz='CET')
                .tz_convert('UTC')
                .tz_localize(None)
            )
            df.index.name = 'utc'
            if ibex_df.empty:
                ibex_df = df        
            else:
                ibex_df = ibex_df.append(df, ignore_index=False)
        ibex_df.reset_index(inplace = True)
        ibex_df.rename(columns = {ibex_df.columns[1]:'price', ibex_df.columns[2]:'volume'}, inplace = True)
        return ibex_df
     
class Tariff(BaseModel):

    id = db.Column(db.Integer, primary_key = True, autoincrement=True, nullable = False)  
    name = db.Column(db.String(64), nullable = False)
    price_day = db.Column(db.Numeric(8,7), nullable=False, default = 0)
    price_night = db.Column(db.Numeric(8,7), nullable=False, default = 0)
    price_peak = db.Column(db.Numeric(8,7), nullable=False, default = 0)
    custom_start_hour = db.Column(db.SmallInteger,nullable=False, default = 0)
    custom_end_hour = db.Column(db.SmallInteger,nullable=False, default = 23)
    custom_week_day_start = db.Column(db.String(3), nullable = False, default = 'Mon')
    custom_week_day_end = db.Column(db.String(3), nullable = False, default = 'Sun')
    custom_price = db.Column(db.Numeric(8,7), nullable=False, default = 0)
    upper_limit = db.Column(db.Numeric(8,7), nullable=False, default = 0)
    lower_limit = db.Column(db.Numeric(8,7), nullable=False, default = 0)
    # itn = db.Column(db.String(33), db.ForeignKey('itn_meta.itn', ondelete='CASCADE', onupdate = 'CASCADE'),  nullable=False)
    

    def __repr__(self):
        return f'Tarrif: {self.name}, price_day: {self.price_day}, price_night: {self.price_night}'

class Invoice(BaseModel):

    id = db.Column(db.Integer, primary_key = True,  nullable = False)
    contractor_id = db.Column(db.Integer, db.ForeignKey('contractor.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    total_qty = db.Column(db.Numeric(12,3),nullable = False, default = 0)
    total_sum = db.Column(db.Numeric(12,4),nullable = False, default = 0)
    grid_sum = db.Column(db.Numeric(12,4),nullable = False, default = 0)
    zko_sum = db.Column(db.Numeric(12,4),nullable = False, default = 0)
    akciz_sum = db.Column(db.Numeric(12,4),nullable = False, default = 0)
    additional_tax_sum = db.Column(db.Numeric(12,4),nullable = False, default = 0)
    ref_file_name = db.Column(db.String(256), unique=True, nullable = False)
    easypay_num = db.Column(db.Integer,nullable=False, default = 0)
    easypay_name = db.Column(db.String(256),  nullable = False, default = 'none')
    is_easypay = db.Column(db.Boolean, nullable = False, default = 0)
    creation_date = db.Column(db.Date, nullable=False)
    maturity_date = db.Column(db.Date, nullable=False)
    price = db.Column(db.Numeric(7,4), nullable=False, default = 0)
    invoice_group_id = db.Column(db.Integer, db.ForeignKey('invoice_group.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    last_updated = db.Column(db.DateTime, default = dt.datetime.utcnow, onupdate=dt.datetime.utcnow)

    def __repr__(self):
        # contractor = Contractor.query.filter(Contractor.id == self.contractor_id).all()
        # contractor_name = contractor[0].name
        # emails = contractor[0].email
        return self.__str__()

    def __str__(self):
        contractor = Contractor.query.filter(Contractor.id == self.contractor_id).all()
        contractor_name = contractor[0].name        
        mail = Mail.query.join(InvoiceGroup, InvoiceGroup.email_id == Mail.id).filter(InvoiceGroup.id == self.invoice_group_id).first()
        emails = mail.name
        return f'{self.id} - {contractor_name} - {self.total_sum} - {self.creation_date} -   {emails}'


db.event.listen(SubContract, 'after_insert', ItnSchedule.autoinsert_new)
db.event.listen(SubContract, 'after_update', ItnSchedule.autoupdate_existing)
db.event.listen(SubContract, 'before_delete', ItnSchedule.before_delete)
# db.event.listen(Contract, 'after_update', ItnSchedule.autoupdate_from_contract)
# db.event.listen(IbexData, 'after_update', ItnSchedule.autoupdate_prices)
# db.event.listen(Contract, 'after_delete', ItnSchedule.after_delete)

# db.event.listen(SubContract, 'before_update', ItnSchedule.autoupdate_existing)
# db.event.listen(SubContract, 'before_update', ItnSchedule.test)

class MeasuringTypeSchema(ma.SQLAlchemySchema):
    class Meta:
        model = MeasuringType
        include_relationships = True
        load_instance = True
    id = auto_field()
    code = auto_field()
    
class ForecastTypeSchema(ma.SQLAlchemySchema):
    class Meta:
        model = ForecastType
        include_relationships = True
        load_instance = True
    id = auto_field()
    code = auto_field()
    # invoice_group = ma.Nested(InvoiceGroupSchema)

class ForecastCoeffsSchema(ma.SQLAlchemySchema):
    class Meta:
        model = ForecastCoeffs
        include_relationships = True
        load_instance = True
    utc = auto_field()
    forecast_type_id = auto_field()
    value = auto_field()    
    forecast_type = ma.Nested(ForecastTypeSchema) 

class ContractorSchema(ma.SQLAlchemySchema):
    class Meta:
        model = Contractor
        include_relationships = True
        load_instance = True
    id = auto_field()
    parent_id = auto_field()
    name = auto_field()
    eic = auto_field()
    address = auto_field()
    vat_number = auto_field()
    email = auto_field()
    acc_411 = auto_field()
    last_updated = ma.DateTime('%Y-%m-%d %H:%M:%S')

    # contracts = auto_field()
    # invoice_groups = auto_field()

class ContractSchema(ma.SQLAlchemySchema):
    class Meta:
        model = Contract
        include_relationships = True
        load_instance = True

    from app.helpers.helper_functions import convert_date_from_utc

    id = auto_field()
    internal_id = auto_field()
    # contractor_id = auto_field()
    subject = auto_field()
    parent_id = auto_field()
    time_zone_id = auto_field()
    signing_date = ma.DateTime('%Y-%m-%d %H:%M:%S')
    start_date = ma.DateTime('%Y-%m-%d %H:%M:%S')
    end_date = ma.DateTime('%Y-%m-%d %H:%M:%S')  
    contractor = ma.Nested(ContractorSchema) 
    invoicing_label = auto_field()

class InvoiceGroupSchema(ma.SQLAlchemySchema):
    class Meta:
        model = InvoiceGroup
        include_relationships = True
        load_instance = True 
    id = auto_field()
    name = auto_field()
    # contractor_id = db.Column(db.Integer, db.ForeignKey('contractor.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    description = auto_field()
    email_id = auto_field()

    contractor = ma.Nested(ContractorSchema)
    sub_contracts = db.relationship("SubContract", back_populates="invoice_group", lazy="dynamic")  

class SubContractSchema(ma.SQLAlchemySchema):
    class Meta:
        model = SubContract
        # json_module = simplejson
        include_relationships = True
        load_instance = True
    itn = auto_field()
    # contract_id = auto_field()
    # object_name = auto_field()    
    # # invoice_group_id = auto_field()
    # # measuring_type_id = auto_field()
    start_date = ma.DateTime('%Y-%m-%d %H:%M:%S')
    end_date = ma.DateTime('%Y-%m-%d %H:%M:%S')   
    zko = auto_field()
    akciz = auto_field()
    has_grid_services = auto_field()
    has_spot_price = auto_field()
    has_balancing = auto_field()    
    make_invoice = auto_field()    
    # last_updated = ma.DateTime('%Y-%m-%d %H:%M:%S')
    contract = ma.Nested(ContractSchema)   
    invoice_group = ma.Nested(InvoiceGroupSchema)
    measuring_type = ma.Nested(MeasuringTypeSchema)
    # # meta = auto_field()

class ItnMetaSchema(ma.SQLAlchemySchema):
    class Meta:
        model = ItnMeta
        include_relationships = True
        load_instance = True




    

