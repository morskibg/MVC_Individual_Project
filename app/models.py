import sys
import datetime as dt
import pandas as pd
import numpy as np
from decimal import Decimal 
from flask import g, flash
from app import db
from sqlalchemy.sql import func
import sqlalchemy as sa
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin
from app import login




class BaseModel(db.Model):
    __abstract__ = True
    def save(self):
        if self not in db.session:
            db.session.add(self)
        db.session.commit()

    def update(self, data: dict):
        for field, value in data.items():
            setattr(self, field, value)
        self.save()

    def delete(self):
        db.session.delete(self)
        db.session.commit()


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

    contractor = db.relationship('Contractor', back_populates = 'contracts')
    contract_type = db.relationship('ContractType', back_populates = 'contracts')
    sub_contracts = db.relationship("SubContract", back_populates="contract", lazy="dynamic",cascade="all, delete")
    time_zone = db.relationship("TimeZone", back_populates="contracts")


    
    def __str__(self):
        from app.helpers.helper_functions import convert_date_from_utc
        date_eet = convert_date_from_utc('EET',self.signing_date)
        return f'{self.internal_id} - {self.contractor.name} - {date_eet}'

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

class InvoiceGroup(BaseModel):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(32), nullable=False, unique = True)
    contractor_id = db.Column(db.Integer, db.ForeignKey('contractor.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    description = db.Column(db.String(128), nullable=True, unique = False)

    contractor = db.relationship('Contractor', back_populates = 'invoice_groups')
    sub_contracts = db.relationship("SubContract", back_populates="invoice_group", lazy="dynamic")

    def __str__(self):
        
        return f'{self.name} - {self.description}'

    def __repr__(self):
        return '<InvoiceGroup:{}, {}, Contractor_id {}>'.format(self.id, self.name, self.contractor_id)


class SubContract(BaseModel):
    itn = db.Column(db.String(33), db.ForeignKey('itn_meta.itn', ondelete='CASCADE', onupdate = 'CASCADE'), primary_key = True)
    contract_id = db.Column(db.Integer, db.ForeignKey('contract.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    object_name = db.Column(db.String(64), nullable = True)
    # price = db.Column(db.Numeric(8,7), nullable=False)
    invoice_group_id = db.Column(db.Integer, db.ForeignKey('invoice_group.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
    measuring_type_id = db.Column(db.SmallInteger, db.ForeignKey('measuring_type.id', ondelete='CASCADE', onupdate = 'CASCADE'), nullable=False)
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
        return '<itn: {}, utc: {}, forecast_vol: {}, consumption_vol: {}, price: {}>'.format(self.itn, self.utc, self.forecast_vol, self.consumption_vol, self.price)

    @staticmethod
    def generate_bulk_list(schedule_df):

        schedule_df['utc'] = schedule_df['utc'].astype(str)
        bulk_list =  schedule_df.to_dict(orient='records')
        return bulk_list


    # def generate_bulk_list(itn, start_date, end_date,  price, measuring_type_id, is_remaining_schedule = True):
        

       
        # time_series = pd.date_range(start=start_date, end=end_date, freq='H')
        
        # df = pd.DataFrame(time_series, columns = ['utc'])
        # df['itn'] = itn  
        # df['price'] = price
        
        # stp_df = pd.read_sql(StpCoeffs.query.filter(StpCoeffs.measuring_type_id == measuring_type_id).statement, db.session.bind)
        
        # if not stp_df.empty:
        #     # generated or updated subcontract is STP, merge with stp table and goes to list of dict creation for bulk insert
        #     df = df.merge(stp_df, on = 'utc', how = 'left')
        #     df = df.fillna(0)
        #     df['forecast_vol'] = df['value'].apply(lambda x: Decimal(str(x)) * Decimal(str(forecast_vol)))
        #     # #print(df.head, file = sys.stdout)
        # else:                     
        #     forcasted_schedule = g.pop('forcasted_schedule', None)
        #     if forcasted_schedule is not None:
        #         # generated or updated subcontract is NOT STP, check for hourly forecast schedule from excel file and goes to list of dict creation for bulk insert 
        #         forcasted_schedule.set_index('date', inplace = True)
        #         forcasted_schedule.index = forcasted_schedule.index.tz_localize('EET', ambiguous='infer').tz_convert('UTC').tz_localize(None)
        #         forcasted_schedule.reset_index(inplace = True)
        #         forcasted_schedule.rename(columns = {forcasted_schedule.columns[0]:'utc'}, inplace = True)
        #         df = df.merge(forcasted_schedule, on = 'utc', how = 'left')
        #         df = df.fillna(0)
        #         df['forecast_vol'] = df[forcasted_schedule.columns[1]].apply(lambda x: Decimal(str(x)))
        #     else:
        #         # remaining_schedule_list_of_dict = g.pop('remaining_schedule_list_of_dict', None)
        #         # if remaining_schedule_list_of_dict is not None:
        #         if is_remaining_schedule:
        #             # generated or updated subcontract is remaning additional and goes to list of dict creation for bulk insert  
                   
        #             #print('in generate add sub', file = sys.stdout)  
        #             remaining_schedule = ItnScheduleTemp.query.all()
        #             # delete_sch = ItnScheduleTemp.__table__.delete()
        #             # db.session.execute(delete_sch)

        #             list_of_dict = []
        #             for schedule in remaining_schedule: 
                                            
        #                         list_of_dict.append(dict(itn = schedule.itn, 
        #                                         utc = schedule.utc,                                                      
        #                                         forecast_vol = schedule.forecast_vol,
        #                                         consumption_vol = schedule.consumption_vol,
        #                                         price = schedule.price))                                               
        #             return list_of_dict
        #         else:   
        #             df['forecast_vol'] = Decimal(str('0'))
            
        # df['consumption_vol'] = -1 
        
                
        # list_of_dict = []
        # for row in list(schedule_df.to_records()): 
                        
        #     list_of_dict.append(dict(itn = row['itn'], 
        #                     utc = dt.datetime.strptime(np.datetime_as_string(row['utc'], unit='s'), '%Y-%m-%dT%H:%M:%S'),                                                      
        #                     forecast_vol = row['forecast_vol'],
        #                     consumption_vol = Decimal(str(row['consumption_vol'])),
        #                     price = Decimal(str(row['price']))))
                            
        # return list_of_dict

    @classmethod
    def autoinsert_new(cls, mapper, connection, target):
        
        
        sub_contract = target
        
        schedule_df = pd.read_sql(ItnScheduleTemp.query \
            .filter(ItnScheduleTemp.itn == sub_contract.itn, ItnScheduleTemp.utc >= sub_contract.start_date, ItnScheduleTemp.utc <= sub_contract.end_date)\
            .statement, db.session.bind) 
        bulk_list = cls.generate_bulk_list(schedule_df)   
         
        # update_or_insert(schedule_df, ItnSchedule.__table__.name)
          
        # bulk_list = cls.generate_bulk_list(itn = sub_contract.itn, start_date = sub_contract.start_date,
        #          end_date = sub_contract.end_date, price=sub_contract.price, 
        #          measuring_type_id = sub_contract.measuring_type_id )
                 
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

        # print('from update callback', file = sys.stdout)
        # print(changes, file = sys.stdout)
        if changes.get('start_date') is not None:

            hist = sa.inspect(target).attrs.start_date.history            
            # print(f'from update start_date {target.start_date} <----> {target.end_date}', file = sys.stdout)
            # print(f'hist_deleted ---> {hist.deleted}')
            delete_sch = ItnSchedule.__table__.delete().where((ItnSchedule.utc >= hist.deleted) & (ItnSchedule.utc < target.start_date) & (ItnSchedule.itn == target.itn))
            connection.execute(delete_sch)
        elif changes.get('end_date') is not None:

            hist = sa.inspect(target).attrs.end_date.history
            # print('from update end_date', file = sys.stdout)
            # print(f'hist_deleted ---> {hist.deleted}')
            # print(f'from update end_date {target.start_date} <----> {target.end_date}', file = sys.stdout)
            delete_sch = ItnSchedule.__table__.delete().where((ItnSchedule.utc <= hist.deleted) & (ItnSchedule.utc > target.end_date) & (ItnSchedule.itn == target.itn))
            connection.execute(delete_sch)
        
        # print('deleted in shedule successiful', file = sys.stdout)

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


db.event.listen(SubContract, 'after_insert', ItnSchedule.autoinsert_new)
db.event.listen(SubContract, 'after_update', ItnSchedule.autoupdate_existing)
db.event.listen(SubContract, 'before_delete', ItnSchedule.before_delete)
# db.event.listen(Contract, 'after_delete', ItnSchedule.after_delete)

# db.event.listen(SubContract, 'before_update', ItnSchedule.autoupdate_existing)
# db.event.listen(SubContract, 'before_update', ItnSchedule.test)

