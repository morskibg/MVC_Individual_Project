import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash
from sqlalchemy import or_

from app.models import *    
from app.helpers.helper_functions import update_or_insert, stringifyer, convert_date_to_utc, convert_date_from_utc

MONEY_ROUND = 6
ENERGY_ROUND = 3

def get_contractors_names_and_411():

    records = (
        db.session
            .query(
                Contractor.id.label('contractor_id'),
                Contractor.name.label('contractor_name'), 
                Contractor.acc_411.label('411-3')       
            )
            .all())

    return records
######################################################################################################################################################
def is_spot_inv_group(inv_group_name, start_date, end_date):

    itn_spot = get_spot_itns(inv_group_name, start_date, end_date)
    itns = db.session.query(itn_spot.c.sub_itn).all()
    
    return len(itns) > 0

def get_all_inv_groups():
    inv_groups = db.session.query(InvoiceGroup.name).all()
    inv_groups = [x[0] for x in inv_groups]
    return inv_groups

def get_stp_itn_by_inv_group_for_period_spot_sub(inv_group_name, start_date, end_date):

    itn_records = (
        db.session
            .query(
                SubContract.itn.label('sub_itn'), 
                SubContract.zko.label('zko'),
                SubContract.akciz.label('akciz') , 
                Contractor.name.label('contractor_name'), 
                InvoiceGroup.description.label('invoice_group_description'), 
                InvoiceGroup.name.label('invoice_group_name'),
                SubContract.make_invoice.label('make_invoice')     
            )
            .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id) 
            .join(MeasuringType)  
            .join(Contractor)                    
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) 
            .filter(SubContract.has_spot_price) #!!!!!!!!!!!!!!!!!!!!!!
            .filter(InvoiceGroup.name == inv_group_name)
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT')))
            .distinct(SubContract.itn) 
            .subquery())

    return itn_records

def get_all_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date):

    itn_records = (
        db.session
            .query(
                SubContract.itn.label('sub_itn'), 
                SubContract.zko.label('zko'),
                SubContract.akciz.label('akciz') , 
                Contractor.name.label('contractor_name'), 
                InvoiceGroup.description.label('invoice_group_description'), 
                InvoiceGroup.name.label('invoice_group_name')       
            )
            .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id) 
            .join(MeasuringType)  
            .join(Contractor)                    
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))             
            .filter(InvoiceGroup.name == inv_group_name)
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT')))
            .distinct(SubContract.itn) 
            .subquery())

    return itn_records

def get_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date):

    itn_records = (
        db.session
            .query(
                SubContract.itn.label('sub_itn'), 
                SubContract.zko.label('zko'),
                SubContract.akciz.label('akciz') , 
                Contractor.name.label('contractor_name'), 
                InvoiceGroup.description.label('invoice_group_description'), 
                InvoiceGroup.name.label('invoice_group_name'),
                SubContract.make_invoice.label('make_invoice')       
            )
            .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id) 
            .join(MeasuringType)  
            .join(Contractor)                    
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) 
            .filter(~SubContract.has_spot_price) #!!!!!!!!!!!!!!!!!!!!!!
            .filter(InvoiceGroup.name == inv_group_name)
            .filter(~((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT')))
            .distinct(SubContract.itn) 
            .subquery())

    return itn_records

def get_stp_consumption_for_period_sub(stp_itn_by_inv_group_for_period_sub, invoice_start_date, invoice_end_date):

    total_consumption_records = (
        db.session
            .query(Distribution.itn.label('itn'), 
                func.sum(Distribution.calc_amount).label('total_consumption')) 
            
            .join(ErpInvoice, ErpInvoice.id == Distribution.erp_invoice_id) 
            .join(stp_itn_by_inv_group_for_period_sub, stp_itn_by_inv_group_for_period_sub.c.sub_itn == Distribution.itn)                         
            .filter(Distribution.tariff.in_(['Достъп','Пренос през електропреносната мрежа', 'Разпределение'])) 
            .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date) 
            .group_by(Distribution.itn)
            .subquery()
    )
    return total_consumption_records

def get_summary_records(consumption_for_period_sub, grid_services_sub, itns, start_date, end_date):

    summary_records = (
        db.session
            .query(ItnMeta.itn.label('Обект (ИТН №)'),
                grid_services_sub.c.grid_services.label('Мрежови услуги (лв.)'),
                consumption_for_period_sub.c.total_consumption.label('Потребление (kWh)'),
                (consumption_for_period_sub.c.total_consumption * Tariff.price_day).label('Сума за енергия'),
                (consumption_for_period_sub.c.total_consumption * itns.c.zko).label('Задължение към обществото'),
                (consumption_for_period_sub.c.total_consumption * itns.c.akciz).label('Акциз'),
                AddressMurs.name.label('Адрес'), 
                itns.c.contractor_name, 
                itns.c.invoice_group_description, 
                itns.c.invoice_group_name,
                itns.c.zko, 
                itns.c.akciz,
                itns.c.make_invoice   
        )
         
        .join(itns, itns.c.sub_itn == ItnMeta.itn)
        .join(consumption_for_period_sub, consumption_for_period_sub.c.itn == ItnMeta.itn)        
        .outerjoin(grid_services_sub, grid_services_sub.c.itn_id == ItnMeta.itn)
        .join(ItnSchedule, ItnSchedule.itn == ItnMeta.itn)
        .outerjoin(AddressMurs,AddressMurs.id == ItnMeta.address_id)
        .join(Tariff, Tariff.id == ItnSchedule.tariff_id)         
        .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)
        .group_by(ItnMeta.itn,grid_services_sub.c.grid_services,consumption_for_period_sub.c.total_consumption,
                 Tariff.price_day, AddressMurs.name, itns.c.zko, itns.c.akciz,itns.c.contractor_name,itns.c.invoice_group_description, itns.c.make_invoice)
        .all()
            
    )
    return summary_records

def get_spot_itns(inv_group_names, start_date, end_date):

    itn_records = (
        db.session
            .query(SubContract.itn.label('sub_itn'))       
            .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id) 
            .join(MeasuringType)  
            .join(Contractor)                    
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) 
            .filter(SubContract.has_spot_price) #!!!!!!!!!!!!!!!!!!!!!!
            .filter(InvoiceGroup.name.in_(inv_group_names))            
            .distinct(SubContract.itn) 
            .subquery())

    return itn_records

def get_spot_fin_results(itns, start_date, end_date):

    total_consumption_records = (
        db.session
            .query(ItnSchedule.itn.label('itn'), 
                func.sum(ItnSchedule.consumption_vol).label('total_consumption'),  
                func.sum(ItnSchedule.consumption_vol * ItnSchedule.price).label('fin_res') )          
            .join(itns, itns.c.sub_itn == ItnSchedule.itn)
            .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)            
            .group_by(ItnSchedule.itn)
            .all()
    )
    return total_consumption_records

def get_summary_records_spot(consumption_for_period_sub, grid_services_sub, itns, start_date, end_date):

    summary_records = (
        db.session
            .query(ItnMeta.itn.label('Обект (ИТН №)'),
                grid_services_sub.c.grid_services.label('Мрежови услуги (лв.)'),
                consumption_for_period_sub.c.total_consumption.label('Потребление (kWh)'),                
                (consumption_for_period_sub.c.total_consumption * itns.c.zko).label('Задължение към обществото'),
                (consumption_for_period_sub.c.total_consumption * itns.c.akciz).label('Акциз'),                
                AddressMurs.name.label('Адрес'), 
                itns.c.contractor_name, 
                itns.c.invoice_group_description, 
                itns.c.invoice_group_name,
                itns.c.zko, 
                itns.c.akciz,
                itns.c.make_invoice
                  
        )
         
        .join(itns, itns.c.sub_itn == ItnMeta.itn)
        .join(consumption_for_period_sub, consumption_for_period_sub.c.itn == ItnMeta.itn)        
        .outerjoin(grid_services_sub, grid_services_sub.c.itn_id == ItnMeta.itn)
        .join(ItnSchedule, ItnSchedule.itn == ItnMeta.itn)
        .outerjoin(AddressMurs,AddressMurs.id == ItnMeta.address_id)
        .join(Tariff, Tariff.id == ItnSchedule.tariff_id)         
        .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)
        .group_by(ItnMeta.itn,grid_services_sub.c.grid_services,consumption_for_period_sub.c.total_consumption,
                 AddressMurs.name, itns.c.zko, itns.c.akciz,itns.c.contractor_name,itns.c.invoice_group_description, itns.c.make_invoice)
        .all()
            
    )
    return summary_records

def get_non_stp_itn_by_inv_group_for_period_spot_sub(inv_group_name, start_date, end_date):

    itn_records = (
        db.session
            .query(
                SubContract.itn.label('sub_itn'), 
                SubContract.zko.label('zko'),
                SubContract.akciz.label('akciz') , 
                Contractor.name.label('contractor_name'), 
                InvoiceGroup.description.label('invoice_group_description'),
                InvoiceGroup.name.label('invoice_group_name'),
                SubContract.make_invoice.label('make_invoice')            
            )
            .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id) 
            .join(MeasuringType)  
            .join(Contractor)   
            .filter(SubContract.has_spot_price) #!!!!!!!!!!!!!!!!!!!!!!     
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) 
            .filter(InvoiceGroup.name == inv_group_name)
            .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT')))
            .distinct(SubContract.itn) 
            .subquery())

    return itn_records

def get_non_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date):

    itn_records = (
        db.session
            .query(
                SubContract.itn.label('sub_itn'), 
                SubContract.zko.label('zko'),
                SubContract.akciz.label('akciz') , 
                Contractor.name.label('contractor_name'), 
                InvoiceGroup.description.label('invoice_group_description'),
                InvoiceGroup.name.label('invoice_group_name'),
                SubContract.make_invoice.label('make_invoice')              
            )
            .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id) 
            .join(MeasuringType)  
            .join(Contractor)   
            .filter(~SubContract.has_spot_price) #!!!!!!!!!!!!!!!!!!!!!!     
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) 
            .filter(InvoiceGroup.name == inv_group_name)
            .filter(((MeasuringType.code == 'UNDIRECT') | (MeasuringType.code == 'DIRECT')))
            .distinct(SubContract.itn) 
            .subquery())

    return itn_records

def get_non_stp_consumption_for_period_sub(non_stp_itn_by_inv_group_for_period_sub, period_start_date, period_end_date):

    total_consumption_records = (
        db.session
            .query(ItnSchedule.itn.label('itn'), 
                func.sum(ItnSchedule.consumption_vol).label('total_consumption'))            
            .join(non_stp_itn_by_inv_group_for_period_sub, non_stp_itn_by_inv_group_for_period_sub.c.sub_itn == ItnSchedule.itn)
            .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)            
            .group_by(ItnSchedule.itn)
            .subquery()
    )
    return total_consumption_records

def get_itn_with_grid_services_sub(inv_group_name, start_date, end_date):

    itn_records = (
        db.session
            .query(
                SubContract.itn.label('sub_itn'),
                Contractor.name.label('contractor_name'),
                Contractor.eic.label('contractor_eic')                
            )
            .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id)  
            .join(Contractor, Contractor.id == InvoiceGroup.contractor_id)                     
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) 
            .filter(InvoiceGroup.name == inv_group_name)
            .filter(SubContract.has_grid_services)            
            .distinct(SubContract.itn) 
            .subquery())

    return itn_records

def get_grid_services_sub(itn_with_grid_services_sub, invoice_start_date, invoice_end_date):

    grid_service_sub_query = (
        db.session
            .query(
                Distribution.itn.label('itn_id'),
                func.round(func.sum(Distribution.value), MONEY_ROUND).label('grid_services')                
            )
            .join(itn_with_grid_services_sub, itn_with_grid_services_sub.c.sub_itn == Distribution.itn)
            .join(ErpInvoice,ErpInvoice.id == Distribution.erp_invoice_id)            
            .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)
            .group_by(Distribution.itn)
            .subquery()
        )    
    return grid_service_sub_query

def get_grid_services_tech_records(itn_with_grid_services_sub, invoice_start_date, invoice_end_date):

    grid_services_tech_records = (
            db.session.query(
                Tech.subscriber_number.label('Абонат №'),                
                AddressMurs.name.label('А д р е с'),
                itn_with_grid_services_sub.c.contractor_name.label('Име на клиент'),
                itn_with_grid_services_sub.c.contractor_eic.label('ЕГН/ЕИК'),
                Tech.itn.label('Идентификационен код'),
                Tech.electric_meter_number.label('Електромер №'),
                Tech.start_date.label('Отчетен период от'),
                Tech.end_date.label('Отчетен период до'),
                func.TIMEDIFF(Tech.end_date,Tech.start_date).label('Брой дни'),
                Tech.scale_number.label('Номер скала'),
                Tech.scale_type.label('Код скала'),
                Tech.time_zone.label('Часова зона'),
                Tech.new_readings.label('Показания  ново'),
                Tech.old_readings.label('Показания старо'),
                Tech.readings_difference.label('Разлика (квтч)'),
                Tech.constant.label('Константа'),
                Tech.correction.label('Корекция (квтч)'),
                Tech.storno.label('Приспаднати (квтч)'),
                Tech.total_amount.label('Общо количество (квтч)'),               
            ) 
            .join(itn_with_grid_services_sub,itn_with_grid_services_sub.c.sub_itn == Tech.itn)  
            .join(ItnMeta, ItnMeta.itn == Tech.itn)
            .join(Distribution, Distribution.itn == Tech.itn) 
            .join(ErpInvoice, ErpInvoice.id == Tech.erp_invoice_id)          
            .join(AddressMurs,AddressMurs.id == ItnMeta.address_id)                 
            .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)  
            .distinct(Distribution.tariff)
            .all()
            )
    return grid_services_tech_records

def get_grid_services_distrib_records(itn_with_grid_services_sub, invoice_start_date, invoice_end_date):

    grid_services_distrib_records = (
        db.session.query(
            Tech.subscriber_number.label('Абонат №'),                
            AddressMurs.name.label('А д р е с'),
            itn_with_grid_services_sub.c.contractor_name.label('Име на клиент'),
            itn_with_grid_services_sub.c.contractor_eic.label('ЕГН/ЕИК'),
            Tech.itn.label('Идентификационен код'),
            Tech.electric_meter_number.label('Електромер №'),
            Distribution.start_date.label('Отчетен период от'),
            Distribution.end_date.label('Отчетен период до'),
            func.TIMEDIFF(Distribution.end_date,Distribution.start_date).label('Брой дни'),
            Distribution.tariff.label('Тарифа/Услуга'),
            Distribution.calc_amount.label('Количество (кВтч/кВАрч)'),
            Distribution.price.label('Единична цена (лв./кВт/ден)/ (лв./кВтч)'),
            Distribution.value.label('Стойност (лв)'),
            ErpInvoice.correction_note.label('Корекция към фактура'),
            ErpInvoice.event.label('Основание за издаване'), 
            
        )
        .join(itn_with_grid_services_sub,itn_with_grid_services_sub.c.sub_itn == Tech.itn)
        .join(Distribution,Distribution.itn == Tech.itn)
        .join(ItnMeta,ItnMeta.itn == Tech.itn)
        .join(AddressMurs,AddressMurs.id == ItnMeta.address_id)            
        .join(ErpInvoice, ErpInvoice.id == Distribution.erp_invoice_id)
        .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)  
        .distinct(Distribution.tariff)
        .all()
        )
    return grid_services_distrib_records

def get_erp_consumption_records_by_grid(erp_name, invoice_start_date, invoice_end_date):

    erp_consumption_records = (
            db.session
                .query(  
                    Erp.name,                                   
                    func.sum(DistributionTemp.calc_amount).label('total_consumption'))                 
                .join(ItnMeta,ItnMeta.itn == DistributionTemp.itn )
                .join(Erp, Erp.id == ItnMeta.erp_id)
                .filter(Erp.name == erp_name)                     
                .filter(DistributionTemp.tariff.in_(['Достъп','Пренос през електропреносната мрежа', 'Разпределение'])) 
                .filter(DistributionTemp.date >= invoice_start_date, DistributionTemp.date <= invoice_end_date) 
                # .group_by(Erp.name)
                # .distinct()
                .all()
        )
    return erp_consumption_records
    
def get_erp_money_records_by_grid(erp_name, invoice_start_date, invoice_end_date):

    erp_money_records = (
        db.session
            .query(  
                Erp.name,                                   
                func.sum(DistributionTemp.value).label('value'))              
            .join(ItnMeta,ItnMeta.itn == DistributionTemp.itn )
            .join(Erp, Erp.id == ItnMeta.erp_id)
            .filter(Erp.name == erp_name)             
            .filter(DistributionTemp.date >= invoice_start_date, DistributionTemp.date <= invoice_end_date) 
            .group_by(Erp.name)
            # .distinct()
            .all()
    )    
    return erp_money_records

def get_total_consumption_by_grid(invoice_start_date, invoice_end_date):

    total_consumption_records = (
        db.session
            .query(                  
                func.sum(DistributionTemp.calc_amount).label('total_consumption'))   
            .filter(DistributionTemp.tariff.in_(['Достъп','Пренос през електропреносната мрежа', 'Разпределение']))               
            .filter(DistributionTemp.date >= invoice_start_date, DistributionTemp.date <= invoice_end_date)                 
            .distinct()
            .all()
    )
    return total_consumption_records

def get_total_money_by_grid(invoice_start_date, invoice_end_date):

    total_sum_records = (
        db.session
            .query(                  
                func.sum(DistributionTemp.value).label('value'))                  
            .filter(DistributionTemp.date >= invoice_start_date, DistributionTemp.date <= invoice_end_date)                 
            .distinct()
            .all()
    )    
    return total_sum_records

def get_tariff_limits(itns, start_date, end_date):

    itns = [x[0] for x in db.session.query(itns).all()]
    
    
    tariff_records = (
        db.session
            .query(Tariff.lower_limit,Tariff.upper_limit)            
            .filter(ItnSchedule.itn.in_(itns))
            .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)  
            .group_by(Tariff.lower_limit,Tariff.upper_limit)          
            .all()
    )
    
    return tariff_records

def get_time_zone(inv_group_name, start_date, end_date):

    contract_records = (db.session
        .query(InvoiceGroup.name, InvoiceGroup.description, Contract.id, TimeZone.code)
        .join(SubContract,SubContract.invoice_group_id == InvoiceGroup.id)
        .join(Contract,Contract.id == SubContract.contract_id)
        .join(TimeZone,TimeZone.id == Contract.time_zone_id)
        .filter(InvoiceGroup.name == inv_group_name)
        .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
        .distinct()
        .all()
    )    
    if len(contract_records) > 1:
        time_zones = [x[3] for x in contract_records]
        if not time_zones.count(time_zones[0]) == len(time_zones):
            print(f'Warning from get time zone ! Multiple contracts with different time zones found ! -- {contract_records}')
        else:
            return contract_records[0][3]
    elif len(contract_records) == 0:
        print(f'Warning ! There is not any contract found !')
    else:
        return contract_records[0][3]
    return None

def get_list_inv_groups_by_contract(internal_id, start_date, end_date):

    itns = (db.session
            .query(Contract.id, InvoiceGroup.name)
            .join(SubContract, SubContract.contract_id == Contract.id)
            .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id)
            .filter(Contract.internal_id == internal_id)
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) 
            # .filter(SubContract.has_spot_price) #!!!!!!!!!!!!!!!!!!!!!!                     
            .distinct(InvoiceGroup.name)  
            .all()
        )
    res = [x[1] for x in itns]
    return res

def has_ibex_real_data(end_date):
    
    records = (db.session
                .query(IbexData.utc, IbexData.price)
                .filter(IbexData.utc == end_date)
                .all()
            )
    if records == []:
        print(f'No schedule data for ibex for {end_date} ! Creating !')
        create_ibex_schedule(end_date)
        has_data = False
    else:
        ibex_price = records[0][1]
        has_data = ibex_price > 0 

    return has_data

def create_ibex_schedule(end_date):

    last_db_ibex = db.session.query(IbexData).order_by(IbexData.utc.desc()).first()
    
    start_date = last_db_ibex.utc + dt.timedelta(hours = 1)
    start_date = convert_date_from_utc('EET', start_date,False)
   
    end_date = convert_date_from_utc('EET', end_date)

    time_series = pd.date_range(start = start_date, end = end_date , freq='h', tz = 'EET')
    forecast_df = pd.DataFrame(time_series, columns = ['utc'])
    forecast_df['forecast_price'] = 0
    forecast_df['volume'] = 0
    forecast_df['price'] = 0
    forecast_df.set_index('utc', inplace = True)
    
    forecast_df.index = forecast_df.index.tz_convert('UTC').tz_localize(None)
    forecast_df.reset_index(inplace = True)
    forecast_df = forecast_df[['utc','price', 'forecast_price', 'volume']]
    stringifyer(forecast_df)
    bulk_update_list = forecast_df.to_dict(orient='records')

    db.session.bulk_insert_mappings(IbexData, bulk_update_list)
    db.session.commit()

# def update_ibex_data():
def get_inv_gr_id_linked_conractors(start_date, end_date, parent_id):

    erp_inv_ids =(
            db.session.query(
                InvoiceGroup.name,
                InvoiceGroup.description,
                Contract,
                Contract.internal_id                     
            )                     
            .join(SubContract,SubContract.invoice_group_id == InvoiceGroup.id)
            .join(Contract, Contract.id == SubContract.contract_id) 
            .join(ContractType, ContractType.id == Contract.contract_type_id)         
            .join(Contractor,Contractor.id == Contract.contractor_id)   
            .join(ItnMeta, ItnMeta.itn == SubContract.itn)                   
            .filter(SubContract.start_date <= start_date, SubContract.end_date > start_date)      
            .filter(or_( Contractor.id == parent_id,  Contractor.parent_id == parent_id))                
            .distinct()
            .all()
        )
    return erp_inv_ids

def get_inv_gr_id_single_erp(erp, contract_type, start_date, end_date, is_mixed):

    # contr_type_list
    if is_mixed == 'True':        
        single_erp_inv_ids =(
            db.session.query(
                InvoiceGroup.name,
                InvoiceGroup.description,
                Contract,
                Contract.internal_id                     
            )                     
            .join(SubContract,SubContract.invoice_group_id == InvoiceGroup.id)
            .join(Contract, Contract.id == SubContract.contract_id) 
            .join(ContractType, ContractType.id == Contract.contract_type_id)         
            .join(Contractor,Contractor.id == Contract.contractor_id)   
            .join(ItnMeta, ItnMeta.itn == SubContract.itn)   
            .join(Erp)  
            .filter(Erp.name == erp)           
            .filter(SubContract.start_date <= start_date, SubContract.end_date > start_date) 
            .filter(ContractType.name == contract_type).order_by(Contractor.name)             
            .distinct()
            .all()
        )

    else:        
        itn_count_per_inv_gr = (
            db.session.query(
                
                InvoiceGroup.id.label('inv_gr_id_all'),
                func.count(SubContract.itn).label('itns_count')
            )
            .join(SubContract, SubContract.invoice_group_id == InvoiceGroup.id)
            .join(ItnMeta, ItnMeta.itn == SubContract.itn)               
            .filter(SubContract.start_date <= start_date, SubContract.end_date > end_date)
            .group_by(InvoiceGroup.id)
            .subquery()
        )

        itn_count_per_inv_gr_erp = (
            db.session.query(
                
                InvoiceGroup.id.label('inv_gr_id_erp'),
                InvoiceGroup.name.label('inv_name'),
                InvoiceGroup.description.label('inv_descr'),
                func.count(SubContract.itn).label('itns_count')
            )
            .join(SubContract, SubContract.invoice_group_id == InvoiceGroup.id)
            .join(ItnMeta, ItnMeta.itn == SubContract.itn)   
            .join(Erp)  
            .filter(Erp.name == erp)          
            .filter(SubContract.start_date <= start_date, SubContract.end_date > end_date)
            .group_by(InvoiceGroup.id)
            .subquery()
        )
        # single_erp_inv_ids =(
        #     db.session.query(
        #         InvoiceGroup.name,
        #         InvoiceGroup.description,
        #         Contract,
        #         itn_count_per_inv_gr_erp.c.inv_gr_id_erp           
        #     )
        #     .join(itn_count_per_inv_gr,itn_count_per_inv_gr.c.inv_gr_id_all == itn_count_per_inv_gr_erp.c.inv_gr_id_erp)
        #     .join(InvoiceGroup, InvoiceGroup.id == itn_count_per_inv_gr_erp.c.inv_gr_id_erp)        
        #     .join(SubContract,SubContract.invoice_group_id == itn_count_per_inv_gr_erp.c.inv_gr_id_erp)
        #     .join(Contract, Contract.id == SubContract.contract_id) 
        #     .join(ContractType, ContractType.id == Contract.contract_type_id) 
        #     .join(Contractor,Contractor.id == Contract.contractor_id)              
        #     .filter(SubContract.start_date <= start_date, SubContract.end_date > start_date)
        #     .filter(ContractType.name == contract_type).order_by(Contractor.name)
        #     .filter(itn_count_per_inv_gr.c.itns_count == itn_count_per_inv_gr_erp.c.itns_count)
        #     .distinct()
        #     .all()
        # )
        single_erp_inv_ids =(
            db.session.query(
                itn_count_per_inv_gr_erp.c.inv_name,
                itn_count_per_inv_gr_erp.c.inv_descr,
                Contract,
                Contract.internal_id,
                itn_count_per_inv_gr_erp.c.inv_gr_id_erp           
            )
            .join(itn_count_per_inv_gr,itn_count_per_inv_gr.c.inv_gr_id_all == itn_count_per_inv_gr_erp.c.inv_gr_id_erp)                   
            .join(SubContract,SubContract.invoice_group_id == itn_count_per_inv_gr_erp.c.inv_gr_id_erp)
            .join(Contract, Contract.id == SubContract.contract_id) 
            .join(ContractType, ContractType.id == Contract.contract_type_id) 
            .join(Contractor,Contractor.id == Contract.contractor_id)              
            .filter(SubContract.start_date <= start_date, SubContract.end_date > start_date)
            .filter(ContractType.name == contract_type).order_by(Contractor.name)
            .filter(itn_count_per_inv_gr.c.itns_count == itn_count_per_inv_gr_erp.c.itns_count)
            # .distinct(InvoiceGroup.name)
            .all()
        )
   
    return single_erp_inv_ids

def get_inv_gr_id_erp(erp, start_date = None, end_date = None):

    if start_date is None and end_date is None:
        now_date = dt.datetime.utcnow()
        start_date = now_date - dt.timedelta(days = 28)
        end_date = start_date + dt.timedelta(days = 10)

    
    single_erp_inv_ids =(
        db.session.query(
            InvoiceGroup.name,
            InvoiceGroup.description,
            Contract,                      
        )                     
        .join(SubContract,SubContract.invoice_group_id == InvoiceGroup.id)
        .join(Contract, Contract.id == SubContract.contract_id)         
        .join(Contractor,Contractor.id == Contract.contractor_id)   
        .join(ItnMeta, ItnMeta.itn == SubContract.itn)   
        .join(Erp)  
        .filter(Erp.name == erp)           
        .filter(SubContract.start_date <= start_date, SubContract.end_date > start_date)              
        .distinct()
        .all()
    )
    # print(f'from erp filter \n{start_date} - {end_date}\n{erp} - {contract_type}\n LEN = {len(single_erp_inv_ids)}')
    return single_erp_inv_ids

def get_inv_groups_by_internal_id_and_dates(internal_id, start_date):

    records = (InvoiceGroup
                .query
                .join(SubContract,SubContract.invoice_group_id == InvoiceGroup.id)
                .join(Contract, Contract.id == SubContract.contract_id)
                .filter(Contract.internal_id == internal_id)
                .filter(SubContract.start_date <= start_date, SubContract.end_date > start_date)
                .all()
                )
    return records

def get_subcontacts_by_internal_id_and_start_date(internal_id, start_date):

    records = (SubContract
                .query
                .join(InvoiceGroup,InvoiceGroup.id == SubContract.invoice_group_id)
                .join(Contract, Contract.id == SubContract.contract_id)
                .filter(Contract.internal_id == internal_id)
                .filter(SubContract.start_date <= start_date, SubContract.end_date > start_date)
                .all()
                )
    return records

def get_subcontracts_by_inv_gr_name_and_date(invoice_group_name, start_date, end_date):

    records = (SubContract
                .query
                .join(InvoiceGroup,InvoiceGroup.id == SubContract.invoice_group_id)
                .filter(InvoiceGroup.name == invoice_group_name)
                .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))
                .all()
    )
    return records

def get_grid_itns_by_erp_for_period(erp_name, start_date, end_date):
    itn_records = (
        db.session
            .query(
                SubContract.itn                                
            )  
            .join(ItnMeta, ItnMeta.itn == SubContract.itn) 
            .join(Erp, Erp.id == ItnMeta.erp_id)
            .filter(Erp.name == erp_name)                  
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))                 
            .filter(SubContract.has_grid_services)            
            .distinct(SubContract.itn) 
            .all())
    grid_itns = [x[0] for x in itn_records]
    return grid_itns

def get_non_grid_itns_by_erp_for_period(erp_name, start_date, end_date):
    itn_records = (
        db.session
            .query(
                SubContract.itn                                
            )  
            .join(ItnMeta, ItnMeta.itn == SubContract.itn) 
            .join(Erp, Erp.id == ItnMeta.erp_id)
            .filter(Erp.name == erp_name)                  
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))                 
            .filter(~SubContract.has_grid_services)            
            .distinct(SubContract.itn) 
            .all())
    non_grid_itns = [x[0] for x in itn_records]
    return non_grid_itns

def get_incomming_grid_itns(erp_name, start_date, end_date):
    incomming_grid_itns = (
        db.session.query(
            IncomingItn.itn
        )
        .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        .join(Erp, Erp.id == ItnMeta.erp_id)  
        .filter(Erp.name == erp_name) 
        .filter(IncomingItn.as_grid == 1)       
        .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date)
        .all()

    )
    incomming_grid_itns = [x[0] for x in incomming_grid_itns]
    return incomming_grid_itns

def get_incomming_non_grid_itns(erp_name, start_date, end_date):
    incomming_non_grid_itns = (
        db.session.query(
            IncomingItn.itn
        )
        .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        .join(Erp, Erp.id == ItnMeta.erp_id)  
        .filter(Erp.name == erp_name) 
        .filter(or_(IncomingItn.as_grid == 0, IncomingItn.as_settelment == 1))       
        .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date)
        .all()

    )
    incomming_non_grid_itns = [x[0] for x in incomming_non_grid_itns]
    return incomming_non_grid_itns

def get_all_itns_by_erp_for_period(erp_name, start_date, end_date):
    itn_records = (
        db.session
            .query(
                SubContract.itn                                
            )  
            .join(ItnMeta, ItnMeta.itn == SubContract.itn) 
            .join(Erp, Erp.id == ItnMeta.erp_id)
            .filter(Erp.name == erp_name)                  
            .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date)))                        
            .distinct(SubContract.itn) 
            .all())
    all_itns = [x[0] for x in itn_records]
    return all_itns

def get_all_incomming_itns(erp_name, start_date, end_date):
    incomming_itns = (
        db.session.query(
            IncomingItn.itn
        )
        .join(ItnMeta, ItnMeta.itn == IncomingItn.itn) 
        .join(Erp, Erp.id == ItnMeta.erp_id)  
        .filter(Erp.name == erp_name)              
        .filter(IncomingItn.date >= start_date, IncomingItn.date <= end_date)
        .all()
    )
    incomming_itns = [x[0] for x in incomming_itns]
    return incomming_itns
