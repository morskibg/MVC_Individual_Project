import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash
from app.models import *    
from app.helper_functions import update_or_insert, stringifyer, convert_date_to_utc

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


def get_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date):

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
                itns.c.akciz   
        )
         
        .join(itns, itns.c.sub_itn == ItnMeta.itn)
        .join(consumption_for_period_sub, consumption_for_period_sub.c.itn == ItnMeta.itn)        
        .outerjoin(grid_services_sub, grid_services_sub.c.itn_id == ItnMeta.itn)
        .join(ItnSchedule, ItnSchedule.itn == ItnMeta.itn)
        .outerjoin(AddressMurs,AddressMurs.id == ItnMeta.address_id)
        .join(Tariff, Tariff.id == ItnSchedule.tariff_id)         
        .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)
        .group_by(ItnMeta.itn,grid_services_sub.c.grid_services,consumption_for_period_sub.c.total_consumption,
                Tariff.price_day, AddressMurs.name, itns.c.zko, itns.c.akciz,itns.c.contractor_name,itns.c.invoice_group_description)
        .all()
            
    )
    return summary_records

def get_non_stp_itn_by_inv_group_for_period_sub(inv_group_name, start_date, end_date):

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

def get_summary_records_non_stp(non_stp_consumption_for_period_sub, grid_services_sub, non_stp_itns, start_date, end_date):

    summary_records = (
        db.session
            .query(ItnMeta.itn.label('Обект (ИТН №)'),

                grid_services_sub.c.grid_services.label('Мрежови услуги (лв.)'),
                non_stp_consumption_for_period_sub.c.total_consumption.label('Потребление (kWh)'),
                (non_stp_consumption_for_period_sub.c.total_consumption * Tariff.price_day).label('Сума за енергия'),
                (non_stp_consumption_for_period_sub.c.total_consumption * non_stp_itns.c.zko).label('Задължение към обществото'),
                (non_stp_consumption_for_period_sub.c.total_consumption * non_stp_itns.c.akciz).label('Акциз'),
                AddressMurs.name.label('Адрес'),            
        )
         
        .join(non_stp_itns, non_stp_itns.c.sub_itn == ItnMeta.itn)
        .join(non_stp_consumption_for_period_sub, non_stp_consumption_for_period_sub.c.itn == ItnMeta.itn)        
        .join(grid_services_sub, grid_services_sub.c.itn_id == ItnMeta.itn)
        .join(ItnSchedule, ItnSchedule.itn == ItnMeta.itn)
        .outerjoin(AddressMurs,AddressMurs.id == ItnMeta.address_id)
        .join(Tariff, Tariff.id == ItnSchedule.tariff_id)         
        .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)
        .group_by(ItnMeta.itn, grid_services_sub.c.grid_services, non_stp_consumption_for_period_sub.c.total_consumption,
                Tariff.price_day, AddressMurs.name, non_stp_itns.c.zko, non_stp_itns.c.akciz)
        .all()
            
    )
    return summary_records

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
                Tech.readings_difference.label('Корекция (квтч)'),
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
    



