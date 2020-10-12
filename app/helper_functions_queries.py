import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash
from app.models import *    
from app.helper_functions import update_or_insert, stringifyer, convert_date_to_utc

MONEY_ROUND = 2
ENERGY_ROUND = 3




def get_inv_group_itn_sub_query(invoicing_group_name, period_start_date, period_end_date):

    inv_group_itn_sub_query = (
        db.session.query(
            ItnMeta.itn.label('sub_itn'),
            SubContract.zko.label('zko'),
            SubContract.akciz.label('akciz'),                
            Contractor.name.label('contractor_name'),
            Contractor.eic.label('contractor_eic')
        )
        .join(SubContract, SubContract.itn == ItnMeta.itn)
        .join(InvoiceGroup)
        .join(Contractor)
        .filter(SubContract.start_date <= period_start_date, SubContract.end_date >= period_end_date)
        .filter(InvoiceGroup.name == invoicing_group_name)
        .subquery()
    )
    return inv_group_itn_sub_query


def get_single_tariff_consumption_records_sub(inv_group_itn_sub_query, period_start_date, period_end_date):

    single_tariff_consumption_records_sub = (
        db.session.query(
            ItnSchedule.itn.label('itn_single'),
            func.round(func.sum(ItnSchedule.consumption_vol), ENERGY_ROUND).label('single_tariff_consumption'),
            ) 
            .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == ItnSchedule.itn)           
            .filter(ItnSchedule.utc >= period_start_date, ItnSchedule.utc <= period_end_date)       
            .group_by(ItnSchedule.itn)                  
            .subquery()
    )
    return single_tariff_consumption_records_sub


def get_grid_services_tech_records(inv_group_itn_sub_query, invoice_start_date, invoice_end_date):

    grid_services_tech_records = (
            db.session.query(
                Tech.subscriber_number.label('Абонат №'),                
                AddressMurs.name.label('А д р е с'),
                inv_group_itn_sub_query.c.contractor_name.label('Име на клиент'),
                inv_group_itn_sub_query.c.contractor_eic.label('ЕГН/ЕИК'),
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
            .join(inv_group_itn_sub_query,inv_group_itn_sub_query.c.sub_itn == Tech.itn)  
            .join(ItnMeta, ItnMeta.itn == Tech.itn)
            .join(Distribution, Distribution.itn == Tech.itn) 
            .join(ErpInvoice, ErpInvoice.id == Tech.erp_invoice_id)          
            .join(AddressMurs,AddressMurs.id == ItnMeta.address_id)                 
            .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)  
            .distinct(Distribution.tariff)
            .all()
            )
    return grid_services_tech_records


def get_grid_services_distrib_records(inv_group_itn_sub_query, invoice_start_date, invoice_end_date):

    grid_services_distrib_records = (
        db.session.query(
            Tech.subscriber_number.label('Абонат №'),                
            AddressMurs.name.label('А д р е с'),
            inv_group_itn_sub_query.c.contractor_name.label('Име на клиент'),
            inv_group_itn_sub_query.c.contractor_eic.label('ЕГН/ЕИК'),
            Tech.itn.label('Идентификационен код'),
            # Tech.electric_meter_number.label('Електромер №'),
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
        .join(inv_group_itn_sub_query,inv_group_itn_sub_query.c.sub_itn == Tech.itn)
        .join(Distribution,Distribution.itn == Tech.itn)
        .join(ItnMeta,ItnMeta.itn == Tech.itn)
        .join(AddressMurs,AddressMurs.id == ItnMeta.address_id)            
        .join(ErpInvoice, ErpInvoice.id == Distribution.erp_invoice_id)
        .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)  
        .distinct(Distribution.tariff)
        .all()
        )
    return grid_services_distrib_records


def get_grid_service_sub_query(inv_group_itn_sub_query, invoice_start_date, invoice_end_date):

    grid_service_sub_query = (
            db.session.query(
                Distribution.itn.label('itn_id'),
                func.round(func.sum(Distribution.value), MONEY_ROUND).label('grid_services')                
            )
            .join(inv_group_itn_sub_query, inv_group_itn_sub_query.c.sub_itn == Distribution.itn)
            .join(ErpInvoice,ErpInvoice.id == Distribution.erp_invoice_id)
            .filter(ErpInvoice.date >= invoice_start_date, ErpInvoice.date <= invoice_end_date)
            .group_by(Distribution.itn)
            .subquery()
        )    
    return grid_service_sub_query


