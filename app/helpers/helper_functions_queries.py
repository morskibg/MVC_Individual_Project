import sys, pytz, datetime as dt
import pandas as pd
import os
import xlrd
import time,re
from decimal import Decimal
from flask import  flash

from app.models import *    
from app.helpers.helper_functions import update_or_insert, stringifyer, convert_date_to_utc

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

# def get_spot_itns(inv_group_name, start_date, end_date):

#     itn_records = (
#         db.session
#             .query(
#                 SubContract.itn.label('sub_itn'), 
#                 SubContract.zko.label('zko'),
#                 SubContract.akciz.label('akciz') , 
#                 Contractor.name.label('contractor_name'), 
#                 InvoiceGroup.description.label('invoice_group_description'), 
#                 InvoiceGroup.name.label('invoice_group_name')       
#             )
#             .join(InvoiceGroup, InvoiceGroup.id == SubContract.invoice_group_id) 
#             .join(MeasuringType)  
#             .join(Contractor)                    
#             .filter(~((SubContract.start_date > end_date) | (SubContract.end_date < start_date))) 
#             .filter(SubContract.has_spot_price) #!!!!!!!!!!!!!!!!!!!!!!
#             .filter(InvoiceGroup.name == inv_group_name)            
#             .distinct(SubContract.itn) 
#             .subquery())

#     return itn_records

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
            # .filter(Erp.name == erp_name)             
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

# def get_tariff_limits(inv_group_name, start_date, end_date):

#     itns = [x[0] for x in db.session.query(get_spot_itns(inv_group_name, start_date, end_date)).all()]
    
#     tariff_records = (
#         db.session
#             .query(Tariff.lower_limit,Tariff.upper_limit)            
#             .filter(ItnSchedule.itn.in_(itns))
#             .filter(ItnSchedule.utc >= start_date, ItnSchedule.utc <= end_date)  
#             .group_by(Tariff.lower_limit,Tariff.upper_limit)          
#             .all()
#     )
    
#     return tariff_records

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
