from app import app,db
from app.models import *
from sqlalchemy import func
from sqlalchemy.sql import label



@app.shell_context_processor
def make_shell_context():
    return {'db': db, 'Contractor': Contractor, 'Contract': Contract, 'ContractType': ContractType,'AddressMurs':AddressMurs,
    'Erp':Erp, 'ItnMeta':ItnMeta,'ErpInvoice':ErpInvoice, 'Distribution':Distribution,'Tech':Tech, 'MeasuringType':MeasuringType,
    'InvoiceGroup':InvoiceGroup,'SubContract':SubContract,'StpCoeffs':StpCoeffs,'ItnSchedule':ItnSchedule,'User':User,'ItnScheduleTemp':ItnScheduleTemp}