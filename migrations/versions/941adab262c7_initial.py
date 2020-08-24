"""initial

Revision ID: 941adab262c7
Revises: 
Create Date: 2020-08-11 19:46:15.128374

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '941adab262c7'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('address_murs',
    sa.Column('id', sa.SmallInteger(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=128), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name')
    )
    op.create_table('contract_type',
    sa.Column('id', sa.SmallInteger(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=16), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('contractor',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('parent_id', sa.Integer(), nullable=True),
    sa.Column('name', sa.String(length=128), nullable=False),
    sa.Column('eic', sa.String(length=32), nullable=False),
    sa.Column('address', sa.String(length=128), nullable=True),
    sa.Column('vat_number', sa.String(length=32), nullable=True),
    sa.Column('email', sa.String(length=128), nullable=True),
    sa.Column('acc_411', sa.String(length=16), nullable=False),
    sa.Column('last_updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('acc_411')
    )
    op.create_table('erp',
    sa.Column('id', sa.SmallInteger(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=8), nullable=False),
    sa.Column('code', sa.String(length=16), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('erp_invoice',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('number', sa.String(length=64), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=False),
    sa.Column('event', sa.String(length=128), nullable=True),
    sa.Column('correction_note', sa.String(length=64), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('number')
    )
    op.create_table('incoming_itn',
    sa.Column('itn', sa.String(length=33), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('itn', 'date')
    )
    op.create_table('measuring_type',
    sa.Column('id', sa.SmallInteger(), autoincrement=True, nullable=False),
    sa.Column('code', sa.String(length=16), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('user',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('username', sa.String(length=64), nullable=True),
    sa.Column('email', sa.String(length=120), nullable=True),
    sa.Column('password_hash', sa.String(length=128), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_user_email'), 'user', ['email'], unique=True)
    op.create_index(op.f('ix_user_username'), 'user', ['username'], unique=True)
    op.create_table('contract',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('internal_id', sa.String(length=32), nullable=False),
    sa.Column('contractor_id', sa.Integer(), nullable=False),
    sa.Column('subject', sa.String(length=128), nullable=True),
    sa.Column('parent_id', sa.Integer(), nullable=True),
    sa.Column('signing_date', sa.DateTime(), nullable=False),
    sa.Column('start_date', sa.DateTime(), nullable=True),
    sa.Column('end_date', sa.DateTime(), nullable=True),
    sa.Column('duration_in_days', sa.SmallInteger(), nullable=False),
    sa.Column('invoicing_interval', sa.SmallInteger(), nullable=False),
    sa.Column('maturity_interval', sa.SmallInteger(), nullable=False),
    sa.Column('contract_type_id', sa.SmallInteger(), nullable=False),
    sa.Column('is_work_days', sa.Boolean(), nullable=False),
    sa.Column('automatic_renewal_interval', sa.SmallInteger(), nullable=True),
    sa.Column('collateral_warranty', sa.String(length=256), nullable=True),
    sa.Column('notes', sa.String(length=512), nullable=True),
    sa.Column('last_updated', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['contract_type_id'], ['contract_type.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['contractor_id'], ['contractor.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('internal_id')
    )
    op.create_index(op.f('ix_contract_end_date'), 'contract', ['end_date'], unique=False)
    op.create_index(op.f('ix_contract_start_date'), 'contract', ['start_date'], unique=False)
    op.create_table('invoice_group',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=128), nullable=False),
    sa.Column('contractor_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['contractor_id'], ['contractor.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name')
    )
    op.create_table('itn_meta',
    sa.Column('itn', sa.String(length=33), nullable=False),
    sa.Column('description', sa.String(length=128), nullable=True),
    sa.Column('grid_voltage', sa.String(length=128), nullable=False),
    sa.Column('address_id', sa.SmallInteger(), nullable=False),
    sa.Column('erp_id', sa.SmallInteger(), nullable=False),
    sa.Column('is_virtual', sa.Boolean(), nullable=False),
    sa.Column('virtual_parent_itn', sa.String(length=33), nullable=True),
    sa.Column('last_updated', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['address_id'], ['address_murs.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['erp_id'], ['erp.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('itn')
    )
    op.create_table('stp_coeffs',
    sa.Column('utc', sa.DateTime(), nullable=False),
    sa.Column('measuring_type_id', sa.SmallInteger(), nullable=False),
    sa.Column('value', sa.Numeric(precision=9, scale=7), nullable=False),
    sa.ForeignKeyConstraint(['measuring_type_id'], ['measuring_type.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('utc', 'measuring_type_id')
    )
    op.create_table('distribution',
    sa.Column('itn', sa.String(length=33), nullable=False),
    sa.Column('start_date', sa.DateTime(), nullable=False),
    sa.Column('end_date', sa.DateTime(), nullable=False),
    sa.Column('tariff', sa.String(length=256), nullable=False),
    sa.Column('calc_amount', sa.Numeric(precision=12, scale=6), nullable=False),
    sa.Column('price', sa.Numeric(precision=10, scale=6), nullable=False),
    sa.Column('value', sa.Numeric(precision=10, scale=3), nullable=False),
    sa.Column('erp_invoice_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['erp_invoice_id'], ['erp_invoice.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['itn'], ['itn_meta.itn'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('itn', 'start_date', 'price', 'value', 'erp_invoice_id')
    )
    op.create_table('itn_schedule',
    sa.Column('itn', sa.String(length=33), nullable=False),
    sa.Column('utc', sa.DateTime(), nullable=False),
    sa.Column('forecast_vol', sa.Numeric(precision=12, scale=6), nullable=False),
    sa.Column('reported_vol', sa.Numeric(precision=12, scale=6), nullable=False),
    sa.Column('price', sa.Numeric(precision=8, scale=4), nullable=False),
    sa.ForeignKeyConstraint(['itn'], ['itn_meta.itn'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('itn', 'utc')
    )
    op.create_table('leaving_itn',
    sa.Column('itn', sa.String(length=33), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['itn'], ['itn_meta.itn'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('itn', 'date')
    )
    op.create_table('sub_contract',
    sa.Column('itn', sa.String(length=33), nullable=False),
    sa.Column('contract_id', sa.Integer(), nullable=False),
    sa.Column('object_name', sa.String(length=64), nullable=True),
    sa.Column('price', sa.Numeric(precision=6, scale=2), nullable=False),
    sa.Column('invoice_group_id', sa.Integer(), nullable=False),
    sa.Column('measuring_type_id', sa.SmallInteger(), nullable=False),
    sa.Column('start_date', sa.DateTime(), nullable=False),
    sa.Column('end_date', sa.DateTime(), nullable=False),
    sa.Column('zko', sa.Numeric(precision=6, scale=2), nullable=False),
    sa.Column('akciz', sa.Numeric(precision=6, scale=2), nullable=False),
    sa.Column('has_grid_services', sa.Boolean(), nullable=False),
    sa.Column('has_spot_price', sa.Boolean(), nullable=False),
    sa.Column('has_balancing', sa.Boolean(), nullable=False),
    sa.ForeignKeyConstraint(['contract_id'], ['contract.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['invoice_group_id'], ['invoice_group.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['itn'], ['itn_meta.itn'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['measuring_type_id'], ['measuring_type.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('itn', 'start_date', 'end_date')
    )
    op.create_table('tech',
    sa.Column('subscriber_number', sa.String(length=64), nullable=False),
    sa.Column('place_number', sa.String(length=16), nullable=False),
    sa.Column('customer_number', sa.String(length=16), nullable=False),
    sa.Column('itn', sa.String(length=33), nullable=False),
    sa.Column('electric_meter_number', sa.String(length=32), nullable=False),
    sa.Column('start_date', sa.DateTime(), nullable=False),
    sa.Column('end_date', sa.DateTime(), nullable=False),
    sa.Column('scale_number', sa.SmallInteger(), nullable=False),
    sa.Column('scale_code', sa.String(length=16), nullable=False),
    sa.Column('scale_type', sa.String(length=8), nullable=False),
    sa.Column('time_zone', sa.String(length=8), nullable=False),
    sa.Column('new_readings', sa.Numeric(precision=10, scale=3), nullable=False),
    sa.Column('old_readings', sa.Numeric(precision=10, scale=3), nullable=False),
    sa.Column('readings_difference', sa.Numeric(precision=10, scale=3), nullable=False),
    sa.Column('constant', sa.SmallInteger(), nullable=False),
    sa.Column('correction', sa.Integer(), nullable=False),
    sa.Column('storno', sa.Numeric(precision=10, scale=3), nullable=False),
    sa.Column('total_amount', sa.Numeric(precision=10, scale=3), nullable=False),
    sa.Column('erp_invoice_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['erp_invoice_id'], ['erp_invoice.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['itn'], ['itn_meta.itn'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('itn', 'start_date', 'scale_code', 'new_readings', 'correction', 'erp_invoice_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('tech')
    op.drop_table('sub_contract')
    op.drop_table('leaving_itn')
    op.drop_table('itn_schedule')
    op.drop_table('distribution')
    op.drop_table('stp_coeffs')
    op.drop_table('itn_meta')
    op.drop_table('invoice_group')
    op.drop_index(op.f('ix_contract_start_date'), table_name='contract')
    op.drop_index(op.f('ix_contract_end_date'), table_name='contract')
    op.drop_table('contract')
    op.drop_index(op.f('ix_user_username'), table_name='user')
    op.drop_index(op.f('ix_user_email'), table_name='user')
    op.drop_table('user')
    op.drop_table('measuring_type')
    op.drop_table('incoming_itn')
    op.drop_table('erp_invoice')
    op.drop_table('erp')
    op.drop_table('contractor')
    op.drop_table('contract_type')
    op.drop_table('address_murs')
    # ### end Alembic commands ###
