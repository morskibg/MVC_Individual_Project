"""mail_table2

Revision ID: 07612b4fc718
Revises: 9b081325b8f7
Create Date: 2020-11-12 22:27:06.493102

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '07612b4fc718'
down_revision = '9b081325b8f7'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_foreign_key(None, 'invoice_group', 'mail', ['email_id'], ['id'], onupdate='CASCADE', ondelete='CASCADE')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'invoice_group', type_='foreignkey')
    # ### end Alembic commands ###