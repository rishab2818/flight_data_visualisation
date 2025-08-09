from alembic import op
import sqlalchemy as sa

revision = '0001_init'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('users',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('username', sa.String(), nullable=False, unique=True),
        sa.Column('password_hash', sa.String(), nullable=False),
        sa.Column('role', sa.String(), default='user'),
        sa.Column('active', sa.Boolean(), default=True),
        sa.Column('created_at', sa.DateTime(timezone=True)))
    op.create_table('datasets',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('owner_id', sa.Integer()),
        sa.Column('project_id', sa.Integer()),
        sa.Column('name', sa.String()),
        sa.Column('original_filename', sa.String()),
        sa.Column('created_at', sa.DateTime(timezone=True)),
        sa.Column('raw_path', sa.String(), nullable=False),
        sa.Column('parquet_path', sa.String()),
        sa.Column('columns_json', sa.Text()),
        sa.Column('packet_count', sa.Integer()),
        sa.Column('plots_json', sa.Text()),
        sa.Column('tags_json', sa.Text()))
    op.create_table('jobs',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('user_id', sa.Integer()),
        sa.Column('dataset_id', sa.String()),
        sa.Column('status', sa.String()),
        sa.Column('progress', sa.Float()),
        sa.Column('message', sa.Text()),
        sa.Column('logs', sa.Text()),
        sa.Column('created_at', sa.DateTime(timezone=True)),
        sa.Column('finished_at', sa.DateTime(timezone=True)))
    op.create_table('projects',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(), unique=True, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True)))
    op.create_table('project_members',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('role', sa.String(), nullable=False))
    op.create_table('plot_presets',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('owner_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('description', sa.Text()),
        sa.Column('config_json', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True)))

def downgrade():
    op.drop_table('plot_presets')
    op.drop_table('project_members')
    op.drop_table('projects')
    op.drop_table('jobs')
    op.drop_table('datasets')
    op.drop_table('users')
