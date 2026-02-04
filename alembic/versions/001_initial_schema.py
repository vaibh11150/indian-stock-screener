"""Initial schema for Indian Stock Screener database.

Revision ID: 001
Revises:
Create Date: 2025-02-04

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create enums
    statement_type = postgresql.ENUM('profit_loss', 'balance_sheet', 'cash_flow', name='statement_type')
    result_nature = postgresql.ENUM('standalone', 'consolidated', name='result_nature')
    period_type = postgresql.ENUM('quarterly', 'half_yearly', 'nine_months', 'annual', name='period_type')

    statement_type.create(op.get_bind(), checkfirst=True)
    result_nature.create(op.get_bind(), checkfirst=True)
    period_type.create(op.get_bind(), checkfirst=True)

    # Companies table
    op.create_table(
        'companies',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('nse_symbol', sa.String(20), nullable=True),
        sa.Column('bse_scrip_code', sa.String(10), nullable=True),
        sa.Column('isin', sa.String(12), nullable=False),
        sa.Column('company_name', sa.String(255), nullable=False),
        sa.Column('industry', sa.String(100), nullable=True),
        sa.Column('sector', sa.String(100), nullable=True),
        sa.Column('bse_group', sa.String(5), nullable=True),
        sa.Column('listing_date', sa.Date(), nullable=True),
        sa.Column('face_value', sa.Numeric(10, 2), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('nse_symbol'),
        sa.UniqueConstraint('bse_scrip_code'),
        sa.UniqueConstraint('isin'),
    )
    op.create_index('idx_companies_nse', 'companies', ['nse_symbol'])
    op.create_index('idx_companies_bse', 'companies', ['bse_scrip_code'])
    op.create_index('idx_companies_isin', 'companies', ['isin'])

    # Financial statements table
    op.create_table(
        'financial_statements',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('statement_type', sa.Enum('profit_loss', 'balance_sheet', 'cash_flow', name='statement_type'), nullable=False),
        sa.Column('result_nature', sa.Enum('standalone', 'consolidated', name='result_nature'), nullable=False),
        sa.Column('period_type', sa.Enum('quarterly', 'half_yearly', 'nine_months', 'annual', name='period_type'), nullable=False),
        sa.Column('period_start', sa.Date(), nullable=False),
        sa.Column('period_end', sa.Date(), nullable=False),
        sa.Column('fiscal_year', sa.String(7), nullable=False),
        sa.Column('fiscal_quarter', sa.SmallInteger(), nullable=True),
        sa.Column('is_audited', sa.Boolean(), default=False),
        sa.Column('source', sa.String(20), nullable=False),
        sa.Column('source_url', sa.Text(), nullable=True),
        sa.Column('filing_date', sa.Date(), nullable=True),
        sa.Column('scraped_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('company_id', 'statement_type', 'result_nature', 'period_type', 'period_end', name='uq_financial_statement'),
    )
    op.create_index('idx_fs_company_period', 'financial_statements', ['company_id', 'period_end'])
    op.create_index('idx_fs_fiscal', 'financial_statements', ['company_id', 'fiscal_year', 'fiscal_quarter'])

    # Financial line items table
    op.create_table(
        'financial_line_items',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('statement_id', sa.BigInteger(), nullable=False),
        sa.Column('field_name', sa.String(100), nullable=False),
        sa.Column('field_value', sa.Numeric(20, 2), nullable=True),
        sa.Column('display_order', sa.SmallInteger(), nullable=True),
        sa.ForeignKeyConstraint(['statement_id'], ['financial_statements.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('statement_id', 'field_name', name='uq_line_item'),
    )
    op.create_index('idx_fli_statement', 'financial_line_items', ['statement_id'])
    op.create_index('idx_fli_field', 'financial_line_items', ['field_name'])
    # Covering index for fast lookups including field_value
    op.execute(
        "CREATE INDEX idx_fli_lookup ON financial_line_items (statement_id, field_name) "
        "INCLUDE (field_value)"
    )

    # Raw filings table
    op.create_table(
        'raw_filings',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('source', sa.String(20), nullable=False),
        sa.Column('filing_type', sa.String(20), nullable=False),
        sa.Column('period_end', sa.Date(), nullable=False),
        sa.Column('raw_content', sa.LargeBinary(), nullable=True),
        sa.Column('parsed_json', postgresql.JSONB(), nullable=True),
        sa.Column('checksum', sa.String(64), nullable=True),
        sa.Column('scraped_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('company_id', 'source', 'filing_type', 'period_end', name='uq_raw_filing'),
    )

    # Daily prices table
    op.create_table(
        'daily_prices',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('trade_date', sa.Date(), nullable=False),
        sa.Column('open_price', sa.Numeric(12, 2), nullable=True),
        sa.Column('high_price', sa.Numeric(12, 2), nullable=True),
        sa.Column('low_price', sa.Numeric(12, 2), nullable=True),
        sa.Column('close_price', sa.Numeric(12, 2), nullable=True),
        sa.Column('adj_close', sa.Numeric(12, 2), nullable=True),
        sa.Column('volume', sa.BigInteger(), nullable=True),
        sa.Column('delivery_qty', sa.BigInteger(), nullable=True),
        sa.Column('source', sa.String(10), default='nse'),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('company_id', 'trade_date', name='uq_daily_price'),
    )
    op.create_index('idx_prices_company_date', 'daily_prices', ['company_id', 'trade_date'])

    # Corporate actions table
    op.create_table(
        'corporate_actions',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('action_type', sa.String(20), nullable=False),
        sa.Column('ex_date', sa.Date(), nullable=True),
        sa.Column('record_date', sa.Date(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('ratio_from', sa.Numeric(10, 4), nullable=True),
        sa.Column('ratio_to', sa.Numeric(10, 4), nullable=True),
        sa.Column('amount', sa.Numeric(12, 4), nullable=True),
        sa.Column('source', sa.String(10), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
        sa.PrimaryKeyConstraint('id'),
    )

    # Shareholding patterns table
    op.create_table(
        'shareholding_patterns',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('quarter_end', sa.Date(), nullable=False),
        sa.Column('promoter_pct', sa.Numeric(6, 2), nullable=True),
        sa.Column('fii_pct', sa.Numeric(6, 2), nullable=True),
        sa.Column('dii_pct', sa.Numeric(6, 2), nullable=True),
        sa.Column('public_pct', sa.Numeric(6, 2), nullable=True),
        sa.Column('govt_pct', sa.Numeric(6, 2), nullable=True),
        sa.Column('pledged_pct', sa.Numeric(6, 2), nullable=True),
        sa.Column('total_shares', sa.BigInteger(), nullable=True),
        sa.Column('source', sa.String(10), nullable=True),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('company_id', 'quarter_end', name='uq_shareholding'),
    )

    # Computed ratios table
    op.create_table(
        'computed_ratios',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('period_end', sa.Date(), nullable=False),
        sa.Column('period_type', sa.Enum('quarterly', 'half_yearly', 'nine_months', 'annual', name='period_type', create_constraint=False), nullable=False),
        sa.Column('is_ttm', sa.Boolean(), default=False),
        sa.Column('result_nature', sa.Enum('standalone', 'consolidated', name='result_nature', create_constraint=False), nullable=False),
        sa.Column('market_cap', sa.Numeric(20, 2), nullable=True),
        sa.Column('pe_ratio', sa.Numeric(10, 2), nullable=True),
        sa.Column('pb_ratio', sa.Numeric(10, 2), nullable=True),
        sa.Column('ev', sa.Numeric(20, 2), nullable=True),
        sa.Column('ev_ebitda', sa.Numeric(10, 2), nullable=True),
        sa.Column('dividend_yield', sa.Numeric(6, 2), nullable=True),
        sa.Column('roe', sa.Numeric(8, 2), nullable=True),
        sa.Column('roce', sa.Numeric(8, 2), nullable=True),
        sa.Column('roa', sa.Numeric(8, 2), nullable=True),
        sa.Column('operating_margin', sa.Numeric(8, 2), nullable=True),
        sa.Column('net_margin', sa.Numeric(8, 2), nullable=True),
        sa.Column('asset_turnover', sa.Numeric(8, 2), nullable=True),
        sa.Column('inventory_days', sa.Numeric(8, 2), nullable=True),
        sa.Column('receivable_days', sa.Numeric(8, 2), nullable=True),
        sa.Column('payable_days', sa.Numeric(8, 2), nullable=True),
        sa.Column('cash_conversion_cycle', sa.Numeric(8, 2), nullable=True),
        sa.Column('debt_equity', sa.Numeric(10, 2), nullable=True),
        sa.Column('current_ratio', sa.Numeric(8, 2), nullable=True),
        sa.Column('interest_coverage', sa.Numeric(10, 2), nullable=True),
        sa.Column('revenue_growth', sa.Numeric(8, 2), nullable=True),
        sa.Column('profit_growth', sa.Numeric(8, 2), nullable=True),
        sa.Column('eps', sa.Numeric(12, 2), nullable=True),
        sa.Column('book_value_per_share', sa.Numeric(12, 2), nullable=True),
        sa.Column('computed_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('company_id', 'period_end', 'period_type', 'is_ttm', 'result_nature', name='uq_computed_ratio'),
    )
    op.create_index('idx_ratios_screen', 'computed_ratios', ['period_type', 'is_ttm'])

    # Quality checks table
    op.create_table(
        'quality_checks',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('check_date', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('field_name', sa.String(100), nullable=True),
        sa.Column('our_value', sa.Numeric(20, 2), nullable=True),
        sa.Column('reference_value', sa.Numeric(20, 2), nullable=True),
        sa.Column('reference_source', sa.String(50), nullable=True),
        sa.Column('pct_deviation', sa.Numeric(8, 4), nullable=True),
        sa.Column('is_acceptable', sa.Boolean(), nullable=True),
        sa.Column('period_end', sa.Date(), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_qc_company', 'quality_checks', ['company_id', 'check_date'])
    # Partial index for quickly finding unacceptable quality checks
    op.execute(
        "CREATE INDEX idx_qc_unacceptable ON quality_checks (company_id, field_name) "
        "WHERE NOT is_acceptable"
    )

    # Scrape log table
    op.create_table(
        'scrape_log',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('scraper_name', sa.String(50), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(20), nullable=False),
        sa.Column('records_scraped', sa.Integer(), default=0),
        sa.Column('records_inserted', sa.Integer(), default=0),
        sa.Column('records_updated', sa.Integer(), default=0),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('duration_seconds', sa.Numeric(8, 2), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('finished_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    op.drop_table('scrape_log')
    op.drop_table('quality_checks')
    op.drop_table('computed_ratios')
    op.drop_table('shareholding_patterns')
    op.drop_table('corporate_actions')
    op.drop_table('daily_prices')
    op.drop_table('raw_filings')
    op.drop_table('financial_line_items')
    op.drop_table('financial_statements')
    op.drop_table('companies')

    # Drop enums
    op.execute('DROP TYPE IF EXISTS period_type')
    op.execute('DROP TYPE IF EXISTS result_nature')
    op.execute('DROP TYPE IF EXISTS statement_type')
