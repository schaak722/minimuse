from datetime import datetime, timedelta

from flask import abort, Blueprint, current_app, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from sqlalchemy import text

from ..decorators import require_admin
from ..extensions import db
from ..models import User
from .forms import UserCreateForm, UserEditForm


admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _require_db_patch_enabled():
    """Hard-gate the DB patch executor.

    This endpoint performs schema mutation and must be disabled by default.
    It should be available only when ALLOW_DB_PATCH=1 is explicitly set.
    """
    if str(current_app.config.get("ALLOW_DB_PATCH", "0")) != "1":
        abort(404)


# -----------------------
# Users
# -----------------------

@admin_bp.get("/users")
@login_required
@require_admin
def users_list():
    q = (request.args.get("q") or "").strip().lower()

    query = User.query
    if q:
        query = query.filter(
            db.or_(
                db.func.lower(User.email).contains(q),
                db.func.lower(User.name).contains(q),
                db.func.lower(User.role).contains(q),
            )
        )

    users = query.order_by(User.created_at.desc()).all()
    return render_template("admin/users_list.html", users=users, q=q)


@admin_bp.get("/users/new")
@admin_bp.post("/users/new")
@login_required
@require_admin
def users_new():
    form = UserCreateForm()
    if form.validate_on_submit():
        email = form.email.data.strip().lower()
        if User.query.filter_by(email=email).first():
            flash("A user with that email already exists.", "danger")
            return render_template("admin/user_form.html", form=form, mode="create")

        user = User(
            email=email,
            name=form.name.data.strip(),
            role=form.role.data,
            is_active=bool(form.is_active.data),
        )
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()

        flash("User created.", "success")
        return redirect(url_for("admin.users_list"))  # blueprint endpoint

    return render_template("admin/user_form.html", form=form, mode="create")


@admin_bp.get("/users/<int:user_id>/edit")
@admin_bp.post("/users/<int:user_id>/edit")
@login_required
@require_admin
def users_edit(user_id: int):
    user = db.session.get(User, user_id)
    if not user:
        flash("User not found.", "danger")
        return redirect(url_for("admin.users_list"))

    form = UserEditForm(obj=user)

    if form.validate_on_submit():
        user.name = form.name.data.strip()
        user.role = form.role.data
        user.is_active = bool(form.is_active.data)

        if form.new_password.data:
            user.set_password(form.new_password.data)

        # prevent admin from locking themselves out accidentally
        if user.id == current_user.id and user.is_active is False:
            flash("You cannot deactivate your own account.", "danger")
            db.session.rollback()
            return render_template("admin/user_form.html", form=form, mode="edit", user=user)

        db.session.commit()
        flash("User updated.", "success")
        return redirect(url_for("admin.users_list"))

    return render_template("admin/user_form.html", form=form, mode="edit", user=user)


# -----------------------
# Schema Check (read-only)
# -----------------------

_EXPECTED_TABLES = [
    "users",
    "saved_searches",
    "items",
    "purchase_orders",
    "purchase_lines",
    "import_batches",
    "sales_orders",
    "sales_lines",
    "daily_metrics",
    "sku_metrics_daily",
    "app_state",
]

# Minimal column expectations (aligned with app/models.py).
# Types are Postgres-friendly SQL fragments.
_EXPECTED_COLUMNS = {
    "users": {
        "id": "INTEGER",
        "email": "VARCHAR(255)",
        "name": "VARCHAR(120)",
        "password_hash": "VARCHAR(255)",
        "role": "VARCHAR(20)",
        "is_active": "BOOLEAN",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    "saved_searches": {
        "id": "INTEGER",
        "user_id": "INTEGER",
        "context": "VARCHAR(30)",
        "name": "VARCHAR(80)",
        "url": "VARCHAR(600)",
        "created_at": "TIMESTAMP",
    },
    "items": {
        "id": "INTEGER",
        "sku": "VARCHAR(80)",
        "description": "VARCHAR(255)",
        "brand": "VARCHAR(80)",
        "supplier": "VARCHAR(120)",
        "colour": "VARCHAR(80)",
        "size": "VARCHAR(40)",
        "weight": "NUMERIC(10,3)",
        "vat_rate": "NUMERIC(5,2)",
        "is_active": "BOOLEAN",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    "purchase_orders": {
        "id": "INTEGER",
        "supplier_name": "VARCHAR(120)",
        "brand": "VARCHAR(80)",
        "order_number": "VARCHAR(80)",
        "order_date": "DATE",
        "arrival_date": "DATE",
        "currency": "VARCHAR(10)",
        "freight_total": "NUMERIC(12,2)",
        "allocation_method": "VARCHAR(20)",
        "created_at": "TIMESTAMP",
    },
    "purchase_lines": {
        "id": "INTEGER",
        "purchase_order_id": "INTEGER",
        "item_id": "INTEGER",
        "sku": "VARCHAR(80)",
        "description": "VARCHAR(255)",
        "colour": "VARCHAR(80)",
        "size": "VARCHAR(40)",
        "qty": "INTEGER",
        "unit_cost_net": "NUMERIC(12,4)",
        "packaging_per_unit": "NUMERIC(12,4)",
        "freight_allocated_total": "NUMERIC(12,4)",
        "freight_allocated_per_unit": "NUMERIC(12,4)",
        "landed_unit_cost": "NUMERIC(12,4)",
        "created_at": "TIMESTAMP",
    },
    "import_batches": {
        "id": "INTEGER",
        "kind": "VARCHAR(40)",
        "filename": "VARCHAR(255)",
        "payload": "JSONB",
        "created_at": "TIMESTAMP",
    },
    "sales_orders": {
        "id": "INTEGER",
        "order_number": "VARCHAR(80)",
        "order_date": "DATE",
        "channel": "VARCHAR(40)",
        "currency": "VARCHAR(10)",
        "customer_name": "VARCHAR(120)",
        "customer_email": "VARCHAR(255)",
        "shipping_charged_gross": "NUMERIC(12,2)",
        "order_discount_gross": "NUMERIC(12,2)",
        "created_at": "TIMESTAMP",
    },
    "sales_lines": {
        "id": "INTEGER",
        "sales_order_id": "INTEGER",
        "item_id": "INTEGER",
        "sku": "VARCHAR(80)",
        "description": "VARCHAR(255)",
        "qty": "INTEGER",
        "unit_price_gross": "NUMERIC(12,4)",
        "line_discount_gross": "NUMERIC(12,4)",
        "order_discount_alloc_gross": "NUMERIC(12,4)",
        "vat_rate": "NUMERIC(5,2)",
        "unit_price_net": "NUMERIC(12,4)",
        "revenue_net": "NUMERIC(12,4)",
        "cost_method": "VARCHAR(20)",
        "unit_cost_basis": "NUMERIC(12,4)",
        "cost_total": "NUMERIC(12,4)",
        "profit": "NUMERIC(12,4)",
        "cost_source_po_id": "INTEGER",
        "created_at": "TIMESTAMP",
    },
    "daily_metrics": {
        "id": "INTEGER",
        "metric_date": "DATE",
        "orders_count": "INTEGER",
        "units": "INTEGER",
        "revenue_net": "NUMERIC(14,4)",
        "cogs": "NUMERIC(14,4)",
        "profit": "NUMERIC(14,4)",
        "discount_gross": "NUMERIC(14,4)",
        "discount_net": "NUMERIC(14,4)",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    "sku_metrics_daily": {
        "id": "INTEGER",
        "metric_date": "DATE",
        "sku": "VARCHAR(80)",
        "units": "INTEGER",
        "revenue_net": "NUMERIC(14,4)",
        "profit": "NUMERIC(14,4)",
        "discount_gross": "NUMERIC(14,4)",
        "discount_net": "NUMERIC(14,4)",
        "created_at": "TIMESTAMP",
    },
    "app_state": {
        "id": "INTEGER",
        "key": "VARCHAR(80)",
        "value": "VARCHAR(255)",
        "updated_at": "TIMESTAMP",
    },
}

_EXPECTED_INDEX_NAMES = [
    "ix_users_email",
    "ix_items_sku",
    "ix_purchase_orders_order_number",
    "ix_purchase_lines_sku",
    "ix_sales_lines_sku",
    "ix_daily_metrics_metric_date",
    "ix_sku_metrics_daily_metric_date",
    "ix_sku_metrics_daily_sku",
    "ix_app_state_key",
    "ix_sales_orders_order_date",
    "ix_sales_orders_channel",
    "ix_sales_lines_sales_order_id",
    "ix_sales_lines_item_id",
]

_EXPECTED_CONSTRAINT_NAMES = [
    "uq_saved_search_user_context_name",
    "uq_sales_orders_channel_order_number",
    "uq_sku_metrics_daily_date_sku",
]


@admin_bp.get("/schema-check")
@login_required
@require_admin
def schema_check():
    existing_tables = {
        r[0]
        for r in db.session.execute(
            text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
        ).fetchall()
    }

    table_rows = [{"name": t, "present": t in existing_tables} for t in _EXPECTED_TABLES]

    existing_cols = {
        (r[0], r[1])
        for r in db.session.execute(
            text("""
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
            """)
        ).fetchall()
    }

    column_rows = []
    for t, cols in _EXPECTED_COLUMNS.items():
        for c, ctype in cols.items():
            present = (t, c) in existing_cols
            sql = None
            if not present and t in existing_tables:
                sql = f"ALTER TABLE {t} ADD COLUMN {c} {ctype};"
            column_rows.append({"table": t, "name": c, "type": ctype, "present": present, "sql": sql})

    existing_indexes = {
        r[0]
        for r in db.session.execute(
            text("""
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
            """)
        ).fetchall()
    }
    index_rows = [{"name": ix, "present": ix in existing_indexes} for ix in _EXPECTED_INDEX_NAMES]

    existing_constraints = {
        r[0]
        for r in db.session.execute(text("SELECT conname FROM pg_constraint")).fetchall()
    }
    constraint_rows = [{"name": cn, "present": cn in existing_constraints} for cn in _EXPECTED_CONSTRAINT_NAMES]

    # Create-table suggestions (only shown when table is missing)
    create_sql = {
        "users": """
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  name VARCHAR(120) NOT NULL DEFAULT 'User',
  password_hash VARCHAR(255) NOT NULL,
  role VARCHAR(20) NOT NULL DEFAULT 'user',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email ON users(email);
CREATE INDEX IF NOT EXISTS ix_users_email ON users(email);
""".strip(),
        "saved_searches": """
CREATE TABLE saved_searches (
  id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  context VARCHAR(30) NOT NULL,
  name VARCHAR(80) NOT NULL,
  url VARCHAR(600) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_saved_search_user_context_name UNIQUE (user_id, context, name)
);
CREATE INDEX IF NOT EXISTS ix_saved_searches_user_id ON saved_searches(user_id);
CREATE INDEX IF NOT EXISTS ix_saved_searches_context ON saved_searches(context);
""".strip(),
        "items": """
CREATE TABLE items (
  id SERIAL PRIMARY KEY,
  sku VARCHAR(80) NOT NULL,
  description VARCHAR(255) NOT NULL,
  brand VARCHAR(80),
  supplier VARCHAR(120),
  colour VARCHAR(80),
  size VARCHAR(40),
  weight NUMERIC(10,3),
  vat_rate NUMERIC(5,2) NOT NULL DEFAULT 18.00,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_items_sku ON items(sku);
CREATE INDEX IF NOT EXISTS ix_items_sku ON items(sku);
""".strip(),
        "purchase_orders": """
CREATE TABLE purchase_orders (
  id SERIAL PRIMARY KEY,
  supplier_name VARCHAR(120),
  brand VARCHAR(80),
  order_number VARCHAR(80) NOT NULL,
  order_date DATE,
  arrival_date DATE,
  currency VARCHAR(10) NOT NULL DEFAULT 'EUR',
  freight_total NUMERIC(12,2),
  allocation_method VARCHAR(20) NOT NULL DEFAULT 'value',
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_purchase_orders_order_number ON purchase_orders(order_number);
""".strip(),
        "purchase_lines": """
CREATE TABLE purchase_lines (
  id SERIAL PRIMARY KEY,
  purchase_order_id INTEGER NOT NULL REFERENCES purchase_orders(id) ON DELETE CASCADE,
  item_id INTEGER NOT NULL REFERENCES items(id),
  sku VARCHAR(80) NOT NULL,
  description VARCHAR(255),
  colour VARCHAR(80),
  size VARCHAR(40),
  qty INTEGER NOT NULL DEFAULT 0,
  unit_cost_net NUMERIC(12,4) NOT NULL DEFAULT 0.0000,
  packaging_per_unit NUMERIC(12,4),
  freight_allocated_total NUMERIC(12,4),
  freight_allocated_per_unit NUMERIC(12,4),
  landed_unit_cost NUMERIC(12,4),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_purchase_lines_sku ON purchase_lines(sku);
""".strip(),
        "import_batches": """
CREATE TABLE import_batches (
  id SERIAL PRIMARY KEY,
  kind VARCHAR(40) NOT NULL DEFAULT 'purchase_import',
  filename VARCHAR(255),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_import_batches_kind ON import_batches(kind);
""".strip(),
        "sales_orders": """
CREATE TABLE sales_orders (
  id SERIAL PRIMARY KEY,
  order_number VARCHAR(80) NOT NULL,
  order_date DATE NOT NULL,
  channel VARCHAR(40) NOT NULL DEFAULT 'unknown',
  currency VARCHAR(10) NOT NULL DEFAULT 'EUR',
  customer_name VARCHAR(120),
  customer_email VARCHAR(255),
  shipping_charged_gross NUMERIC(12,2),
  order_discount_gross NUMERIC(12,2),
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_sales_orders_channel_order_number UNIQUE (channel, order_number)
);
CREATE INDEX IF NOT EXISTS ix_sales_orders_order_date ON sales_orders(order_date);
CREATE INDEX IF NOT EXISTS ix_sales_orders_channel ON sales_orders(channel);
""".strip(),
        "sales_lines": """
CREATE TABLE sales_lines (
  id SERIAL PRIMARY KEY,
  sales_order_id INTEGER NOT NULL REFERENCES sales_orders(id) ON DELETE CASCADE,
  item_id INTEGER NOT NULL REFERENCES items(id),
  sku VARCHAR(80) NOT NULL,
  description VARCHAR(255),
  qty INTEGER NOT NULL DEFAULT 0,
  unit_price_gross NUMERIC(12,4) NOT NULL DEFAULT 0.0000,
  line_discount_gross NUMERIC(12,4),
  order_discount_alloc_gross NUMERIC(12,4),
  vat_rate NUMERIC(5,2) NOT NULL DEFAULT 18.00,
  unit_price_net NUMERIC(12,4),
  revenue_net NUMERIC(12,4),
  cost_method VARCHAR(20) NOT NULL DEFAULT 'weighted_avg',
  unit_cost_basis NUMERIC(12,4),
  cost_total NUMERIC(12,4),
  profit NUMERIC(12,4),
  cost_source_po_id INTEGER,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_sales_lines_sku ON sales_lines(sku);
CREATE INDEX IF NOT EXISTS ix_sales_lines_sales_order_id ON sales_lines(sales_order_id);
CREATE INDEX IF NOT EXISTS ix_sales_lines_item_id ON sales_lines(item_id);
""".strip(),
        "daily_metrics": """
CREATE TABLE daily_metrics (
  id SERIAL PRIMARY KEY,
  metric_date DATE NOT NULL UNIQUE,
  orders_count INTEGER NOT NULL DEFAULT 0,
  units INTEGER NOT NULL DEFAULT 0,
  revenue_net NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  cogs NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  profit NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  discount_gross NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  discount_net NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_daily_metrics_metric_date ON daily_metrics(metric_date);
""".strip(),
        "sku_metrics_daily": """
CREATE TABLE sku_metrics_daily (
  id SERIAL PRIMARY KEY,
  metric_date DATE NOT NULL,
  sku VARCHAR(80) NOT NULL,
  units INTEGER NOT NULL DEFAULT 0,
  revenue_net NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  profit NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  discount_gross NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  discount_net NUMERIC(14,4) NOT NULL DEFAULT 0.0000,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_sku_metrics_daily_date_sku UNIQUE (metric_date, sku)
);
CREATE INDEX IF NOT EXISTS ix_sku_metrics_daily_metric_date ON sku_metrics_daily(metric_date);
CREATE INDEX IF NOT EXISTS ix_sku_metrics_daily_sku ON sku_metrics_daily(sku);
""".strip(),
        "app_state": """
CREATE TABLE app_state (
  id SERIAL PRIMARY KEY,
  key VARCHAR(80) NOT NULL UNIQUE,
  value VARCHAR(255),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_app_state_key ON app_state(key);
""".strip(),
    }

    return render_template(
        "admin/schema_check.html",
        table_rows=table_rows,
        column_rows=column_rows,
        index_rows=index_rows,
        constraint_rows=constraint_rows,
        create_sql=create_sql,
    )


# -----------------------
# DB Patch (poor-man's migrations) - gated
# -----------------------

def _db_patch_statements():
    """Idempotent schema patch for common drift points. Safe to run repeatedly."""
    stmts = []

    # (kept exactly as you provided)
    # ---- saved_searches
    stmts.append("""
    CREATE TABLE IF NOT EXISTS saved_searches (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      context VARCHAR(40) NOT NULL,
      name VARCHAR(120) NOT NULL,
      params JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    stmts.append("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_saved_searches_user_id'
      ) THEN
        ALTER TABLE saved_searches
          ADD CONSTRAINT fk_saved_searches_user_id
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
      END IF;
    END$$;
    """)

    stmts.append("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_saved_search_user_context_name'
      ) THEN
        ALTER TABLE saved_searches
          ADD CONSTRAINT uq_saved_search_user_context_name UNIQUE (user_id, context, name);
      END IF;
    END$$;
    """)

    stmts.append("CREATE INDEX IF NOT EXISTS ix_saved_searches_user_id ON saved_searches(user_id);")
    stmts.append("CREATE INDEX IF NOT EXISTS ix_saved_searches_context ON saved_searches(context);")

    # ---- import_batches (preview/resolve)
    stmts.append("""
    CREATE TABLE IF NOT EXISTS import_batches (
      id SERIAL PRIMARY KEY,
      kind VARCHAR(40),
      filename VARCHAR(255),
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    stmts.append("ALTER TABLE import_batches ADD COLUMN IF NOT EXISTS kind VARCHAR(40);")
    stmts.append("ALTER TABLE import_batches ADD COLUMN IF NOT EXISTS filename VARCHAR(255);")
    stmts.append("ALTER TABLE import_batches ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;")
    stmts.append("ALTER TABLE import_batches ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();")
    stmts.append("CREATE INDEX IF NOT EXISTS ix_import_batches_kind ON import_batches(kind);")

    # ---- purchase_orders / purchase_lines
    stmts.append("""
    CREATE TABLE IF NOT EXISTS purchase_orders (
      id SERIAL PRIMARY KEY,
      supplier_name VARCHAR(120),
      brand VARCHAR(80),
      order_number VARCHAR(80),
      order_date DATE,
      arrival_date DATE,
      currency VARCHAR(10),
      freight_total NUMERIC(12,2),
      allocation_method VARCHAR(20),
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS supplier_name VARCHAR(120);")
    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS brand VARCHAR(80);")
    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS order_number VARCHAR(80);")
    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS order_date DATE;")
    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS arrival_date DATE;")
    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS currency VARCHAR(10);")
    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS freight_total NUMERIC(12,2);")
    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS allocation_method VARCHAR(20);")
    stmts.append("ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();")
    stmts.append("CREATE INDEX IF NOT EXISTS ix_purchase_orders_order_number ON purchase_orders(order_number);")

    stmts.append("""
    CREATE TABLE IF NOT EXISTS purchase_lines (
      id SERIAL PRIMARY KEY,
      purchase_order_id INTEGER NOT NULL,
      item_id INTEGER,
      sku VARCHAR(80),
      description VARCHAR(255),
      colour VARCHAR(80),
      size VARCHAR(40),
      qty INTEGER,
      unit_cost_net NUMERIC(12,4),
      packaging_per_unit NUMERIC(12,4),
      freight_allocated_total NUMERIC(12,4),
      freight_allocated_per_unit NUMERIC(12,4),
      landed_unit_cost NUMERIC(12,4),
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS purchase_order_id INTEGER;")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS item_id INTEGER;")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS sku VARCHAR(80);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS description VARCHAR(255);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS colour VARCHAR(80);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS size VARCHAR(40);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS qty INTEGER;")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS unit_cost_net NUMERIC(12,4);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS packaging_per_unit NUMERIC(12,4);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS freight_allocated_total NUMERIC(12,4);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS freight_allocated_per_unit NUMERIC(12,4);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS landed_unit_cost NUMERIC(12,4);")
    stmts.append("ALTER TABLE purchase_lines ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();")

    stmts.append("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_purchase_lines_po'
      ) THEN
        ALTER TABLE purchase_lines
          ADD CONSTRAINT fk_purchase_lines_po
          FOREIGN KEY (purchase_order_id) REFERENCES purchase_orders(id) ON DELETE CASCADE;
      END IF;
    END$$;
    """)

    stmts.append("CREATE INDEX IF NOT EXISTS ix_purchase_lines_sku ON purchase_lines(sku);")
    stmts.append("CREATE INDEX IF NOT EXISTS ix_purchase_lines_purchase_order_id ON purchase_lines(purchase_order_id);")

    # ---- sales_orders / sales_lines
    stmts.append("""
    CREATE TABLE IF NOT EXISTS sales_orders (
      id SERIAL PRIMARY KEY,
      order_number VARCHAR(80) NOT NULL,
      order_date DATE,
      channel VARCHAR(40) NOT NULL DEFAULT 'unknown',
      currency VARCHAR(10) DEFAULT 'EUR',
      customer_name VARCHAR(120),
      customer_email VARCHAR(255),
      shipping_charged_gross NUMERIC(12,2),
      order_discount_gross NUMERIC(12,2),
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS order_number VARCHAR(80);")
    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS order_date DATE;")
    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS channel VARCHAR(40);")
    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS currency VARCHAR(10);")
    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS customer_name VARCHAR(120);")
    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS customer_email VARCHAR(255);")
    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS shipping_charged_gross NUMERIC(12,2);")
    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS order_discount_gross NUMERIC(12,2);")
    stmts.append("ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();")

    stmts.append("CREATE INDEX IF NOT EXISTS ix_sales_orders_order_date ON sales_orders(order_date);")
    stmts.append("CREATE INDEX IF NOT EXISTS ix_sales_orders_channel ON sales_orders(channel);")

    stmts.append("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_sales_orders_channel_order_number'
      ) THEN
        ALTER TABLE sales_orders
          ADD CONSTRAINT uq_sales_orders_channel_order_number UNIQUE (channel, order_number);
      END IF;
    END$$;
    """)

    stmts.append("""
    CREATE TABLE IF NOT EXISTS sales_lines (
      id SERIAL PRIMARY KEY,
      sales_order_id INTEGER NOT NULL,
      item_id INTEGER,
      sku VARCHAR(80),
      description VARCHAR(255),
      qty INTEGER,
      unit_price_gross NUMERIC(12,4),
      line_discount_gross NUMERIC(12,4),
      order_discount_alloc_gross NUMERIC(12,4),
      vat_rate NUMERIC(5,2),
      unit_price_net NUMERIC(12,4),
      revenue_net NUMERIC(12,4),
      cost_method VARCHAR(20),
      unit_cost_basis NUMERIC(12,4),
      cost_total NUMERIC(12,4),
      profit NUMERIC(12,4),
      cost_source_po_id INTEGER,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS sales_order_id INTEGER;")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS item_id INTEGER;")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS sku VARCHAR(80);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS description VARCHAR(255);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS qty INTEGER;")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS unit_price_gross NUMERIC(12,4);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS line_discount_gross NUMERIC(12,4);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS order_discount_alloc_gross NUMERIC(12,4);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS vat_rate NUMERIC(5,2);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS unit_price_net NUMERIC(12,4);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS revenue_net NUMERIC(12,4);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS cost_method VARCHAR(20);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS unit_cost_basis NUMERIC(12,4);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS cost_total NUMERIC(12,4);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS profit NUMERIC(12,4);")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS cost_source_po_id INTEGER;")
    stmts.append("ALTER TABLE sales_lines ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();")

    stmts.append("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_sales_lines_so'
      ) THEN
        ALTER TABLE sales_lines
          ADD CONSTRAINT fk_sales_lines_so
          FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id) ON DELETE CASCADE;
      END IF;
    END$$;
    """)

    stmts.append("CREATE INDEX IF NOT EXISTS ix_sales_lines_sku ON sales_lines(sku);")
    stmts.append("CREATE INDEX IF NOT EXISTS ix_sales_lines_sales_order_id ON sales_lines(sales_order_id);")
    stmts.append("CREATE INDEX IF NOT EXISTS ix_sales_lines_item_id ON sales_lines(item_id);")

    # ---- dashboard metrics tables (Phase 4)
    stmts.append("""
    CREATE TABLE IF NOT EXISTS daily_metrics (
      id SERIAL PRIMARY KEY,
      metric_date DATE NOT NULL UNIQUE,
      revenue_net NUMERIC(14,2) NOT NULL DEFAULT 0,
      cogs NUMERIC(14,2) NOT NULL DEFAULT 0,
      profit NUMERIC(14,2) NOT NULL DEFAULT 0,
      discount_net NUMERIC(14,2) NOT NULL DEFAULT 0,
      orders_count INTEGER NOT NULL DEFAULT 0,
      recomputed_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    stmts.append("""
    CREATE TABLE IF NOT EXISTS sku_metrics_daily (
      id SERIAL PRIMARY KEY,
      metric_date DATE NOT NULL,
      sku VARCHAR(80) NOT NULL,
      units INTEGER NOT NULL DEFAULT 0,
      revenue_net NUMERIC(14,2) NOT NULL DEFAULT 0,
      profit NUMERIC(14,2) NOT NULL DEFAULT 0,
      disc_net NUMERIC(14,2) NOT NULL DEFAULT 0,
      recomputed_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    stmts.append("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_sku_metrics_daily_date_sku'
      ) THEN
        ALTER TABLE sku_metrics_daily
          ADD CONSTRAINT uq_sku_metrics_daily_date_sku UNIQUE (metric_date, sku);
      END IF;
    END$$;
    """)

    stmts.append("CREATE INDEX IF NOT EXISTS ix_sku_metrics_daily_sku ON sku_metrics_daily(sku);")
    stmts.append("CREATE INDEX IF NOT EXISTS ix_sku_metrics_daily_date ON sku_metrics_daily(metric_date);")

    return [s.strip() for s in stmts if s.strip()]


@admin_bp.get("/db-patch")
@login_required
@require_admin
def db_patch_home():
    _require_db_patch_enabled()
    return render_template("admin/db_patch.html", results=None, error=None)


@admin_bp.post("/db-patch")
@login_required
@require_admin
def db_patch_run():
    _require_db_patch_enabled()
    results = []
    error = None
    stmts = _db_patch_statements()

    try:
        for sql in stmts:
            db.session.execute(text(sql))
            results.append({"ok": True, "sql": sql})

        db.session.commit()
        flash("DB patch completed successfully.", "success")

    except Exception as e:
        db.session.rollback()
        error = str(e)
        flash("DB patch failed. See details below.", "danger")
        # mark last statement as failed if we didn't already
        if results and results[-1].get("ok") is True:
            results[-1]["ok"] = False

    results = list(reversed(results))
    return render_template("admin/db_patch.html", results=results, error=error)


# -----------------------
# Metrics recompute (Admin)
# -----------------------

def _safe_parse_date(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _recompute_metrics_range(d_from, d_to):
    # delete then rebuild (keeps things consistent if sales were removed/edited)
    db.session.execute(
        text("DELETE FROM sku_metrics_daily WHERE metric_date >= :d_from AND metric_date <= :d_to"),
        {"d_from": d_from, "d_to": d_to},
    )
    db.session.execute(
        text("DELETE FROM daily_metrics WHERE metric_date >= :d_from AND metric_date <= :d_to"),
        {"d_from": d_from, "d_to": d_to},
    )

    # daily_metrics
    db.session.execute(
        text(
            """
            INSERT INTO daily_metrics (
              metric_date, orders_count, units, revenue_net, cogs, profit, discount_gross, discount_net, created_at, updated_at
            )
            SELECT
              so.order_date::date AS metric_date,
              COUNT(DISTINCT so.id) AS orders_count,
              COALESCE(SUM(sl.qty), 0) AS units,
              COALESCE(SUM(sl.revenue_net), 0) AS revenue_net,
              COALESCE(SUM(sl.cost_total), 0) AS cogs,
              COALESCE(SUM(sl.profit), 0) AS profit,
              COALESCE(SUM(COALESCE(sl.line_discount_gross,0) + COALESCE(sl.order_discount_alloc_gross,0)), 0) AS discount_gross,
              COALESCE(SUM(
                (COALESCE(sl.line_discount_gross,0) + COALESCE(sl.order_discount_alloc_gross,0))
                / (1 + (sl.vat_rate / 100))
              ), 0) AS discount_net,
              NOW() AS created_at,
              NOW() AS updated_at
            FROM sales_lines sl
            JOIN sales_orders so ON so.id = sl.sales_order_id
            WHERE so.order_date IS NOT NULL
              AND so.order_date >= :d_from
              AND so.order_date <= :d_to
            GROUP BY so.order_date::date
            """
        ),
        {"d_from": d_from, "d_to": d_to},
    )

    # sku_metrics_daily
    db.session.execute(
        text(
            """
            INSERT INTO sku_metrics_daily (
              metric_date, sku, units, revenue_net, profit, discount_gross, discount_net, created_at
            )
            SELECT
              so.order_date::date AS metric_date,
              sl.sku AS sku,
              COALESCE(SUM(sl.qty), 0) AS units,
              COALESCE(SUM(sl.revenue_net), 0) AS revenue_net,
              COALESCE(SUM(sl.profit), 0) AS profit,
              COALESCE(SUM(COALESCE(sl.line_discount_gross,0) + COALESCE(sl.order_discount_alloc_gross,0)), 0) AS discount_gross,
              COALESCE(SUM(
                (COALESCE(sl.line_discount_gross,0) + COALESCE(sl.order_discount_alloc_gross,0))
                / (1 + (sl.vat_rate / 100))
              ), 0) AS discount_net,
              NOW() AS created_at
            FROM sales_lines sl
            JOIN sales_orders so ON so.id = sl.sales_order_id
            WHERE so.order_date IS NOT NULL
              AND so.order_date >= :d_from
              AND so.order_date <= :d_to
            GROUP BY so.order_date::date, sl.sku
            """
        ),
        {"d_from": d_from, "d_to": d_to},
    )

    # stamp app_state
    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    db.session.execute(
        text(
            """
            INSERT INTO app_state (key, value, updated_at)
            VALUES ('metrics_last_recompute', :val, NOW())
            ON CONFLICT (key) DO UPDATE
              SET value = EXCLUDED.value,
                  updated_at = NOW()
            """
        ),
        {"val": stamp},
    )


@admin_bp.get("/metrics")
@login_required
@require_admin
def metrics_home():
    today = datetime.utcnow().date()
    d_from = (today - timedelta(days=30)).isoformat()
    d_to = today.isoformat()
    return render_template("admin/metrics_recompute.html", d_from=d_from, d_to=d_to)


@admin_bp.post("/metrics")
@login_required
@require_admin
def metrics_recompute():
    d_from = _safe_parse_date(request.form.get("from"))
    d_to = _safe_parse_date(request.form.get("to"))

    if not d_from or not d_to:
        flash("Please select a valid From and To date.", "danger")
        return redirect(url_for("admin.metrics_home"))

    if d_from > d_to:
        flash("From date cannot be after To date.", "danger")
        return redirect(url_for("admin.metrics_home"))

    try:
        _recompute_metrics_range(d_from, d_to)
        db.session.commit()
        flash("Metrics recomputed successfully.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Metrics recompute failed: {e}", "danger")

    return redirect(url_for("admin.metrics_home"))
