from datetime import datetime, timedelta
from decimal import Decimal

from flask import Blueprint, render_template
from flask_login import login_required, current_user

from ..decorators import require_role
from ..extensions import db
from ..models import (
    SalesOrder,
    SalesLine,
    DailyMetric,
    SkuMetricDaily,
    AppState,
)

main_bp = Blueprint("main", __name__, url_prefix="")


@main_bp.get("/")
@login_required
def root():
    return dashboard()


def _d(x) -> Decimal:
    try:
        return Decimal(str(x or 0))
    except Exception:
        return Decimal("0")


def _period_starts(today):
    start_7d = today - timedelta(days=6)
    start_month = today.replace(day=1)
    start_year = today.replace(month=1, day=1)
    return start_7d, start_month, start_year


def _has_agg(d_from, d_to) -> bool:
    n = (
        db.session.query(db.func.count(DailyMetric.id))
        .filter(DailyMetric.metric_date >= d_from)
        .filter(DailyMetric.metric_date <= d_to)
        .scalar()
    ) or 0
    return n > 0


def _kpi_from_agg(d_from, d_to):
    row = (
        db.session.query(
            db.func.coalesce(db.func.sum(DailyMetric.orders_count), 0).label("orders"),
            db.func.coalesce(db.func.sum(DailyMetric.units), 0).label("units"),
            db.func.coalesce(db.func.sum(DailyMetric.revenue_net), 0).label("rev"),
            db.func.coalesce(db.func.sum(DailyMetric.cogs), 0).label("cogs"),
            db.func.coalesce(db.func.sum(DailyMetric.profit), 0).label("profit"),
            db.func.coalesce(db.func.sum(DailyMetric.discount_net), 0).label("disc_net"),
        )
        .filter(DailyMetric.metric_date >= d_from)
        .filter(DailyMetric.metric_date <= d_to)
        .one()
    )

    rev = _d(row.rev)
    prof = _d(row.profit)
    disc_net = _d(row.disc_net)
    margin = (prof / rev * Decimal("100")) if rev > 0 else Decimal("0")

    return {
        "orders": int(row.orders or 0),
        "units": int(row.units or 0),
        "revenue_net": rev,
        "profit": prof,
        "margin_pct": margin,
        "discount_net": disc_net,
        "profit_no_discount": prof + disc_net,
    }


def _kpi_from_live(d_from, d_to):
    # Live fallback if aggregates not built yet
    disc_gross = (
        db.func.coalesce(SalesLine.line_discount_gross, 0) +
        db.func.coalesce(SalesLine.order_discount_alloc_gross, 0)
    )
    vat_factor = db.literal(1) + (SalesLine.vat_rate / db.literal(100))
    disc_net_expr = disc_gross / vat_factor

    row = (
        db.session.query(
            db.func.count(db.func.distinct(SalesOrder.id)).label("orders"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("units"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev"),
            db.func.coalesce(db.func.sum(SalesLine.cost_total), 0).label("cogs"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
            db.func.coalesce(db.func.sum(disc_net_expr), 0).label("disc_net"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
        .filter(SalesOrder.order_date >= d_from)
        .filter(SalesOrder.order_date <= d_to)
        .one()
    )

    rev = _d(row.rev)
    prof = _d(row.profit)
    disc_net = _d(row.disc_net)
    margin = (prof / rev * Decimal("100")) if rev > 0 else Decimal("0")

    return {
        "orders": int(row.orders or 0),
        "units": int(row.units or 0),
        "revenue_net": rev,
        "profit": prof,
        "margin_pct": margin,
        "discount_net": disc_net,
        "profit_no_discount": prof + disc_net,
    }


def _kpi(d_from, d_to):
    return _kpi_from_agg(d_from, d_to) if _has_agg(d_from, d_to) else _kpi_from_live(d_from, d_to)


def _top_skus_units_mtd(d_from, d_to):
    # Prefer aggregates; fallback to live group if missing
    if _has_agg(d_from, d_to):
        return (
            db.session.query(
                SkuMetricDaily.sku.label("sku"),
                db.func.coalesce(db.func.sum(SkuMetricDaily.units), 0).label("units"),
                db.func.coalesce(db.func.sum(SkuMetricDaily.revenue_net), 0).label("rev"),
                db.func.coalesce(db.func.sum(SkuMetricDaily.profit), 0).label("profit"),
            )
            .filter(SkuMetricDaily.metric_date >= d_from)
            .filter(SkuMetricDaily.metric_date <= d_to)
            .group_by(SkuMetricDaily.sku)
            .order_by(db.desc(db.func.coalesce(db.func.sum(SkuMetricDaily.units), 0)))
            .limit(10)
            .all()
        )

    return (
        db.session.query(
            SalesLine.sku.label("sku"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("units"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
        .filter(SalesOrder.order_date >= d_from)
        .filter(SalesOrder.order_date <= d_to)
        .group_by(SalesLine.sku)
        .order_by(db.desc(db.func.coalesce(db.func.sum(SalesLine.qty), 0)))
        .limit(10)
        .all()
    )


def _top_discount_skus_mtd(d_from, d_to):
    if _has_agg(d_from, d_to):
        return (
            db.session.query(
                SkuMetricDaily.sku.label("sku"),
                db.func.coalesce(db.func.sum(SkuMetricDaily.units), 0).label("units"),
                db.func.coalesce(db.func.sum(SkuMetricDaily.revenue_net), 0).label("rev"),
                db.func.coalesce(db.func.sum(SkuMetricDaily.discount_net), 0).label("disc_net"),
            )
            .filter(SkuMetricDaily.metric_date >= d_from)
            .filter(SkuMetricDaily.metric_date <= d_to)
            .group_by(SkuMetricDaily.sku)
            .order_by(db.desc(db.func.coalesce(db.func.sum(SkuMetricDaily.discount_net), 0)))
            .limit(10)
            .all()
        )

    disc_gross = (
        db.func.coalesce(SalesLine.line_discount_gross, 0) +
        db.func.coalesce(SalesLine.order_discount_alloc_gross, 0)
    )
    vat_factor = db.literal(1) + (SalesLine.vat_rate / db.literal(100))
    disc_net_expr = disc_gross / vat_factor

    return (
        db.session.query(
            SalesLine.sku.label("sku"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("units"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev"),
            db.func.coalesce(db.func.sum(disc_net_expr), 0).label("disc_net"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
        .filter(SalesOrder.order_date >= d_from)
        .filter(SalesOrder.order_date <= d_to)
        .group_by(SalesLine.sku)
        .order_by(db.desc(db.func.coalesce(db.func.sum(disc_net_expr), 0)))
        .limit(10)
        .all()
    )


def _low_margin_summary_mtd(d_from, d_to):
    # Returns: (neg_count, low_count, worst_rows[list])
    margin_threshold = Decimal("20")

    if _has_agg(d_from, d_to):
        rows = (
            db.session.query(
                SkuMetricDaily.sku.label("sku"),
                db.func.coalesce(db.func.sum(SkuMetricDaily.revenue_net), 0).label("rev"),
                db.func.coalesce(db.func.sum(SkuMetricDaily.profit), 0).label("profit"),
            )
            .filter(SkuMetricDaily.metric_date >= d_from)
            .filter(SkuMetricDaily.metric_date <= d_to)
            .group_by(SkuMetricDaily.sku)
            .all()
        )
    else:
        rows = (
            db.session.query(
                SalesLine.sku.label("sku"),
                db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev"),
                db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
            )
            .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
            .filter(SalesOrder.order_date >= d_from)
            .filter(SalesOrder.order_date <= d_to)
            .group_by(SalesLine.sku)
            .all()
        )

    neg_count = 0
    low_count = 0
    worst = []

    for sku, rev_raw, prof_raw in rows:
        rev = _d(rev_raw)
        prof = _d(prof_raw)
        margin = (prof / rev * Decimal("100")) if rev > 0 else Decimal("0")

        if prof < 0:
            neg_count += 1
        if rev > 0 and margin < margin_threshold:
            low_count += 1

        worst.append({"sku": sku, "rev": rev, "profit": prof, "margin": margin})

    worst.sort(key=lambda x: (float(x["margin"]), float(x["profit"])))
    worst = worst[:10]
    return neg_count, low_count, worst, margin_threshold


@main_bp.get("/dashboard")
@login_required
@require_role("viewer")
def dashboard():
    today = datetime.utcnow().date()
    start_7d, start_month, start_year = _period_starts(today)

    kpi_7d = _kpi(start_7d, today)
    kpi_mtd = _kpi(start_month, today)
    kpi_ytd = _kpi(start_year, today)

    top_skus_units = _top_skus_units_mtd(start_month, today)
    top_discount_skus = _top_discount_skus_mtd(start_month, today)

    neg_count, low_count, worst_margin_rows, margin_threshold = _low_margin_summary_mtd(start_month, today)

    state = AppState.query.filter_by(key="metrics_last_recompute").first()
    last_recompute = state.value if state else None

    return render_template(
        "main/dashboard.html",
        today=today.isoformat(),
        last_recompute=last_recompute,
        kpi_7d=kpi_7d,
        kpi_mtd=kpi_mtd,
        kpi_ytd=kpi_ytd,
        top_skus_units=top_skus_units,
        top_discount_skus=top_discount_skus,
        neg_count=neg_count,
        low_count=low_count,
        worst_margin_rows=worst_margin_rows,
        margin_threshold=margin_threshold,
        user=current_user,
    )
