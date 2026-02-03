from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from flask import Blueprint, Response, flash, redirect, render_template, request, url_for
from flask_login import login_required

from ..decorators import require_role
from ..extensions import db
from ..models import SalesOrder, SalesLine
from ..models import DailyMetric, SkuMetricDaily
from ..models import DailyMetric, SkuMetricDaily, AppState

reports_bp = Blueprint("reports", __name__, url_prefix="/reports")


def _safe_date(val):
    """
    Supports: YYYY-MM-DD, DD/MM/YYYY, DD/MM/YY, YYYY-MM-DD HH:MM:SS
    """
    s = (val or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _safe_decimal(val, default=Decimal("0")):
    if val is None:
        return default
    s = str(val).strip()
    if s == "":
        return default
    s = s.replace(",", ".")
    try:
        return Decimal(s)
    except InvalidOperation:
        return default


def _period_starts(today: date):
    start_7d = today - timedelta(days=6)           # inclusive 7 days
    start_month = today.replace(day=1)
    start_year = today.replace(month=1, day=1)
    return start_7d, start_month, start_year


def _sum_sales_range(d_from: date, d_to: date):
    """
    Returns dict with: orders_count, units, revenue_net, profit, margin_pct,
    discount_gross, discount_net (profit lost), profit_no_discount
    """
    # discount gross = line + allocated order discount
    disc_gross = (
        db.func.coalesce(SalesLine.line_discount_gross, 0) +
        db.func.coalesce(SalesLine.order_discount_alloc_gross, 0)
    )

    # discount net = gross / (1 + vat/100)  (line-specific VAT rate)
    vat_factor = db.literal(1) + (SalesLine.vat_rate / db.literal(100))
    disc_net = disc_gross / vat_factor

    row = (
        db.session.query(
            db.func.count(db.func.distinct(SalesOrder.id)).label("orders"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("units"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev_net"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
            db.func.coalesce(db.func.sum(disc_gross), 0).label("disc_gross"),
            db.func.coalesce(db.func.sum(disc_net), 0).label("disc_net"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
        .filter(SalesOrder.order_date >= d_from)
        .filter(SalesOrder.order_date <= d_to)
        .one()
    )

    rev = _safe_decimal(row.rev_net)
    prof = _safe_decimal(row.profit)
    disc_net_val = _safe_decimal(row.disc_net)

    margin = Decimal("0")
    if rev > 0:
        margin = (prof / rev) * Decimal("100")

    # Profit without discounts = profit + discount_net (costs unchanged)
    prof_no_disc = prof + disc_net_val

    return {
        "orders_count": int(row.orders or 0),
        "units": int(row.units or 0),
        "revenue_net": rev,
        "profit": prof,
        "margin_pct": margin,
        "discount_gross": _safe_decimal(row.disc_gross),
        "discount_net": disc_net_val,                 # this is the profit lost to discounts (net)
        "profit_no_discount": prof_no_disc,
    }


@reports_bp.get("")
@login_required
@require_role("viewer")
def index():
    today = datetime.utcnow().date()
    start_7d, start_month, start_year = _period_starts(today)

    # Last recompute info
    state = AppState.query.filter_by(key="metrics_last_recompute").first()
    last_recompute = state.value if state else None

    # Pull KPI totals from aggregates (if missing, show zeros)
    def _kpi_from_daily(d_from, d_to):
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

        rev = _safe_decimal(row.rev)
        prof = _safe_decimal(row.profit)
        disc_net = _safe_decimal(row.disc_net)

        margin = Decimal("0")
        if rev > 0:
            margin = (prof / rev) * Decimal("100")

        return {
            "orders_count": int(row.orders or 0),
            "units": int(row.units or 0),
            "revenue_net": rev,
            "profit": prof,
            "margin_pct": margin,
            "discount_net": disc_net,
            "profit_no_discount": prof + disc_net,
        }

    kpi_7d = _kpi_from_daily(start_7d, today)
    kpi_mtd = _kpi_from_daily(start_month, today)
    kpi_ytd = _kpi_from_daily(start_year, today)

    # Top sold items (MTD) from sku_metrics_daily
    top_skus_units = (
        db.session.query(
            SkuMetricDaily.sku,
            db.func.coalesce(db.func.sum(SkuMetricDaily.units), 0).label("units"),
            db.func.coalesce(db.func.sum(SkuMetricDaily.revenue_net), 0).label("rev"),
            db.func.coalesce(db.func.sum(SkuMetricDaily.profit), 0).label("profit"),
        )
        .filter(SkuMetricDaily.metric_date >= start_month)
        .filter(SkuMetricDaily.metric_date <= today)
        .group_by(SkuMetricDaily.sku)
        .order_by(db.desc(db.func.coalesce(db.func.sum(SkuMetricDaily.units), 0)))
        .limit(10)
        .all()
    )

    # Biggest discount impact SKUs (MTD) — discount_net = profit lost to discounts (net)
    top_discount_skus = (
        db.session.query(
            SkuMetricDaily.sku,
            db.func.coalesce(db.func.sum(SkuMetricDaily.units), 0).label("units"),
            db.func.coalesce(db.func.sum(SkuMetricDaily.revenue_net), 0).label("rev"),
            db.func.coalesce(db.func.sum(SkuMetricDaily.discount_net), 0).label("disc_net"),
        )
        .filter(SkuMetricDaily.metric_date >= start_month)
        .filter(SkuMetricDaily.metric_date <= today)
        .group_by(SkuMetricDaily.sku)
        .order_by(db.desc(db.func.coalesce(db.func.sum(SkuMetricDaily.discount_net), 0)))
        .limit(10)
        .all()
    )

    # Low/negative margin SKUs (MTD)
    sku_rollup = (
        db.session.query(
            SkuMetricDaily.sku,
            db.func.coalesce(db.func.sum(SkuMetricDaily.revenue_net), 0).label("rev"),
            db.func.coalesce(db.func.sum(SkuMetricDaily.profit), 0).label("profit"),
        )
        .filter(SkuMetricDaily.metric_date >= start_month)
        .filter(SkuMetricDaily.metric_date <= today)
        .group_by(SkuMetricDaily.sku)
        .all()
    )

    margin_threshold = Decimal("20")
    neg_count = 0
    low_count = 0
    low_rows = []

    for sku, rev_raw, prof_raw in sku_rollup:
        rev = _safe_decimal(rev_raw)
        prof = _safe_decimal(prof_raw)
        margin = Decimal("0")
        if rev > 0:
            margin = (prof / rev) * Decimal("100")

        if prof < 0:
            neg_count += 1
        if rev > 0 and margin < margin_threshold:
            low_count += 1
            low_rows.append({"sku": sku, "rev_net": rev, "profit": prof, "margin": margin})

    low_rows.sort(key=lambda x: (float(x["margin"]), float(x["profit"])))
    low_rows = low_rows[:10]

    return render_template(
        "reports/index.html",
        today=today.isoformat(),
        kpi_7d=kpi_7d,
        kpi_mtd=kpi_mtd,
        kpi_ytd=kpi_ytd,
        last_recompute=last_recompute,
        top_skus_units=top_skus_units,
        top_discount_skus=top_discount_skus,
        low_margin_skus=low_rows,
        neg_count=neg_count,
        low_count=low_count,
        margin_threshold=margin_threshold,
    )


@reports_bp.get("/sales-summary")
@login_required
@require_role("viewer")
def sales_summary():
    today = datetime.utcnow().date()
    start_month = today.replace(day=1)

    d_from = _safe_date(request.args.get("from") or "") or start_month
    d_to = _safe_date(request.args.get("to") or "") or today
    channel = (request.args.get("channel") or "").strip().lower()

    if d_from > d_to:
        flash("From date cannot be after To date.", "warning")
        return redirect(url_for("reports.sales_summary"))

    # Base query for the selected range (optional channel)
    base_q = (
        db.session.query(SalesLine, SalesOrder)
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
        .filter(SalesOrder.order_date >= d_from)
        .filter(SalesOrder.order_date <= d_to)
    )
    if channel:
        base_q = base_q.filter(db.func.lower(SalesOrder.channel) == channel)

    # KPI summary for selected range
    kpi = _sum_sales_range(d_from, d_to) if not channel else _sum_sales_range(d_from, d_to)
    # If channel is set, recompute KPI using channel filter (we need a channel-aware version)
    if channel:
        disc_gross = (
            db.func.coalesce(SalesLine.line_discount_gross, 0) +
            db.func.coalesce(SalesLine.order_discount_alloc_gross, 0)
        )
        vat_factor = db.literal(1) + (SalesLine.vat_rate / db.literal(100))
        disc_net = disc_gross / vat_factor

        row = (
            db.session.query(
                db.func.count(db.func.distinct(SalesOrder.id)).label("orders"),
                db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("units"),
                db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev_net"),
                db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
                db.func.coalesce(db.func.sum(disc_gross), 0).label("disc_gross"),
                db.func.coalesce(db.func.sum(disc_net), 0).label("disc_net"),
            )
            .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
            .filter(SalesOrder.order_date >= d_from)
            .filter(SalesOrder.order_date <= d_to)
            .filter(db.func.lower(SalesOrder.channel) == channel)
            .one()
        )

        rev = _safe_decimal(row.rev_net)
        prof = _safe_decimal(row.profit)
        disc_net_val = _safe_decimal(row.disc_net)
        margin = Decimal("0")
        if rev > 0:
            margin = (prof / rev) * Decimal("100")

        kpi = {
            "orders_count": int(row.orders or 0),
            "units": int(row.units or 0),
            "revenue_net": rev,
            "profit": prof,
            "margin_pct": margin,
            "discount_gross": _safe_decimal(row.disc_gross),
            "discount_net": disc_net_val,
            "profit_no_discount": prof + disc_net_val,
        }

    # Channel breakdown table (always useful, even if filtering on one channel)
    ch_query = (
        db.session.query(
            SalesOrder.channel.label("channel"),
            db.func.count(db.func.distinct(SalesOrder.id)).label("orders"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("units"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev_net"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
        .filter(SalesOrder.order_date >= d_from)
        .filter(SalesOrder.order_date <= d_to)
        .group_by(SalesOrder.channel)
        .order_by(db.desc(db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0)))
        .all()
    )

    # Channel dropdown values
    channels = [r[0] for r in db.session.query(SalesOrder.channel).distinct().order_by(SalesOrder.channel.asc()).all()]

    return render_template(
        "reports/sales_summary.html",
        d_from=d_from.isoformat(),
        d_to=d_to.isoformat(),
        channel=channel,
        channels=channels,
        kpi=kpi,
        channel_rows=ch_query,
    )


@reports_bp.get("/sales-summary.csv")
@login_required
@require_role("viewer")
def sales_summary_csv():
    today = datetime.utcnow().date()
    start_month = today.replace(day=1)

    d_from = _safe_date(request.args.get("from") or "") or start_month
    d_to = _safe_date(request.args.get("to") or "") or today

    channel = (request.args.get("channel") or "").strip().lower()

    q = (
        db.session.query(
            SalesOrder.channel.label("channel"),
            db.func.count(db.func.distinct(SalesOrder.id)).label("orders"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("units"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev_net"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
        .filter(SalesOrder.order_date >= d_from)
        .filter(SalesOrder.order_date <= d_to)
    )
    if channel:
        q = q.filter(db.func.lower(SalesOrder.channel) == channel)

    rows = q.group_by(SalesOrder.channel).order_by(db.desc(db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0))).all()

    # Simple CSV (small enough for v1; streaming is Phase 5)
    import io, csv
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Channel", "Orders", "Units", "Revenue Net", "Profit", "Margin %"])

    for r in rows:
        rev = _safe_decimal(r.rev_net)
        prof = _safe_decimal(r.profit)
        margin = Decimal("0")
        if rev > 0:
            margin = (prof / rev) * Decimal("100")

        w.writerow([
            r.channel,
            int(r.orders or 0),
            int(r.units or 0),
            f"{rev:.2f}",
            f"{prof:.2f}",
            f"{margin:.2f}",
        ])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=reports_sales_summary.csv"},
    )

@reports_bp.get("/trends")
@login_required
@require_role("viewer")
def trends():
    today = datetime.utcnow().date()
    default_from = today - timedelta(days=29)  # last 30 days

    d_from = _safe_date(request.args.get("from") or "") or default_from
    d_to = _safe_date(request.args.get("to") or "") or today

    rows = (
        DailyMetric.query
        .filter(DailyMetric.metric_date >= d_from)
        .filter(DailyMetric.metric_date <= d_to)
        .order_by(DailyMetric.metric_date.asc())
        .all()
    )

    # Totals
    total_rev = sum(Decimal(str(r.revenue_net or 0)) for r in rows)
    total_profit = sum(Decimal(str(r.profit or 0)) for r in rows)
    total_disc_net = sum(Decimal(str(r.discount_net or 0)) for r in rows)

    total_margin = Decimal("0")
    if total_rev > 0:
        total_margin = (total_profit / total_rev) * Decimal("100")

    return render_template(
        "reports/trends.html",
        d_from=d_from.isoformat(),
        d_to=d_to.isoformat(),
        rows=rows,
        total_rev=total_rev,
        total_profit=total_profit,
        total_margin=total_margin,
        total_disc_net=total_disc_net,
    )

@reports_bp.get("/trends.csv")
@login_required
@require_role("viewer")
def trends_csv():
    today = datetime.utcnow().date()
    default_from = today - timedelta(days=29)

    d_from = _safe_date(request.args.get("from") or "") or default_from
    d_to = _safe_date(request.args.get("to") or "") or today

    rows = (
        DailyMetric.query
        .filter(DailyMetric.metric_date >= d_from)
        .filter(DailyMetric.metric_date <= d_to)
        .order_by(DailyMetric.metric_date.asc())
        .all()
    )

    import io, csv
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Date", "Orders", "Units", "Revenue Net", "COGS", "Profit", "Margin %", "Discount Net"])

    for r in rows:
        rev = Decimal(str(r.revenue_net or 0))
        prof = Decimal(str(r.profit or 0))
        margin = Decimal("0")
        if rev > 0:
            margin = (prof / rev) * Decimal("100")

        w.writerow([
            r.metric_date.isoformat(),
            int(r.orders_count or 0),
            int(r.units or 0),
            f"{rev:.2f}",
            f"{Decimal(str(r.cogs or 0)):.2f}",
            f"{prof:.2f}",
            f"{margin:.2f}",
            f"{Decimal(str(r.discount_net or 0)):.2f}",
        ])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=reports_trends.csv"},
    )
