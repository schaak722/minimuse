import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode

from flask import Blueprint, render_template, redirect, url_for, flash, request, Response
from flask_login import login_required, current_user

from ..extensions import db
from ..decorators import require_role, require_edit_permission
from ..utils.csv_stream import stream_csv
from ..models import (
    Item,
    ImportBatch,
    SalesOrder,
    SalesLine,
    PurchaseOrder,
    PurchaseLine,
    SavedSearch,
)

sales_bp = Blueprint("sales", __name__, url_prefix="/sales")


# -------------------------
# Import templates + strict validation
# -------------------------

SALES_IMPORT_HEADERS = [
    "Order Number",
    "Order Date",
    "Channel",
    "Currency",
    "Customer Name",
    "Customer Email",
    "Shipping Charged Gross",
    "Order Discount Gross",
    "SKU",
    "Item Description",
    "Qty",
    "Unit Price Gross",
    "Line Discount Gross",
]


@sales_bp.get("/import-template.csv")
@login_required
@require_edit_permission
def import_template_csv():
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(SALES_IMPORT_HEADERS)
    resp = Response(out.getvalue(), mimetype="text/csv")
    resp.headers["Content-Disposition"] = "attachment; filename=sales_import_template.csv"
    return resp


def _validate_headers(fieldnames, expected_headers):
    given = [(h or "").strip() for h in (fieldnames or [])]
    expected = list(expected_headers)
    missing = [h for h in expected if h not in given]
    extra = [h for h in given if h not in expected]
    return (len(missing) == 0 and len(extra) == 0), missing, extra, given


# -------------------------
# Helpers (CSV + parsing)
# -------------------------

def _norm_header(s: str) -> str:
    return "".join(ch.lower() for ch in (s or "").strip() if ch.isalnum())


def _pick(row: dict, header_map: dict, *keys: str) -> str:
    for k in keys:
        h = header_map.get(_norm_header(k))
        if h and h in row:
            return (row.get(h) or "").strip()
    return ""


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


def _safe_int(val, default=0):
    try:
        s = str(val).strip()
        if s == "":
            return default
        return int(Decimal(s.replace(",", ".")))
    except Exception:
        return default


def _safe_date(val):
    """
    Supports: YYYY-MM-DD, DD/MM/YYYY, DD/MM/YY, YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SS
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


def _gross_to_net(gross: Decimal, vat_rate: Decimal) -> Decimal:
    """
    Malta: prices are VAT-inclusive. net = gross / (1 + vat/100)
    """
    vr = vat_rate if vat_rate is not None else Decimal("18.00")
    factor = Decimal("1.0") + (Decimal(str(vr)) / Decimal("100.0"))
    if factor <= 0:
        return gross
    return (gross / factor)


def _effective_po_date(po: PurchaseOrder):
    """
    For cost selection: use arrival_date, else order_date, else created_at.date()
    """
    if po.arrival_date:
        return po.arrival_date
    if po.order_date:
        return po.order_date
    if po.created_at:
        return po.created_at.date()
    return None


def _line_landed_cost(pl: PurchaseLine) -> Decimal:
    """
    Prefer landed_unit_cost. If missing, fallback to unit_cost_net + packaging_per_unit.
    """
    if pl.landed_unit_cost is not None:
        return Decimal(str(pl.landed_unit_cost))
    unit = Decimal(str(pl.unit_cost_net or 0))
    pkg = Decimal(str(pl.packaging_per_unit or 0))
    return unit + pkg


def _compute_unit_cost_basis(sku: str, sale_date, method: str):
    """
    method:
      - 'weighted_avg': weighted average landed unit cost for purchases with effective_date <= sale_date
      - 'last': last available purchase landed unit cost with effective_date <= sale_date
    Fallbacks:
      - if no purchases before sale_date, use latest purchase overall
      - if no purchases at all, return (0, None)
    Returns: (unit_cost_basis: Decimal, cost_source_po_id: int|None)
    """
    sku = (sku or "").strip()
    if not sku:
        return (Decimal("0"), None)

    rows = (
        db.session.query(PurchaseLine, PurchaseOrder)
        .join(PurchaseOrder, PurchaseLine.purchase_order_id == PurchaseOrder.id)
        .filter(PurchaseLine.sku == sku)
        .all()
    )

    if not rows:
        return (Decimal("0"), None)

    enriched = []
    for pl, po in rows:
        eff = _effective_po_date(po)
        qty = int(pl.qty or 0)
        if qty <= 0:
            continue
        cost = _line_landed_cost(pl)
        enriched.append((eff, po.id, qty, cost))

    if not enriched:
        return (Decimal("0"), None)

    before = [r for r in enriched if r[0] is not None and sale_date is not None and r[0] <= sale_date]
    candidates = before if before else enriched

    candidates_sorted = sorted(
        candidates,
        key=lambda x: (x[0] is None, x[0]),
        reverse=True,
    )

    if (method or "weighted_avg") == "last":
        eff, po_id, qty, cost = candidates_sorted[0]
        return (Decimal(str(cost)), int(po_id) if po_id else None)

    total_qty = sum(Decimal(qty) for _, _, qty, _ in candidates)
    if total_qty <= 0:
        return (Decimal("0"), None)

    total_cost = sum(Decimal(qty) * Decimal(str(cost)) for _, _, qty, cost in candidates)
    avg = total_cost / total_qty
    return (avg, None)


# -------------------------
# Views
# -------------------------

@sales_bp.get("")
@login_required
@require_role("viewer")
def list_sales_orders():
    q = (request.args.get("q") or "").strip().lower()
    channel = (request.args.get("channel") or "").strip().lower()
    date_from = _safe_date(request.args.get("from") or "")
    date_to = _safe_date(request.args.get("to") or "")

    page = int(request.args.get("page") or 1)
    per_page = int(request.args.get("per_page") or 50)
    if per_page not in (25, 50, 100):
        per_page = 50
    if page < 1:
        page = 1

    query = SalesOrder.query

    if q:
        query = query.filter(
            db.or_(
                db.func.lower(SalesOrder.order_number).contains(q),
                db.func.lower(SalesOrder.customer_name).contains(q),
                db.func.lower(SalesOrder.customer_email).contains(q),
            )
        )

    if channel:
        query = query.filter(db.func.lower(SalesOrder.channel) == channel)

    if date_from:
        query = query.filter(SalesOrder.order_date >= date_from)
    if date_to:
        query = query.filter(SalesOrder.order_date <= date_to)

    total = query.count()
    orders = (
        query.order_by(SalesOrder.order_date.desc(), SalesOrder.id.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )


    # Distinct channels for dropdown
    channels = [
        r[0]
        for r in db.session.query(SalesOrder.channel)
        .distinct()
        .order_by(SalesOrder.channel.asc())
        .all()
    ]

    # Saved searches (user-scoped)
    saved = (
        SavedSearch.query
        .filter_by(user_id=current_user.id, context="sales")
        .order_by(SavedSearch.created_at.desc())
        .limit(20)
        .all()
    )

    # Totals per order (single grouped query)
    order_ids = [o.id for o in orders]
    totals_map = {}
    if order_ids:
        totals = (
            db.session.query(
                SalesLine.sales_order_id,
                db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0),
                db.func.coalesce(db.func.sum(SalesLine.cost_total), 0),
                db.func.coalesce(db.func.sum(SalesLine.profit), 0),
                db.func.coalesce(db.func.sum(SalesLine.qty), 0),
            )
            .filter(SalesLine.sales_order_id.in_(order_ids))
            .group_by(SalesLine.sales_order_id)
            .all()
        )
        totals_map = {
            oid: {"rev": rev, "cost": cost, "profit": prof, "units": units}
            for oid, rev, cost, prof, units in totals
        }

    # Pagination helpers for template
    has_prev = page > 1
    has_next = (page * per_page) < total

    params = {
        "q": q or "",
        "channel": channel or "",
        "from": (date_from.isoformat() if date_from else ""),
        "to": (date_to.isoformat() if date_to else ""),
    }
    qs_no_page = urlencode({k: v for k, v in params.items() if v != ""})

    return render_template(
        "sales/orders_list.html",
        orders=orders,
        q=q,
        channel=channel,
        date_from=(date_from.isoformat() if date_from else ""),
        date_to=(date_to.isoformat() if date_to else ""),
        channels=channels,
        totals_map=totals_map,
        page=page,
        per_page=per_page,
        total=total,
        has_prev=has_prev,
        has_next=has_next,
        qs_no_page=qs_no_page,
        saved_searches=saved,
    )


@sales_bp.get("/<int:order_id>")
@login_required
@require_role("viewer")
def sales_order_detail(order_id: int):
    so = db.session.get(SalesOrder, order_id)
    if not so:
        flash("Sales order not found.", "danger")
        return redirect(url_for("sales.list_sales_orders"))

    lines = SalesLine.query.filter_by(sales_order_id=so.id).order_by(SalesLine.sku.asc()).all()

    total_units = sum(l.qty or 0 for l in lines)
    total_rev_net = sum(Decimal(str(l.revenue_net or 0)) for l in lines)
    total_cost = sum(Decimal(str(l.cost_total or 0)) for l in lines)
    total_profit = sum(Decimal(str(l.profit or 0)) for l in lines)

    # Discount analytics
    total_discount_gross = sum(
        Decimal(str((l.line_discount_gross or 0))) + Decimal(str((l.order_discount_alloc_gross or 0)))
        for l in lines
    )

    total_discount_net = sum(
        _gross_to_net(
            Decimal(str((l.line_discount_gross or 0))) + Decimal(str((l.order_discount_alloc_gross or 0))),
            Decimal(str(l.vat_rate or Decimal("18.00"))),
        )
        for l in lines
    )

    profit_no_discount = (total_rev_net + total_discount_net) - total_cost
    profit_lost_to_discounts = profit_no_discount - total_profit

    margin = Decimal("0")
    if total_rev_net > 0:
        margin = (total_profit / total_rev_net) * Decimal("100")

    return render_template(
        "sales/order_detail.html",
        so=so,
        lines=lines,
        total_units=total_units,
        total_rev_net=total_rev_net,
        total_cost=total_cost,
        total_profit=total_profit,
        margin=margin,
        total_discount_gross=total_discount_gross,
        total_discount_net=total_discount_net,
        profit_no_discount=profit_no_discount,
        profit_lost_to_discounts=profit_lost_to_discounts,
    )


# -------------------------
# Import (Upload -> Preview -> Commit)
# -------------------------

@sales_bp.get("/import")
@login_required
@require_edit_permission
def import_upload():
    return render_template(
        "sales/import_upload.html",
        expected_headers=SALES_IMPORT_HEADERS,
        header_issues=None,
        row_errors=None,
    )


@sales_bp.post("/import")
@login_required
@require_edit_permission
def import_parse():
    f = request.files.get("file")
    if not f or f.filename == "":
        flash("Please choose a CSV file.", "danger")
        return redirect(url_for("sales.import_upload"))

    raw = f.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        flash("CSV must be UTF-8 encoded.", "danger")
        return redirect(url_for("sales.import_upload"))

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        flash("CSV appears empty or invalid.", "danger")
        return redirect(url_for("sales.import_upload"))

    ok_headers, missing, extra, given = _validate_headers(reader.fieldnames, SALES_IMPORT_HEADERS)
    if not ok_headers:
        return render_template(
            "sales/import_upload.html",
            expected_headers=SALES_IMPORT_HEADERS,
            header_issues={"missing": missing, "extra": extra, "given": given},
            row_errors=None,
        )

    rows = list(reader)

    # Strict row validation (fail-fast: do not create an ImportBatch if any errors)
    errors = []
    for i, r in enumerate(rows, start=2):
        order_number = (r.get("Order Number") or "").strip()
        order_date_raw = (r.get("Order Date") or "").strip()
        sku = (r.get("SKU") or "").strip()
        qty_raw = (r.get("Qty") or "").strip()
        unit_price_raw = (r.get("Unit Price Gross") or "").strip()

        if not order_number:
            errors.append({"row": i, "field": "Order Number", "issue": "required", "value": ""})
        if not sku:
            errors.append({"row": i, "field": "SKU", "issue": "required", "value": ""})

        if not order_date_raw or not _safe_date(order_date_raw):
            errors.append({"row": i, "field": "Order Date", "issue": "invalid date (use YYYY-MM-DD or DD/MM/YYYY)", "value": order_date_raw})

        try:
            qty_val = int(Decimal(qty_raw.replace(",", "."))) if qty_raw != "" else 0
            if qty_val <= 0:
                raise ValueError("qty must be > 0")
        except Exception:
            errors.append({"row": i, "field": "Qty", "issue": "invalid integer > 0", "value": qty_raw})

        try:
            up = Decimal(unit_price_raw.replace(",", "."))
            if up <= 0:
                raise ValueError("unit price must be > 0")
        except Exception:
            errors.append({"row": i, "field": "Unit Price Gross", "issue": "invalid decimal > 0", "value": unit_price_raw})

        for fld in ("Shipping Charged Gross", "Order Discount Gross", "Line Discount Gross"):
            v = (r.get(fld) or "").strip()
            if v:
                try:
                    Decimal(v.replace(",", "."))
                except Exception:
                    errors.append({"row": i, "field": fld, "issue": "invalid decimal", "value": v})

    if errors:
        return render_template(
            "sales/import_upload.html",
            expected_headers=SALES_IMPORT_HEADERS,
            header_issues=None,
            row_errors=errors,
        )

    groups = {}
    for r in rows:
        order_number = (r.get("Order Number") or "").strip()
        order_date = _safe_date((r.get("Order Date") or "").strip())
        channel = (r.get("Channel") or "unknown").strip().lower() or "unknown"
        currency = (r.get("Currency") or "EUR").strip().upper() or "EUR"

        customer_name = (r.get("Customer Name") or "").strip() or None
        customer_email = (r.get("Customer Email") or "").strip() or None

        shipping = (r.get("Shipping Charged Gross") or "").strip()
        order_discount = (r.get("Order Discount Gross") or "").strip()

        sku = (r.get("SKU") or "").strip()
        desc = (r.get("Item Description") or "").strip() or None

        qty = (r.get("Qty") or "").strip()
        unit_price_gross = (r.get("Unit Price Gross") or "").strip()
        line_discount_gross = (r.get("Line Discount Gross") or "").strip()

        key = (channel, order_number)
        if key not in groups:
            groups[key] = {
                "order_number": order_number,
                "order_date": order_date.isoformat() if order_date else None,
                "channel": channel,
                "currency": currency,
                "customer_name": customer_name,
                "customer_email": customer_email,
                "shipping_charged_gross": str(Decimal(shipping.replace(",", "."))) if shipping else None,
                "order_discount_gross": str(Decimal(order_discount.replace(",", "."))) if order_discount else None,
                "lines": [],
            }
        else:
            if shipping and not groups[key].get("shipping_charged_gross"):
                groups[key]["shipping_charged_gross"] = str(Decimal(shipping.replace(",", ".")))
            if order_discount and not groups[key].get("order_discount_gross"):
                groups[key]["order_discount_gross"] = str(Decimal(order_discount.replace(",", ".")))

        groups[key]["lines"].append(
            {
                "sku": sku,
                "description": desc,
                "qty": qty,
                "unit_price_gross": unit_price_gross,
                "line_discount_gross": line_discount_gross or "0",
            }
        )

    if not groups:
        flash("No orders detected. Ensure your CSV includes an order number and SKU columns.", "danger")
        return redirect(url_for("sales.import_upload"))

    missing_skus = set()
    for g in groups.values():
        for ln in g["lines"]:
            s = (ln.get("sku") or "").strip()
            if s and not Item.query.filter_by(sku=s).first():
                missing_skus.add(s)

    payload = {
        "orders": list(groups.values()),
        "missing_skus": sorted(list(missing_skus)),
        "missing_skus_count": len(missing_skus),
        "stats": {
            "orders_count": len(groups),
            "lines_count": sum(len(g["lines"]) for g in groups.values()),
            "skipped_no_order": 0,
            "skipped_no_sku": 0,
        },
    }

    batch = ImportBatch(kind="sales_import", filename=f.filename, payload=payload)
    db.session.add(batch)
    db.session.commit()

    return redirect(url_for("sales.import_preview", batch_id=batch.id))


@sales_bp.get("/import/<int:batch_id>/preview")
@login_required
@require_edit_permission
def import_preview(batch_id: int):
    batch = db.session.get(ImportBatch, batch_id)
    if not batch:
        flash("Import batch not found.", "danger")
        return redirect(url_for("sales.import_upload"))

    payload = batch.payload
    return render_template("sales/import_preview.html", batch=batch, payload=payload)


@sales_bp.post("/import/<int:batch_id>/commit")
@login_required
@require_edit_permission
def import_commit(batch_id: int):
    batch = db.session.get(ImportBatch, batch_id)
    if not batch:
        flash("Import batch not found.", "danger")
        return redirect(url_for("sales.import_upload"))

    payload = batch.payload

    create_missing = (request.form.get("create_missing") == "1")
    cost_method = (request.form.get("cost_method") or "weighted_avg").strip()

    created_orders = 0
    created_lines = 0
    created_items = 0
    skipped_existing = 0
    skipped_missing_sku = 0

    for o in payload.get("orders", []):
        order_number = (o.get("order_number") or "").strip()
        if not order_number:
            continue

        channel = (o.get("channel") or "unknown").strip().lower()
        currency = (o.get("currency") or "EUR").strip().upper()
        order_date = _safe_date(o.get("order_date") or "") or datetime.utcnow().date()

        existing = SalesOrder.query.filter_by(channel=channel, order_number=order_number).first()
        if existing:
            skipped_existing += 1
            continue

        shipping_gross = _safe_decimal(o.get("shipping_charged_gross"), default=Decimal("0"))
        order_disc_gross = _safe_decimal(o.get("order_discount_gross"), default=Decimal("0"))

        so = SalesOrder(
            order_number=order_number,
            order_date=order_date,
            channel=channel,
            currency=currency,
            customer_name=o.get("customer_name"),
            customer_email=o.get("customer_email"),
            shipping_charged_gross=shipping_gross if shipping_gross != 0 else None,
            order_discount_gross=order_disc_gross if order_disc_gross != 0 else None,
        )
        db.session.add(so)
        db.session.flush()
        created_orders += 1

        prepared = []
        for ln in o.get("lines", []):
            sku = (ln.get("sku") or "").strip()
            if not sku:
                continue

            item = Item.query.filter_by(sku=sku).first()
            if not item and create_missing:
                item = Item(
                    sku=sku,
                    description=(ln.get("description") or sku)[:255],
                    vat_rate=Decimal("18.00"),
                    is_active=True,
                )
                db.session.add(item)
                db.session.flush()
                created_items += 1

            if not item:
                skipped_missing_sku += 1
                continue

            qty = _safe_int(ln.get("qty"), default=0)
            if qty <= 0:
                continue

            unit_price_gross = _safe_decimal(ln.get("unit_price_gross"), default=Decimal("0"))
            line_discount_gross = _safe_decimal(ln.get("line_discount_gross"), default=Decimal("0"))

            gross_line = unit_price_gross * Decimal(qty)
            base_after_line_discount = gross_line - line_discount_gross
            if base_after_line_discount < 0:
                base_after_line_discount = Decimal("0")

            prepared.append(
                {
                    "item": item,
                    "sku": sku,
                    "description": (ln.get("description") or item.description),
                    "qty": qty,
                    "unit_price_gross": unit_price_gross,
                    "line_discount_gross": line_discount_gross,
                    "base_after_line_discount": base_after_line_discount,
                }
            )

        total_base = sum(p["base_after_line_discount"] for p in prepared) or Decimal("0")
        order_discount_total = order_disc_gross if order_disc_gross is not None else Decimal("0")

        for p in prepared:
            alloc = Decimal("0")
            if order_discount_total > 0 and total_base > 0:
                alloc = (order_discount_total * (p["base_after_line_discount"] / total_base))

            gross_after_all_discounts = p["base_after_line_discount"] - alloc
            if gross_after_all_discounts < 0:
                gross_after_all_discounts = Decimal("0")

            vat_rate = Decimal(str(p["item"].vat_rate or Decimal("18.00")))

            unit_price_net = _gross_to_net(p["unit_price_gross"], vat_rate)
            revenue_net = _gross_to_net(gross_after_all_discounts, vat_rate)

            unit_cost_basis, cost_source_po_id = _compute_unit_cost_basis(p["sku"], order_date, cost_method)
            cost_total = unit_cost_basis * Decimal(p["qty"])
            profit = revenue_net - cost_total

            sl = SalesLine(
                sales_order_id=so.id,
                item_id=p["item"].id,
                sku=p["sku"],
                description=(p["description"] or "")[:255],
                qty=p["qty"],
                unit_price_gross=p["unit_price_gross"],
                line_discount_gross=p["line_discount_gross"] if p["line_discount_gross"] != 0 else None,
                order_discount_alloc_gross=alloc if alloc != 0 else None,
                vat_rate=vat_rate,
                unit_price_net=unit_price_net,
                revenue_net=revenue_net,
                cost_method=cost_method,
                unit_cost_basis=unit_cost_basis,
                cost_total=cost_total,
                profit=profit,
                cost_source_po_id=cost_source_po_id,
            )
            db.session.add(sl)
            created_lines += 1

    db.session.commit()

    flash(
        f"Sales import complete. Created orders: {created_orders}, lines: {created_lines}, "
        f"new SKUs: {created_items}, skipped existing orders: {skipped_existing}, "
        f"skipped lines (missing SKUs): {skipped_missing_sku}.",
        "success",
    )
    return redirect(url_for("sales.list_sales_orders"))


# -------------------------
# Item-level report (SKU)
# -------------------------

@sales_bp.get("/items-report")
@login_required
@require_role("viewer")
def items_report():
    q = (request.args.get("q") or "").strip().lower()
    channel = (request.args.get("channel") or "").strip().lower()
    date_from = _safe_date(request.args.get("from") or "")
    date_to = _safe_date(request.args.get("to") or "")

    query = (
        db.session.query(
            SalesLine.sku.label("sku"),
            db.func.max(SalesLine.description).label("description"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("qty_sold"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("revenue_net"),
            db.func.coalesce(db.func.sum(SalesLine.cost_total), 0).label("cost_total"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
    )

    if q:
        query = query.filter(
            db.or_(
                db.func.lower(SalesLine.sku).contains(q),
                db.func.lower(SalesLine.description).contains(q),
            )
        )

    if channel:
        query = query.filter(db.func.lower(SalesOrder.channel) == channel)

    if date_from:
        query = query.filter(SalesOrder.order_date >= date_from)
    if date_to:
        query = query.filter(SalesOrder.order_date <= date_to)

    rows = (
        query.group_by(SalesLine.sku)
        .order_by(db.desc(db.func.coalesce(db.func.sum(SalesLine.profit), 0)))
        .limit(500)
        .all()
    )

    channels = [
        r[0]
        for r in db.session.query(SalesOrder.channel)
        .distinct()
        .order_by(SalesOrder.channel.asc())
        .all()
    ]

    total_qty = sum(int(r.qty_sold or 0) for r in rows)
    total_rev = sum(Decimal(str(r.revenue_net or 0)) for r in rows)
    total_cost = sum(Decimal(str(r.cost_total or 0)) for r in rows)
    total_profit = sum(Decimal(str(r.profit or 0)) for r in rows)
    total_margin = Decimal("0")
    if total_rev > 0:
        total_margin = (total_profit / total_rev) * Decimal("100")

    return render_template(
        "sales/items_report.html",
        rows=rows,
        q=q,
        channel=channel,
        date_from=(date_from.isoformat() if date_from else ""),
        date_to=(date_to.isoformat() if date_to else ""),
        channels=channels,
        total_qty=total_qty,
        total_rev=total_rev,
        total_cost=total_cost,
        total_profit=total_profit,
        total_margin=total_margin,
    )


@sales_bp.get("/items-report.csv")
@login_required
@require_role("viewer")
def items_report_csv():
    q = (request.args.get("q") or "").strip().lower()
    channel = (request.args.get("channel") or "").strip().lower()
    date_from = _safe_date(request.args.get("from") or "")
    date_to = _safe_date(request.args.get("to") or "")

    query = (
        db.session.query(
            SalesLine.sku.label("sku"),
            db.func.max(SalesLine.description).label("description"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("qty_sold"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("revenue_net"),
            db.func.coalesce(db.func.sum(SalesLine.cost_total), 0).label("cost_total"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
    )

    if q:
        query = query.filter(
            db.or_(
                db.func.lower(SalesLine.sku).contains(q),
                db.func.lower(SalesLine.description).contains(q),
            )
        )
    if channel:
        query = query.filter(db.func.lower(SalesOrder.channel) == channel)
    if date_from:
        query = query.filter(SalesOrder.order_date >= date_from)
    if date_to:
        query = query.filter(SalesOrder.order_date <= date_to)

    rows = (
        query.group_by(SalesLine.sku)
        .order_by(db.desc(db.func.coalesce(db.func.sum(SalesLine.profit), 0)))
        .yield_per(500)
    )

    headers = ["SKU", "Description", "Qty Sold", "Revenue Net", "Cost Total", "Profit", "Margin %"]

    def row_fn(r):
        rev = Decimal(str(r.revenue_net or 0))
        prof = Decimal(str(r.profit or 0))
        margin = Decimal("0")
        if rev > 0:
            margin = (prof / rev) * Decimal("100")
        return [
            r.sku,
            r.description or "",
            int(r.qty_sold or 0),
            f"{rev:.2f}",
            f"{Decimal(str(r.cost_total or 0)):.2f}",
            f"{prof:.2f}",
            f"{margin:.2f}",
        ]

    return stream_csv(rows, headers, row_fn, filename="sales_items_report.csv")


# -------------------------
# Discount report
# -------------------------

@sales_bp.get("/discount-report")
@login_required
@require_role("viewer")
def discount_report():
    q = (request.args.get("q") or "").strip().lower()
    channel = (request.args.get("channel") or "").strip().lower()
    date_from = _safe_date(request.args.get("from") or "")
    date_to = _safe_date(request.args.get("to") or "")

    query = (
        db.session.query(
            SalesLine.sku.label("sku"),
            db.func.max(SalesLine.description).label("description"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("qty_sold"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("revenue_net"),
            db.func.coalesce(db.func.sum(SalesLine.cost_total), 0).label("cost_total"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
            db.func.coalesce(db.func.sum(SalesLine.line_discount_gross), 0).label("line_discount_gross"),
            db.func.coalesce(db.func.sum(SalesLine.order_discount_alloc_gross), 0).label("order_discount_alloc_gross"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
    )

    if q:
        query = query.filter(
            db.or_(
                db.func.lower(SalesLine.sku).contains(q),
                db.func.lower(SalesLine.description).contains(q),
            )
        )
    if channel:
        query = query.filter(db.func.lower(SalesOrder.channel) == channel)
    if date_from:
        query = query.filter(SalesOrder.order_date >= date_from)
    if date_to:
        query = query.filter(SalesOrder.order_date <= date_to)

    rows = (
        query.group_by(SalesLine.sku)
        .order_by(
            db.desc(
                db.func.coalesce(db.func.sum(SalesLine.line_discount_gross), 0)
                + db.func.coalesce(db.func.sum(SalesLine.order_discount_alloc_gross), 0)
            )
        )
        .limit(500)
        .all()
    )

    channels = [
        r[0]
        for r in db.session.query(SalesOrder.channel)
        .distinct()
        .order_by(SalesOrder.channel.asc())
        .all()
    ]

    total_qty = sum(int(r.qty_sold or 0) for r in rows)
    total_rev = sum(Decimal(str(r.revenue_net or 0)) for r in rows)
    total_cost = sum(Decimal(str(r.cost_total or 0)) for r in rows)
    total_profit = sum(Decimal(str(r.profit or 0)) for r in rows)
    total_disc_gross = sum(
        Decimal(str(r.line_discount_gross or 0)) + Decimal(str(r.order_discount_alloc_gross or 0))
        for r in rows
    )

    total_margin = Decimal("0")
    if total_rev > 0:
        total_margin = (total_profit / total_rev) * Decimal("100")

    approx_gross_sales = total_rev * Decimal("1.18")
    discount_pct = Decimal("0")
    if approx_gross_sales > 0:
        discount_pct = (total_disc_gross / approx_gross_sales) * Decimal("100")

    return render_template(
        "sales/discount_report.html",
        rows=rows,
        q=q,
        channel=channel,
        date_from=(date_from.isoformat() if date_from else ""),
        date_to=(date_to.isoformat() if date_to else ""),
        channels=channels,
        total_qty=total_qty,
        total_rev=total_rev,
        total_cost=total_cost,
        total_profit=total_profit,
        total_margin=total_margin,
        total_disc_gross=total_disc_gross,
        approx_gross_sales=approx_gross_sales,
        discount_pct=discount_pct,
    )


@sales_bp.get("/discount-report.csv")
@login_required
@require_role("viewer")
def discount_report_csv():
    q = (request.args.get("q") or "").strip().lower()
    channel = (request.args.get("channel") or "").strip().lower()
    date_from = _safe_date(request.args.get("from") or "")
    date_to = _safe_date(request.args.get("to") or "")

    query = (
        db.session.query(
            SalesLine.sku.label("sku"),
            db.func.max(SalesLine.description).label("description"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("qty_sold"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("revenue_net"),
            db.func.coalesce(db.func.sum(SalesLine.cost_total), 0).label("cost_total"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
            db.func.coalesce(db.func.sum(SalesLine.line_discount_gross), 0).label("line_discount_gross"),
            db.func.coalesce(db.func.sum(SalesLine.order_discount_alloc_gross), 0).label("order_discount_alloc_gross"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
    )

    if q:
        query = query.filter(
            db.or_(
                db.func.lower(SalesLine.sku).contains(q),
                db.func.lower(SalesLine.description).contains(q),
            )
        )
    if channel:
        query = query.filter(db.func.lower(SalesOrder.channel) == channel)
    if date_from:
        query = query.filter(SalesOrder.order_date >= date_from)
    if date_to:
        query = query.filter(SalesOrder.order_date <= date_to)

    rows = (
        query.group_by(SalesLine.sku)
        .order_by(
            db.desc(
                db.func.coalesce(db.func.sum(SalesLine.line_discount_gross), 0)
                + db.func.coalesce(db.func.sum(SalesLine.order_discount_alloc_gross), 0)
            )
        )
        .yield_per(500)
    )

    headers = ["SKU", "Description", "Qty Sold", "Revenue Net", "Cost Total", "Profit", "Discount Gross", "Discount % (approx)"]

    def row_fn(r):
        disc_gross = Decimal(str(r.line_discount_gross or 0)) + Decimal(str(r.order_discount_alloc_gross or 0))
        rev_net = Decimal(str(r.revenue_net or 0))
        approx_gross = rev_net * Decimal("1.18")
        disc_pct = Decimal("0")
        if approx_gross > 0:
            disc_pct = (disc_gross / approx_gross) * Decimal("100")

        return [
            r.sku,
            r.description or "",
            int(r.qty_sold or 0),
            f"{rev_net:.2f}",
            f"{Decimal(str(r.cost_total or 0)):.2f}",
            f"{Decimal(str(r.profit or 0)):.2f}",
            f"{disc_gross:.2f}",
            f"{disc_pct:.2f}",
        ]

    return stream_csv(rows, headers, row_fn, filename="sales_discount_report.csv")


# -------------------------
# Alerts: negative margin / low margin / high discount
# -------------------------

@sales_bp.get("/alerts")
@login_required
@require_role("viewer")
def alerts():
    q = (request.args.get("q") or "").strip().lower()
    channel = (request.args.get("channel") or "").strip().lower()
    date_from = _safe_date(request.args.get("from") or "")
    date_to = _safe_date(request.args.get("to") or "")

    margin_threshold = _safe_decimal(request.args.get("margin") or "20", default=Decimal("20"))
    discount_threshold = _safe_decimal(request.args.get("discount") or "15", default=Decimal("15"))

    query = (
        db.session.query(
            SalesLine.sku.label("sku"),
            db.func.max(SalesLine.description).label("description"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("qty_sold"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("revenue_net"),
            db.func.coalesce(db.func.sum(SalesLine.cost_total), 0).label("cost_total"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
            db.func.coalesce(db.func.sum(SalesLine.line_discount_gross), 0).label("line_discount_gross"),
            db.func.coalesce(db.func.sum(SalesLine.order_discount_alloc_gross), 0).label("order_discount_alloc_gross"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
    )

    if q:
        query = query.filter(
            db.or_(
                db.func.lower(SalesLine.sku).contains(q),
                db.func.lower(SalesLine.description).contains(q),
            )
        )

    if channel:
        query = query.filter(db.func.lower(SalesOrder.channel) == channel)

    if date_from:
        query = query.filter(SalesOrder.order_date >= date_from)
    if date_to:
        query = query.filter(SalesOrder.order_date <= date_to)

    rows = query.group_by(SalesLine.sku).all()

    alert_rows = []
    counts = {"negative_profit": 0, "low_margin": 0, "high_discount": 0}

    for r in rows:
        rev_net = Decimal(str(r.revenue_net or 0))
        profit = Decimal(str(r.profit or 0))
        cost_total = Decimal(str(r.cost_total or 0))
        qty_sold = int(r.qty_sold or 0)

        disc_gross = Decimal(str(r.line_discount_gross or 0)) + Decimal(str(r.order_discount_alloc_gross or 0))

        margin_pct = Decimal("0")
        if rev_net > 0:
            margin_pct = (profit / rev_net) * Decimal("100")

        approx_gross_sales = rev_net * Decimal("1.18")
        discount_pct = Decimal("0")
        if approx_gross_sales > 0:
            discount_pct = (disc_gross / approx_gross_sales) * Decimal("100")

        is_negative = profit < 0
        is_low_margin = (rev_net > 0) and (margin_pct < margin_threshold)
        is_high_discount = (disc_gross > 0) and (discount_pct > discount_threshold)

        if is_negative:
            counts["negative_profit"] += 1
        if is_low_margin:
            counts["low_margin"] += 1
        if is_high_discount:
            counts["high_discount"] += 1

        if is_negative or is_low_margin or is_high_discount:
            alert_rows.append(
                {
                    "sku": r.sku,
                    "description": r.description or "",
                    "qty_sold": qty_sold,
                    "revenue_net": rev_net,
                    "cost_total": cost_total,
                    "profit": profit,
                    "margin_pct": margin_pct,
                    "discount_gross": disc_gross,
                    "discount_pct": discount_pct,
                    "flag_negative": is_negative,
                    "flag_low_margin": is_low_margin,
                    "flag_high_discount": is_high_discount,
                }
            )

    def _sort_key(x):
        return (
            0 if x["flag_negative"] else 1,
            float(x["margin_pct"]),
            -float(x["discount_pct"]),
            -float(x["profit"]),
        )

    alert_rows.sort(key=_sort_key)

    channels = [
        r[0]
        for r in db.session.query(SalesOrder.channel)
        .distinct()
        .order_by(SalesOrder.channel.asc())
        .all()
    ]

    return render_template(
        "sales/alerts.html",
        rows=alert_rows,
        counts=counts,
        q=q,
        channel=channel,
        date_from=(date_from.isoformat() if date_from else ""),
        date_to=(date_to.isoformat() if date_to else ""),
        channels=channels,
        margin_threshold=margin_threshold,
        discount_threshold=discount_threshold,
    )


@sales_bp.get("/alerts.csv")
@login_required
@require_role("viewer")
def alerts_csv():
    q = (request.args.get("q") or "").strip().lower()
    channel = (request.args.get("channel") or "").strip().lower()
    date_from = _safe_date(request.args.get("from") or "")
    date_to = _safe_date(request.args.get("to") or "")

    margin_threshold = _safe_decimal(request.args.get("margin") or "20", default=Decimal("20"))
    discount_threshold = _safe_decimal(request.args.get("discount") or "15", default=Decimal("15"))

    query = (
        db.session.query(
            SalesLine.sku.label("sku"),
            db.func.max(SalesLine.description).label("description"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("qty_sold"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("revenue_net"),
            db.func.coalesce(db.func.sum(SalesLine.cost_total), 0).label("cost_total"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
            db.func.coalesce(db.func.sum(SalesLine.line_discount_gross), 0).label("line_discount_gross"),
            db.func.coalesce(db.func.sum(SalesLine.order_discount_alloc_gross), 0).label("order_discount_alloc_gross"),
        )
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
    )

    if q:
        query = query.filter(
            db.or_(
                db.func.lower(SalesLine.sku).contains(q),
                db.func.lower(SalesLine.description).contains(q),
            )
        )
    if channel:
        query = query.filter(db.func.lower(SalesOrder.channel) == channel)
    if date_from:
        query = query.filter(SalesOrder.order_date >= date_from)
    if date_to:
        query = query.filter(SalesOrder.order_date <= date_to)

    rows = query.group_by(SalesLine.sku).yield_per(500)

    headers = [
        "SKU", "Description", "Qty Sold", "Revenue Net", "Cost Total", "Profit", "Margin %",
        "Discount Gross", "Discount % (approx)", "NEGATIVE_PROFIT", "LOW_MARGIN", "HIGH_DISCOUNT"
    ]

    def row_fn(r):
        rev_net = Decimal(str(r.revenue_net or 0))
        profit = Decimal(str(r.profit or 0))
        cost_total = Decimal(str(r.cost_total or 0))
        qty_sold = int(r.qty_sold or 0)

        disc_gross = Decimal(str(r.line_discount_gross or 0)) + Decimal(str(r.order_discount_alloc_gross or 0))

        margin_pct = Decimal("0")
        if rev_net > 0:
            margin_pct = (profit / rev_net) * Decimal("100")

        approx_gross_sales = rev_net * Decimal("1.18")
        discount_pct = Decimal("0")
        if approx_gross_sales > 0:
            discount_pct = (disc_gross / approx_gross_sales) * Decimal("100")

        flag_negative = profit < 0
        flag_low_margin = (rev_net > 0) and (margin_pct < margin_threshold)
        flag_high_discount = (disc_gross > 0) and (discount_pct > discount_threshold)

        if not (flag_negative or flag_low_margin or flag_high_discount):
            # Skip non-alert rows in CSV too
            return None

        return [
            r.sku,
            r.description or "",
            qty_sold,
            f"{rev_net:.2f}",
            f"{cost_total:.2f}",
            f"{profit:.2f}",
            f"{margin_pct:.2f}",
            f"{disc_gross:.2f}",
            f"{discount_pct:.2f}",
            "YES" if flag_negative else "",
            "YES" if flag_low_margin else "",
            "YES" if flag_high_discount else "",
        ]

    # stream_csv expects every row_fn to return a list; so we wrap an iterator that skips None
    def filtered_rows():
        for rr in rows:
            out = row_fn(rr)
            if out is not None:
                yield out

    return stream_csv(
        filtered_rows(),
        headers=headers,
        row_fn=lambda x: x,  # already formatted
        filename="sales_alerts.csv",
    )


# -------------------------
# Export CSV (orders list) - STREAMING
# -------------------------

@sales_bp.get("/export.csv")
@login_required
@require_role("viewer")
def export_orders_csv():
    q = (request.args.get("q") or "").strip().lower()
    channel = (request.args.get("channel") or "").strip().lower()
    date_from = _safe_date(request.args.get("from") or "")
    date_to = _safe_date(request.args.get("to") or "")

    base = SalesOrder.query
    if q:
        base = base.filter(
            db.or_(
                db.func.lower(SalesOrder.order_number).contains(q),
                db.func.lower(SalesOrder.customer_name).contains(q),
                db.func.lower(SalesOrder.customer_email).contains(q),
            )
        )
    if channel:
        base = base.filter(db.func.lower(SalesOrder.channel) == channel)
    if date_from:
        base = base.filter(SalesOrder.order_date >= date_from)
    if date_to:
        base = base.filter(SalesOrder.order_date <= date_to)

    totals_sq = (
        db.session.query(
            SalesLine.sales_order_id.label("oid"),
            db.func.coalesce(db.func.sum(SalesLine.revenue_net), 0).label("rev"),
            db.func.coalesce(db.func.sum(SalesLine.cost_total), 0).label("cost"),
            db.func.coalesce(db.func.sum(SalesLine.profit), 0).label("profit"),
            db.func.coalesce(db.func.sum(SalesLine.qty), 0).label("units"),
        )
        .group_by(SalesLine.sales_order_id)
        .subquery()
    )

    rows = (
        base.outerjoin(totals_sq, totals_sq.c.oid == SalesOrder.id)
        .add_columns(totals_sq.c.rev, totals_sq.c.cost, totals_sq.c.profit, totals_sq.c.units)
        .order_by(SalesOrder.order_date.desc(), SalesOrder.id.desc())
        .yield_per(500)
    )

    headers = ["Order Date", "Order Number", "Channel", "Currency", "Units", "Revenue Net", "Cost", "Profit", "Margin %"]

    def row_fn(row):
        so, rev, cost, prof, units = row
        rev_d = Decimal(str(rev or 0))
        prof_d = Decimal(str(prof or 0))
        margin = Decimal("0")
        if rev_d > 0:
            margin = (prof_d / rev_d) * Decimal("100")

        return [
            so.order_date.isoformat() if so.order_date else "",
            so.order_number or "",
            so.channel or "",
            so.currency or "",
            int(units or 0),
            f"{rev_d:.2f}",
            f"{Decimal(str(cost or 0)):.2f}",
            f"{prof_d:.2f}",
            f"{margin:.2f}",
        ]

    return stream_csv(rows, headers, row_fn, filename="sales_orders_export.csv")
