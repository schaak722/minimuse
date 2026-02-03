from datetime import datetime
from flask import Blueprint, render_template, request, url_for, jsonify
from flask_login import login_required, current_user

from ..decorators import require_role
from ..extensions import db
from ..models import Item, PurchaseOrder, PurchaseLine, SalesOrder, SalesLine

import hashlib
from ..utils.cache import TTLCache

search_bp = Blueprint("search", __name__, url_prefix="")
_search_cache = TTLCache(ttl_seconds=30, max_items=600)

def _q(s: str) -> str:
    return (s or "").strip()


def _fmt_date(d):
    try:
        return d.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _item_url_for_sku(sku: str) -> str:
    # Viewer cannot access edit screen; safest is list filtered by SKU
    return url_for("catalog.items_list", q=sku)


def _po_url(po_id: int) -> str:
    return url_for("purchases.purchase_detail", po_id=po_id)


def _so_url(order_id: int) -> str:
    return url_for("sales.sales_order_detail", order_id=order_id)


def _search_catalog(q: str, limit: int = 8):
    ql = q.lower()
    rows = (
        Item.query
        .filter(
            db.or_(
                db.func.lower(Item.sku).contains(ql),
                db.func.lower(Item.description).contains(ql),
            )
        )
        .order_by(Item.sku.asc())
        .limit(limit)
        .all()
    )
    out = []
    for it in rows:
        out.append({
            "title": it.sku,
            "subtitle": (it.description or "")[:80],
            "url": _item_url_for_sku(it.sku),
        })
    return out


def _search_purchase_orders(q: str, limit: int = 8):
    ql = q.lower()
    rows = (
        PurchaseOrder.query
        .filter(
            db.or_(
                db.func.lower(PurchaseOrder.order_number).contains(ql),
                db.func.lower(PurchaseOrder.supplier_name).contains(ql),
                db.func.lower(PurchaseOrder.brand).contains(ql),
            )
        )
        .order_by(PurchaseOrder.created_at.desc())
        .limit(limit)
        .all()
    )
    out = []
    for po in rows:
        supplier = po.supplier_name or "Supplier"
        brand = po.brand or ""
        meta = f"{supplier}" + (f" • {brand}" if brand else "")
        out.append({
            "title": f"PO {po.order_number}",
            "subtitle": meta,
            "url": _po_url(po.id),
        })
    return out


def _search_purchase_lines(q: str, limit: int = 8):
    # Helpful when user types a SKU and wants to find which PO it came in on
    ql = q.lower()
    rows = (
        db.session.query(PurchaseLine, PurchaseOrder)
        .join(PurchaseOrder, PurchaseLine.purchase_order_id == PurchaseOrder.id)
        .filter(db.func.lower(PurchaseLine.sku).contains(ql))
        .order_by(PurchaseOrder.created_at.desc())
        .limit(limit)
        .all()
    )
    out = []
    for pl, po in rows:
        supplier = po.supplier_name or "Supplier"
        out.append({
            "title": f"{pl.sku} in PO {po.order_number}",
            "subtitle": f"{supplier} • qty {pl.qty or 0}",
            "url": _po_url(po.id),
        })
    return out


def _search_sales_orders(q: str, limit: int = 8):
    ql = q.lower()
    rows = (
        SalesOrder.query
        .filter(
            db.or_(
                db.func.lower(SalesOrder.order_number).contains(ql),
                db.func.lower(SalesOrder.channel).contains(ql),
                db.func.lower(SalesOrder.customer_name).contains(ql),
                db.func.lower(SalesOrder.customer_email).contains(ql),
            )
        )
        .order_by(SalesOrder.order_date.desc())
        .limit(limit)
        .all()
    )
    out = []
    for so in rows:
        meta = f"{so.channel} • { _fmt_date(so.order_date) }"
        out.append({
            "title": f"Order {so.order_number}",
            "subtitle": meta,
            "url": _so_url(so.id),
        })
    return out


def _search_sales_lines(q: str, limit: int = 8):
    # Helpful when user searches a SKU and wants orders containing it
    ql = q.lower()
    rows = (
        db.session.query(SalesLine, SalesOrder)
        .join(SalesOrder, SalesLine.sales_order_id == SalesOrder.id)
        .filter(db.func.lower(SalesLine.sku).contains(ql))
        .order_by(SalesOrder.order_date.desc())
        .limit(limit)
        .all()
    )
    out = []
    for sl, so in rows:
        meta = f"{so.channel} • { _fmt_date(so.order_date) } • qty {sl.qty or 0}"
        out.append({
            "title": f"{sl.sku} in Order {so.order_number}",
            "subtitle": meta,
            "url": _so_url(so.id),
        })
    return out


def run_global_search(q: str):
    """
    Returns grouped results for UI.
    """
    q = _q(q)
    if not q:
        return {"catalog": [], "purchases": [], "sales": []}

    # Keep it fast: small limits per group
    catalog = _search_catalog(q, limit=8)

    purchases = []
    purchases.extend(_search_purchase_orders(q, limit=6))
    purchases.extend(_search_purchase_lines(q, limit=6))

    sales = []
    sales.extend(_search_sales_orders(q, limit=6))
    sales.extend(_search_sales_lines(q, limit=6))

    # Trim groups (avoid overly long dropdown)
    return {
        "catalog": catalog[:8],
        "purchases": purchases[:10],
        "sales": sales[:10],
    }


@search_bp.get("/search")
@login_required
@require_role("viewer")
def search_page():
    q = _q(request.args.get("q"))
    results = run_global_search(q) if len(q) >= 1 else {"catalog": [], "purchases": [], "sales": []}
    return render_template("search/results.html", q=q, results=results)


@search_bp.get("/api/search")
@login_required
@require_role("viewer")
def api_search():
    q = _q(request.args.get("q"))
    if len(q) < 2:
        return jsonify({"q": q, "results": {"catalog": [], "purchases": [], "sales": []}})

    # Cache key: normalized q + role (viewer vs user/admin)
    # (Role matters only if later you hide/expand results)
    role = getattr(current_user, "role", "viewer") or "viewer"
    key_raw = f"v1|{role}|{q.lower()}"
    key = hashlib.sha1(key_raw.encode("utf-8")).hexdigest()

    def build():
        return run_global_search(q)

    results = _search_cache.get_or_set(key, build)
    return jsonify({"q": q, "results": results})
