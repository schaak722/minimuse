import csv
import io
from decimal import Decimal, InvalidOperation

from flask import Blueprint, render_template, redirect, url_for, flash, request, Response, jsonify
from flask_login import login_required

from ..extensions import db
from ..decorators import require_role, require_edit_permission
from ..models import Item
from .forms import ItemForm

import hashlib
from ..utils.cache import TTLCache


catalog_bp = Blueprint("catalog", __name__, url_prefix="/catalog")
_catalog_cache = TTLCache(ttl_seconds=45, max_items=800)

def _safe_decimal(val, default=None):
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


@catalog_bp.get("/items")
@login_required
@require_role("viewer")
def items_list():
    q = (request.args.get("q") or "").strip()
    brand = (request.args.get("brand") or "").strip()
    supplier = (request.args.get("supplier") or "").strip()
    status = (request.args.get("status") or "active").strip()  # active|inactive|all

    page = int(request.args.get("page") or 1)
    per_page = 50

    query = Item.query

    if q:
        ql = q.lower()
        query = query.filter(
            db.or_(
                db.func.lower(Item.sku).contains(ql),
                db.func.lower(Item.description).contains(ql),
            )
        )

    if brand:
        query = query.filter(db.func.lower(Item.brand).contains(brand.lower()))

    if supplier:
        query = query.filter(db.func.lower(Item.supplier).contains(supplier.lower()))

    if status == "active":
        query = query.filter(Item.is_active.is_(True))
    elif status == "inactive":
        query = query.filter(Item.is_active.is_(False))

    # distinct filter lists
    brands = [b[0] for b in db.session.query(Item.brand).filter(Item.brand.isnot(None), Item.brand != "").distinct().order_by(Item.brand).all()]
    suppliers = [s[0] for s in db.session.query(Item.supplier).filter(Item.supplier.isnot(None), Item.supplier != "").distinct().order_by(Item.supplier).all()]

    total = query.count()
    items = (
        query.order_by(Item.sku.asc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    total_pages = max(1, (total + per_page - 1) // per_page)

    return render_template(
        "catalog/items_list.html",
        items=items,
        q=q,
        brand=brand,
        supplier=supplier,
        status=status,
        brands=brands,
        suppliers=suppliers,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
    )


@catalog_bp.get("/items/new")
@catalog_bp.post("/items/new")
@login_required
@require_edit_permission
def items_new():
    form = ItemForm()
    if form.validate_on_submit():
        sku = form.sku.data.strip()
        if Item.query.filter_by(sku=sku).first():
            flash("SKU already exists.", "danger")
            return render_template("catalog/item_form.html", form=form, mode="create")

        item = Item(
            sku=sku,
            description=form.description.data.strip(),
            brand=(form.brand.data or "").strip() or None,
            supplier=(form.supplier.data or "").strip() or None,
            colour=(form.colour.data or "").strip() or None,
            size=(form.size.data or "").strip() or None,
            weight=form.weight.data,
            vat_rate=form.vat_rate.data,
            is_active=bool(form.is_active.data),
        )
        db.session.add(item)
        db.session.commit()
        flash("Item created.", "success")
        return redirect(url_for("catalog.items_list"))

    return render_template("catalog/item_form.html", form=form, mode="create")


@catalog_bp.get("/items/<int:item_id>/edit")
@catalog_bp.post("/items/<int:item_id>/edit")
@login_required
@require_edit_permission
def items_edit(item_id: int):
    item = db.session.get(Item, item_id)
    if not item:
        flash("Item not found.", "danger")
        return redirect(url_for("catalog.items_list"))

    form = ItemForm(obj=item)
    # SKU editing: keep field but we will prevent duplicates
    if form.validate_on_submit():
        new_sku = form.sku.data.strip()
        existing = Item.query.filter(Item.sku == new_sku, Item.id != item.id).first()
        if existing:
            flash("Another item already uses that SKU.", "danger")
            return render_template("catalog/item_form.html", form=form, mode="edit", item=item)

        item.sku = new_sku
        item.description = form.description.data.strip()
        item.brand = (form.brand.data or "").strip() or None
        item.supplier = (form.supplier.data or "").strip() or None
        item.colour = (form.colour.data or "").strip() or None
        item.size = (form.size.data or "").strip() or None
        item.weight = form.weight.data
        item.vat_rate = form.vat_rate.data
        item.is_active = bool(form.is_active.data)

        db.session.commit()
        flash("Item updated.", "success")
        return redirect(url_for("catalog.items_list", q=request.args.get("q", "")))

    return render_template("catalog/item_form.html", form=form, mode="edit", item=item)


@catalog_bp.get("/export.csv")
@login_required
@require_role("viewer")
def export_csv():
    # export current filters
    q = (request.args.get("q") or "").strip()
    brand = (request.args.get("brand") or "").strip()
    supplier = (request.args.get("supplier") or "").strip()
    status = (request.args.get("status") or "active").strip()

    query = Item.query
    if q:
        ql = q.lower()
        query = query.filter(
            db.or_(
                db.func.lower(Item.sku).contains(ql),
                db.func.lower(Item.description).contains(ql),
            )
        )
    if brand:
        query = query.filter(db.func.lower(Item.brand).contains(brand.lower()))
    if supplier:
        query = query.filter(db.func.lower(Item.supplier).contains(supplier.lower()))
    if status == "active":
        query = query.filter(Item.is_active.is_(True))
    elif status == "inactive":
        query = query.filter(Item.is_active.is_(False))

    items = query.order_by(Item.sku.asc()).all()

    def generate():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["SKU", "Description", "Brand", "Supplier", "Colour", "Size", "Weight_kg", "VAT_rate_pct", "Active"])
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for it in items:
            writer.writerow([
                it.sku,
                it.description,
                it.brand or "",
                it.supplier or "",
                it.colour or "",
                it.size or "",
                str(it.weight or ""),
                str(it.vat_rate),
                "1" if it.is_active else "0",
            ])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=catalog_export.csv"},
    )


@catalog_bp.get("/import")
@catalog_bp.post("/import")
@login_required
@require_edit_permission
def import_csv():
    """
    Imports rows immediately.
    Expected columns (case-insensitive):
      sku, description, brand, supplier, colour, size, weight_kg, vat_rate_pct, active
    """
    if request.method == "GET":
        return render_template("catalog/import_csv.html")

    f = request.files.get("file")
    if not f or f.filename == "":
        flash("Please choose a CSV file.", "danger")
        return redirect(url_for("catalog.import_csv"))

    raw = f.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        flash("CSV must be UTF-8 encoded.", "danger")
        return redirect(url_for("catalog.import_csv"))

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        flash("CSV appears empty or invalid.", "danger")
        return redirect(url_for("catalog.import_csv"))

    # normalize headers
    headers = {h.lower().strip(): h for h in reader.fieldnames}

    def get(row, key):
        h = headers.get(key)
        if not h:
            return ""
        return (row.get(h) or "").strip()

    created = 0
    updated = 0
    errors = 0
    error_samples = []

    for idx, row in enumerate(reader, start=2):  # header line = 1
        sku = get(row, "sku")
        desc = get(row, "description")

        if not sku or not desc:
            errors += 1
            if len(error_samples) < 8:
                error_samples.append(f"Line {idx}: missing SKU or Description")
            continue

        item = Item.query.filter_by(sku=sku).first()
        if not item:
            item = Item(sku=sku, description=desc)
            db.session.add(item)
            created += 1
        else:
            item.description = desc
            updated += 1

        item.brand = get(row, "brand") or None
        item.supplier = get(row, "supplier") or None
        item.colour = get(row, "colour") or None
        item.size = get(row, "size") or None

        w = get(row, "weight_kg")
        item.weight = _safe_decimal(w, default=None)

        vat = get(row, "vat_rate_pct") or "18"
        item.vat_rate = _safe_decimal(vat, default=Decimal("18.00"))

        active = get(row, "active")
        if active == "":
            item.is_active = True
        else:
            item.is_active = str(active).strip().lower() in ("1", "true", "yes", "y", "active")

    db.session.commit()

    flash(f"Import complete. Created: {created}, Updated: {updated}, Errors: {errors}.", "success" if errors == 0 else "warning")
    if error_samples:
        flash("Some issues: " + " | ".join(error_samples), "warning")

    return redirect(url_for("catalog.items_list"))


@catalog_bp.get("/api/search")
@login_required
@require_role("viewer")
def api_search_items():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])

    key_raw = f"v1|{q.lower()}"
    key = hashlib.sha1(key_raw.encode("utf-8")).hexdigest()

    def build():
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
            .limit(12)
            .all()
        )
        return [{"sku": r.sku, "description": r.description or ""} for r in rows]

    return jsonify(_catalog_cache.get_or_set(key, build))
