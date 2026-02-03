from urllib.parse import urlencode

from flask import Blueprint, request, redirect, url_for, flash
from flask_login import login_required, current_user

from ..extensions import db
from ..decorators import require_edit_permission, require_role
from ..models import SavedSearch

saved_searches_bp = Blueprint("saved_searches", __name__, url_prefix="/saved-searches")


_CONTEXT_ENDPOINT = {
    "sales": "sales.list_sales_orders",
    "purchases": "purchases.list_purchase_orders",
}


@saved_searches_bp.post("/save")
@login_required
@require_edit_permission
def save():
    context = (request.form.get("context") or "").strip().lower()
    name = (request.form.get("name") or "").strip()

    if context not in _CONTEXT_ENDPOINT:
        flash("Invalid saved search context.", "danger")
        return redirect(request.referrer or url_for("main.dashboard"))

    if not name:
        flash("Please provide a name for the saved search.", "danger")
        return redirect(request.referrer or url_for(_CONTEXT_ENDPOINT[context]))

    # Build params from the current filter fields (ignore empty)
    params = {}
    for k, v in request.form.items():
        if k in ("context", "name"):
            continue
        v = (v or "").strip()
        if v != "":
            params[k] = v

    base_url = url_for(_CONTEXT_ENDPOINT[context])
    qs = urlencode(params)
    full_url = f"{base_url}?{qs}" if qs else base_url

    existing = SavedSearch.query.filter_by(
        user_id=current_user.id, context=context, name=name
    ).first()

    if existing:
        existing.url = full_url
    else:
        db.session.add(SavedSearch(
            user_id=current_user.id,
            context=context,
            name=name,
            url=full_url,
        ))

    db.session.commit()
    flash("Saved search stored.", "success")
    return redirect(request.referrer or url_for(_CONTEXT_ENDPOINT[context]))


@saved_searches_bp.post("/<int:saved_id>/delete")
@login_required
@require_edit_permission
def delete(saved_id: int):
    ss = db.session.get(SavedSearch, saved_id)
    if not ss or ss.user_id != current_user.id:
        flash("Saved search not found.", "danger")
        return redirect(request.referrer or url_for("main.dashboard"))

    db.session.delete(ss)
    db.session.commit()
    flash("Saved search deleted.", "success")
    return redirect(request.referrer or url_for("main.dashboard"))
