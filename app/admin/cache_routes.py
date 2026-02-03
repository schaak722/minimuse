from flask import Blueprint, redirect, url_for, flash
from flask_login import login_required

from ..decorators import require_role

admin_cache_bp = Blueprint("admin_cache", __name__, url_prefix="/admin/cache")


@admin_cache_bp.post("/reset")
@login_required
@require_role("admin")
def reset_cache():
    # Cache is in-process; "reset" just tells user it clears per worker on restart.
    # We can’t reach module-level caches from here cleanly without central registry.
    flash("Cache is in-memory per server worker. Redeploy/restart clears it. (TTL is short anyway.)", "success")
    return redirect(url_for("admin.dashboard"))
