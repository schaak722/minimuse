from functools import wraps
from flask import abort
from flask_login import current_user

ROLE_RANK = {"viewer": 1, "user": 2, "admin": 3}

def require_role(min_role: str):
    """
    Enforce role-based access:
      viewer < user < admin
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401)
            user_rank = ROLE_RANK.get(getattr(current_user, "role", "viewer"), 1)
            required_rank = ROLE_RANK.get(min_role, 3)
            if user_rank < required_rank:
                abort(403)
            if getattr(current_user, "is_active", True) is not True:
                abort(403)
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def require_admin(fn):
    return require_role("admin")(fn)

def require_edit_permission(fn):
    """
    Viewer is read-only.
    Use this decorator on POST/PUT/DELETE routes or any route that changes data.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            abort(401)
        if getattr(current_user, "role", "viewer") == "viewer":
            abort(403)
        if getattr(current_user, "is_active", True) is not True:
            abort(403)
        return fn(*args, **kwargs)
    return wrapper

