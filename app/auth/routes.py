from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, current_user
from .forms import LoginForm
from ..models import User
from ..extensions import db

auth_bp = Blueprint("auth", __name__, url_prefix="")

@auth_bp.get("/login")
@auth_bp.post("/login")
def login():
    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))

    form = LoginForm()
    if form.validate_on_submit():
        email = form.email.data.strip().lower()
        password = form.password.data

        user = User.query.filter_by(email=email).first()
        if not user or not user.check_password(password):
            flash("Invalid email or password.", "danger")
            return render_template("auth/login.html", form=form)

        if not user.is_active:
            flash("Your account is inactive. Contact an administrator.", "danger")
            return render_template("auth/login.html", form=form)

        login_user(user, remember=True)
        next_url = request.args.get("next") or url_for("main.dashboard")
        return redirect(next_url)

    return render_template("auth/login.html", form=form)

@auth_bp.get("/logout")
def logout():
    if current_user.is_authenticated:
        logout_user()
    return redirect(url_for("auth.login"))

