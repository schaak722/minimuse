from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SelectField, BooleanField, SubmitField
from wtforms.validators import DataRequired, Email, Length, Optional
from ..models import ROLE_CHOICES

class UserCreateForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Email(), Length(max=255)])
    name = StringField("Name", validators=[DataRequired(), Length(max=120)])
    role = SelectField("Role", choices=[(r, r) for r in ROLE_CHOICES], validators=[DataRequired()])
    password = PasswordField("Temporary password", validators=[DataRequired(), Length(min=6, max=128)])
    is_active = BooleanField("Active", default=True)
    submit = SubmitField("Create user")

class UserEditForm(FlaskForm):
    name = StringField("Name", validators=[DataRequired(), Length(max=120)])
    role = SelectField("Role", choices=[(r, r) for r in ROLE_CHOICES], validators=[DataRequired()])
    new_password = PasswordField("New password (optional)", validators=[Optional(), Length(min=6, max=128)])
    is_active = BooleanField("Active", default=True)
    submit = SubmitField("Save changes")

