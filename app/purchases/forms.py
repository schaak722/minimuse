from flask_wtf import FlaskForm
from wtforms import DecimalField, SelectField, SubmitField
from wtforms.validators import Optional, NumberRange

class PurchaseCostsForm(FlaskForm):
    freight_total = DecimalField("Freight total (EUR)", validators=[Optional(), NumberRange(min=0)], places=2)
    allocation_method = SelectField(
        "Allocation method",
        choices=[("value", "By line value"), ("qty", "By quantity")],
        default="value",
    )
    submit = SubmitField("Save & Recalculate")

