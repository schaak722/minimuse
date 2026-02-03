from flask_wtf import FlaskForm
from wtforms import StringField, DecimalField, BooleanField, SelectField, SubmitField
from wtforms.validators import DataRequired, Length, Optional, NumberRange

class ItemForm(FlaskForm):
    sku = StringField("SKU", validators=[DataRequired(), Length(max=80)])
    description = StringField("Description", validators=[DataRequired(), Length(max=255)])

    brand = StringField("Brand", validators=[Optional(), Length(max=80)])
    supplier = StringField("Supplier", validators=[Optional(), Length(max=120)])

    colour = StringField("Colour", validators=[Optional(), Length(max=80)])
    size = StringField("Size", validators=[Optional(), Length(max=40)])

    weight = DecimalField("Weight (kg)", validators=[Optional(), NumberRange(min=0)], places=3)
    vat_rate = DecimalField("VAT rate (%)", validators=[DataRequired(), NumberRange(min=0, max=100)], places=2, default=18)

    is_active = BooleanField("Active", default=True)

    submit = SubmitField("Save")

