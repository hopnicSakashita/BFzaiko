from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SelectField, DateField, SubmitField
from wtforms.validators import Optional, DataRequired

from app.models_master import CztrMstModel
from app.constants import FormChoiceConstants

class NoncoatStockSearchForm(FlaskForm):
    product_id = StringField('製品ID', validators=[Optional()])
    lot = IntegerField('LOT', validators=[Optional()])
    base = SelectField('ベース', choices=FormChoiceConstants.BASE_CHOICES, validators=[Optional()])
    adp = SelectField('加入度数', choices=FormChoiceConstants.ADP_CHOICES, validators=[Optional()])
    lr = SelectField('LR', choices=FormChoiceConstants.LR_CHOICES, validators=[Optional()])
    clr = SelectField('カラー', choices=FormChoiceConstants.COLOR_CHOICES, validators=[Optional()])

class ShipmentSearchForm(FlaskForm):
    """出荷一覧検索フォーム"""
    # 出荷先の選択肢を取得
    destination_choices = CztrMstModel.get_destination_choices()

    base = SelectField('ベース', choices=FormChoiceConstants.BASE_CHOICES, validators=[Optional()])
    adp = SelectField('加入度数', choices=FormChoiceConstants.ADP_CHOICES, validators=[Optional()])
    lr = SelectField('L/R', choices=FormChoiceConstants.LR_CHOICES, validators=[Optional()])
    color = SelectField('色', choices=FormChoiceConstants.COLOR_CHOICES, validators=[Optional()])
    proc_type = SelectField('コーティング', choices=FormChoiceConstants.COATING_CHOICES, validators=[Optional()])
    shipment_date = DateField('出荷日', validators=[Optional()])
    destination = SelectField('出荷先', choices=destination_choices, validators=[Optional()]) 
    order_no = IntegerField('客先注文番号', validators=[Optional()])
    shipment_status = SelectField('出荷状況', choices=FormChoiceConstants.SHIPMENT_STATUS_CHOICES, validators=[Optional()])
    order_date = DateField('手配日', validators=[Optional()])

class ProcOrderSearchForm(FlaskForm):
    """加工手配検索フォーム"""
    shipment_date = DateField('出荷日', validators=[Optional()])

class HardcoatStockForm(FlaskForm):
    shipment_date = DateField('出荷日', validators=[DataRequired()])
    submit = SubmitField('作成')

class BrcpSearchForm(FlaskForm):
    """受注データ検索フォーム"""
    order_date = DateField('受注日')
    product_id = StringField('製品ID')
    base = SelectField('ベース', choices=FormChoiceConstants.BASE_CHOICES, coerce=str)
    adp = SelectField('加入度数', choices=FormChoiceConstants.ADP_CHOICES, coerce=str)
    lr = SelectField('L/R', choices=FormChoiceConstants.LR_CHOICES, coerce=str)
    clr = SelectField('色', choices=FormChoiceConstants.COLOR_CHOICES, coerce=str)
    proc = SelectField('加工区分', choices=FormChoiceConstants.PROC_CHOICES, coerce=str)
    order_company = SelectField('出荷先', choices=[('', '全て')], coerce=str)
    order_no = IntegerField('客先受注番号')
    zan_select = SelectField('受注残', choices=FormChoiceConstants.ORDER_REMAIN_CHOICES, coerce=str)

class GprrForm(FlaskForm):
    """グラデ加工依頼データフォーム"""
    spec = SelectField('規格', choices=None, coerce=str)
    color = SelectField('色', choices=None, coerce=str)
    req_to = SelectField('依頼先', choices=None, coerce=str)
    req_date = DateField('依頼日', validators=[DataRequired()])
    qty = IntegerField('数量', validators=[DataRequired()])
    submit = SubmitField('登録')
