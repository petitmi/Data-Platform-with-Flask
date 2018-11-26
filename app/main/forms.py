from wtforms import StringField,IntegerField,SubmitField,DecimalField
from flask_wtf import FlaskForm
from wtforms.validators import DataRequired

#登录表单
class BrushForm(FlaskForm):
    mutiple=IntegerField('1000X',validators=[DataRequired()])
    email=StringField('邮箱', [DataRequired()])
    url=StringField('网址',[DataRequired()])
    submit=SubmitField('开始刷')

