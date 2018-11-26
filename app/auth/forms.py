from wtforms import StringField,PasswordField,SubmitField,DecimalField
from flask_wtf import FlaskForm
from wtforms.validators import DataRequired, Length, Email, EqualTo, NumberRange

#登录表单
class LoginForm(FlaskForm):
    username=StringField('用户名',validators=[DataRequired()])
    password=PasswordField('密码', [
        DataRequired()
    ])
    submit=SubmitField('登录')

