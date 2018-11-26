from app.auth.forms import LoginForm
from flask_login import login_user, logout_user
from flask import flash,redirect,render_template
from flask_login import login_required
from ..models import User
from . import auth
from flask import url_for,request,session
import datetime
import re

@auth.route("/login",methods=["POST","GET"])
def login():
    form=LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data.encode('utf-8')).first()
        if user is not None and user.password == form.password.data:
            login_user(user)
            if 'next' in request.referrer:
                next='/'+re.findall(re.compile(r'.*next=%2F(.*)'),request.referrer)[0]
            else:
                next = url_for('main.index')
            flash('登录成功')
            return redirect(next)
        else:
            flash('用户名或密码不对')
            return redirect('auth/login')

    print('UA:', request.user_agent.string)
    print('\033[1;35m' + request.remote_addr + ' - ' + datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S') + ' - ' + request.path + '\033[0m')
    return render_template('login.html',form=form)

@auth.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect('/')