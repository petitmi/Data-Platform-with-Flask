from flask import render_template,request,flash,session
from flask_login import login_required
from app import db,REMOTE_HOST
from . import circle
from ..decorators import  permission_required
from ..models import Permission
from pyecharts_javascripthon.api import TRANSLATOR
from pyecharts import Bar,Line,Overlap
from app.configs.circle_sql_config import *
from app.configs.time_config import *
import pandas as pd


def get_rp_values():
    yesterday_sql=(datetime.datetime.now()-datetime.timedelta(1)).strftime('%Y-%m-%d')
    result={'data_totality': db.session.execute(sql_data_totality % yesterday_sql).fetchall()}
    return result

@circle.route('/circle-rp',methods=["POST","GET"])
@login_required
@permission_required(Permission.CIRCLE)

def get_dr_values(thatdate_sql):
    result={'data_daily': db.session.execute(sql_data_daily % (thatdate_sql, thatdate_sql)).fetchall()}
    result['posts_daily']= db.session.execute(sql_posts_detail % thatdate_sql).fetchall()
    result['boards_daily']= db.session.execute(sql_boards_detail % thatdate_sql).fetchall()
    overlap_day=olp_bar_line(sql_data_daily % (thatdate_sql, thatdate_sql))
    result['overlap_day'] = overlap_day.render_embed()
    return result

@circle.route('/circle-dr',methods=["POST","GET"])
@login_required
@permission_required(Permission.CIRCLE)
def circle_dr():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST' :
        thatdate_sql = request.form.get('input')
        if thatdate_sql>yesterday_sql :
            flash('选择的日期未经历')
    if request.method=='GET'or request.form.get('input') =='':
        thatdate_sql = yesterday_sql

    result_dr=get_dr_values(thatdate_sql)
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('circle-dr.html',
                           thatdate=thatdate_sql,
                           data_daily=result_dr['data_daily'],
                           posts_daily=result_dr['posts_daily'],
                           boards_daily=result_dr['boards_daily'],
                           overlap_day=result_dr['overlap_day'])

def get_mr_values(thatdate_sql):
    overlap_month=olp_bar_line(sql_data_monthly%(get_month_1st(thatdate_sql),thatdate_sql))
    result={'data_monthly': db.session.execute(
        sql_data_monthly % (get_month_1st(thatdate_sql), thatdate_sql)).fetchall()}
    result['overlap_month'] = overlap_month.render_embed()

    return result

@circle.route('/circle-mr',methods=["POST","GET"])
@login_required
@permission_required(Permission.CIRCLE)
def circle_mr():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST' :
        thatdate_sql = request.form.get('input')
        if thatdate_sql>yesterday_sql :
            flash('选择的日期未经历')
    if request.method=='GET'or request.form.get('input') =='':
        thatdate_sql = yesterday_sql
    result_mr=get_mr_values(thatdate_sql)
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('circle-mr.html',
                           thatdate=thatdate_sql,
                           overlap_month=result_mr['overlap_month'],
                           data_monthly=result_mr['data_monthly'])

def olp_bar_line(sql):
    result_circle_day = pd.read_sql(sql, db.engine).fillna(0)
    attr=result_circle_day['data_date'].sort_index(ascending=False).values.tolist()
    d1 = result_circle_day['login_uv'].sort_index(ascending=False).values.tolist()
    d3=result_circle_day['newly_login_uv'].sort_index(ascending=False).values.tolist()
    bar = Bar(width=1200, height=600)
    bar.add("登录uv", attr, d1)
    line = Line()
    line.add("新增登录uv", attr, d3, yaxis_interval=5)
    overlap_day = Overlap(width=600,height=260)
    overlap_day.add(bar)
    overlap_day.add(line, yaxis_index=1, is_add_yaxis=True)
    return overlap_day
