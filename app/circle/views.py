from flask import render_template,request,flash,session
from flask_login import login_required
from .. import db,REMOTE_HOST
from . import circle
from ..decorators import  permission_required
from ..models import Permission
from pyecharts import Bar,Line,Overlap
from ..configs.circle_sql_config import *
from ..configs.time_config import *
from ..configs.config import *
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



def get_articles_values(date_end,days_form,keyword):
    import requests
    import json
    import re
    import pymysql
    dbconn_cine107 = pymysql.connect(host=host_cine1, port=port_cine1, user=user_cine1, password=password_cine1, db=db_cine1_cine107,
                                      charset='utf8')
    sqlconn_cine107=dbconn_cine107.cursor()

    end_date=date_end.strftime('%Y%m%d')
    date_start=date_end-datetime.timedelta(days=days_form)
    start_date=date_start.strftime('%Y%m%d')
    siteListUrl = "https://api.baidu.com/json/tongji/v1/ReportService/getData"
    data = {
        "header": {
            'username': bdtj_username,
            'password': bdtj_password,
            'token': bdjt_token,
            'Content-type': 'application/json'
        },
        "body": {
            'site_id': bdtj_site_id,
            "domain": bdtj_domain,
            'status': 0,
            'method': 'visit/toppage/a',
            # 开始统计时间
            'start_date': start_date,
            # 结束统计时间
            'end_date': end_date,
            # 获得pv和uv数据
            'metrics': 'pv_count,visitor_count,visit1_count,outward_count,exit_count,average_stay_time,exit_ratio',
            'order': 'visitor_count,desc'
        }
    }
    data = json.dumps(data)
    r = requests.post(siteListUrl, data=data).content
    urls_fields = json.loads(r)['body']['data'][0]['result']['fields']
    urls_items_title = json.loads(r)['body']['data'][0]['result']['items'][0]
    urls_items_value = json.loads(r)['body']['data'][0]['result']['items'][1]
    dct_urls = {}

    if keyword=='stream':
        for item_no in range(len(urls_items_title)):
            if ('stream/' in urls_items_title[item_no][0]['name']) and urls_items_value[item_no][1]>19:
                dct_urls[urls_items_title[item_no][0]['name']] = {}
                #正则匹配
                article_re=re.search(r'stream\/(\d+)[\?\/$]?', urls_items_title[item_no][0]['name'])
                #id内容
                aricle_id=article_re.group(1)
                #id位置
                article_link=article_re.end()
                #查询标题
                sqlconn_cine107.execute("select v_title from streams where id=%s"%aricle_id)
                article_title=sqlconn_cine107.fetchall()[0][0]
                #进入字典
                dct_urls[urls_items_title[item_no][0]['name']]['title']=article_title
                dct_urls[urls_items_title[item_no][0]['name']]['link']=urls_items_title[item_no][0]['name'][article_link:]
                for field_no in range(1, len(urls_fields)):
                    dct_urls[urls_items_title[item_no][0]['name']][urls_fields[field_no]] = urls_items_value[item_no][field_no - 1]
    else:
        for item_no in range(len(urls_items_title)):
            if (keyword in urls_items_title[item_no][0]['name']) and urls_items_value[item_no][1]>19:
                dct_urls[urls_items_title[item_no][0]['name']] = {}
                dct_urls[urls_items_title[item_no][0]['name']]['title']=urls_items_title[item_no][0]['name']
                for field_no in range(1, len(urls_fields)):
                    dct_urls[urls_items_title[item_no][0]['name']][urls_fields[field_no]] = urls_items_value[item_no][field_no - 1]
    dbconn_cine107.close()
    result={}
    result['dct_urls']=dct_urls
    result['date_start']=date_start.strftime('%Y-%m-%d')
    result['date_end']=date_end

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


@circle.route('/articles-rp',methods=["POST","GET"])
@login_required
def articles_rp():
    date_end_default=datetime.datetime.today()
    days_form=0
    dct_selected={'stream':"107文章",'cinehello':"cinehello全站","zhuanti":"专题","company":"公司"}
    if request.method == 'POST' :
        days_form =int(request.form.get('days_form'))
        keyword=request.form.get('selected')
        date_end_form=datetime.datetime.strptime(request.form.get('date_end'),'%Y-%m-%d')
        if date_end_form<date_end_default :
            date_end=date_end_form
        else:
            date_end=date_end_default
            flash('时间格式有误')
    elif request.method=='GET':
        keyword = 'stream'
        date_end = date_end_default
    else:
        flash('时间格式有误')
    results_articles=get_articles_values(date_end=date_end,days_form=days_form,keyword=keyword)

    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('articles-rp.html',
                           date_end=date_end.strftime('%Y-%m-%d'),
                           date_start=results_articles['date_start'],
                           dct_urls=results_articles['dct_urls'],
                           days_form=days_form,
                           dct_selected=dct_selected,
                           keyword=keyword)

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
