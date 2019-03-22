from flask import render_template,request,flash,session
from flask_login import login_required
from app import db,REMOTE_HOST
from . import edu
from ..decorators import permission_required
from ..models import Permission
from pyecharts import Bar,Line,Overlap,Liquid
from ..configs.edu_sql_config import *
from ..configs.time_config import *
import pandas as pd

def olp(attr,bar1,bar2,bar3,line1,line2,line3,bar1_title,bar2_title,bar3_title,line1_title,line2_title,line3_title,title,width,height):
    bar = Bar(title=title)
    bar.add(bar1_title, attr, bar1)
    if bar2 !=0:
        bar.add(bar2_title, attr, bar2,yaxis_interval=5)
    if bar3 !=0:
        bar.add(bar3_title, attr, bar3,yaxis_interval=5)

    line = Line()
    line.add(line1_title, attr, line1, yaxis_formatter=" ￥", yaxis_interval=5)
    if line2 !=0:
        line.add(line2_title, attr, line2,yaxis_formatter=" ￥", yaxis_interval=5)
    if line3 !=0:
        line.add(line3_title, attr, line3, yaxis_formatter=" ￥", yaxis_interval=5)

    overlap = Overlap(width=width, height=height)
    overlap.add(bar)
    overlap.add(line, yaxis_index=1, is_add_yaxis=True)
    return overlap

def lqd(liquid_data):
    liquid = Liquid("转化率")
    liquid.add("Liquid",liquid_data,is_liquid_animation=False, shape='diamond')
    return liquid


@edu.route('/edu-littleclass',methods=["POST","GET"])
@login_required
@permission_required(Permission.CIRCLE)
def littleclass():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    result_littleclass_total = pd.read_sql_query(sql_littleclass_total, db.engine).fillna(0)
    result_littleclass_post_total =pd.read_sql_query(sql_littleclass_post_total.format(yesterday_sql), db.engine).fillna(0)
    result_littleclass_day_total =pd.read_sql_query(sql_littleclass_day_total.format(yesterday_sql), db.engine).fillna(0)
    result_littleclass_day_post =pd.read_sql_query(sql_littleclass_day_post.format(yesterday_sql).format(yesterday_sql), db.engine).fillna(0)
    cvr_total=result_littleclass_total['cvr_deblock'].apply(lambda x: x / 100).values.tolist()
    cvr_day_total=result_littleclass_day_total['cvr_deblock'].apply(lambda x: x / 100).values.tolist()

    liquid_total=lqd(cvr_total)
    liquid_day_total=lqd(cvr_day_total)

    result_littleclass_total=result_littleclass_total.values.tolist()
    result_littleclass_post_total=result_littleclass_post_total.values.tolist()
    result_littleclass_day_total=result_littleclass_day_total.values.tolist()
    result_littleclass_day_post=result_littleclass_day_post.values.tolist()

    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')

    return render_template('edu-littleclass.html' ,
                           result_littleclass_total=result_littleclass_total,
                           result_littleclass_post_total=result_littleclass_post_total,
                           result_littleclass_day_total=result_littleclass_day_total,
                           result_littleclass_day_post=result_littleclass_day_post,
                           liquid_total=liquid_total.render_embed(),
                           liquid_day_total=liquid_day_total.render_embed())


def get_rp_values():
    today_sql=datetime.datetime.now().strftime('%Y-%m-%d')
    result={'edu_year': db.session.execute(sql_edu_year.format(today_sql)).fetchall()}
    result['edu_xiaoe'] = db.session.execute(sql_edu_xiaoe % today_sql).fetchall()
    result['edu_class_all']=db.session.execute(sql_edu_class_all).fetchall()
    return result

@edu.route('/edu-rp',methods=["POST","GET"])
@login_required
@permission_required(Permission.EDU)
def edu_rp():
    result_rp=get_rp_values()
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('edu-rp.html',
                           edu_year=result_rp['edu_year'],
                           edu_xiaoe=result_rp['edu_xiaoe'],
                           edu_class_all=result_rp['edu_class_all'])


def get_dr_values(thatdate_sql):
    results = {}
    results['days_list'] = get_days_list(days=7, thatdate=thatdate_sql).sql_list()
    sql_time_days_start = results['days_list'][0] + ' 00:00:00'
    sql_time_days_end = results['days_list'][6] + ' 23:59:59'
    sql_yest_start=results['days_list'][6] + ' 00:00:00'
    sql_yest_end=results['days_list'][6] + ' 23:59:59'
    sql_1day_start=results['days_list'][5] + ' 00:00:00'
    sql_1day_end=results['days_list'][5] + ' 23:59:59'
    sql_7day_start=results['days_list'][0] + ' 00:00:00'
    sql_7day_end=results['days_list'][0] + ' 23:59:59'
    results['edu_7days'] = pd.read_sql_query(sql_edu_7days.format(sql_time_days_start,sql_time_days_end), db.engine).fillna(0)
    attr_day = results['edu_7days']['date'].sort_index(ascending=False).values.tolist()
    bar1_day = results['edu_7days']['sales_count'].sort_index(ascending=False).values.tolist()
    line1_day = results['edu_7days']['sales_amount'].sort_index(ascending=False).values.tolist()
    overlap_day = olp(attr=attr_day, bar1=bar1_day, bar2=0, bar3=0, line1=line1_day, line2=0, line3=0, bar1_title="日销量",
                      bar2_title=0, bar3_title=0,
                      line1_title="日流水", line2_title=0, line3_title=0, title="日数据", width=600, height=260)
    results['edu_800vip_day']= db.session.execute(sql_edu_800vip.format(sql_yest_start, sql_yest_end)).fetchall()
    results['edu_yesterday'] = db.session.execute(sql_edu_yesterday.format(sql_yest_start,sql_yest_end)).fetchall()
    results['edu_1day'] = db.session.execute(sql_edu_1day.format(sql_1day_start,sql_1day_end)).fetchall()
    results['edu_7day'] = db.session.execute(sql_edu_7day.format(sql_7day_start,sql_7day_end)).fetchall()
    results['edu_class_day'] = db.session.execute(sql_edu_class .format (sql_yest_start, sql_yest_end)).fetchall()
    results['overlap_day'] = overlap_day.render_embed()
    businesses  = pd.read_sql(sql_business.format(sql_yest_start, sql_yest_end), con=db.engine)
    results['businesses']={'business_name':businesses['business_name'].values.tolist(),
                           'business_yest':businesses['business_yest'].values.tolist(),
                           'business_edu_activate': businesses['business_edu_activate'].values.tolist(),
                           'business_edu_ec': businesses['business_edu_ec'].values.tolist()
                           }
    cities  = pd.read_sql(sql_city.format(sql_yest_start, sql_yest_end), con=db.engine)
    results['cities']={'city_name':cities['city_name'].values.tolist(),
                           'city_members':cities['city_members'].values.tolist()}
    return results

@edu.route('/edu-dr',methods=["POST","GET"])
@login_required
@permission_required(Permission.EDU)
def edu_dr():
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
    return render_template('edu-dr.html',
                           thatdate=thatdate_sql,
                           edu_800vip_day=result_dr['edu_800vip_day'],
                           edu_yesterday=result_dr['edu_yesterday'],
                           edu_1day=result_dr['edu_1day'],
                           edu_7day=result_dr['edu_7day'],
                           edu_class_day=result_dr['edu_class_day'],
                           overlap_day=result_dr['overlap_day'],
                           businesses=result_dr['businesses'],
                           cities=result_dr['cities']
                           )


def get_wr_values(thatdate_sql):
    thatdate_monday_sql = get_monday(thatdate_sql)
    result_ec_week = pd.read_sql_query(sql_edu_week_compared .format(thatdate_sql) , db.engine).fillna(0)
    attr_week=result_ec_week['周'].sort_index(ascending=False).values.tolist()
    bar1_week=result_ec_week['周销量'].sort_index(ascending=False).values.tolist()
    line1_week=result_ec_week['周流水'].sort_index(ascending=False).values.tolist()
    overlap_week=olp(attr=attr_week,bar1=bar1_week,bar2=0,bar3=0,line1=line1_week,line2=0,line3=0,bar1_title="周销量",bar2_title=0,bar3_title=0,
                    line1_title="周流水",line2_title=0,line3_title=0,title="周数据",width=1000,height=300)
    result={'edu_800vip_week' : db.session.execute(sql_edu_800vip.format(thatdate_monday_sql, thatdate_sql)).fetchall()}
    result['edu_week'] = db.session.execute(sql_edu_week .format (thatdate_monday_sql, thatdate_sql)).fetchall()
    result['edu_class_week'] = db.session.execute(sql_edu_class .format (thatdate_monday_sql, thatdate_sql)).fetchall()
    result['overlap_week'] = overlap_week.render_embed()
    return result

@edu.route('/edu-wr',methods=["POST","GET"])
@login_required
@permission_required(Permission.EDU)
def edu_wr():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST' :
        thatdate_sql = request.form.get('input')
        if thatdate_sql>yesterday_sql :
            flash('选择的日期未经历')
    if request.method=='GET'or request.form.get('input') =='':
        thatdate_sql = yesterday_sql
    result_wr=get_wr_values(thatdate_sql)
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('edu-wr.html',
                           thatdate=thatdate_sql,
                           edu_800vip_week=result_wr['edu_800vip_week'],
                           edu_week=result_wr['edu_week'],
                           edu_class_week=result_wr['edu_class_week'],
                           overlap_week=result_wr['overlap_week'])

def get_mr_values(thatdate_sql):
    thatdate_month_1st_sql = get_month_1st(thatdate_sql)
    thatdate_sql_end=thatdate_sql+" 23:59:59"
    result_ec_month = pd.read_sql_query(sql_edu_month_compared, db.engine).fillna(0)
    result_edu_xiaoe = pd.read_sql_query(sql_edu_xiaoe, db.engine).fillna(0)
    attr_month = result_ec_month['月份'].values.tolist()
    bar1_month = result_ec_month['2017月销量'].values.tolist()
    bar2_month = result_ec_month['2018月销量'].values.tolist()
    result_ec_month.loc[7, ['2018月销量']] = result_ec_month[(result_ec_month['月份'] == 8)]['2018月销量'].values[0] + \
                                          result_edu_xiaoe.sales_count.values[0]
    result_ec_month.loc[7, ['2018月流水']] = result_ec_month[(result_ec_month['月份'] == 8)]['2018月流水'].values[0] + \
                                          result_edu_xiaoe.sales_amount.values[0]
    bar3_month = result_ec_month['2019月销量'].values.tolist()
    line1_month = result_ec_month['2017月流水'].values.tolist()
    line2_month = result_ec_month['2018月流水'].values.tolist()
    line3_month = result_ec_month['2019月流水'].values.tolist()
    attr_month = [(str(int(x)) + '月') for x in attr_month]

    overlap_month = olp(attr=attr_month, bar1=bar1_month, bar2=bar2_month, bar3=bar3_month, line1=line1_month,
                        line2=line2_month, line3=line3_month,
                        bar1_title="2017月销量", bar2_title="2018月销量", bar3_title="2019月销量",
                        line1_title="2017月流水", line2_title="2018月流水", line3_title="2019月流水", title="月数据", width=1000,
                        height=300)

    result={'edu_month': db.session.execute(sql_edu_month .format (thatdate_month_1st_sql, thatdate_sql_end)).fetchall()}
    result['edu_class_month'] = db.session.execute(sql_edu_class .format (thatdate_month_1st_sql, thatdate_sql_end)).fetchall()
    result['edu_800vip_month'] = db.session.execute(
        sql_edu_800vip.format(thatdate_month_1st_sql, thatdate_sql_end)).fetchall()
    result['overlap_month'] = overlap_month.render_embed()

    return result

@edu.route('/edu-mr',methods=["POST","GET"])
@login_required
@permission_required(Permission.EDU)
def edu_mr():
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
    return render_template('edu-mr.html',
                           thatdate=thatdate_sql,
                           edu_800vip_month=result_mr['edu_800vip_month'],
                           edu_month=result_mr['edu_month'],
                           edu_class_month=result_mr['edu_class_month'],
                           overlap_month=result_mr['overlap_month'])


