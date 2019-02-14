from flask import render_template,request,flash,session
from flask_login import login_required
from app import db,REMOTE_HOST
from . import ec
from ..decorators import  permission_required
from ..models import Permission
from pyecharts_javascripthon.api import TRANSLATOR
from pyecharts import Bar,Line,Overlap
from app.configs.ec_sql_config import *
from app.configs.time_config import *
import numpy as np
import pandas as pd
from functools import reduce
import datetime
@ec.route('/ec-tables',methods=["POST","GET"])
@login_required
@permission_required(Permission.EC)
def ec_tables():
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('ec-tables.html',
                           result_ec_table_week=db.session.execute(sql_ec_table_week).fetchall(),
                           result_ec_table_month=db.session.execute(sql_ec_table_month).fetchall())


def get_rp_values():
    today_sql=datetime.datetime.now().strftime('%Y-%m-%d')
    sql_today = get_sql_time(today_sql,0)
    result={}
    result['ec_year']=db.session.execute(sql_ec_year.format(today_sql)).fetchall()
    result['ec_offline_2018']=db.session.execute(sql_ec_offline_2018.format(sql_today['time_end'])).fetchall()
    result['ec_vip']=db.session.execute(sql_ec_99vip.format('2015-01-01', sql_today['time_end'])).fetchall()
    result['ec_vip_sale']=db.session.execute(sql_ec_99vip_sale_all).fetchall()
    result['ec_goods'] = db.session.execute(sql_ec_goods .format ('2010-01-01', sql_today['time_end'])).fetchall()
    return result

@ec.route('/ec-rp',methods=["POST","GET"])
@login_required
@permission_required(Permission.EC)
def ec_rp():
    result_rp=get_rp_values()
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('ec-rp.html',ec_year=result_rp['ec_year'],
                           offline_2018=result_rp['ec_offline_2018'],
                           ec_goods=result_rp['ec_goods'],
                           ec_vip=result_rp['ec_vip'],
                           ec_vip_sale=result_rp['ec_vip_sale'])


def get_sql_time(thatdate_sql,day):
    thatdate=datetime.datetime.strptime(thatdate_sql, '%Y-%m-%d')
    sql_someday=(thatdate-datetime.timedelta(days=day)).strftime('%Y-%m-%d')
    sql_time={}
    sql_time['time_start']=sql_someday+ ' 00:00:00'
    sql_time['time_end']=sql_someday+ ' 23:59:59'
    return sql_time

def get_dr_values(thatdate_sql):
    sql_yest = get_sql_time(thatdate_sql,0)
    sql_1day=get_sql_time(thatdate_sql,1)
    sql_7day=get_sql_time(thatdate_sql,7)

    result_ec = pd.read_sql_query(sql_ec_7days.format(sql_7day['time_start'],sql_yest['time_end']), db.engine).fillna(0)
    attr_day = result_ec['date'].sort_index(ascending=False).values.tolist()
    bar1_day = result_ec['sales_count'].sort_index(ascending=False).values.tolist()
    line1_day = result_ec['sales_amount'].sort_index(ascending=False).values.tolist()
    overlap_day = olp(attr=attr_day, bar1=bar1_day, bar2=0, bar3=0, line1=line1_day, line2=0, line3=0, bar1_title='日销量',
                      bar2_title=0, bar3_title=0, line1_title='日流水', line2_title=0, line3_title=0,
                      title='自主下单日数据', width=600, height=260)
    result={}

    result['ec_vip_day' ]= db.session.execute(sql_ec_99vip.format(sql_yest['time_start'], sql_yest['time_end'])).fetchall()
    print(sql_ec_99vip_sale.format(sql_yest['time_start'], sql_yest['time_end']))
    result['ec_vip_day_sale' ]= db.session.execute(sql_ec_99vip_sale.format(sql_yest['time_start'], sql_yest['time_end'])).fetchall()
    result['ec_yesterday'] = db.session.execute(sql_ec_yesterday.format(sql_yest['time_start'], sql_yest['time_end'])).fetchall()
    result['ec_1day'] = db.session.execute(sql_ec_1day.format(sql_1day['time_start'], sql_1day['time_end'])).fetchall()
    result['ec_7day'] = db.session.execute(sql_ec_7day.format(sql_7day['time_start'], sql_7day['time_end'])).fetchall()
    result['ec_type_day'] = db.session.execute(sql_ec_type .format(sql_yest['time_start'], sql_yest['time_end'])).fetchall()
    result['script_list'] = overlap_day.get_js_dependencies()
    result['overlap_day'] = overlap_day.render_embed()

    return result

@ec.route('/ec-dr',methods=["POST","GET"])
@login_required
@permission_required(Permission.EC)
def ec_dr():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST' :
        thatdate_sql = request.form.get('input')
        if thatdate_sql>yesterday_sql :
            flash('选择的日期未经历')
            thatdate_sql=yesterday_sql
    if request.method=='GET'or request.form.get('input') =='':
        thatdate_sql = yesterday_sql
    result_dr=get_dr_values(thatdate_sql)
    print(result_dr)
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('ec-dr.html',ec_vip_day=result_dr['ec_vip_day'],
                           ec_vip_day_sale=result_dr['ec_vip_day_sale'],
                           ec_yesterday=result_dr['ec_yesterday'],
                           ec_1day=result_dr['ec_1day'],
                           ec_7day=result_dr['ec_7day'],
                           ec_type_day=result_dr['ec_type_day'],
                           script_list=result_dr['script_list'],
                           overlap_day=result_dr['overlap_day'],
                           thatdate=thatdate_sql)


def get_wr_values(thatdate_sql):
    thatdate_monday_sql = get_monday(thatdate_sql)
    sql_monday_time=get_sql_time(thatdate_monday_sql,0)
    sql_yest_time=get_sql_time(thatdate_sql,0)

    result_ec_week = pd.read_sql_query(sql_ec_week_compared .format(sql_yest_time['time_end']), db.engine).fillna(0)
    result_ec_chanjet_week = pd.read_sql_query(sql_ec_chanjet_week_compared .format(sql_yest_time['time_end']), db.engine).fillna(0)
    attr_week = result_ec_week['week_num'].sort_index(ascending=False).values.tolist()
    # bar1_week = result_ec_week['周目标'].sort_index(ascending=False).values.tolist()
    bar2_week = result_ec_week['sales_count'].sort_index(ascending=False).values.tolist()
    line1_week = result_ec_chanjet_week['sales_amount'].sort_index(ascending=False).values.tolist()
    line2_week = result_ec_week['sales_amount'].sort_index(ascending=False).values.tolist()
    overlap_week = olp(attr=attr_week, bar2=0, bar1=bar2_week, bar3=0, line1=line1_week, line2=line2_week, line3=0,
                       bar2_title='周目标',
                       bar1_title='周自主销量', bar3_title=0, line1_title='周顾问流水', line2_title='周自主流水', line3_title=0,
                       title='自主下单周数据', width=1000, height=300)
    result={}
    result['ec_vip_week']=db.session.execute(sql_ec_99vip.format(sql_monday_time['time_start'],sql_yest_time['time_end'])).fetchall()
    result['ec_vip_week_sale']=db.session.execute(sql_ec_99vip_sale.format(sql_monday_time['time_start'],sql_yest_time['time_end'])).fetchall()
    result['ec_week'] = db.session.execute(sql_ec_week .format(sql_monday_time['time_start'],sql_yest_time['time_end'])).fetchall()
    result['ec_type_week'] = db.session.execute(sql_ec_type .format (sql_monday_time['time_start'],sql_yest_time['time_end'])).fetchall()
    result['ec_goods_week'] = db.session.execute(sql_ec_goods .format (sql_monday_time['time_start'],sql_yest_time['time_end'])).fetchall()
    result['overlap_week'] = overlap_week.render_embed()
    return result

@ec.route('/ec-wr', methods=["POST", "GET"])
@login_required
@permission_required(Permission.EC)
def ec_wr():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST':
        thatdate_sql = request.form.get('input')
        if thatdate_sql > yesterday_sql:
            flash('选择的日期未经历')
            thatdate_sql = yesterday_sql
    if request.method == 'GET' or request.form.get('input') == '':
        thatdate_sql = yesterday_sql
    result_wr=get_wr_values(thatdate_sql)
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('ec-wr.html',ec_vip_week=result_wr['ec_vip_week'],
                           ec_week=result_wr['ec_week'],
                           ec_vip_week_sale=result_wr['ec_vip_week_sale'],
                           thatdate=thatdate_sql,
                           ec_type_week=result_wr['ec_type_week'],
                           ec_goods_week=result_wr['ec_goods_week'],
                           overlap_week=result_wr['overlap_week'])

def get_mr_values(thatdate_sql):
    thatdate_month_1st_sql = get_month_1st(thatdate_sql)


    result_ec = pd.read_sql_query(sql_ec_month_compared, db.engine).fillna(0)
    # 线下2017
    result_ec_consult_2017 = pd.read_sql(sql_ec_month_offline_2017, db.engine).fillna(0)
    # 线下2018
    result_ec_offline_2018 = pd.read_sql(sql_ec_month_offline_2018, db.engine).fillna(0)
    # 畅接通2018
    result_ec_chanjet_2018 = pd.read_sql(sql_ec_month_chanjet_2018, db.engine).fillna(0)
    # 合并2018
    result_ec_consult_2018 = pd.concat([result_ec_offline_2018, result_ec_chanjet_2018], ignore_index=True)
    # 补充行数
    x = np.zeros((12 - len(result_ec_offline_2018) - len(result_ec_chanjet_2018), 2))
    result_ec_consult_2018 = result_ec_consult_2018.append(
        pd.DataFrame(x, index=range(len(result_ec_consult_2018), 12), columns=['sales_count', 'sales_amount']),
        sort=True)

    attr_month_online = result_ec['月份'].values.tolist()
    attr_month_online = [(str(x) + '月') for x in attr_month_online]

    bar1_month_online = (result_ec['2017月销量'])
    bar2_month_online = (result_ec['2018月销量'])
    line1_month_online = (result_ec['2017月流水'])
    line2_month_online = (result_ec['2018月流水'])

    bar1_month_consult = result_ec_consult_2017['sales_count']
    bar2_month_consult = result_ec_consult_2018['sales_count']
    line1_month_consult = result_ec_consult_2017['sales_amount']
    line2_month_consult = result_ec_consult_2018['sales_amount']

    compare_2017_sales = bar1_month_online + bar1_month_consult
    compare_2018_sales = bar2_month_online + bar2_month_consult
    compare_2017_amount = line1_month_online + line1_month_consult
    compare_2018_amount = line2_month_online + line2_month_consult
    # 对比数据为线上线下相加
    overlap_month_online = olp(attr=attr_month_online, bar1=compare_2017_sales.values.tolist(),
                               bar2=compare_2018_sales.values.tolist(), bar3=0,
                               line1=compare_2017_amount.values.tolist(), line2=compare_2018_amount.values.tolist(),
                               line3=0, bar1_title='2017月销量',
                               bar2_title='2018月销量', bar3_title=0, line1_title='2017月流水', line2_title='2018月流水',
                               line3_title=0,
                               title='2017、2018月对比数据', width=1000, height=300)

    attr_month_consult = result_ec_consult_2017['month'].values.tolist()
    attr_month_consult = [(str(x) + '月') for x in attr_month_consult]

    overlap_month_consult = olp(attr=attr_month_consult, bar1=bar2_month_online.values.tolist(),
                                bar2=bar2_month_consult.values.tolist(), bar3=0,
                                line1=line2_month_online.values.tolist(), line2=line2_month_consult.values.tolist(),
                                line3=0, bar1_title='自主月销量',
                                bar2_title='顾问月销量', bar3_title=0, line1_title='自主月流水', line2_title='顾问月流水',
                                line3_title=0,
                                title='2018月数据', width=1000, height=300)


    sql_month_1st_time=get_sql_time(thatdate_month_1st_sql,0)
    sql_yest_time=get_sql_time(thatdate_sql,0)

    result={}
    result['overlap_month_online']=overlap_month_online.render_embed()
    result['overlap_month_consult']=overlap_month_consult.render_embed()

    result['ec_vip_month'] = db.session.execute(sql_ec_99vip.format(sql_month_1st_time['time_start'], sql_yest_time['time_end'])).fetchall()
    result['ec_vip_month_sale'] = db.session.execute(sql_ec_99vip_sale.format(sql_month_1st_time['time_start'], sql_yest_time['time_end'])).fetchall()
    result['ec_month'] = db.session.execute(sql_ec_month .format (sql_month_1st_time['time_start'], sql_yest_time['time_end'])).fetchall()
    result['ec_chanjet_month'] = db.session.execute(sql_ec_chanjet_month .format (sql_month_1st_time['time_start'], sql_yest_time['time_end'])).fetchall()
    result['thatdate'] = thatdate_sql
    result['ec_type_month'] = db.session.execute(sql_ec_type  .format (sql_month_1st_time['time_start'], sql_yest_time['time_end'])).fetchall()
    result['ec_goods_month'] = db.session.execute(sql_ec_goods .format (sql_month_1st_time['time_start'], sql_yest_time['time_end'])).fetchall()
    result['ec_goods_day'] = db.session.execute(sql_ec_goods  .format (sql_month_1st_time['time_start'], sql_yest_time['time_end'])).fetchall()
    host = REMOTE_HOST
    my_width = "100%"
    my_height = 300
    return result




@ec.route('/ec-mr',methods=["POST","GET"])
@login_required
@permission_required(Permission.EC)
def ec_mr():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST' :
        thatdate_sql = request.form.get('input')
        if thatdate_sql>yesterday_sql :
            flash('选择的日期未经历')
            thatdate_sql=yesterday_sql
    if request.method=='GET'or request.form.get('input') =='':
        thatdate_sql = yesterday_sql
    result_mr=get_mr_values(thatdate_sql)
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('ec-mr.html',
                           overlap_month_online= result_mr['overlap_month_online'],
                           overlap_month_consult=result_mr['overlap_month_consult'],
                           ec_vip_month=result_mr['ec_vip_month'],
                           ec_vip_month_sale=result_mr['ec_vip_month_sale'],
                           ec_month=result_mr['ec_month'],
                           ec_chanjet_month=result_mr['ec_chanjet_month'],
                           thatdate=result_mr['thatdate'],
                           ec_type_month= result_mr['ec_type_month'],
                           ec_goods_month=result_mr['ec_goods_month'],
                           ec_goods_day=result_mr['ec_goods_day']

                           )

@ec.route('/ec-repur',methods=["POST","GET"])
@login_required
@permission_required(Permission.EC)
def ec_repur():
    sql = 'select receiver_name,pay_time from ec_orders where order_state=1;'
    yesterday_sql = datetime.datetime.now() - datetime.timedelta(1)
    d = pd.read_sql_query(sql, con=db.engine)
    weeklist_one = weeklist(yesterday_sql,7)
    dup_oneweek=dup(d,weeklist_one)
    weeklist_double = weeklist(yesterday_sql,14)
    dup_twoweek=dup(d,weeklist_double)
    monthlist = re_monthlist(yesterday_sql)
    dup_month=dup(d,monthlist)
    return render_template('repurchase.html',
                           dup_oneweek=dup_oneweek,
                           dup_twoweek=dup_twoweek,
                           dup_month=dup_month)


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



def weeklist(yesterday_sql,repurchase_unit):
    datelist = []  # 最终为所需要的列表
    weeklist_weekday = yesterday_sql.weekday()#昨日为周几
    datelist.append(yesterday_sql.strftime('%Y-%m-%d'))#添加昨日
    weeklist_monday = (yesterday_sql-datetime.timedelta(weeklist_weekday))#字符串格式周一
    a = weeklist_monday-datetime.timedelta(repurchase_unit)
    for i in range(0, 7):
        datelist.append(a.strftime('%Y-%m-%d'))
        a = a - datetime.timedelta(days=repurchase_unit)
    weeklist = list(reversed(datelist))
    return weeklist
#自然月列表
def re_monthlist(yesterday_sql):
    datelist=[]
    datelist.append(yesterday_sql.strftime('%Y-%m-%d'))
    month_firstday=datetime.datetime(yesterday_sql.year,yesterday_sql.month,1)
    datelist.append(month_firstday.strftime('%Y-%m-%d'))
    for i in range(0,7):
        if yesterday_sql.month-i >1:
            item=datetime.datetime(yesterday_sql.year,yesterday_sql.month-i-1,1)
        elif yesterday_sql.month-i==1:
            item=datetime.datetime(yesterday_sql.year-1,12,1)
        else:
            item=datetime.datetime(yesterday_sql.year-1,yesterday_sql.month+11-i,1)
        datelist.append(item.strftime('%Y-%m-%d'))
    monthlist=list(reversed(datelist))
    return monthlist
#复购率
def dup(d,datelist):
    dup = []
    for i in range(len(datelist) - 1):  # 初始周
        # 计算当周收货人数
        buyer_week = set(d[np.logical_and(d['pay_time'] >=datelist[i],
                                           d['pay_time'] <datetime.datetime.strptime(datelist[i+1], "%Y-%m-%d"))].receiver_name)#计算周买家
        dup_row = [datelist[i],len(buyer_week)]
        for j in range(len(datelist) - 1):  # 复购周
            if i <j:
                buyer_dupweek = set(d[np.logical_and(d['pay_time'] >= datelist[j],
                                                      d['pay_time'] < datelist[j + 1])].receiver_name)#复购周买家
                buyer_dup = reduce(lambda x, y: x & y, (buyer_week, buyer_dupweek))  #求交集
                dup_row.append('%.1f%%' % (len(buyer_dup) * 100 / len(buyer_week) if len(buyer_week)>0 else 0 ))  # 复购率
        dup.append(dup_row)
    return dup

