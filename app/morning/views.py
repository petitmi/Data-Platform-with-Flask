from flask import render_template,request,flash,session
from flask_login import login_required
from . import morning
from ..decorators import  permission_required
from ..models import Permission
from pyecharts_javascripthon.api import TRANSLATOR
from pyecharts import Bar,Line,Overlap,Page
from ..configs.morning_sql_config import *
from ..configs.time_config import *
import pandas as pd
import datetime
import pymysql
from ..configs.config import *
from elasticsearch import Elasticsearch
from app import db

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


def get_dr_values(thatdate_sql):
    thatdate=datetime.datetime.strptime(thatdate_sql, '%Y-%m-%d')
    sql_yest = thatdate.strftime('%Y-%m-%d')

    db_circlecenter= pymysql.connect(host=DB_HOST, port=DB_PORT,user=DB_USER, password=DB_PASSWORD, db=DB_DB, charset='utf8')
    results={}
    results['days_list']=get_days_list(days=15,thatdate=thatdate_sql).sql_list()
    sql_time_days_start=results['days_list'][0]+' 00:00:00'
    sql_time_days_end=results['days_list'][14]+' 23:59:59'
    sql_time_yest_start=sql_yest+' 00:00:00'
    sql_time_yest_end=sql_yest+' 23:59:59'
    print(sql_time_days_end)
    #昨日
    results['activate_day']=pd.read_sql_query(sql_activate_day.format(sql_time_yest_start,sql_time_yest_end), con=db_circlecenter).values[0][0]
    results['login_day_newly']=pd.read_sql_query(sql_login_day_newly.format(sql_time_yest_start,sql_time_yest_end), con=db_circlecenter).values[0][0]
    results['messages_day']=pd.read_sql_query(sql_messages_day.format(sql_time_yest_start,sql_time_yest_end), con=db_circlecenter).values[0][0]

    #图表
    sql_days=tuple(results['days_list'])
    activate_days=pd.read_sql_query(sql_activate_days.format(sql_time_days_start,sql_time_days_end), con=db_circlecenter)
    login_newly_days=pd.read_sql(sql_login_newly_days.format(sql_time_days_start,sql_time_days_end), con=db_circlecenter)
    feed_count_days=pd.read_sql_query(sql_feed_count_days.format(sql_time_days_start,sql_time_days_end), con=db_circlecenter)
    feed_author_days=pd.read_sql_query(sql_feed_author_days.format(sql_time_days_start,sql_time_days_end), con=db_circlecenter)
    works_days=pd.read_sql_query(sql_works_days.format(sql_time_days_start,sql_time_days_end), con=db_circlecenter)
    claimers_days=pd.read_sql_query(sql_claimers_days.format(sql_time_days_start,sql_time_days_end), con=db_circlecenter)


    charts_data=activate_days.merge(login_newly_days,how='left',on=['date','date']).merge(feed_count_days,how='left',on=['date','date']).merge(feed_author_days,how='left',on=['date','date']).merge(works_days,how='left',on=['date','date']).merge(claimers_days,how='left',on=['date','date']).fillna(0)
    results['activate_days']=charts_data['activate_days'].values.tolist()
    results['login_newly_days']=charts_data['login_newly_days'].values.tolist()
    results['feed_count_days']=charts_data['feed_count_days'].values.tolist()
    results['feed_author_days']=charts_data['feed_author_days'].values.tolist()
    results['works_days']=charts_data['works_days'].values.tolist()
    results['claimers_days']=charts_data['claimers_days'].values.tolist()

    #绿表
    app_daily_days=pd.read_sql(sql_app_daily_days.format(sql_time_days_start,sql_time_days_end),con=db.engine)
    results['authorized_members_days']=pd.read_sql_query(sql_authorized_days.format(sql_time_days_start,sql_time_days_end), con=db_circlecenter).count_authorized.tolist()
    results['activate_members_fine_days']=results['activate_days'][:]
    results['activate_members_fine_days'].reverse()
    results['process_date']=app_daily_days.date.tolist()
    results['login_members_days']=app_daily_days.login_members.tolist()
    results['login_newly_members_days']=results['login_newly_days'][:]
    results['login_newly_members_days'].reverse()
    # results['binding_members_days']=app_daily_days.binding_members.tolist()
    results['activate_members_days']=app_daily_days.activate_members.tolist()
    results['active_members_days']=app_daily_days.active_members.tolist()
    results['active_times_days']=app_daily_days.active_times.tolist()

    return results

def get_rp_values():
    db_circlecenter= pymysql.connect(host=DB_HOST, port=DB_PORT,user=DB_USER, password=DB_PASSWORD, db=DB_DB, charset='utf8')

    results={'activate_all':pd.read_sql_query(sql_activate_all, con=db_circlecenter).values[0][0]}
    # results['contact_all']=pd.read_sql_query(sql_contact_all, con=db_circlecenter).values[0][0]
    # results['relation_contact_all']=pd.read_sql_query(sql_relation_contact_all, con=db_circlecenter).values[0][0]
    results['works_all']=pd.read_sql_query(sql_works_all,con=db_circlecenter).values[0][0]
    results['works_checked']=pd.read_sql_query(sql_works_checked,con=db_circlecenter).values[0][0]
    results['works_complete']=pd.read_sql_query(sql_works_complete,con=db_circlecenter).values[0][0]
    results['workers_all']=pd.read_sql_query(sql_workers_all,con=db_circlecenter).values[0][0]
    results['claimers_all']=pd.read_sql_query(sql_claimers_all,con=db_circlecenter).values[0][0]

    #栏目数据
    columns_casts_count=pd.read_sql_query(sql_columns_casts_count, con=db_circlecenter,index_col=['column_id'])
    columns_clips_count=pd.read_sql_query(sql_columns_clips_count, con=db_circlecenter,index_col=['column_id'])
    columns_chats_count=pd.read_sql_query(sql_columns_chats_count, con=db_circlecenter,index_col=['column_id'])
    columns_pv=pd.read_sql_query(sql_columns_pv, con=db_circlecenter,index_col=['column_id'])
    columns_data=columns_pv.join(columns_casts_count,lsuffix='_pv', rsuffix='_casts').join(columns_chats_count, rsuffix='_chats').join(columns_clips_count,rsuffix='_clips')
    #栏目期数据
    casts_pv = pd.read_sql_query(sql_casts_pv, con=db_circlecenter, index_col=['cast_id'])
    casts_chats_count = pd.read_sql_query(sql_casts_chats_count, con=db_circlecenter, index_col=['cast_id'])
    casts_clips_count = pd.read_sql_query(sql_casts_clips_count, con=db_circlecenter, index_col=['cast_id'])
    casts_data = casts_pv.join(casts_chats_count, rsuffix='_chats').join(casts_clips_count, lsuffix='_pv',rsuffix='_casts')
    #栏目id按pv排序
    columnid_list=[x for x in casts_data.column_id.drop_duplicates().values]
    #列表
    colunms_list=columns_data.values.tolist()
    #结果
    columns_data={columnid_list[0]:colunms_list[0]}
    columncasts_data = {columnid_list[0]: casts_data[casts_data['column_id']==columnid_list[0]].values.tolist()}
    for i in range(1,len(columnid_list)):
        columns_data[columnid_list[i]]=colunms_list[i]
        columncasts_data[columnid_list[i]]= casts_data[casts_data['column_id']==columnid_list[i]].values.tolist()

    results['columns_id']=columnid_list
    results['columncasts_data']=columncasts_data
    results['columns_data']=columns_data
    return results

@morning.route('/morning-rp',methods=["POST","GET"])
@login_required
@permission_required(Permission.ADMIN)
def morning_rp():
    results_rp=get_rp_values()
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('morning-rp.html',
                           activate_all=results_rp['activate_all'],
                           # contact_all=results_rp['contact_all'],
                           # relation_contact_all=results_rp['relation_contact_all'],
                           #                           active_all=results_rp['active_all'],
                           columns_data=results_rp['columns_data'],
                           columncasts_data=results_rp['columncasts_data'],
                           columns_id=results_rp['columns_id'],
                           works_all=results_rp['works_all'],
                           works_checked=results_rp['works_checked'],
                           works_complete=results_rp['works_complete'],
                           workers_all=results_rp['workers_all'],
                           claimers_all=results_rp['claimers_all']
                           )

@morning.route('/morning-dr',methods=["POST","GET"])
@login_required
@permission_required(Permission.ADMIN)
def morning_dr():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST' :
        thatdate_sql = request.form.get('input')
        if thatdate_sql>yesterday_sql :
            flash('选择的日期未经历')
    if request.method=='GET'or request.form.get('input') =='':
        thatdate_sql = yesterday_sql

    results_dr=get_dr_values(thatdate_sql)
    overlap_newly_day=olp(attr=results_dr['days_list'],bar1=results_dr['activate_days'],bar2=0,bar3=0,
                    line1=results_dr['login_newly_days'],line2=0,line3=0,bar1_title='新激活用户',bar2_title=0,bar3_title=0,
        line1_title='新登录用户',line2_title=0,line3_title=0,title='日拉新数据',width=1200,height=260)
    overlap_au_day=olp(attr=results_dr['days_list'],bar1=results_dr['feed_count_days'],bar2=0,bar3=0,
                    line1=results_dr['feed_author_days'],line2=0,line3=0,bar1_title='动态条数',bar2_title=0,bar3_title=0,
        line1_title='动态发布者',line2_title=0,line3_title=0,title='日动态数据',width=1200,height=260)

    overlap_works_day=olp(attr=results_dr['days_list'],bar1=results_dr['works_days'],bar2=0,bar3=0,
                    line1=results_dr['claimers_days'],line2=0,line3=0,bar1_title='作品数量',bar2_title=0,bar3_title=0,
        line1_title='认领人数',line2_title=0,line3_title=0,title='日作品认领数据',width=1200,height=260)

    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('morning-dr.html',thatdate=thatdate_sql,
                           activate_day=results_dr['activate_day'],
                           login_day_newly=results_dr['login_day_newly'],
                           messages_day=results_dr['messages_day'],
                           active_day=results_dr['active_members_days'][0],
                           claimers_day=results_dr['claimers_days'][14],
                           feed_author_days=results_dr['feed_author_days'][14],
                           # contact_day=results_dr['contact_day'],
                           # relation_contact_day=results_dr['relation_contact_day'],
                           process_date=results_dr['process_date'],
                           login_members_days=results_dr['login_members_days'],
                           # binding_members_days=results_dr['binding_members_days'],
                           login_newly_members_days=results_dr['login_newly_members_days'],
                           active_members_days=results_dr['active_members_days'],
                           active_times_days=results_dr['active_times_days'],
                           activate_members_days=results_dr['activate_members_days'],
                           activate_members_fine_days=results_dr['activate_members_fine_days'],
                           authorized_members_days=results_dr['authorized_members_days'],
                           overlap_newly_day=overlap_newly_day.render_embed(),
                           overlap_au_day=overlap_au_day.render_embed(),
                           overlap_works_day=overlap_works_day.render_embed())


