from flask import render_template,request,flash,redirect,url_for,session
from flask_login import login_required
from app import db,REMOTE_HOST
from . import main
from ..decorators import admin_required,permission_required
from ..configs.time_config import *
from ..configs.main_sql_config import *
from ..configs.config import *
from werkzeug.utils import secure_filename
import os
import pandas as pd
from pyecharts import Bar,Pie,Line,Overlap
from .forms import *
from ..models import Permission
import requests
import re
import pymysql
from elasticsearch import Elasticsearch
import pprint
def olp(attr,bar1,bar2,bar3,line1,line2,line3,bar1_title,bar2_title,bar3_title,line1_title,line2_title,line3_title,title,width,height):

    bar = Bar(title=title)
    bar.add(bar1_title, attr, bar1,is_label_show=True)
    if bar2 !=0:
        bar.add(bar2_title, attr, bar2,yaxis_interval=5)
    if bar3 !=0:
        bar.add(bar3_title, attr, bar3,yaxis_interval=5)

    line = Line()
    line.add(line1_title, attr, line1, yaxis_formatter=" ￥", yaxis_interval=5,is_label_show=True)
    if line2 !=0:
        line.add(line2_title, attr, line2,yaxis_formatter=" ￥", yaxis_interval=5)
    if line3 !=0:
        line.add(line3_title, attr, line3, yaxis_formatter=" ￥", yaxis_interval=5)

    overlap = Overlap(width=width, height=height)
    overlap.add(bar)
    overlap.add(line, yaxis_index=1, is_add_yaxis=True)
    return overlap

def pyec_bar(attr,bar1,bar2,bar3,bar4,bar1_title,bar2_title,bar3_title,bar4_title,title,width,height):
    bar = Bar(title,width=width,height=height)
    if bar1 !=0:
        bar.add(bar1_title, attr, bar1,is_label_show=True)
    if bar2 !=0:
        bar.add(bar2_title, attr, bar2,is_label_show=True)
    if bar3 !=0:
        bar.add(bar3_title, attr, bar3,is_label_show=True)
    if bar4 !=0:
        bar.add(bar4_title, attr, bar4,is_label_show=True)
    return bar
def py_pie(attr,pie_v,v_title,title):
    pie = Pie(title,width=400)
    if pie_v !=0:
        pie.add(v_title, attr, pie_v,    radius=[30, 55],
                is_legend_show=False,
                is_label_show=True)
    return pie



@main.route("/")
@login_required
@admin_required
def index():
    now=datetime.datetime.now()
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('memeda.html',now=now)

@main.route('/contact_me')
def contact_me():
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('contact_me.html')


def get_member_values(member_id,es_conn,db_circlecenter,time_end,hours_form):
    #设置时间
    time_start=time_end-datetime.timedelta(hours=hours_form)

    now_hour=datetime.datetime(time_end.year,time_end.month,time_end.day,time_end.hour)
    hour_lst=[now_hour]
    es_start=time_start.strftime('%Y-%m-%dT%H:%M:%S+0800')
    es_end=time_end.strftime('%Y-%m-%dT%H:%M:%S+0800')
    sql_start=time_start.strftime('%Y-%m-%d %H:%M:%S')
    sql_end=time_end.strftime('%Y-%m-%d %H:%M:%S')

    #时间Dataframe
    for i in range(1,hours_form+1):
        hour_lst.append(now_hour-datetime.timedelta(hours=i))
    time_hours_df=pd.DataFrame({'hour':hour_lst})
    #查member信息
    member_uuid=pd.read_sql_query(sql_member_uuid%member_id, con=db_circlecenter).values[0][0]
    member_name=pd.read_sql_query(sql_member_uuid%member_id, con=db_circlecenter).values[0][1]
    results={}
    #设置sql
    sql_follows_hours=(sql_follow_hours % member_id).format(sql_start, sql_end)
    sql_follows_all =(sql_follow_all % member_id).format(sql_start, sql_end)
    sql_activities_hours=(sql_activity_hours % member_id).format(sql_start, sql_end)
    sql_activities_all =(sql_activity_all% member_id).format(sql_start, sql_end)
    #设置es
    es_profile_hours['body']['query']['bool']['filter']['range']['time']['gte'] = es_start
    es_profile_hours['body']['query']['bool']['filter']['range']['time']['lte'] = es_end
    es_profile_hours['body']['query']['bool']['must'][1]['term']['path'] = member_id
    es_all_hours['body']['query']['bool']['filter']['range']['time']['gte'] = es_start
    es_all_hours['body']['query']['bool']['filter']['range']['time']['lte'] = es_end
    es_all_hours['body']['query']['bool']['must'][0]['term']['member_uuid.keyword'] = member_uuid

    #查询
    ##关注
    follow_hours_df= pd.read_sql_query(sql_follows_hours, con=db_circlecenter)
    follow_all_df= pd.read_sql_query(sql_follows_all, con=db_circlecenter)

    #动态
    activity_hours_df=pd.read_sql_query(sql_activities_hours, con=db_circlecenter)
    activity_all_df=pd.read_sql_query(sql_activities_all, con=db_circlecenter)

    #影人档案
    profile = es_conn.search(index=es_profile_hours['index'],body=es_profile_hours['body'])
    profile_hist=profile['aggregations']['view_hours']['buckets']
    profile_all=profile['hits']['total']
    #总动作
    all= es_conn.search(index=es_all_hours['index'],body=es_all_hours['body'])['aggregations']['all_hours']['buckets']

    results['follow_all']=follow_all_df.values.tolist()[0][0]
    results['activity_all']=activity_all_df.values.tolist()[0][0]
    results['profile_all']=profile_all

    profile_key_lst=[]
    profile_value_lst=[]
    all_key_lst=[]
    all_value_lst=[]
    for i in profile_hist:
        profile_key=datetime.datetime.strptime(i['key_as_string'],'%Y-%m-%dT%H:%M:%S.000Z')+datetime.timedelta(hours=8)
        profile_key_lst.append(profile_key)
        profile_value_lst.append(i['doc_count'])
    for i in all:
        all_key=datetime.datetime.strptime(i['key_as_string'],'%Y-%m-%dT%H:%M:%S.000Z')+datetime.timedelta(hours=8)
        all_key_lst.append(all_key)
        all_value_lst.append(i['doc_count'])
    profile_hours_df=pd.DataFrame({'hour':profile_key_lst,'profile':profile_value_lst})
    all_hours_df=pd.DataFrame({'hour':all_key_lst,'all':all_value_lst})
    #时间格式
    follow_hours_df['hour']=pd.to_datetime(follow_hours_df['hour'])
    activity_hours_df['hour']=pd.to_datetime(activity_hours_df['hour'])

    #结果集DataFrame
    hours = pd.merge(time_hours_df,follow_hours_df,on='hour',how='left')
    hours = pd.merge(hours,activity_hours_df,on='hour',how='left')
    hours=pd.merge(hours,profile_hours_df,on='hour',how='left')
    hours=pd.merge(hours,all_hours_df,on='hour',how='left').fillna(0).sort_index(ascending= False)
    time_lst=hours['hour'].tolist()
    time_hours_lst=[]
    for i in time_lst:
        time_hours_lst.append(i.strftime('%m/%d-%Hh'))
    follower_hours_lst=hours['follower_count'].tolist()
    activity_hours_lst=hours['activity_count'].tolist()
    profile_hours_lst=hours['profile'].tolist()
    all_hours_lst=hours['all'].tolist()

    overlap_popular = olp(attr=time_hours_lst, bar1=follower_hours_lst, bar2=0, bar3=0, line1=profile_hours_lst, line2=0, line3=0,
                          bar1_title='被关注',bar2_title=0, bar3_title=0, line1_title='影人档案被查看', line2_title=0, line3_title=0,
                          title='关注度数据', width=1200, height=260)
    overlap_active = olp(attr=time_hours_lst, bar1=activity_hours_lst, bar2=0, bar3=0, line1=all_hours_lst, line2=0, line3=0,
                          bar1_title='动态发布数',bar2_title=0, bar3_title=0, line1_title='所有活动', line2_title=0, line3_title=0,
                          title='活跃数据', width=1200, height=260)
    results['overlap_popular']=overlap_popular.render_embed()
    results['overlap_active']=overlap_active.render_embed()
    results['time_start']=sql_start
    results['member_name']=member_name
    return results

@main.route('/member', methods=['GET', 'POST'])
@main.route('/member/', methods=['GET', 'POST'])
@main.route('/member/<member_id>', methods=['GET', 'POST'])
def member(member_id=None):
    es_conn = Elasticsearch(
            [ES_host],
            http_auth=ES_http_auth,
            scheme=ES_scheme,
            port=ES_port)
    db_circlecenter = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_DB,
                                      charset='utf8')
    time_end_default=datetime.datetime.now()
    hours_form=48
    if request.method == 'POST' :
        member_id = request.form.get('member_id')
        hours_form =int(request.form.get('hours_form'))
        time_end_form=datetime.datetime.strptime(request.form.get('time_end'),'%Y-%m-%d %H:%M:%S')
        if time_end_form<time_end_default :
            time_end=time_end_form
        else:
            time_end=time_end_default
            flash('时间格式有误')
    elif request.method=='GET'or request.form.get('member_id') =='':
        if member_id==None:
            member_id='30724'
        time_end = time_end_default
    else:
        flash('时间格式有误')
    results_member=get_member_values(member_id=member_id,es_conn=es_conn,db_circlecenter=db_circlecenter,time_end=time_end,hours_form=hours_form)
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('member.html',
                           overlap_popular=results_member['overlap_popular'],
                           overlap_active=results_member['overlap_active'],
                           member_id=member_id,
                           member_name=results_member['member_name'],
                           time_end=time_end.strftime('%Y-%m-%d %H:%M:%S'),
                           time_start=results_member['time_start'],
                           follow_all=results_member['follow_all'],
                           activity_all=results_member['activity_all'],
                           profile_all=results_member['profile_all'],
                           hours_form=hours_form)


def get_article_values(dbconn_xmmz,article_id,date_end,days_form):
    date_start=date_end - datetime.timedelta(days=days_form+1)
    days_lst = []
    for i in range(days_form):
        date = (date_end - datetime.timedelta(days=i))
        days_lst.append(date)

    days_df = pd.DataFrame(days_lst, columns=['date'])
    sql = """select date,sum(visitor_count) uv,AVG(exit_ratio) exit_ratio from articles_streams where article_id='%(article_id)s' and 
    date between '%(date_start)s' and '%(date_end)s' group by date;"""
    sql_title="""select title from articles_streams where article_id='%s' """
    result_df = pd.read_sql(sql % {'article_id': article_id, 'date_start': date_start, 'date_end': date_end},
                            con=dbconn_xmmz)

    result_title = pd.read_sql(sql_title%article_id,con=dbconn_xmmz).values[0][0]
    result_df = days_df.merge(result_df, on=['date'], how='left')
    result_df=result_df.fillna(0)

    result={}
    result['date_start']=date_start
    result['date_end']=date_end
    result['result']={}
    result['result']['date']=result_df['date'].values.tolist()[::-1]
    result['result']['uv']=result_df['uv'].values.tolist()[::-1]
    exit_ratio_lst=result_df['exit_ratio'].values.tolist()[::-1]
    result['result']['exit_ratio'] = ['%.1f'%x for x in exit_ratio_lst]
    result['article_title']=result_title

    overlap_stream = olp(attr=result['result']['date'], bar1=result['result']['uv'], bar2=0, bar3=0,
                        line1=result['result']['exit_ratio'], line2=0, line3=0,
                        bar1_title='日期',bar2_title=0, bar3_title=0, line1_title='退出率(%)', line2_title=0, line3_title=0,
                        title='文章', width=1200, height=260)

    result['overlap_stream']=overlap_stream.render_embed()
    return result

@main.route('/article', methods=['GET', 'POST'])
@main.route('/article/', methods=['GET', 'POST'])
@main.route('/article/<article_id>', methods=['GET', 'POST'])
def article(article_id=None):

    date_end_default=datetime.date.today()-datetime.timedelta(days=1)
    days_form=15
    dbconn_xmmz = pymysql.connect(host=host_cine2, port=port_cine2, password=password_cine2, user=user_cine2,
                                  db=db_cine2_xmmz)
    if request.method == 'POST' :
        article_id = request.form.get('article_id')
        print(article_id)
        days_form =int(request.form.get('days_form'))
        date_end_form=datetime.datetime.strptime(request.form.get('date_end'),'%Y-%m-%d')
        date_end_form=datetime.date(date_end_form.year,date_end_form.month,date_end_form.day)
        print(type(date_end_form))
        print(type(date_end_default))
        if date_end_form<date_end_default :
            date_end=date_end_form
        else:
            date_end=date_end_default
            flash('时间格式有误')
    elif request.method=='GET'or request.form.get('member_id') =='':
        if article_id==None:
            sql_article_uv_max = """select article_id from articles_streams where TO_DAYS(NOW()) - TO_DAYS(date) = 1 order by visitor_count desc limit 1"""
            article_id = pd.read_sql(sql_article_uv_max, con=dbconn_xmmz).values[0][0]
        date_end = date_end_default
    else:
        flash('时间格式有误')
    results_article=get_article_values(dbconn_xmmz=dbconn_xmmz,article_id=article_id,date_end=date_end,days_form=days_form)
    dbconn_xmmz.close()
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('article.html',
                           overlap_stream=results_article['overlap_stream'],
                           article_id=article_id,
                           date_end=date_end.strftime('%Y-%m-%d'),
                           date_start=results_article['date_start'],
                           article_title=results_article['article_title'],
                           days_form=days_form)


@main.route('/screen')
@login_required
def screen():
    es_conn = Elasticsearch(
            [ES_host],
            http_auth=ES_http_auth,
            scheme=ES_scheme,
            port=ES_port)
    db_circlecenter = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_DB,
                                      charset='utf8')
    now=datetime.datetime.now()
    today=datetime.date.today()
    yesterday=today-datetime.timedelta(days=1)
    lastweek=today-datetime.timedelta(days=7)

    #设置时间格式
    sql_today_end=now.strftime('%Y-%m-%d %H:%M:%S')
    sql_today_start=today.strftime('%Y-%m-%d %H:%M:%S')
    sql_yesterday_start=yesterday.strftime('%Y-%m-%d %H:%M:%S')
    sql_yesterday_end=sql_yesterday_start[:11]+sql_today_end[-8:]
    sql_lastweek_start=lastweek.strftime('%Y-%m-%d %H:%M:%S')
    sql_lastweek_end=sql_lastweek_start[:11]+sql_today_end[-8:]

    es_today_start = today.strftime('%Y-%m-%dT00:00:00+0800')
    es_today_end = now.strftime('%Y-%m-%dT%H:%M:%S+0800')
    es_yesterday_start=yesterday.strftime('%Y-%m-%dT00:00:00+0800')
    es_yesterday_end = es_yesterday_start[:11]+es_today_end[11:]
    es_lastweek_start=lastweek.strftime('%Y-%m-%dT00:00:00+0800')
    es_lastweek_end = es_lastweek_start[:11]+es_today_end[11:]

    #激活
    results = {'activate_all': pd.read_sql_query(sql_activate_all, con=db_circlecenter).values[0][0]}
    results['activate_today']=pd.read_sql_query(sql_activate.format(sql_today_start,sql_today_end), con=db_circlecenter).values[0][0]
    results['activate_yesterday']=pd.read_sql_query(sql_activate.format(sql_yesterday_start,sql_yesterday_end), con=db_circlecenter).values[0][0]
    results['activate_lastweek']=pd.read_sql_query(sql_activate.format(sql_lastweek_start,sql_lastweek_end), con=db_circlecenter).values[0][0]
    results['activate_comp_yes']='%.1f'%((results['activate_today']/results['activate_yesterday']-1)*100)
    results['activate_comp_lasw']='%.1f'%((results['activate_today']/results['activate_lastweek']-1)*100)
    ##最新动态
    activity_post=pd.read_sql_query(sql_activity, con=db_circlecenter).values
    #动态id
    activity_id=activity_post[0][0]
    #作者id
    activity_author_id=activity_post[0][1]
    #动态类型
    results['activity_type']=activity_post[0][4][:-7]
    #动态发布时间
    results['activity_time']=str(activity_post[0][2])[-8:]
    if activity_post[0][3]=='Link':
        activity=pd.read_sql_query(sql_activity_link.format(activity_id), con=db_circlecenter).values
    else:
        activity=pd.read_sql_query(sql_activity_content.format(activity_id), con=db_circlecenter).values
    #动态内容
    activity_content=activity[0][0]
    if activity_content is not None and len(activity_content)>50:
        results['activity_content']=activity[0][0][:50]
    else:
        results['activity_content']=activity[0][0]
    #作者信息
    author=pd.read_sql_query(sql_activity_author.format(activity_author_id), con=db_circlecenter).values
    results['author_id']=author[0][0]
    results['author_name']=author[0][1]

    #总机构激活
    results['activate_all_org']=pd.read_sql_query(sql_activate_all_org, con=db_circlecenter).values[0][0]
    #今日机构激活
    results['activate_today_org']=pd.read_sql_query(sql_activate_org.format(sql_today_start,sql_today_end), con=db_circlecenter).values[0][0]
    #最新激活
    new_member=pd.read_sql_query(sql_new_member, con=db_circlecenter)
    ##member_id
    results['new_member_id']=new_member.values[0][0]
    ##真实姓名
    results['new_member_realname']=new_member.values[0][2]
    ##职业id
    member_business_id=pd.read_sql_query(sql_member_business_id.format(results['new_member_id']), con=db_circlecenter).values[0][0]
    ##获取职业
    results['member_business']=pd.read_sql_query(sql_member_business.format(member_business_id), con=db_circlecenter).values[0][0]

    #判断是否有头像
    if  new_member.values[0][1] is None:
        results['new_member_avater'] ='None'
    else :
        results['new_member_avater'] ='avator'

    #调用头像接口
    if new_member.values[0][1] is not None:
        avator=requests.get('https://paipianbang.cdn.cinehello.com/uploads/avatars/%s'%new_member.values[0][1]).content
        if os.path.exists(path_avator):
            os.remove(path_avator)
            with open(path_avator.format(new_member.values[0][1]), 'wb') as f:
                f.write(avator)
        else :
            with open(path_avator.format(new_member.values[0][1]), 'wb') as f:
                f.write(avator)

    #今日
    es_active_total['body']['query']['bool']['filter']['range']['time']['gte'] = es_today_start
    es_active_total['body']['query']['bool']['filter']['range']['time']['lte'] = es_today_end
    total_today = es_conn.search(index=es_active_total['index'],
                        body=es_active_total['body'])
    ##今日活跃
    results['active_today']=total_today['aggregations']['member_count']['value']

    #昨日
    es_active_total['body']['query']['bool']['filter']['range']['time']['gte'] = es_yesterday_start
    es_active_total['body']['query']['bool']['filter']['range']['time']['lte'] = es_yesterday_end
    total_yesterday = es_conn.search(index=es_active_total['index'],
                        body=es_active_total['body'])
    ##昨日活跃
    results['active_yesterday']=total_yesterday['aggregations']['member_count']['value']
    ##比较
    results['active_comp_yes']='%.1f'%((results['active_today']/results['active_yesterday']-1)*100)

    #上周
    es_active_total['body']['query']['bool']['filter']['range']['time']['gte'] = es_lastweek_start
    es_active_total['body']['query']['bool']['filter']['range']['time']['lte'] = es_lastweek_end
    total_lastweek = es_conn.search(index=es_active_total['index'],
                                     body=es_active_total['body'])
    ##上周活跃
    results['active_lastweek'] = total_lastweek['aggregations']['member_count']['value']
    ##较上周
    results['active_comp_lasw']='%.1f'%((results['active_today']/results['active_lastweek']-1)*100)

    #认领
    results['claimers_day']=pd.read_sql_query(sql_claimers_day.format(sql_today_start,sql_today_end), con=db_circlecenter).values[0][0]
    results['claimers_all']=pd.read_sql_query(sql_claimers_all, con=db_circlecenter).values[0][0]

    return render_template('screen.html',activate_all=results['activate_all'],
                           activate_today=results['activate_today'],
                           activate_yesterday=results['activate_comp_yes'],
                           activate_lastweek=results['activate_comp_lasw'],
                           activate_all_org=results['activate_all_org'],
                           activate_today_org=results['activate_today_org'],
                           new_member_realname=results['new_member_realname'],
                           new_member_id=results['new_member_id'],
                           new_member_avator=results['new_member_avater'],
                           member_business=results['member_business'],
                           active_today=results['active_today'],
                           active_yesterday=results['active_comp_yes'],
                           active_lastweek=results['active_comp_lasw'],
                           author_id=results['author_id'],
                           author_name=results['author_name'],
                           activity_type=results['activity_type'],
                           activity_content=results['activity_content'],
                           activity_time=results['activity_time'],
                           claimers_day=results['claimers_day'],
                           claimers_all=results['claimers_all'],

                           now=sql_today_end)

@main.route('/upload', methods=['GET', 'POST'])
@login_required
def upload_file():
    if request.method == 'POST':
        print('UA:', request.user_agent.string)
        print('\033[1;35m' + request.remote_addr + ' - ' + request.method + ' - ' + datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S') + ' - ' + request.path + '\033[0m')
        if 'file' not in request.files:
            flash('文件不对')
            return redirect(request.url)
        file = request.files['file']
        if file.filename != 'sales.csv':
            flash('文件名称不对:需要sales.csv')
            return redirect(request.url)
        if file and file.filename == 'sales.csv':
            filename = secure_filename(file.filename)
            file_path=os.path.join('/root/xmmz/dbxmmz/', filename)
            if (os.path.exists(file_path)):
                os.remove(file_path)
                file.save(file_path)
                os.system(r"python3 /root/xmmz/dbxmmz/insert_chanjet.py")
                flash('上传成功')
            else:
                file.save(file_path)
                os.system(r"python3 /root/xmmz/dbxmmz/insert_chanjet.py")
                flash('上传成功')
            return redirect(url_for('main.upload_file',
                                    filename=filename))

    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')

    return render_template('upload.html')


def get_sum_values(thatdate_sql):

    yesterday_month_1st_sql = get_month_1st(thatdate_sql)
    year_sql=thatdate_sql[:4]

    #项目

    result_month_lst= pd.read_sql(sql_project_month%year_sql, db.engine).values.tolist()
    result_ec_chanject_month_lst = pd.read_sql(sql_ec_chanjet_month%year_sql,  db.engine).values.tolist()
    result_ec_offline_month_lst  = pd.read_sql(sql_ec_offline_month%year_sql,  db.engine).values.tolist()
    result_ec_online_month_lst  = pd.read_sql(sql_ec_online_month%year_sql,  db.engine).values.tolist()
    result_csc_month_month_lst  = pd.read_sql(sql_csc_month%year_sql,  db.engine).values.tolist()
    result_ppxy_month_lst  = pd.read_sql(sql_ppxy_month%year_sql,  db.engine).values.tolist()
    result_littleclass_month_lst  = pd.read_sql(sql_littleclass_month%year_sql,  db.engine).values.tolist()
    for item in [result_ec_chanject_month_lst,result_ec_offline_month_lst,result_ec_online_month_lst,
                 result_csc_month_month_lst,result_ppxy_month_lst,result_littleclass_month_lst]:
        if item !=[]:
            for i in item:
                result_month_lst.append(i)
    result_month_dct={}
    for month in range(1,13):
        result_month_dct[month]={'合计':{'合计':0},'学习': {'师徒制': 0, '产教融合': 0, 'CSC':0,'非CSC':0, '媒体课程一体化':0},
            '媒体': {'媒体广告/活动': 0, '一录同行':0,'厂商服务/活动':0},
            'VIP会员': {'器材-顾问销售': 0, '器材-自主下单': 0},
            '城市':{'重庆':0,'电影周':0,'场景库':0}
            }
    #循环列表
    for item_lst in result_month_lst:
        #得到字典每个月
        for month in result_month_dct:
            #列表月=字典月
            if item_lst[0]==month:
                for department in result_month_dct[month]:
                    for project in result_month_dct[month][department]:
                        #列表project=字典project
                        if item_lst[1]==project:
                            result_month_dct[month][department][project]+=item_lst[2]
    # 月合计
    for month in result_month_dct:
        project_sum = 0
        for department in result_month_dct[month]:
            for project in result_month_dct[month][department]:
                project_sum += result_month_dct[month][department][project]
        result_month_dct[month]['合计']['合计'] = project_sum

    #部门
    result_department={}
    for department in result_month_dct[1]:
        department_sum = 0
        for month in result_month_dct:
            for project in result_month_dct[month][department]:
                department_sum+=result_month_dct[month][department][project]
        result_department[department]=department_sum

    #项目汇总
    result_project_thismonth_dct=result_month_dct[datetime.date.today().month]
    result_project_thisyear_dct={'合计':{'合计':0},'学习': {'师徒制': 0, '产教融合': 0, 'CSC':0,'非CSC':0, '媒体课程一体化':0},
            '媒体': {'媒体广告/活动': 0, '一录同行':0,'厂商服务/活动':0},
            'VIP会员': {'器材-顾问销售': 0, '器材-自主下单': 0},
            '城市':{'重庆':0,'电影周':0,'场景库':0}
            }
    for department in result_month_dct[1]:
        for project in result_month_dct[1][department]:
            project_month_sum = 0
            for month in result_month_dct:
                project_month_sum+=result_month_dct[month][department][project]
            result_project_thisyear_dct[department][project]=project_month_sum

    #图表
    result_pie={}
    result_pie['attr']=result_month_dct.keys()
    attr_bar=[(str(x) + '月') for x in result_pie['attr']]
    for department in result_month_dct[1]:
        department_sum_lst = []
        for month in result_month_dct:
            department_sum=0
            for project in result_month_dct[month][department]:
                department_sum+=result_month_dct[month][department][project]
            department_sum_lst.append(department_sum)
        result_pie[department]=department_sum_lst
    bar1=result_pie['学习']
    bar2=result_pie['媒体']
    bar3=result_pie['VIP会员']
    bar4=result_pie['城市']
    py_bar=pyec_bar(attr=attr_bar,bar1=bar1,bar2=bar2,bar3=bar3,bar4=bar4,bar1_title='学习',bar2_title='媒体',
                    bar3_title='VIP会员-器材',bar4_title='城市',title='部门销售额月度图',
                      width=1000,height=300)
    department_key=list(result_department.keys())
    department_value=list(result_department.values())
    del department_key[0]
    del department_value[0]
    attr_pie=department_key
    pie_v=department_value

    pie_department=py_pie(attr=attr_pie,pie_v=pie_v,v_title='部门',title='部门销售额')

    results={'result_department':result_department,
             'result_project_thisyear':result_project_thisyear_dct,
             'result_project_thismonth':result_project_thismonth_dct,
             'result_month':result_month_dct,
             'pie_department':pie_department,
             'py_bar':py_bar}
    return results

@main.route("/index-sum",methods=["POST","GET"])
@login_required
@permission_required(Permission.SUPER)
def index_sum():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST' :
        thatdate_sql = request.form.get('input')
        if thatdate_sql>yesterday_sql :
            flash('选择的日期未经历')
    if request.method=='GET'or request.form.get('input') =='':
        thatdate_sql = yesterday_sql
    results_sum=get_sum_values(thatdate_sql)
    year_sql=thatdate_sql[:4]

    #汇总
    py_bar = results_sum['py_bar']
    pie_department = results_sum['pie_department']
    ##学习、设备线上

    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('index-sum.html',
                           result_department=results_sum['result_department'],
                           result_project_thisyear=results_sum['result_project_thisyear'],
                           result_project_thismonth=results_sum['result_project_thismonth'],
                           result_month=results_sum['result_month'],
                           py_bar=py_bar.render_embed(),
                           pie_department=pie_department.render_embed(),
                           year_sql=year_sql,
                           thatdate=thatdate_sql
                           )


def get_autho_business():
    dbconn_xmmz = pymysql.connect(host=host_cine2, port=port_cine2, user=user_cine2,
                                  password=password_cine2,
                                  charset='utf8')

    authoritors_count = pd.read_sql(sql_authoritors, con=dbconn_xmmz)
    claimers_count = pd.read_sql(sql_calimers, con=dbconn_xmmz)
    feeders_count = pd.read_sql(sql_feeders, con=dbconn_xmmz)
    activate_count = pd.read_sql(sql_activate_business, con=dbconn_xmmz)

    business_result = authoritors_count.merge(claimers_count, how="left", on='business_merge_name'). \
        merge(feeders_count, how="left", on='business_merge_name') \
        .merge(activate_count, how="left", on='business_merge_name')
    business_result = (business_result).fillna(0)
    business_result[business_result.columns.difference(['business_merge_name'])] = business_result[
        business_result.columns.difference(['business_merge_name'])].astype(int)
    business_result.sort_values(by=['authoritors_count'], ascending=False, inplace=True)
    business_result.columns = ['职业', '认证人数', '认领人数','认证用户发布动态人数','认证用户3月内活跃人数']


    return business_result
@main.route("/business",methods=["GET"])
@login_required
def business():
    ##学习、设备线上
    business_result_pd=get_autho_business()
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('business.html',
                           business_result=business_result_pd)