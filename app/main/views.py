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

    member_uuid=pd.read_sql_query(sql_member_uuid%member_id, con=db_circlecenter).values[0][0]
    member_name=pd.read_sql_query(sql_member_uuid%member_id, con=db_circlecenter).values[0][1]
    results={}
    #设置sql
    sql_follows_hours=(sql_follow_hours % member_id).format(sql_start, sql_end)
    sql_activities_hours=(sql_activity_hours % member_id).format(sql_start, sql_end)
    #设置es
    es_profile_hours['body']['query']['bool']['filter']['range']['time']['gte'] = es_start
    es_profile_hours['body']['query']['bool']['filter']['range']['time']['lte'] = es_end
    es_profile_hours['body']['query']['bool']['must'][1]['term']['path'] = member_id
    es_all_hours['body']['query']['bool']['filter']['range']['time']['gte'] = es_start
    es_all_hours['body']['query']['bool']['filter']['range']['time']['lte'] = es_end
    es_all_hours['body']['query']['bool']['must'][0]['term']['member_uuid.keyword'] = member_uuid

    #查询
    #关注
    follow_hours_df= pd.read_sql_query(sql_follows_hours, con=db_circlecenter)
    #动态
    activity_hours_df=pd.read_sql_query(sql_activities_hours, con=db_circlecenter)
    #影人档案
    profile = es_conn.search(index=es_profile_hours['index'],body=es_profile_hours['body'])['aggregations']['view_hours']['buckets']
    #总动作
    all= es_conn.search(index=es_all_hours['index'],body=es_all_hours['body'])['aggregations']['all_hours']['buckets']
    profile_key_lst=[]
    profile_value_lst=[]
    all_key_lst=[]
    all_value_lst=[]
    for i in profile:
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
            time_end=time_end_default
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
    return render_template('member.html',
                           overlap_popular=results_member['overlap_popular'],
                           overlap_active=results_member['overlap_active'],
                           member_id=member_id,
                           member_name=results_member['member_name'],
                           time_end=time_end.strftime('%Y-%m-%d %H:%M:%S'),
                           time_start=results_member['time_start'],
                           hours_form=hours_form)




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
    #时间
    results['activity_time']=str(activity_post[0][2])[-8:]
    activity=pd.read_sql_query(sql_activity_content.format(activity_id), con=db_circlecenter).values
    #动态内容
    activity_content=activity[0][0]
    if activity_content is not None and len(activity_content)>30:
        results['activity_content']=activity[0][0][:30]
    else:
        results['activity_content']=activity[0][0]

    results['activity_type']=activity[0][1]

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
    result_csc= pd.read_sql(sql_csc%year_sql, db.engine).fillna(0)
    result_ppxy= pd.read_sql(sql_ppxy%year_sql, db.engine).fillna(0)
    result_ec_online=pd.read_sql(sql_ec_online%year_sql,db.engine).fillna(0)
    result_ec_offline=pd.read_sql(sql_ec_offline%year_sql,db.engine).fillna(0)
    result_ec_chanjet=pd.read_sql(sql_ec_chanjet%year_sql,db.engine).fillna(0)
    result_littleclass=pd.read_sql(sql_littleclass%year_sql,db.engine).fillna(0)

    result_ec_online_thismonth=pd.read_sql(sql_ec_online_thismonth.format(yesterday_month_1st_sql,thatdate_sql),db.engine).fillna(0)
    result_ec_chanjet_thismonth=pd.read_sql(sql_ec_chanjet_thismonth.format(yesterday_month_1st_sql,thatdate_sql),db.engine).fillna(0)
    result_csc_thismonth=pd.read_sql(sql_csc_thismonth.format(yesterday_month_1st_sql,thatdate_sql),db.engine).fillna(0)
    result_ppxy_thismonth=pd.read_sql(sql_ppxy_thismonth.format(yesterday_month_1st_sql,thatdate_sql),db.engine).fillna(0)
    result_littleclass_thismonth=pd.read_sql(sql_littleclass_thismonth.format(yesterday_month_1st_sql,thatdate_sql),db.engine).fillna(0)


    ##部门
    result_department =  pd.read_sql(sql_department%year_sql,  db.engine).fillna(0)
    result_department.loc[result_department['department_name']=='学习','sales_amount']+=result_csc['sales_amount'].values+result_ppxy['sales_amount'].values
    result_department.loc[result_department['department_name']=='设备','sales_amount']+=result_ec_online['sales_amount'].values+\
                                                                                      result_ec_offline['sales_amount'].values+ \
                                                                                      result_ec_chanjet['sales_amount'].values
    result_department.loc[result_department['department_name']=='学习','sales_amount']+=result_littleclass['sales_amount'].values
    result_department=result_department.values.tolist()

    ##项目汇
    result_project = pd.read_sql(sql_project%year_sql,  db.engine).fillna(0)
    result_project.loc[result_project['project_name']=='CSC','sales_amount']+=result_csc['sales_amount'].values
    result_project.loc[result_project['project_name']=='非CSC','sales_amount']+=result_ppxy['sales_amount'].values
    result_project.loc[result_project['project_name']=='自主下单','sales_amount']+=result_ec_online['sales_amount'].values
    result_project.loc[result_project['project_name']=='顾问销售','sales_amount']+= result_ec_offline['sales_amount'].values+ \
                                                                                      result_ec_chanjet['sales_amount'].values
    result_project.loc[result_project['project_name']=='媒体课程一体化','sales_amount']+=result_littleclass['sales_amount'].values
    result_project=result_project.values.tolist()

    #月数据
    result_csc_month= pd.read_sql(sql_csc_month%year_sql,  db.engine).fillna(0)
    result_csc_month.set_index('month', inplace=True)
    result_ppxy_month= pd.read_sql(sql_ppxy_month%year_sql,  db.engine).fillna(0)
    result_ppxy_month.set_index('month', inplace=True)
    result_littleclass_month=pd.read_sql(sql_littleclass_month%year_sql,db.engine).fillna(0)
    result_littleclass_month.set_index('month', inplace=True)
    ##部门录入
    result_class_month=pd.read_sql(sql_class_month%year_sql, db.engine).fillna(0)
    result_class_month.set_index('month', inplace=True)
    ##设备
    result_ec_online_month = pd.read_sql_query(sql_ec_month_online%year_sql, db.engine).fillna(0).set_index('month')
    result_ec_offline_month = pd.read_sql(sql_ec_month_offline%year_sql,  db.engine).fillna(0).set_index('month')
    result_ec_chanjet_month=pd.read_sql(sql_ec_month_chanjet%year_sql,db.engine).fillna(0).set_index('month')

    ###合并线下
    result_ec_consult_month=result_ec_chanjet_month+result_ec_offline_month



    ##合并设备、学习
    result_class_month['sales_amount_edu']+=result_csc_month['sales_amount_edu']+result_ppxy_month['sales_amount_edu']+result_littleclass_month['sales_amount_edu']
    result_class_month['sales_amount_device']+=result_ec_online_month['sales_amount_online']+result_ec_consult_month['sales_amount_consult']
    attr=result_class_month.index.values.tolist()

    attr_bar=[(str(x) + '月') for x in attr]
    bar1=result_class_month['sales_amount_edu'].values.tolist()
    bar2=result_class_month['sales_amount_media'].values.tolist()
    bar3=result_class_month['sales_amount_device'].values.tolist()
    bar4=result_class_month['sales_amount_city'].values.tolist()
    py_bar=pyec_bar(attr=attr_bar,bar1=bar1,bar2=bar2,bar3=bar3,bar4=bar4,bar1_title='学习',bar2_title='媒体',
                    bar3_title='设备',bar4_title='城市',title='部门销售额月度图',
                      width=1000,height=300)
    attr_pie=[(x[0]) for x in result_department]
    pie_v=[(x[1]) for x in result_department]

    pie_department=py_pie(attr=attr_pie,pie_v=pie_v,v_title='部门',title='部门销售额')

    #项目
    ##项目当月数据
    result_project_thismonth = pd.read_sql(sql_project_thismonth.format(yesterday_month_1st_sql,thatdate_sql),  db.engine).fillna(0)
    result_project_thismonth.loc[result_project_thismonth['project_name']=='CSC','sales_amount']+=result_csc_thismonth['sales_amount'].values
    result_project_thismonth.loc[result_project_thismonth['project_name']=='非CSC','sales_amount']+=result_ppxy_thismonth['sales_amount'].values
    result_project_thismonth.loc[result_project_thismonth['project_name']=='自主下单','sales_amount']+=result_ec_online_thismonth['sales_amount'].values
    result_project_thismonth.loc[result_project_thismonth['project_name']=='顾问销售','sales_amount']+=result_ec_chanjet_thismonth['sales_amount'].values
    result_project_thismonth.loc[result_project_thismonth['project_name']=='媒体课程一体化','sales_amount']+=result_littleclass_thismonth['sales_amount'].values

    result_project_thismonth=result_project_thismonth.values.tolist()

    ##项目月数据
    result_project_month_compared = pd.read_sql(sql_project_month_compared%year_sql,  db.engine,index_col=['month']).fillna(0)
    result_ec_chanject_month_compared = pd.read_sql(sql_ec_chanjet_month_compared%year_sql,  db.engine,index_col=['month']).fillna(0)
    result_ec_offline_month_compared = pd.read_sql(sql_ec_offline_month_compared%year_sql,  db.engine,index_col=['month']).fillna(0)
    result_ec_online_month_compared = pd.read_sql(sql_ec_online_month_compared%year_sql,  db.engine,index_col=['month']).fillna(0)
    result_csc_month_month_compared = pd.read_sql(sql_csc_month_compared%year_sql,  db.engine,index_col=['month']).fillna(0)
    result_ppxy_month_compared = pd.read_sql(sql_ppxy_month_compared%year_sql,  db.engine,index_col=['month']).fillna(0)
    result_littleclass_month_compared = pd.read_sql(sql_littleclass_month_compared%year_sql,  db.engine,index_col=['month']).fillna(0)
    ###合并线下
    ####改名字才可以求和
    result_ec_offline_month_compared.columns = ['sales_amount_ec_chanjet']
    result_ec_chanject_month_compared+=result_ec_offline_month_compared
    result_month_compared=pd.concat([result_project_month_compared, result_ec_chanject_month_compared], axis=1)
    result_month_compared=pd.concat([result_month_compared,result_ec_online_month_compared], axis=1)
    result_month_compared=pd.concat([result_month_compared,result_csc_month_month_compared], axis=1)
    result_month_compared=pd.concat([result_month_compared,result_ppxy_month_compared], axis=1)
    result_month_compared=pd.concat([result_month_compared,result_littleclass_month_compared], axis=1)
    result_month_compared=result_month_compared.reset_index().sort_index(ascending=False)
    result_month_compared=result_month_compared[['month','sales_amount_cjrh','sales_amount_csc','sales_amount_ppxy',
                                                 'sales_amount_littleclass','sales_amount_stz','sales_amount_gg','sales_amount_yltx',
                                                 'sales_amount_wx','sales_amount_zl','sales_amount_rs','sales_amount_ec_online',
                                                 'sales_amount_ec_chanjet','sales_amount_cjk','sales_amount_cq','sales_amount_dyz']]
    project_month_sum=result_month_compared.iloc[:,1:].sum(axis=1)
    result_month_compared.insert(loc=1, column='sum', value=project_month_sum)
    result_month_compared=result_month_compared.values.tolist()
    results={'result_department':result_department,
             'result_project':result_project,
             'result_project_thismonth':result_project_thismonth,
             'result_month_compared':result_month_compared,
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
                           result_project=results_sum['result_project'],
                           result_project_thismonth=results_sum['result_project_thismonth'],
                           result_month_compared=results_sum['result_month_compared'],
                           py_bar=py_bar.render_embed(),
                           pie_department=pie_department.render_embed(),
                           year_sql=year_sql,
                           thatdate=thatdate_sql
                           )
