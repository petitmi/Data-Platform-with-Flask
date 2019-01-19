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
from pyecharts import Bar,Pie
from .forms import *
from ..models import Permission
import requests
import re
import pymysql
from elasticsearch import Elasticsearch



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

@main.route('/brush-flow', methods=['GET', 'POST'])
def brush_flow():
    form = BrushForm()
    if form.validate_on_submit():
        email = form.email.data
        url=form.url.data
        mutiple=form.mutiple.data
        if not isinstance(mutiple, int) :
            flash("乘数不对")
        elif not re.match("^.+\\@(\\[?)[a-zA-Z0-9\\-\\.]+\\.([a-zA-Z]{2,3}|[0-9]{1,3})(\\]?)$", email) != None:
            flash("email不对")
        else:
            try:
                requests.head(url).status_code==200
                flash("已开始，请查阅邮箱")
            except :
                flash("url格式不对或无法访问")

    return render_template('brush-flow.html', form=form)

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

@main.route('/shitsweeper')
def shitsweeper():
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('shitsweeper.html')

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
    sql_today_end=now.strftime('%Y-%m-%d %H:%M:%S')
    sql_today_start=today.strftime('%Y-%m-%d %H:%M:%S')
    sql_yesterday_start=(today-datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    sql_yesterday_end=sql_yesterday_start[:11]+sql_today_end[-8:]
    sql_lastweek_start=(today-datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    sql_lastweek_end=sql_lastweek_start[:11]+sql_today_end[-8:]
    results = {'activate_all': pd.read_sql_query(sql_activate_all, con=db_circlecenter).values[0][0]}
    results['activate_today']=pd.read_sql_query(sql_activate.format(sql_today_start,sql_today_end), con=db_circlecenter).values[0][0]
    results['activate_yesterday']=pd.read_sql_query(sql_activate.format(sql_yesterday_start,sql_yesterday_end), con=db_circlecenter).values[0][0]
    results['activate_lastweek']=pd.read_sql_query(sql_activate.format(sql_lastweek_start,sql_lastweek_end), con=db_circlecenter).values[0][0]
    results['activate_comp_yes']='%.1f'%((results['activate_today']/results['activate_yesterday']-1)*100)
    results['activate_comp_lasw']='%.1f'%((results['activate_today']/results['activate_lastweek']-1)*100)

    activity_id=pd.read_sql_query(sql_activity, con=db_circlecenter).values[0][0]
    activity_author_id=pd.read_sql_query(sql_activity, con=db_circlecenter).values[0][1]
    activity=pd.read_sql_query(sql_activity_content.format(activity_id), con=db_circlecenter).values
    results['activity_content']=activity[0][0]
    results['activity_type']=activity[0][1]
    
    author=pd.read_sql_query(sql_activity_author.format(activity_author_id), con=db_circlecenter).values
    results['author_id']=author[0][0]
    results['author_name']=author[0][1]


    results['activate_all_org']=pd.read_sql_query(sql_activate_all_org, con=db_circlecenter).values[0][0]
    results['activate_today_org']=pd.read_sql_query(sql_activate_org.format(sql_today_start,sql_today_end), con=db_circlecenter).values[0][0]
    new_member=pd.read_sql_query(sql_new_member, con=db_circlecenter)
    results['new_member_id']=new_member.values[0][0]
    results['new_member_realname']=new_member.values[0][2]
    member_business_id=pd.read_sql_query(sql_member_business_id.format(results['new_member_id']), con=db_circlecenter).values[0][0]
    results['member_business']=pd.read_sql_query(sql_member_business.format(member_business_id), con=db_circlecenter).values[0][0]

    if  new_member.values[0][1] is None:
        results['new_member_avater'] ='None'
    else :
        results['new_member_avater'] ='avator'


    if new_member.values[0][1] is not None:
        avator=requests.get('https://paipianbang.cdn.cinehello.com/uploads/avatars/%s'%new_member.values[0][1]).content
        if os.path.exists(path_avator):
            os.remove(path_avator)
            with open(path_avator.format(new_member.values[0][1]), 'wb') as f:
                f.write(avator)

    today_es_start = today.strftime('%Y-%m-%dT00:00:00+0800')
    today_es_end = today.strftime('%Y-%m-%dT23:59:59+0800')
    es_active_total['body']['query']['bool']['filter']['range']['time']['gte'] = today_es_start
    es_active_total['body']['query']['bool']['filter']['range']['time']['lte'] = today_es_end
    #查询总数
    total = es_conn.search(index=es_active_total['index'],
                        body=es_active_total['body'])
    ##人数
    active_today=total['aggregations']['member_count']
    results['active_today']=active_today['value']

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
                           author_id=results['author_id'],
                           author_name=results['author_name'],
                           activity_type=results['activity_type'],
                           activity_content=results['activity_content'],
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
