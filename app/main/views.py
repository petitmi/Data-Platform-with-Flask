from flask import render_template,request,flash,redirect,url_for,session
from flask_login import login_required
from app import db,REMOTE_HOST
from . import main
from ..decorators import admin_required,permission_required
from ..configs.time_config import *
from ..configs.main_sql_config import *
from werkzeug.utils import secure_filename
import os
import pandas as pd
from pyecharts import Bar,Pie
from .forms import *
from ..models import Permission
import requests
import re

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
    bar.render()
    return bar
def py_pie(attr,pie_v,v_title,title):
    pie = Pie(title,width=400)
    if pie_v !=0:
        pie.add(v_title, attr, pie_v,    radius=[30, 55],
                is_legend_show=False,
                is_label_show=True)
    pie.render()
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

@main.route("/index-sum")
@login_required
@permission_required(Permission.SUPER)
def index_sum():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    yesterday_month_1st_sql = get_month_1st(yesterday_sql)

    #汇总

    ##学习、设备线上
    result_csc= pd.read_sql(sql_csc,  db.engine).fillna(0)
    result_ppxy= pd.read_sql(sql_ppxy,  db.engine).fillna(0)
    result_ec_online=pd.read_sql(sql_ec_online,db.engine).fillna(0)
    result_ec_offline=pd.read_sql(sql_ec_offline,db.engine).fillna(0)
    result_ec_chanjet=pd.read_sql(sql_ec_chanjet,db.engine).fillna(0)
    result_littleclass=pd.read_sql(sql_littleclass,db.engine).fillna(0)
    result_ec_online_thismonth=pd.read_sql(sql_ec_online_thismonth%(yesterday_month_1st_sql,yesterday_sql),db.engine).fillna(0)
    result_ec_chanjet_thismonth=pd.read_sql(sql_ec_chanjet_thismonth%(yesterday_month_1st_sql,yesterday_sql),db.engine).fillna(0)
    result_csc_thismonth=pd.read_sql(sql_csc_thismonth%(yesterday_month_1st_sql,yesterday_sql),db.engine).fillna(0)
    result_ppxy_thismonth=pd.read_sql(sql_ppxy_thismonth%(yesterday_month_1st_sql,yesterday_sql),db.engine).fillna(0)
    result_littleclass_thismonth=pd.read_sql(sql_littleclass_thismonth%(yesterday_month_1st_sql,yesterday_sql),db.engine).fillna(0)


    ##部门
    result_department =  pd.read_sql(sql_department,  db.engine).fillna(0)
    result_department.loc[result_department['department_name']=='学习','sales_amount']+=result_csc['sales_amount'].values+result_ppxy['sales_amount'].values
    result_department.loc[result_department['department_name']=='设备','sales_amount']+=result_ec_online['sales_amount'].values+\
                                                                                      result_ec_offline['sales_amount'].values+ \
                                                                                      result_ec_chanjet['sales_amount'].values
    result_department.loc[result_department['department_name']=='学习','sales_amount']+=result_littleclass['sales_amount'].values
    result_department=result_department.values.tolist()
    ##项目汇
    result_project = pd.read_sql(sql_project,  db.engine).fillna(0)
    result_project.loc[result_project['project_name']=='CSC','sales_amount']+=result_csc['sales_amount'].values
    result_project.loc[result_project['project_name']=='非CSC','sales_amount']+=result_ppxy['sales_amount'].values
    result_project.loc[result_project['project_name']=='自主下单','sales_amount']+=result_ec_online['sales_amount'].values
    result_project.loc[result_project['project_name']=='顾问销售','sales_amount']+= result_ec_offline['sales_amount'].values+ \
                                                                                      result_ec_chanjet['sales_amount'].values
    result_project.loc[result_project['project_name']=='媒体课程一体化','sales_amount']+=result_littleclass['sales_amount'].values
    result_project=result_project.values.tolist()

    #月数据
    result_csc_month= pd.read_sql(sql_csc_month,  db.engine).fillna(0)
    result_csc_month.set_index('month', inplace=True)
    result_ppxy_month= pd.read_sql(sql_ppxy_month,  db.engine).fillna(0)
    result_ppxy_month.set_index('month', inplace=True)
    result_littleclass_month=pd.read_sql(sql_littleclass_month,db.engine).fillna(0)
    result_littleclass_month.set_index('month', inplace=True)
    ##部门录入
    result_class_month=pd.read_sql(sql_class_month, db.engine).fillna(0)
    result_class_month.set_index('month', inplace=True)
    ##设备
    result_ec_online_month = pd.read_sql_query(sql_ec_month_online, db.engine).fillna(0).set_index('month')
    result_ec_offline_month = pd.read_sql(sql_ec_month_offline,  db.engine).fillna(0).set_index('month')
    result_ec_chanjet_month=pd.read_sql(sql_ec_month_chanjet,db.engine).fillna(0).set_index('month')

    ###合并线下
    # result_ec_consult_month=pd.concat([result_ec_offline_month,result_ec_chanjet_month], ignore_index=True)
    result_ec_consult_month=result_ec_chanjet_month+result_ec_offline_month


    #补充数据到12月
    # ##部门录入
    # x_class = np.zeros((12-len(result_class_month),4))
    # result_class_month = result_class_month.append(
    #     pd.DataFrame(x_class, index=range(len(result_class_month)+1, 13),
    #                  columns=['sales_amount_media','sales_amount_edu','sales_amount_device','sales_amount_city']),sort=True)
    # ##设备线下
    # x_consult = np.zeros((12-len(result_ec_consult_month), 1))
    # result_ec_consult_month = result_ec_consult_month.append(
    #     pd.DataFrame(x_consult, index=range(len(result_ec_consult_month), 12), columns=[ 'sales_amount_consult']),sort=True)
    # ##设备线上
    # x_online = np.zeros((12-len(result_ec_online_month), 1))
    # result_ec_online_month = result_ec_online_month.append(
    #     pd.DataFrame(x_online, index=range(len(result_ec_online_month), 12), columns=[ 'sales_amount_consult']),sort=True)
    # ##拍片学院
    # x_ppxy= np.zeros((12-len(result_ppxy_month),1))
    # result_ppxy_month = result_ppxy_month.append(
    #     pd.DataFrame(x_ppxy, index=range(len(result_ppxy_month)+1, 13),
    #                  columns=['sales_amount_edu']),sort=True)
    # ##CSC
    # x_csc= np.zeros((12-len(result_csc_month),1))
    # result_csc_month = result_csc_month.append(
    #     pd.DataFrame(x_csc, index=range(len(result_csc_month)+1, 13),
    #                  columns=['sales_amount_edu']),sort=True)
    #作图
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
    result_project_thismonth = pd.read_sql(sql_project_thismonth%(yesterday_month_1st_sql,yesterday_sql),  db.engine).fillna(0)
    result_project_thismonth.loc[result_project_thismonth['project_name']=='CSC','sales_amount']+=result_csc_thismonth['sales_amount'].values
    result_project_thismonth.loc[result_project_thismonth['project_name']=='非CSC','sales_amount']+=result_ppxy_thismonth['sales_amount'].values
    result_project_thismonth.loc[result_project_thismonth['project_name']=='自主下单','sales_amount']+=result_ec_online_thismonth['sales_amount'].values
    result_project_thismonth.loc[result_project_thismonth['project_name']=='顾问销售','sales_amount']+=result_ec_chanjet_thismonth['sales_amount'].values
    result_project_thismonth.loc[result_project_thismonth['project_name']=='媒体课程一体化','sales_amount']+=result_littleclass_thismonth['sales_amount'].values

    result_project_thismonth=result_project_thismonth.values.tolist()

    ##项目月数据
    result_project_month_compared = pd.read_sql(sql_project_month_compared,  db.engine,index_col=['month']).fillna(0)
    result_ec_chanject_month_compared = pd.read_sql(sql_ec_chanjet_month_compared,  db.engine,index_col=['month']).fillna(0)
    result_ec_offline_month_compared = pd.read_sql(sql_ec_offline_month_compared,  db.engine,index_col=['month']).fillna(0)
    result_ec_online_month_compared = pd.read_sql(sql_ec_online_month_compared,  db.engine,index_col=['month']).fillna(0)
    result_csc_month_month_compared = pd.read_sql(sql_csc_month_compared,  db.engine,index_col=['month']).fillna(0)
    result_ppxy_month_compared = pd.read_sql(sql_ppxy_month_compared,  db.engine,index_col=['month']).fillna(0)
    result_littleclass_month_compared = pd.read_sql(sql_littleclass_month_compared,  db.engine,index_col=['month']).fillna(0)
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
                                                 'sales_amount_littleclass','sales_amount_gg','sales_amount_yltx',
                                                 'sales_amount_wx','sales_amount_zl','sales_amount_rs','sales_amount_ec_online',
                                                 'sales_amount_ec_chanjet','sales_amount_cjk','sales_amount_cq']]
    project_month_sum=result_month_compared.iloc[:,1:].sum(axis=1)
    result_month_compared.insert(loc=1, column='sum', value=project_month_sum)
    result_month_compared=result_month_compared.values.tolist()
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('index-sum.html',
                           result_department=result_department,
                           result_project=result_project,
                           result_project_thismonth=result_project_thismonth,
                           result_month_compared=result_month_compared,
                           py_bar=py_bar.render_embed(),
                           pie_department=pie_department.render_embed()
                           )
