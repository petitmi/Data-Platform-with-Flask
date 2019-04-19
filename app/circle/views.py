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
import pymysql



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



def get_articles_values(date_end,days_form,selected_type,keyword_title,keyword_url):
    import requests
    import json
    import re
    import pymysql
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
    #全部结果字典
    for item_no in range(len(urls_items_title)):
        if urls_items_value[item_no][1] > 19:
            dct_urls[urls_items_title[item_no][0]['name']] = {}
            for field_no in range(1, len(urls_fields)):
                dct_urls[urls_items_title[item_no][0]['name']][urls_fields[field_no]] = urls_items_value[item_no][field_no - 1]

    #选择类型为文章时筛选结果
    def content_articles(selected_type):
        dct_results={}
        if selected_type == 'stream':
            re_stream=re.compile('stream\/(\d+)[\?\/$]?')
            sql="select v_title from streams where id=%s"
            dbinfo={'host':host_cine1,'port':port_cine1,'user':user_cine1,'password':password_cine1,'db':db_cine1_cine107}
        if selected_type == 'articles':
            re_stream=re.compile('articles\/(\d+)[\?\/$]?')
            sql="select title from posts where id=%s"
            dbinfo={'host':host_cine2,'port':port_cine2,'user':user_cine2,'password':password_cine2,'db':db_cine2_circle}

        dbconn= pymysql.connect(host=dbinfo['host'], port=dbinfo['port'], user=dbinfo['user'], password=dbinfo['password'], db=dbinfo['db'],
                                      charset='utf8')
        sqlconn=dbconn.cursor()
        for url in list(dct_urls.keys()):
            article_re=re.search(re_stream,url)
            if article_re is not None:
                dct_results[url]=dct_urls[url]
                aricle_id=article_re.group(1)
                dct_results[url]['article_id']=aricle_id
                dct_results[url]['article_curve']='http://morning-data.cinehello.com/article/%s'%aricle_id
                article_link_type=article_re.end()
                sqlconn.execute(sql%aricle_id)
                title_result=sqlconn.fetchone()
                if title_result is None:
                    article_title='不存在-%s'%aricle_id
                else:
                    article_title = title_result[0]
                    if len(article_title)>39:
                        article_title=article_title[:38]
                if selected_type=='stream' :
                    if 'mstream' in url:
                        dct_results[url]['title']=article_title+'【移动端】'
                    else:
                        dct_results[url]['title'] = article_title+'【PC】'
                else:
                    dct_results[url]['title'] = article_title
                dct_results[url]['link_type']=url[article_link_type:]



        dbconn.close()
        return dct_results

    #获取最后结果
    if selected_type=='stream':
        dct_results=content_articles(selected_type='stream')
    elif selected_type == 'articles':
        dct_results=content_articles(selected_type='articles')
    elif selected_type=='all':
        dct_results={}
        for url in dct_urls:
            dct_results[url]=dct_urls[url]
            dct_results[url]['title'] = url[:80]
    else:
        dct_results={}
        for url in list(dct_urls.keys()):
            if selected_type in url:
                dct_results[url] = dct_urls[url]
                dct_results[url]['title']=url[:80]

    #有关键词检索
    if keyword_url !='' or keyword_title!='':
        for url in list(dct_results):
            if keyword_url not in url or keyword_title not in dct_results[url]['title']:
                dct_results.pop(url)

    result={}
    result['dct_urls']=dct_results
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
    dct_selected={"all":"全部",'stream':"107文章",'articles':"cinehello文章","zhuanti":"专题","company":"公司"}
    if request.method == 'POST' :
        days_form =int(request.form.get('days_form'))
        selected_type=request.form.get('selected')
        keyword_url=request.form.get('keyword_url')
        keyword_title=request.form.get('keyword_title')
        date_end_form=datetime.datetime.strptime(request.form.get('date_end'),'%Y-%m-%d')
        if date_end_form<date_end_default :
            date_end=date_end_form
        else:
            date_end=date_end_default
            flash('时间格式有误')
    elif request.method=='GET':
        keyword_title =''
        keyword_url=''
        selected_type = 'stream'
        date_end = date_end_default
    else:
        flash('时间格式有误')
    results_articles=get_articles_values(date_end=date_end,days_form=days_form,selected_type=selected_type,keyword_title=keyword_title,keyword_url=keyword_url)

    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('articles-rp.html',
                           date_end=date_end.strftime('%Y-%m-%d'),
                           date_start=results_articles['date_start'],
                           dct_urls=results_articles['dct_urls'],
                           days_form=days_form,
                           dct_selected=dct_selected,
                           selected_type=selected_type,
                           keyword_title=keyword_title,
                           keyword_url=keyword_url)

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

def get_operations_values(thatdate_sql):
    thatdate_sql_start=str(thatdate_sql)+' 00:00:00'
    thatdate_sql_end=str(thatdate_sql)+' 23:59:59'

    dbconn_xmmz = pymysql.connect(host=host_cine1, user=user_cine1, port=port_cine1, password=password_cine1,
                                  db=db_cine1_cine107)
    operations_members_pd=pd.read_sql(sql_operations,con=dbconn_xmmz)
    operations_members_pd=operations_members_pd.drop('member_ids', axis=1).join(
        operations_members_pd['member_ids'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('member_ids'))
    #全部operations
    operations_all_tpl=tuple(operations_members_pd['member_ids'].tolist())
    #全部文章
    articles_all_pd=pd.read_sql(sql_operations_articles.format(thatdate_sql_start,thatdate_sql_end,operations_all_tpl),con=dbconn_xmmz)
    articles_all_pd.sort_values(by='stream_count',ascending=False, inplace=True)
    header_all_articles_lst=list(articles_all_pd.columns.values)
    value_all_articles_lst=articles_all_pd.values.tolist()
    ##标题
    articles_headers_lst = []
    for header in header_all_articles_lst:
        articles_headers_lst.append(header)

    articles_operations = {}
    for articles in value_all_articles_lst:
        articles_operations[articles[0]] = {}
        for header_num in range(len(articles_headers_lst)):
            articles_operations[articles[0]][articles_headers_lst[header_num]] = articles[header_num]
    operations_members_pd['member_ids']=operations_members_pd['member_ids'].astype('int')
    dbconn_xmmz.close()
    all_operations_articles_pd=operations_members_pd.merge(articles_all_pd,how='left',left_on='member_ids',right_on='member_id')

    #获得人员统计
    #分组聚合
    aggregation = {'member_id': ['count'], 'stream_count': ['sum'],'datu_count': ['sum'], 'wenku_count': ['sum'], 'toutiao_count': ['sum']}
    operations_members_grouped = all_operations_articles_pd.groupby(all_operations_articles_pd['v_name_cn']).agg(aggregation)
    #修改列名
    operations_members_grouped.columns = ['_'.join(col).strip() for col in operations_members_grouped.columns.values]
    #点击降序
    operations_members_grouped=operations_members_grouped.sort_values('stream_count_sum',ascending=False)
    #人员、标题、数据
    members_vname_lst=list(operations_members_grouped.index.values)
    header_operations_members_lst=list(operations_members_grouped.columns.values)
    value_operations_members_lst=operations_members_grouped.values.tolist()
    members_headers_lst=[]
    for header in header_operations_members_lst:
        members_headers_lst.append(header)
    members_operations = {}
    for member_num in range(len(members_vname_lst)):
        members_operations[members_vname_lst[member_num]] = {}
        for header_num in range(len(members_headers_lst)):
            members_operations[members_vname_lst[member_num]][header_operations_members_lst[header_num]] = value_operations_members_lst[member_num][header_num]
    results={}
    results['articles_operations']=articles_operations
    results['members_operations']=members_operations

    return results

@circle.route('/articles-operations',methods=["POST","GET"])
@login_required
@permission_required(Permission.CIRCLE)
def articles_operations():
    today_sql = datetime.date.today()
    if request.method == 'POST' :
        thatdate_sql = datetime.datetime.strptime(request.form.get('date_end'),'%Y-%m-%d')
        thatdate_sql=datetime.date(thatdate_sql.year,thatdate_sql.month,thatdate_sql.day)

        if thatdate_sql>today_sql :
            flash('选择的日期未经历')
    if request.method=='GET'or request.form.get('input') =='':
        thatdate_sql = today_sql-datetime.timedelta(days=1)
    results_operations=get_operations_values(thatdate_sql)
    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('articles-operations.html',
                           thatdate=thatdate_sql,
                           articles_operations=results_operations['articles_operations'],
                           members_operations=results_operations['members_operations'],
                           date_end=thatdate_sql)



