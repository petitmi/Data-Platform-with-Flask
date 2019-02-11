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
    today_sql = datetime.datetime.now() .strftime('%Y-%m-%d')
    # t_start=datetime.datetime.now()
    db_circlecenter= pymysql.connect(host=DB_HOST, port=DB_PORT,user=DB_USER, password=DB_PASSWORD, db=DB_DB, charset='utf8')
    results={'activate_day':pd.read_sql_query(sql_activate_day%thatdate_sql, con=db_circlecenter).values[0][0]}
    # t_activate_day=datetime.datetime.now()
    # d_activate_day=t_activate_day-t_start

    # results['contact_day']=pd.read_sql_query(sql_contact_day%thatdate_sql, con=db_circlecenter).values[0][0]
    # t_contact_day=datetime.datetime.now()
    # d_contact_day=t_contact_day-t_activate_day

    # results['relation_contact_day']=pd.read_sql_query(sql_relation_contact_day%thatdate_sql, con=db_circlecenter).values[0][0]
    # t_relation_contact_day=datetime.datetime.now()
    # d_relation_contact_day=t_relation_contact_day-t_contact_day

    results['login_day_newly']=pd.read_sql_query(sql_login_day_newly%thatdate_sql, con=db_circlecenter).values[0][0]
    # t_login_day_newly=datetime.datetime.now()
    # d_login_day_newly=t_login_day_newly-t_relation_contact_day

    results['7days_list']=get_days_list(days=15,thatdate=thatdate_sql).sql_list()
    sql_time_start=results['7days_list'][0]+' 00:00:00'
    sql_time_end=results['7days_list'][14]+' 23:59:59'
    # t_7days_list=datetime.datetime.now()
    # d_7days_list=t_7days_list-t_login_day_newly

    # print('d_activate_day:',d_activate_day,'\nd_contact_day:',d_contact_day,'\nd_relation_contact_day:',
    #       d_relation_contact_day,'\nd_login_day_newly:',d_login_day_newly,'\nd_7days_list:',d_7days_list)

    sql_7days=tuple(results['7days_list'])

    activate_7days=pd.read_sql_query(sql_activate_7days.format(sql_time_start,sql_time_end), con=db_circlecenter)
    # t_activate_7days=datetime.datetime.now()
    # d_activate_7days=t_activate_7days-t_7days_list

    login_newly_7days=pd.read_sql(sql_login_newly_7days.format(sql_time_start,sql_time_end), con=db_circlecenter)

    # t_login_newly_7days=datetime.datetime.now()
    # d_login_newly_7days=t_login_newly_7days-t_activate_7days

    feed_count_7days=pd.read_sql_query(sql_feed_count_7days.format(sql_time_start,sql_time_end), con=db_circlecenter)
    # t_feed_count_7days=datetime.datetime.now()
    # d_feed_count_7days=t_feed_count_7days-t_login_newly_7days

    feed_author_7days=pd.read_sql_query(sql_feed_author_7days.format(sql_time_start,sql_time_end), con=db_circlecenter)
    # t_feed_author_7days=datetime.datetime.now()
    # d_feed_author_7days=t_feed_author_7days-t_feed_count_7days

    works_7days=pd.read_sql_query(sql_works_7days.format(sql_time_start,sql_time_end), con=db_circlecenter)
    # t_works_7dayss=datetime.datetime.now()
    # d_works_7days=t_works_7dayss-t_feed_author_7days

    claimers_7days=pd.read_sql_query(sql_claimers_7days.format(sql_time_start,sql_time_end), con=db_circlecenter)
    # t_claimers_7days=datetime.datetime.now()
    # d_claimers_7days=t_claimers_7days-t_works_7dayss



    # print('d_activate_7days:',d_activate_7days,'\nd_login_newly_7days:',d_login_newly_7days,'\nd_feed_count_7days:',
    #       d_feed_count_7days,'\nd_feed_author_7days:',d_feed_author_7days,'\nd_works_7days:',d_works_7days,'\nd_claimers_7days:',d_claimers_7days)

    charts_data=activate_7days.merge(login_newly_7days,how='left',on=['date','date']).merge(feed_count_7days,how='left',on=['date','date']).merge(feed_author_7days,how='left',on=['date','date']).merge(works_7days,how='left',on=['date','date']).merge(claimers_7days,how='left',on=['date','date']).fillna(0)

    results['activate_7days']=charts_data['activate_7days'].values.tolist()
    results['login_newly_7days']=charts_data['login_newly_7days'].values.tolist()
    results['feed_count_7days']=charts_data['feed_count_7days'].values.tolist()
    results['feed_author_7days']=charts_data['feed_author_7days'].values.tolist()
    results['works_7days']=charts_data['works_7days'].values.tolist()
    results['claimers_7days']=charts_data['claimers_7days'].values.tolist()

    es = Elasticsearch(
            [ES_host],
            http_auth=ES_http_auth,
            scheme=ES_scheme,
            port=ES_port)
    date_es_list = get_days_list(days=14,thatdate=thatdate_sql).es_list()
    date_sql_list=get_days_list(days=14,thatdate=thatdate_sql).sql_list()

    results['process_date']=[]
    results['login_7days_uv']=[]
    results['login_7days_pv']=[]
    results['binding_7days_pv']=[]
    results['binding_7days_uv']=[]
    results['active_7days_pv']=[]
    results['active_7days_uv']=[]
    results['activate_7days_pv']=[]
    results['activate_7days_uv']=[]
    for i in range(len(date_es_list)-1):
        results['process_date'].append(date_sql_list[i])
        login_7days=es.search(index="logstash-*",
                  body={"query": {"bool": {"must": [{"bool": {
                      "should": [{"bool": {"must": [{"term": {"path": "auth"}}, {"term": {"path": "wechat"}}]}},
                                 {"term": {"path": "sign_in"}}]}},{"bool": {"should": [{"term": {"ua": "ppb"}},{"term": {"ua": "okhttp"}}]}}],
                                           "filter": {"range": {"time": {"gte": "%s" % date_es_list[i],"lte": "%s" % date_es_list[i + 1]}}}}},
                        "aggs": {"uv": {"cardinality": {"field": "ex_data.sign_in_member_uuid.keyword"}}},
                        "size": 0})
        results['login_7days_uv'].append(login_7days["aggregations"]['uv']['value'])
        results['login_7days_pv'].append(login_7days["hits"]['total'])

        binding_7days = es.search(index="logstash-*",
                            body={"query": {"bool": {"must": [{"bool": {
                                      "should": [{"term": {"path": "set_mobile.json"}},
                                                 {"term": {"path": "integrate.json"}}]}},
                                                                    {"bool": {"should": [{"term": {"ua": "ppb"}},
                                                                                         {"term": {"ua": "okhttp"}}]}}],
                                                           "filter": {"range": {"time": {"gte": "%s" % date_es_list[i],
                                                                                         "lte": "%s" % date_es_list[
                                                                                             i + 1]}}}}},
                                        "aggs": {"uv": {"cardinality": {"field": "ex_data.sign_in_member_uuid.keyword"}}},
                                        "size": 0})
        results['binding_7days_pv'].append(binding_7days["hits"]['total'])
        results['binding_7days_uv'].append(binding_7days["aggregations"]['uv']['value'])

        active_7days = es.search(index="logstash-*",
                           body={"query": {
                                     "bool": {"must": [{"term": {"path": "activities"}}, {"term": {"path": "v3"}},
                                                       {"bool": {"should": [{"term": {"ua": "ppb"}},
                                                                            {"term": {"ua": "okhttp"}}]}}],
                                              "filter": {"range": {"time": {"gte": "%s" % date_es_list[i],
                                                                            "lte": "%s" % date_es_list[i + 1]}}}}},
                                       "aggs": {"uv": {"cardinality": {"field": "member_uuid.keyword"}}},
                                       "size": 0})
        results['active_7days_pv'].append('%.1f'%(active_7days["hits"]['total']/active_7days["aggregations"]['uv']['value']))
        results['active_7days_uv'].append(active_7days["aggregations"]['uv']['value'])

        activate_7days = es.search(index="logstash-*",
                               body={"query": {"bool": {"must": [{"term": {"path": "address_books"}},
                                                                     {"bool": {"should": [{"term": {"ua": "ppb"}}, {
                                                                         "term": {"ua": "okhttp"}}]}}],
                                                            "filter": {"range": {"time": {"gte": "%s" % date_es_list[i],
                                                                                          "lte": "%s" % date_es_list[
                                                                                              i + 1]}}}}},
                                         "aggs": {"uv": {"cardinality": {"field": "member_uuid.keyword"}}},
                                         "size": 0})
        results['activate_7days_pv'].append(activate_7days["hits"]['total'])
        results['activate_7days_uv'].append(activate_7days["aggregations"]['uv']['value'])
    results['authorized_days']=pd.read_sql_query(sql_authorized_days.format(sql_time_start,sql_time_end), con=db_circlecenter).count_authorized.values.tolist()
    results['process_date']=list(reversed(results['process_date']))
    results['login_7days_uv']=list(reversed(results['login_7days_uv']))
    results['login_7days_pv']=list(reversed(results['login_7days_pv']))
    results['binding_7days_uv']=list(reversed(results['binding_7days_uv']))
    results['binding_7days_pv']=list(reversed(results['binding_7days_pv']))
    results['active_7days_uv']=list(reversed(results['active_7days_uv']))
    results['active_7days_pv']=list(reversed(results['active_7days_pv']))
    results['activate_7days_uv']=list(reversed(results['activate_7days_uv']))
    results['activate_7days_pv']=list(reversed(results['activate_7days_pv']))

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
    es = Elasticsearch(
            [ES_host],
            http_auth=ES_http_auth,
            scheme=ES_scheme,
            port=ES_port)
#    results['active_all'] = es.search(index="logstash-*",
#                            body={"query":{"bool":{"must":[{"term":{"path":"activities"}},{"term":{"path":"v3"}},
#                                                           {"bool":{"should":[{"term":{"ua":"ppb"}},{"term":{"ua":"okhttp"}}]}}]}},
#                                  "aggs":{"uv":{"cardinality":{"field":"member_uuid.keyword"}}},"size":0})["aggregations"]['uv']['value']
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
    overlap_newly_day=olp(attr=results_dr['7days_list'],bar1=results_dr['activate_7days'],bar2=0,bar3=0,
                    line1=results_dr['login_newly_7days'],line2=0,line3=0,bar1_title='新激活用户',bar2_title=0,bar3_title=0,
        line1_title='新登录用户',line2_title=0,line3_title=0,title='日拉新数据',width=1200,height=260)
    overlap_au_day=olp(attr=results_dr['7days_list'],bar1=results_dr['feed_count_7days'],bar2=0,bar3=0,
                    line1=results_dr['feed_author_7days'],line2=0,line3=0,bar1_title='动态条数',bar2_title=0,bar3_title=0,
        line1_title='动态发布者',line2_title=0,line3_title=0,title='日动态数据',width=1200,height=260)

    overlap_works_day=olp(attr=results_dr['7days_list'],bar1=results_dr['works_7days'],bar2=0,bar3=0,
                    line1=results_dr['claimers_7days'],line2=0,line3=0,bar1_title='作品数量',bar2_title=0,bar3_title=0,
        line1_title='认领人数',line2_title=0,line3_title=0,title='日作品认领数据',width=1200,height=260)

    print('UA:',request.user_agent.string)
    print('\033[1;35m'+session['user_id']+' - '+request.remote_addr+' - '+request.method+' - '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+' - '+request.path+'\033[0m')
    return render_template('morning-dr.html',thatdate=thatdate_sql,
                           activate_day=results_dr['activate_day'],
                           login_day_newly=results_dr['login_day_newly'],
                           # contact_day=results_dr['contact_day'],
                           # relation_contact_day=results_dr['relation_contact_day'],
                           process_date=results_dr['process_date'],
                           login_7days_uv=results_dr['login_7days_uv'],
                           login_7days_pv=results_dr['login_7days_pv'],
                           binding_7days_pv=results_dr['binding_7days_pv'],
                           binding_7days_uv=results_dr['binding_7days_uv'],
                           active_7days_pv=results_dr['active_7days_pv'],
                           active_7days_uv=results_dr['active_7days_uv'],
                           activate_7days_pv=results_dr['activate_7days_pv'],
                           activate_7days_uv=results_dr['activate_7days_uv'],
                           authorized_days=results_dr['authorized_days'],
                           overlap_newly_day=overlap_newly_day.render_embed(),
                           overlap_au_day=overlap_au_day.render_embed(),
                           overlap_works_day=overlap_works_day.render_embed())


