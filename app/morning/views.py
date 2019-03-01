from flask import render_template, request, flash, session
from flask_login import login_required
from . import morning
from ..decorators import permission_required
from ..models import Permission
from pyecharts_javascripthon.api import TRANSLATOR
from pyecharts import Bar, Line, Overlap, Page
from ..configs.morning_sql_config import *
from ..configs.time_config import *
import pandas as pd
import datetime
import pymysql
from ..configs.config import *
from elasticsearch import Elasticsearch
from app import db
import itertools
import pprint


def olp(attr, bar1, bar2, bar3, line1, line2, line3, bar1_title, bar2_title, bar3_title, line1_title, line2_title,
        line3_title, title, width, height):
    bar = Bar(title=title)
    bar.add(bar1_title, attr, bar1)
    if bar2 != 0:
        bar.add(bar2_title, attr, bar2, yaxis_interval=5)
    if bar3 != 0:
        bar.add(bar3_title, attr, bar3, yaxis_interval=5)
    line = Line()
    line.add(line1_title, attr, line1, yaxis_formatter=" ￥", yaxis_interval=5)
    if line2 != 0:
        line.add(line2_title, attr, line2, yaxis_formatter=" ￥", yaxis_interval=5)
    if line3 != 0:
        line.add(line3_title, attr, line3, yaxis_formatter=" ￥", yaxis_interval=5)

    overlap = Overlap(width=width, height=height)
    overlap.add(bar)
    overlap.add(line, yaxis_index=1, is_add_yaxis=True)
    return overlap

def gt():
    nt=datetime.datetime.now()
    return nt

def get_dr_values(thatdate_sql):
    a=gt()
    db_circlecenter = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_DB,
                                      charset='utf8')
    b=gt()
    ctime={}
    ctime['connct_circlecenter']=b-a
    results = {}
    results['days_list'] = get_days_list(days=15, thatdate=thatdate_sql).sql_list()
    sql_time_days_start = results['days_list'][0] + ' 00:00:00'
    sql_time_days_end = results['days_list'][14] + ' 23:59:59'
    c=gt()
    ctime['']=c-b

    # 组合图
    activate_members_fin_days = pd.read_sql_query(
        sql_activate_members_fin_days.format(sql_time_days_start, sql_time_days_end), con=db_circlecenter)
    d=gt()
    ctime['result_activate_fine']=d-c
    login_newly_days = pd.read_sql(sql_login_newly_days.format(sql_time_days_start, sql_time_days_end),
                                   con=db_circlecenter)
    e=gt()
    ctime['result_login_newly']=e-d
    feed_count_editor_days= pd.read_sql_query(sql_feed_count_editor_days.format(sql_time_days_start, sql_time_days_end),
                                        con=db_circlecenter)
    f=gt()
    ctime['result_feed_count']=f-e
    feed_author_user_days = pd.read_sql_query(sql_feed_author_user_days.format(sql_time_days_start, sql_time_days_end),
                                         con=db_circlecenter)
    g=gt()
    ctime['result_feed_author'] = g-f
    works_days = pd.read_sql_query(sql_works_days.format(sql_time_days_start, sql_time_days_end), con=db_circlecenter)
    h=gt()
    ctime['result_works'] = h-g
    claimers_days = pd.read_sql_query(sql_claimers_days.format(sql_time_days_start, sql_time_days_end),
                                      con=db_circlecenter)
    # i=gt()
    # ctime['result_claimers'] = i-h
    # authorized_members_days = pd.read_sql_query(sql_authorized_days.format(sql_time_days_start, sql_time_days_end),
    #                                             con=db_circlecenter)
    j=gt()
    ctime['result_authorized'] = j-h
    app_daily_days = pd.read_sql(sql_app_daily_days.format(sql_time_days_start, sql_time_days_end), con=db.engine)
    k=gt()
    ctime['result_appdaily'] = k-j
    app_circle_days = pd.read_sql(sql_circle_days.format(sql_time_days_start, sql_time_days_end), con=db.engine)
    l=gt()
    ctime['result_appcircle'] = l-k
    merged_data = activate_members_fin_days.merge(login_newly_days, how='left', on=['date', 'date']). \
        merge(feed_count_editor_days, how='left', on=['date', 'date']). \
        merge(feed_author_user_days, how='left', on=['date', 'date']). \
        merge(works_days, how='left', on=['date', 'date']). \
        merge(claimers_days, how='left', on=['date', 'date']). \
        merge(app_daily_days, how='left', on=['date', 'date']). \
        merge(app_circle_days, how='left', on=['date', 'date']). \
        fillna(0)
    m=gt()
    ctime['merge'] = m-l
    # 绿表
    results['process_date_table'] = merged_data['date'].values.tolist()
    results['active_members_days_table'] = merged_data['active_members'].values.tolist()
    results['active_times_days_table'] = merged_data['active_times'].values.tolist()
    results['login_newly_members_days_table'] = merged_data['login_newly_days'].values.tolist()
    results['activate_members_days_table'] = merged_data['activate_members'].values.tolist()
    results['activate_members_fin_days_table'] = merged_data['activate_members_fin_days'].values.tolist()
    results['authorize_members_days_table'] = merged_data['authorize_members'].values.tolist()
    results['claimers_days_table'] = merged_data['claimers_days'].values.tolist()
    results['feed_count_editor_days_table'] = merged_data['feed_count_editor_days'].values.tolist()
    results['feed_author_user_days_table'] = merged_data['feed_author_user_days'].values.tolist()
    results['works_days_table'] = merged_data['works_days'].values.tolist()
    results['comments_days_table'] = merged_data['comments_days'].values.tolist()
    results['marks_days_table'] = merged_data['marks_days'].values.tolist()
    results['messages_days_table'] = merged_data['messages_days'].values.tolist()
    n=gt()
    ctime['tolist'] = n-m
    # 组合图
    results['activate_members_days_chart'] = results['activate_members_days_table'][:]
    results['login_newly_members_days_chart'] = results['login_newly_members_days_table'][:]
    results['feed_count_editor_days_chart'] = results['feed_count_editor_days_table'][:]
    results['feed_author_user_days_chart'] = results['feed_author_user_days_table'][:]
    results['works_days_chart'] = results['works_days_table'][:]
    results['claimers_days_chart'] = results['claimers_days_table'][:]
    results['activate_members_days_chart'].reverse()
    results['login_newly_members_days_chart'].reverse()
    results['feed_count_editor_days_chart'].reverse()
    results['feed_author_user_days_chart'].reverse()
    results['works_days_chart'].reverse()
    results['claimers_days_chart'].reverse()
    o=gt()
    ctime['reverse'] = o-n
    # pprint.pprint(ctime)
    return results


def get_cast_rp_values():
    results = {}
    # 栏目数据
    db_circlecenter = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_DB,
                                      charset='utf8')
    columns_casts_count = pd.read_sql_query(sql_columns_casts_count, con=db_circlecenter, index_col=['column_id'])
    columns_clips_count = pd.read_sql_query(sql_columns_clips_count, con=db_circlecenter, index_col=['column_id'])
    columns_chats_count = pd.read_sql_query(sql_columns_chats_count, con=db_circlecenter, index_col=['column_id'])
    columns_pv = pd.read_sql_query(sql_columns_pv, con=db_circlecenter, index_col=['column_id'])
    columns_data = columns_pv.join(columns_casts_count, lsuffix='_pv', rsuffix='_casts').join(columns_chats_count,
                                                                                              rsuffix='_chats').join(
        columns_clips_count, rsuffix='_clips')
    # 栏目期数据
    casts_pv = pd.read_sql_query(sql_casts_pv, con=db_circlecenter, index_col=['cast_id'])
    casts_chats_count = pd.read_sql_query(sql_casts_chats_count, con=db_circlecenter, index_col=['cast_id'])
    casts_clips_count = pd.read_sql_query(sql_casts_clips_count, con=db_circlecenter, index_col=['cast_id'])
    casts_data = casts_pv.join(casts_chats_count, rsuffix='_chats').join(casts_clips_count, lsuffix='_pv',
                                                                         rsuffix='_casts')
    # 栏目id按pv排序
    columnid_list = [x for x in casts_data.column_id.drop_duplicates().values]
    # 列表
    colunms_list = columns_data.values.tolist()
    # 结果
    columns_data = {columnid_list[0]: colunms_list[0]}
    columncasts_data = {columnid_list[0]: casts_data[casts_data['column_id'] == columnid_list[0]].values.tolist()}
    for i in range(1, len(columnid_list)):
        columns_data[columnid_list[i]] = colunms_list[i]
        columncasts_data[columnid_list[i]] = casts_data[casts_data['column_id'] == columnid_list[i]].values.tolist()

    results['columns_id'] = columnid_list
    results['columncasts_data'] = columncasts_data
    results['columns_data'] = columns_data
    return results


def get_morning_rp_values():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    db_circlecenter = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_DB,
                                      charset='utf8')
    results = {}
    results['circle_all'] = db.session.execute(sql_circle_all % yesterday_sql).fetchall()[0]
    results['activate_all'] = pd.read_sql_query(sql_activate_all, con=db_circlecenter).values[0][0]
    # results['contact_all']=pd.read_sql_query(sql_contact_all, con=db_circlecenter).values[0][0]
    # results['relation_contact_all']=pd.read_sql_query(sql_relation_contact_all, con=db_circlecenter).values[0][0]
    results['works_all'] = pd.read_sql_query(sql_works_all, con=db_circlecenter).values[0][0]
    results['works_checked'] = pd.read_sql_query(sql_works_checked, con=db_circlecenter).values[0][0]
    results['works_complete'] = pd.read_sql_query(sql_works_complete, con=db_circlecenter).values[0][0]
    results['workers_all'] = pd.read_sql_query(sql_workers_all, con=db_circlecenter).values[0][0]
    results['workers_checked'] = pd.read_sql_query(sql_workers_checked, con=db_circlecenter).values[0][0]
    results['workers_unclaimed'] = pd.read_sql_query(sql_workers_unclaimed, con=db_circlecenter).values[0][0]
    results['claimers_all'] = pd.read_sql_query(sql_claimers_all, con=db_circlecenter).values[0][0]
    results['members_genres'] = {}
    genres_o = ['edu', 'ec', 'vip', 'activate']
    genres_1 = list(itertools.combinations(genres_o, 1))
    genres_2 = list(itertools.combinations(genres_o, 2))
    genres_3 = list(itertools.combinations(genres_o, 3))
    genres_4 = list(itertools.combinations(genres_o, 4))
    genres = [genres_1, genres_2, genres_3, genres_4]
    for genres_pc in genres:
        for genres in genres_pc:
            if len(genres) == 1:
                sql_condition = 'is_%s=1 ' % genres[0]
                key = genres[0]
            else:
                sql_condition = 'is_%s=1 ' % genres[0]
                key = genres[0]
                for genre in genres[1:]:
                    sql_condition += 'and is_%s=1 ' % genre
                    key += '_%s' % genre
            key = key.replace('edu', '学员').replace('ec', '电商').replace('activate', '激活')
            results['members_genres'][key] = db.session.execute(sql_genres % sql_condition).fetchall()[0][0]
    return results


@morning.route('/casts-rp', methods=["POST", "GET"])
@login_required
@permission_required(Permission.ADMIN)
def casts_rp():
    results_casts_rp = get_cast_rp_values()
    print('UA:', request.user_agent.string)
    print('\033[1;35m' + session[
        'user_id'] + ' - ' + request.remote_addr + ' - ' + request.method + ' - ' + datetime.datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S') + ' - ' + request.path + '\033[0m')
    return render_template('casts-rp.html',
                           columns_data=results_casts_rp['columns_data'],
                           columncasts_data=results_casts_rp['columncasts_data'],
                           columns_id=results_casts_rp['columns_id'])


@morning.route('/morning-rp', methods=["POST", "GET"])
@login_required
@permission_required(Permission.ADMIN)
def morning_rp():
    results_morning_rp = get_morning_rp_values()
    print('UA:', request.user_agent.string)
    print('\033[1;35m' + session[
        'user_id'] + ' - ' + request.remote_addr + ' - ' + request.method + ' - ' + datetime.datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S') + ' - ' + request.path + '\033[0m')
    return render_template('morning-rp.html',
                           activate_all=results_morning_rp['activate_all'],
                           # contact_all=results_morning_rp['contact_all'],
                           # relation_contact_all=results_rp['relation_contact_all'],
                           # active_all=results_rp['active_all'],
                           works_all=results_morning_rp['works_all'],
                           works_checked=results_morning_rp['works_checked'],
                           works_complete=results_morning_rp['works_complete'],
                           workers_all=results_morning_rp['workers_all'],
                           claimers_all=results_morning_rp['claimers_all'],
                           circle_all=results_morning_rp['circle_all'],
                           members_genres=results_morning_rp['members_genres'],
                           workers_checked=results_morning_rp['workers_checked'],
                           workers_unclaimed=results_morning_rp['workers_unclaimed']
                           )


@morning.route('/morning-dr', methods=["POST", "GET"])
@login_required
@permission_required(Permission.ADMIN)
def morning_dr():
    yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    if request.method == 'POST':
        thatdate_sql = request.form.get('input')
        if thatdate_sql > yesterday_sql:
            flash('选择的日期未经历')
    if request.method == 'GET' or request.form.get('input') == '':
        thatdate_sql = yesterday_sql

    results_dr = get_dr_values(thatdate_sql)
    p=gt()
    overlap_newly_day = olp(attr=results_dr['days_list'],
                            bar1=results_dr['activate_members_days_chart'],bar2=results_dr['feed_count_editor_days_chart'],
                            bar3=results_dr['works_days_chart'],line1=results_dr['login_newly_members_days_chart'],
                            line2=results_dr['feed_author_user_days_chart'],line3=results_dr['claimers_days_chart'],
                            bar1_title='新激活用户',bar2_title='编辑动态条数',bar3_title='作品数量',
                            line1_title='新登录用户',line2_title='用户动态发布者',line3_title='认领人数',
                            title='',width=1200,height=350)
    q=gt()
    time_chart=q-p
    print('time_chart',time_chart)
    print('UA:', request.user_agent.string)
    print('\033[1;35m' + session[
        'user_id'] + ' - ' + request.remote_addr + ' - ' + request.method + ' - ' + datetime.datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S') + ' - ' + request.path + '\033[0m')
    return render_template('morning-dr.html', thatdate=thatdate_sql,
                           # contact_day=results_dr['contact_day'],
                           # relation_contact_day=results_dr['relation_contact_day'],
                           process_date=results_dr['process_date_table'],
                           login_newly_members_days=results_dr['login_newly_members_days_table'],
                           active_members_days=results_dr['active_members_days_table'],
                           active_times_days=results_dr['active_times_days_table'],
                           activate_members_days=results_dr['activate_members_days_table'],
                           activate_members_fin_days=results_dr['activate_members_fin_days_table'],
                           authorize_members_days=results_dr['authorize_members_days_table'],
                           claimers_days=results_dr['claimers_days_table'],
                           feed_count_editor_days=results_dr['feed_count_editor_days_table'],
                           feed_author_user_days=results_dr['feed_author_user_days_table'],
                           works_days=results_dr['works_days_table'],
                           comments_days=results_dr['comments_days_table'],
                           marks_days=results_dr['marks_days_table'],
                           messages_days=results_dr['messages_days_table'],
                           overlap_newly_day=overlap_newly_day.render_embed()
                           )
