import requests
import json
import re
import pymysql
from ..configs.config import *
import datetime
from flask import jsonify
from . import api

def get_articles_values(date_end,days_form,selected_type,keyword_title,keyword_url):

    date_end_str=date_end
    date_end_dt=datetime.datetime.strptime(date_end_str,'%Y%m%d')
    date_start_dt=date_end_dt-datetime.timedelta(days=days_form)
    date_start_str=date_start_dt.strftime('%Y%m%d')

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
            'start_date': date_start_str,
            # 结束统计时间
            'end_date': date_end_str,
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
            #如果是stream去107
            re_stream=re.compile('stream\/(\d+)[\?\/$]?')
            sql="select v_title from streams where id=%s"
            dbinfo={'host':host_cine1,'port':port_cine1,'user':user_cine1,'password':password_cine1,'db':db_cine1_cine107}
        if selected_type == 'articles':
            #如果是article去circlecenter
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
                if selected_type == 'stream':
                    sql_author_name="""select b.v_name from streams a left join members b on a.member_id=b.id where a.id=%s;"""
                if selected_type == 'articles':
                    sql_author_name="""select b.real_name from posts a left join members b on a.member_id=b.id where a.id=%s;"""
                sqlconn.execute(sql_author_name%aricle_id)
                author_name=sqlconn.fetchone()[0]
                dct_results[url]['article_id']=aricle_id
                dct_results[url]['author_name']=author_name
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
        dct_results['type'] = '107站'
    elif selected_type == 'articles':
        dct_results=content_articles(selected_type='articles')
        dct_results['type'] = 'app'
    elif selected_type=='all':
        dct_results={}
        for url in dct_urls:
            dct_results[url]=dct_urls[url]
            dct_results[url]['title'] = url[:80]
        dct_results['type'] = '全站'

    else:
        dct_results={}
        for url in list(dct_urls.keys()):
            if selected_type in url:
                dct_results[url] = dct_urls[url]
                dct_results[url]['title']=url[:80]
        dct_results['type'] = selected_type


    #有关键词检索
    if keyword_url !='default' or keyword_title!='default':
        for url in list(dct_results):
            if keyword_url not in url or keyword_title not in dct_results[url]['title']:
                dct_results.pop(url)

    result={}
    result['dct_urls']=dct_results
    result['date_start']=date_start_str
    result['date_end']=date_end
    return result


@api.route('/api-articles',methods=["GET"])
def api_articles():
    from flask import request
    # a=request.form["date_end"]

    date_end=request.args.get("date_end")
    print(date_end)
    days_form=int(request.args["days_form"])
    selected_type=request.args["selected_type"]
    keyword_title=request.args["keyword_title"]
    keyword_url=request.args["keyword_url"]
    article_results=get_articles_values(date_end=date_end,days_form=days_form,selected_type=selected_type,
                                        keyword_title=keyword_title,keyword_url=keyword_url)
    print(article_results)
    return jsonify(article_results)
