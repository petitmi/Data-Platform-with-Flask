import requests
import json
import re
import pymysql
from ..configs.config import *
import datetime
from . import api

def get_articles_values(date_end):
    end_date = date_end
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
            'start_date': end_date,
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
    # 全部结果字典
    for item_no in range(len(urls_items_title)):
        if urls_items_value[item_no][1] > 19:
            dct_urls[urls_items_title[item_no][0]['name']] = {}
            for field_no in range(1, len(urls_fields)):
                dct_urls[urls_items_title[item_no][0]['name']][urls_fields[field_no]] = urls_items_value[item_no][
                    field_no - 1]

    # 选择类型为文章时筛选结果
    def content_articles(selected_type):
        dct_results = {}
        if selected_type == 'stream':
            re_content = re.compile('stream\/(\d+)[\?\/$]?')
            sql = "select v_title from streams where id=%s"
            dbinfo = {'host': host_cine1, 'port': port_cine1, 'user': user_cine1, 'password': password_cine1,
                      'db': db_cine1_cine107}
        if selected_type == 'articles':
            re_content = re.compile('articles\/(\d+)[\?\/$]?')
            sql = "select title from posts where id=%s"
            dbinfo = {'host': host_cine2, 'port': port_cine2, 'user': user_cine2, 'password': password_cine2,
                      'db': db_cine2_circle}

        dbconn = pymysql.connect(host=dbinfo['host'], port=dbinfo['port'], user=dbinfo['user'],
                                 password=dbinfo['password'],
                                 db=dbinfo['db'],
                                 charset='utf8')
        sqlconn = dbconn.cursor()
        for url in list(dct_urls.keys()):
            article_re = re.search(re_content, url)
            if article_re is not None:
                dct_results[url] = dct_urls[url]
                aricle_id = article_re.group(1)
                article_link_type = article_re.end()
                sqlconn.execute(sql % aricle_id)
                title_result = sqlconn.fetchone()
                if title_result is None:
                    article_title = '不存在-%s' % aricle_id
                else:
                    article_title = title_result[0]
                    if len(article_title) > 39:
                        article_title = article_title[:38]
                if selected_type == 'stream':
                    if 'mstream' in url:
                        dct_results[url]['title'] = article_title + '【移动端】'
                    else:
                        dct_results[url]['title'] = article_title + '【PC】'
                else:
                    dct_results[url]['title'] = article_title
                dct_results[url]['article_id']=int(aricle_id)
                dct_results[url]['link_type'] = url[article_link_type:]
        dbconn.close()
        return dct_results


    # 获取最后结果
    dct_results = content_articles(selected_type='stream')
    result = {}
    result['dct_urls'] = dct_results
    result['date_end'] = date_end
    return result


@api.route('/api-articles',methods=["POST"])
def api_articles():
    from flask import request
    end_date=request.form.get("end_date")
    article_results=get_articles_values(date_end=end_date)
    return str(article_results)
