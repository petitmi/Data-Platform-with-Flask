# sql_department="""select a.department_name,ifnull(sum(sales_amount),0) sales_amount
# from (select distinct department_id,department_name,order_id from finances_type) a
# left join (select * from finances_sum where year(received_time) ='%s'  or received_time is null
# ) b on a.department_id=b.department_id where a.order_id>0  group by a.department_name order by a.order_id;"""
#
#
# sql_class_month="""select mt.month,ifnull(dt.sales_amount_media,0) sales_amount_media,ifnull(dt.sales_amount_edu,0) sales_amount_edu,ifnull(dt.sales_amount_device,0) sales_amount_device,ifnull(dt.sales_amount_city,0) sales_amount_city
# from month_list mt left join(
# select month(b.received_time) month,
# sum(case when a.department_name='媒体' then sales_amount else 0 end) sales_amount_media,
# sum(case when a.department_name='学习' then sales_amount else 0 end) sales_amount_edu,
# sum(case when a.department_name='设备' then sales_amount else 0 end) sales_amount_device,
# sum(case when a.department_name='城市' then sales_amount else 0 end) sales_amount_city
# from
# (select distinct department_id,department_name from finances_type) a
# left join finances_sum b on a.department_id=b.department_id
# where sales_amount>0  and year(b.received_time) ='%s'
# group by month(b.received_time) order by month)dt
# on mt.month=dt.month
# """
# sql_csc_month="""select a.month,ifnull(sales_amount_edu,0) sales_amount_edu
# from month_list a left join
# (select month(pay_time) month ,sum( orderamount + wallet_pay ) sales_amount_edu from  edu_orders
# where brand_name='拍片学院'  and year(pay_time)='%s' group by month)b on a.month=b.month """
# sql_ppxy_month="""select a.month,ifnull(sales_amount_edu,0) sales_amount_edu
# from month_list a left join
# (select month(pay_time) month ,sum( orderamount + wallet_pay ) sales_amount_edu from  edu_orders
# where brand_name='CSC电影学院' and year(pay_time)='%s' group by month)b on a.month=b.month """
# sql_ec_month_chanjet="""select a.month,ifnull(sales_amount_consult,0) sales_amount_consult
# from month_list a left join
# (select month(order_date) month,sum(price_tax_sell*goods_num) sales_amount_consult from ec_orders_chanjet
# where year(order_date)='%s' group by month(order_date))b on a.month=b.month"""
# sql_ec_month_offline="""select a.month,ifnull(sales_amount_consult,0) sales_amount_consult
# from month_list a left join
# (select month,sales_amount sales_amount_consult from ec_month_offline
# where year='%s' and month<7 order by month)b on a.month=b.month"""
# sql_ec_month_online="""select a.month,ifnull(sales_amount_online,0) sales_amount_online
# from month_list a left join
# (select month(pay_time) month,cast(sum(pay_amount) as UNSIGNED) sales_amount_online from ec_orders
# where order_state=1 and  year(pay_time)='%s'  group by month(pay_time) order by month(pay_time))b on a.month=b.month"""
# sql_littleclass_month="""select a.month,ifnull(sales_amount_edu,0) sales_amount_edu
# from month_list a left join
# (select month(pay_time) month,sum(sales_amount) sales_amount_edu from littleclass_orders
# where year(pay_time)='%s' group by month(pay_time) order by month(pay_time))b on a.month=b.month"""
#
#
# sql_ec_chanjet_thismonth="""select sum(price_tax_sell*goods_num) sales_amount  from ec_orders_chanjet where date(order_date) between '{0}' and '{1}'"""
# sql_ec_online_thismonth="""select cast(sum(pay_amount) as UNSIGNED) sales_amount from ec_orders where order_state=1 and  date(pay_time) between '{0}' and '{1}' """
# sql_csc_thismonth="""select sum( orderamount + wallet_pay ) sales_amount from edu_orders where brand_name='拍片学院' and order_status='success'  and date(pay_time) between '{0}' and '{1}'"""
# sql_ppxy_thismonth="""select sum( orderamount + wallet_pay ) sales_amount from edu_orders where brand_name='CSC电影学院' and order_status='success'   and date(pay_time) between '{0}' and '{1}'"""
# sql_littleclass_thismonth="""select sum(sales_amount) sales_amount from littleclass_orders where date(pay_time) between '{0}' and '{1}'"""
# sql_project_thismonth="""select a.department_name,a.project_name,ifnull(sum(case when date(b.received_time) between '{0}' and '{1}' then sales_amount else null end),0) sales_amount
# from finances_type a left join finances_sum b on a.project_id=b.project_id
# where a.order_id>0
# group by a.project_name
# order by a.order_id ,a.project_id desc;"""
#
# sql_ec_chanjet="""select sum(price_tax_sell*goods_num) sales_amount from ec_orders_chanjet where year(order_date)='%s' """
# sql_ec_offline="""select sum(sales_amount) sales_amount from ec_month_offline where year='%s' and month<7 """
# sql_ec_online="""select cast(sum(pay_amount) as UNSIGNED) sales_amount from ec_orders where order_state=1 and  year(pay_time)='%s' """
# sql_csc="""select sum( orderamount + wallet_pay ) sales_amount from edu_orders
# where brand_name='拍片学院' and order_status='success'  and year(pay_time)='%s'"""
# sql_ppxy="""select sum( orderamount + wallet_pay ) sales_amount from edu_orders
# where brand_name='CSC电影学院' and order_status='success' and year(pay_time)='%s'"""
# sql_littleclass="""select sum(sales_amount) sales_amount  from littleclass_orders where year(pay_time) ='%s'"""
# sql_project="""select a.department_name,a.project_name,ifnull(sum(sales_amount),0) sales_amount
# from finances_type a left join finances_sum b on a.project_id=b.project_id
# where  year(b.received_time) ='%s' or b.received_time is null
# group by a.project_name
# order by a.order_id ,a.project_id desc;"""

# sql_ec_chanjet_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_ec_chanjet from month_list a left join
# (select month(order_date) month,sum(price_tax_sell*goods_num) sales_amount  from ec_orders_chanjet
# where year(order_date)='%s' group by month(order_date) )b on a.month=b.month;"""
# sql_ec_offline_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_ec_offline from month_list a left join
# (select month,sales_amount from ec_month_offline where year='%s' and month<7)b on a.month=b.month;"""
# sql_ec_online_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_ec_online from month_list a left join
# (select month(pay_time) month,cast(sum(pay_amount) as UNSIGNED) sales_amount from ec_orders
# where order_state=1 and  year(pay_time)='%s'
# group by month(pay_time) )b on a.month=b.month;"""
# sql_csc_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_csc from month_list a left join
# (select month(pay_time) month,sum( orderamount + wallet_pay ) sales_amount from edu_orders
# where brand_name='拍片学院' and order_status='success'  and year(pay_time) ='%s' group by month(pay_time))b on a.month=b.month;"""
# sql_ppxy_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_ppxy from month_list a left join
# (select month(pay_time) month,sum( orderamount + wallet_pay ) sales_amount from edu_orders
# where brand_name='CSC电影学院' and order_status='success'   and year(pay_time)='%s' group by month(pay_time))b on a.month=b.month;"""
# sql_littleclass_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_littleclass from month_list a left join
# (select month(pay_time) month,sum(sales_amount) sales_amount from littleclass_orders
# where year(pay_time) ='%s' group by month(pay_time))b on a.month=b.month;"""
#
# sql_project_month_compared="""select a.month,ifnull(sales_amount_yltx,0) sales_amount_yltx,ifnull(sales_amount_rs,0) sales_amount_rs,
# ifnull(sales_amount_cjrh,0) sales_amount_cjrh,ifnull(sales_amount_cjk,0) sales_amount_cjk,ifnull(sales_amount_gg,0) sales_amount_gg,
# ifnull(sales_amount_zl,0) sales_amount_zl,ifnull(sales_amount_wx,0) sales_amount_wx,ifnull(sales_amount_cq,0) sales_amount_cq,
# ifnull(sales_amount_dyz,0) sales_amount_dyz,ifnull(sales_amount_stz,0) sales_amount_stz
# from month_list a left join
# (select month(received_time) month,
# sum(case when `project_id`=314 then sales_amount else 0 end) sales_amount_cjrh, /*产教融合*/
# sum(case when `project_id`=313 then sales_amount else 0 end) sales_amount_gg,/*广告*/
# sum(case when `project_id`=312 then sales_amount else 0 end) sales_amount_yltx,/*一录同行*/
# sum(case when `project_id`=315 then sales_amount else 0 end) sales_amount_wx,/*维修*/
# sum(case when `project_id`=311 then sales_amount else 0 end) sales_amount_zl,/*租赁*/
# sum(case when `project_id`=310 then sales_amount else 0 end) sales_amount_rs,/*二手*/
# sum(case when `project_id`=309 then sales_amount else 0 end) sales_amount_cjk,/*场景库*/
# sum(case when `project_id`=308 then sales_amount else 0 end) sales_amount_cq/*重庆*/,
# sum(case when `project_id`=321 then sales_amount else 0 end) sales_amount_dyz/*电影周*/,
# sum(case when `project_id`=323 then sales_amount else 0 end) sales_amount_stz/*师徒制*/
#
# from finances_sum where year(received_time)='%s'
# group by month(received_time))b on a.month=b.month ;"""

sql_ec_chanjet_month="""select month(order_date) month,'器材-顾问销售',sum(price_tax_sell*goods_num) sales_amount  from ec_orders_chanjet 
where year(order_date)='%s' group by month(order_date) ;"""
sql_ec_offline_month="""select month,'器材-顾问销售',sales_amount from ec_month_offline where year='%s' and month<7;"""
sql_ec_online_month="""select month(pay_time) month,'器材-自主下单',cast(sum(pay_amount) as UNSIGNED) sales_amount from ec_orders 
where order_state=1 and  year(pay_time)='%s' 
group by month(pay_time);"""
sql_csc_month="""select month(pay_time) month,'CSC',sum( orderamount + wallet_pay ) sales_amount from edu_orders 
where brand_name='拍片学院' and order_status='success'  and year(pay_time) ='%s' group by month(pay_time);"""
sql_ppxy_month="""select month(pay_time) month,'非CSC',sum( orderamount + wallet_pay ) sales_amount from edu_orders 
where brand_name='CSC电影学院' and order_status='success'   and year(pay_time)='%s' group by month(pay_time);"""
sql_littleclass_month="""select month(pay_time) month,'媒体课程一体化',sum(sales_amount) sales_amount from littleclass_orders 
where year(pay_time) ='%s' group by month(pay_time);"""

sql_project_month="""select month(received_time),b.project_name,sum(sales_amount) from finances_sum a 
left join finances_type b on a.project_id=b.project_id where year(received_time)='%s'
group by month(received_time),project_name ;;"""

# **********************************************************************************************************************
# screen
sql_activate="""select count(distinct member_id) active_all from person_infos  
where  actived_sites like '%unsung_hero%' and hero_actived_at between '{0}' and '{1}';"""
sql_activate_all="""select count(distinct member_id) active_all from person_infos  
where  actived_sites like '%unsung_hero%';"""
sql_new_member="""select id,`avatar`,real_name from members where id in 
(select member_id from (SELECT member_id FROM person_infos WHERE actived_sites LIKE '%unsung%' order by hero_actived_at desc limit 1)b);"""

sql_activate_org="""select count(distinct member_id) active_all from org_infos  
where  actived_sites like '%unsung_hero%' and hero_actived_at between '{0}' and '{1}';"""
sql_activate_all_org="""select count(distinct member_id) active_all from org_infos  
where  actived_sites like '%unsung_hero%';"""
sql_member_business_id="""select `business_id` from `member_businesses` where `member_id`='{0}'"""
sql_member_business="""select name from businesses where id='{0}'"""
sql_activity="""select `trackable_id`,`owner_id`,`created_at`,`trackable_type`,`key` from activities 
where `recipient_id` = 3865 and `recipient_type` = 'Board'  and `key` in ('video.create','album.create','link.create') and status='normal'
order by `created_at` desc limit 1;"""
sql_activity_author="""select id,real_name from members where id='{0}';"""
sql_activity_content="""select content from posts where id='{0}';"""
sql_activity_link="""select description from links where id='{0}'"""
es_active_total={'index':"logstash-*",
                  'body':{"query": {"bool": {"must": [{"term": {"path": "activities"}}, {"term": {"path": "v3"}},
                                                       {"bool": {"should": [{"term": {"ua": "ppb"}},{"term": {"ua": "okhttp"}}]}}],
                                             "filter": {"range": {"time": {"gte": '',"lte": ''}}}}},
                          "aggs": {"member_count": {"cardinality": {"field": "member_uuid.keyword"}}},
                          "size": 0}}

sql_claimers_day="""select count(distinct member_id) claimers_days from claim_logs 
where status=1 and created_at between '{0}' and '{1}';"""
sql_claimers_all="""select count(distinct member_id) claimers_days from claim_logs 
where status=1"""
#################3#####################################################################################################
es_profile_hours={'index':"logstash-*",
                  'body':{"query":{"bool":{"must":[{"term":{"path":"profile"}},{"term":{"path":""}}],
                                   "filter":{"range":{"time":{"gte":"","lte":""}}}}},
                          "aggs":{"view_hours":{"date_histogram":{"field":"@timestamp","interval":"hour"}}},
                          "size":0}}
es_all_hours={'index':"logstash-*",
                  'body':{"query":{"bool":{"must":[{"term":{"member_uuid.keyword":""}}],
                                   "filter":{"range":{"time":{"gte":"","lte":""}}}}},
                          "aggs": {"all_hours": {"date_histogram": {"field": "@timestamp", "interval": "hour"}}},
                          "size":0}}
sql_follow_hours="""select DATE_FORMAT(concat(year(created_at),'-',month(created_at),'-',day(created_at),' ',hour(created_at),':','00',':','00'), '%%Y-%%m-%%d %%H:%%i:%%S') hour,
count(1) follower_count from follows 
where followable_id='%s' and created_at between '{0}' and '{1}'  group by day(created_at),hour(created_at)"""

sql_follow_all="""select count(1) follower_count from follows 
where followable_id='%s' and created_at between '{0}' and '{1}' """

sql_activity_hours="""select DATE_FORMAT(concat(year(created_at),'-',month(created_at),'-',day(created_at),' ',hour(created_at),':','00',':','00'), '%%Y-%%m-%%d %%H:%%i:%%S') hour,
count(1) activity_count from activities 
where owner_id='%s' and recipient_id = 3865 and recipient_type = 'Board'  
and `key` in ('video.create','album.create','link.create') and status='normal' 
and created_at between '{0}' and '{1}'  group by day(created_at),hour(created_at)"""

sql_activity_all="""select count(1) activity_count from activities  
where owner_id='%s' and recipient_id = 3865 and recipient_type = 'Board'  
and `key` in ('video.create','album.create','link.create') and status='normal' 
and created_at between '{0}' and '{1}' """

sql_member_uuid="""select uuid,real_name from members where id='%s'"""

# ########################################################################################################################
# sql_vip_id="""select distinct id uid, member_id,uname,FROM_UNIXTIME(addtime,'%Y-%m-%d %H:%i:%s') from `lr_user` where is_member>0;"""
# sql_vip_goods="""select order_id,FROM_UNIXTIME(paytime,'%Y-%m-%d %H:%i:%s') paytime,b.name good_name,b.price good_price,
# address_xq address,tel phone,receiver receiver_name from lr_order a left join lr_order_product b on a.id=b.order_id
# where status in (20,30,40,50) and uid='{0}' ;"""
