
sql_ec_chanjet_month="""select month(order_date) month,'器材-顾问销售',sum(price_tax_sell*goods_num) sales_amount  from ec_orders_chanjet 
where year(order_date)='%s' group by month(order_date) ;"""
sql_ec_offline_month="""select month,'器材-顾问销售',sales_amount from ec_month_offline where year='%s' and month<7;"""
sql_ec_online_month="""select month(pay_time) month,'器材-自主下单',cast(sum(pay_amount) as UNSIGNED) sales_amount from ec_orders 
where order_state=1 and  year(pay_time)='%s' 
group by month(pay_time);"""
sql_ppxy_month="""select month(pay_time) month,'非CSC',sum( orderamount + wallet_pay ) sales_amount from edu_orders 
where brand_name='拍片学院' and order_status='success'  and year(pay_time) ='%s' group by month(pay_time);"""
sql_csc_month="""select month(pay_time) month,'CSC',sum( orderamount + wallet_pay ) sales_amount from edu_orders 
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
# ########################################################################################################################


sql_authoritors = """select case when c.business_merge is null then b.business_name else c.business_merge end business_merge_name,
count(distinct a.member_id) authoritors_count
from circlecenter.authorities a 
left join data_analysis.members b on a.member_id=b.member_id 
left join data_analysis.business_merge c on b.business_name=c.business_origin
left join circlecenter.members d on a.member_id=d.id
where a.authable_type='Member' and a.deleted_at is null  and d.type='person' 
group by case when c.business_merge is null then b.business_name else c.business_merge end;"""
sql_calimers = """select case when c.business_merge is null then b.business_name else c.business_merge end business_merge_name,
count(distinct a.member_id) claimers_count
from circlecenter.filmographies a 
left join data_analysis.members b on a.member_id=b.member_id 
left join data_analysis.business_merge c on b.business_name=c.business_origin
where a.member_id is not null and a.real_name is not null 
group by case when c.business_merge is null then b.business_name else c.business_merge end;"""
sql_feeders = """select  case when e.business_merge is null then b.business_name else e.business_merge end business_merge_name,
count(distinct d.owner_id) feeders_count
from circlecenter.authorities a 
left join data_analysis.members b on a.member_id=b.member_id 
left join circlecenter.members c on a.member_id=c.id 
left join circlecenter.activities d on a.member_id=d.owner_id
 left join data_analysis.business_merge e on b.business_name=e.business_origin
where a.authable_type='Member' and a.deleted_at is null and c.type='person' and d.recipient_id = 3865 and d.recipient_type = 'Board' and `key` in ('video.create','album.create','link.create') 
group by case when e.business_merge is null then b.business_name else e.business_merge end;"""

sql_activate = """select case when c.business_merge is null then b.business_name else c.business_merge end business_merge_name,count(distinct a.member_id) activate_count
from circlecenter.authorities a 
left join data_analysis.members b on a.member_id=b.member_id 
left join data_analysis.business_merge c on b.business_name=c.business_origin
left join circlecenter.members d on a.member_id=d.id
inner join (select distinct member_uuid from data_analysis.app_details where type='active' and time > date_sub(curdate(),interval 3 month)) e on d.uuid=e.member_uuid
where a.authable_type='Member' and a.deleted_at is null  and d.type='person'   
group by case when c.business_merge is null then b.business_name else c.business_merge end;"""
