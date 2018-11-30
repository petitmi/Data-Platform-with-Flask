#总激活
sql_activate_all="""select count(distinct member_id) active_all from person_infos  where  actived_sites like '%%unsung_hero%%' ;"""

sql_contact_all="""select count(distinct mobile) contact_all from address_books"""

sql_relation_contact_all="""select count(1) from address_books"""

#当日登录用es

#当日激活
sql_activate_day="""select count(distinct member_id) active_day from person_infos  where  actived_sites like '%%unsung_hero%%' and date(hero_actived_at) = '%s';"""
#新登录
sql_login_day_newly="""select count(distinct member_id) login_day_newly from person_infos where date(updated_at) = '%s';"""

sql_contact_day="""select count(distinct mobile) contact_day from address_books where date(created_at)='%s';"""
sql_relation_contact_day="""select count(1) from address_books where date(created_at)='%s'"""

sql_login_newly_7days="""select count(distinct member_id) login_day_newly from person_infos 
where updated_at between date_add('{0}',interval -6 day)  and '{0}'group by date(updated_at) order by date(updated_at)
"""
sql_activate_7days="""select date(updated_at) active_date,count(distinct member_id) active_day 
from person_infos
where actived_sites like '%unsung_hero%'  and updated_at between date_add('{0}',interval -6 day)  and '{0}'
group by date(updated_at) order by date(updated_at)
"""
json_login_day={"query":{"bool":{"must": [{"bool": {"should": [{"bool": {"must": [{"term": {"path": "wechat"}},{"term": {"path": "auth"}}]}},{"term": {"path": "sign_in"}}]}},
                                   {"bool": {"should": [{"term": {"ua": "ppb"}},{"term": {"ua": "okhttp"}}]}}],
                          "filter":{"range": {"time": {"gte": "2018-11-15T00:00:00","lte": "2018-11-15T23:59:59Z"}}}}}}
json_login_all={"query":{"bool":{"must": [{"bool": {"should": [{"bool": {"must": [{"term": {"path": "wechat"}},{"term": {"path": "auth"}}]}},{"term": {"path": "sign_in"}}]}},
                                   {"bool": {"should": [{"term": {"ua": "ppb"}},{"term": {"ua": "okhttp"}}]}}]}}}
json_binding_day={"query": {"bool": {"must": [{"bool": { "should": [{"term": {"path": "set_mobile.json"}},{"term": {"path": "integrate.json"}}]}},
                                              {"bool": {"should": [{"term": {"ua": "ppb"}},{"term": {"ua": "okhttp"}}] }}],
                                     "filter": {"range": {"time": {"gte": "2018-11-15T00:00:00","lte": "2018-11-15T23:59:59Z"} }}}}}


sql_actived_business="""SELECT businesses.name,COUNT(member_businesses.id)
FROM member_businesses INNER JOIN businesses ON businesses.id = member_businesses.`business_id` AND businesses.`kind`= 3
WHERE member_id IN (SELECT member_id FROM person_infos WHERE actived_sites LIKE '%unsung%') GROUP BY businesses.`id` ORDER BY COUNT(member_businesses.`id`) DESC"""

#动态
sql_feed_author_7days="""select count(distinct owner_id) from activities 
where recipient_id = 3865 and recipient_type = 'Board' and `key` in ('video.create','album.create','link.create') and  created_at between date_add('{0}',interval -6 day)  and '{0}' 
group by date(created_at) order by date(created_at) asc;"""
sql_feed_count_7days="""select count(1) from activities 
where recipient_id = 3865 and recipient_type = 'Board'  and `key` in ('video.create','album.create','link.create') and created_at between date_add('{0}',interval -6 day)  and '{0}'
 group by date(created_at) order by date(created_at) asc;"""

#栏目
sql_columns_pv="""select b.id column_id,b.title column_title,sum(a.visits) column_pv
from casts a left join cast_columns b on a.cast_column_id=b.id where b.id is not null group by b.id order by column_pv desc;"""
sql_columns_casts_count="""select b.id column_id,count(distinct a.id) casts_count
from casts a left join cast_columns b on a.cast_column_id=b.id 
where b.id is not null group by b.id order by casts_count desc;"""
sql_columns_chats_count="""select c.id column_id,count(1) chat_count 
from cast_chats a left join casts b on a.cast_id=b.id 
left join cast_columns c on b.cast_column_id=c.id 
where uuid is not null and uuid!='fakeid' and c.id is not null group by c.id"""
sql_columns_clips_count="""select c.id column_id,count(1) clip_count 
from cast_clip a left join casts b on a.cast_id=b.id 
left join cast_columns c on b.cast_column_id=c.id 
where  c.id is not null group by c.id"""

#期
sql_casts_pv="""select  b.id column_id,a.id cast_id,b.title column_title,a.title,a.begin_time begin_date,ifnull(sum(a.visits),0) cast_pv
from casts a 
left join (select b.id,b.title,sum(a.visits) column_pv
from casts a left join cast_columns b on a.cast_column_id=b.id where b.id is not null group by b.id order by sum(a.visits) desc) b on a.cast_column_id=b.id 
where b.id is not null group by a.id order by column_pv desc,cast_pv desc;"""
sql_casts_chats_count="""select b.id cast_id,ifnull(count(1),0) chat_count 
from cast_chats a left join casts b on a.cast_id=b.id 
where uuid is not null and uuid!='fakeid' and b.id is not null group by b.id"""
sql_casts_clips_count="""select b.id cast_id,ifnull(count(1),0) clip_count 
from cast_clip a left join casts b on a.cast_id=b.id 
where  b.id is not null group by b.id"""

