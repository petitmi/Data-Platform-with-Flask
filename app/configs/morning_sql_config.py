#总激活
sql_activate_all="""select count(distinct member_id) active_all from person_infos  where  actived_sites like '%%unsung_hero%%' ;"""

sql_contact_all="""select count(distinct mobile) contact_all from address_books"""

sql_relation_contact_all="""select count(1) from address_books"""

sql_circle_all="""select total_boards,active_boards,total_article_posts,total_video_posts,total_publishers,total_marks,total_comments,total_messages
 from data_totality where data_date ='%s'"""

sql_genres="""select count(*) from members where %s ;"""


#作品
#作品总数
sql_works_all="""select count(1) from films where `category`=1;"""
#作品
sql_works_checked="""select count(distinct film_id) from filmographies where real_name is not null"""
#完全作品
sql_works_complete="""select count(1) from (select film_id,count(1) count_film from filmographies 
where real_name is not null group by film_id having count_film>50)a"""

#已录入职务总数
sql_workers_all="""select count(*) workers_all from unsung_heros2;"""
#已审核职务总数
sql_workers_checked="""select count(*) workers_workers_confirmed from filmographies where real_name is not null;"""
#待认领职务总数
sql_workers_unclaimed="""select count(case when member_id is null then 1 else null end) workers_unclaimed
from filmographies where real_name is not null;"""
#认领人数
sql_claimers_all="""select count(distinct member_id) from filmographies where member_id is not null and real_name is not null;"""


#######################################################################################################################
#当日
##激活
sql_activate_day="""select count(distinct member_id) active_day from person_infos  
where  actived_sites like '%%unsung_hero%%' and hero_actived_at  between '{0}' and '{1}';"""
##首次登录
sql_login_day_newly="""select count(0) from tracks where created_at between '{0}' and '{1}' and trackable_type='Member'and `action`=100"""
##总通讯录
sql_contact_day="""select count(distinct mobile) contact_day from address_books 
where created_at between '{0}' and '{1}';"""
##开启通讯录
sql_relation_contact_day="""select count(1) from address_books 
where created_at between '{0}' and '{1}'"""
##私信人数
sql_messages_day="""select count(distinct `sender_id`) from messages where kind=0 and created_at between '{0}' and '{1}'"""


#多日
##新登录

sql_login_newly_days="""select date(created_at) date,count(0) login_newly_days from tracks where created_at between '{0}' and '{1}'
 and trackable_type='Member'and `action`=100 group by date(created_at)  order by date(created_at) limit 15"""
##激活成功
sql_activate_members_fin_days="""select date(updated_at) date,count(distinct member_id) activate_members_fin_days
from person_infos
where actived_sites like '%unsung_hero%'  and hero_actived_at between '{0}' and '{1}'
group by date(hero_actived_at) order by date(hero_actived_at) desc"""
##授权通讯录
###即便跳过授权也会返回一个空的通讯录
# sql_authorized_days="""select date(created_at) date,count(distinct member_id) authorized_days from `address_books`
# where created_at between '{0}' and '{1}' group by date(created_at) order by date(created_at) desc"""
##作品
sql_works_days="""select date(created_at) date,count(1) works_days from films 
where `category`=1  and created_at between '{0}' and '{1}'
group by date(created_at) order by date(created_at) desc;"""
##认领人
sql_claimers_days="""select date(created_at) date,count(distinct member_id) claimers_days from claim_logs 
where status=1 and created_at between '{0}' and '{1}'
group by date(created_at) order by date(created_at) desc; """
##用户动态发布人数
sql_feed_author_user_days="""select date(created_at) date,count(distinct owner_id) feed_author_user_days from activities 
where recipient_id = 3865 and recipient_type = 'Board' and `key` in ('video.create','album.create','link.create') 
and  created_at between '{0}' and '{1}' and owner_id not in (46562,777367,770325,28017,900368,34797,759790)
group by date(created_at) order by date(created_at) desc;"""
##编辑动态数量
sql_feed_count_editor_days="""select date(created_at) date,count(1) feed_count_editor_days from activities 
where recipient_id = 3865 and recipient_type = 'Board'  and `key` in ('video.create','album.create','link.create') 
and created_at between '{0}' and '{1}' and owner_id in (46562,777367,770325,28017,900368,34797,759790)
 group by date(created_at) order by date(created_at) desc;"""

##评论点赞私信
sql_circle_days="""select data_date date,comments comments_days,marks marks_days,messages messages_days
    from data_daily where data_date between '{0}' and '{1}' order by data_date desc"""


##es活跃登录激活授权
sql_app_daily_days="""select date,activate_members,active_members,active_times,authorize_members from app_daily 
where date between '{0}' and '{1}' order by date desc """


#激活职业
sql_actived_business="""SELECT businesses.name,COUNT(member_businesses.id)
FROM member_businesses INNER JOIN businesses ON businesses.id = member_businesses.`business_id` AND businesses.`kind`= 3
WHERE member_id IN 
(SELECT member_id FROM person_infos WHERE actived_sites LIKE '%unsung%') GROUP BY businesses.`id` ORDER BY COUNT(member_businesses.`id`) DESC"""




#######################################################################################################################################

#栏目
sql_columns_pv="""select b.id column_id,b.title column_title,sum(a.visits) column_pv
from casts a left join cast_columns b on a.cast_column_id=b.id 
where b.id is not null group by b.id order by column_pv desc;"""
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

###################################################################################################
sql_business_activate="""select case when `business_name` is null then '未知' else `business_name` end 'business_name',
count(*)'business_yest'
from members 
where activate_time between '{0}' and '{1}' and is_activate=1 group by business_name order by count(*) desc limit 10;"""

sql_business="""select case when business_name is not null then business_name else '未知' end business_name,count(*) 'business_all',
count(case when is_edu=1 then 1 else null END)business_edu,count(case when is_ec=1 then 1 else null end)business_ec,
count(case when is_activate=1 then 1 else null end)business_activate from members group by business_name order by count(*)  desc"""