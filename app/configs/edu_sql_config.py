sql_edu_year="""select case when pay_time is null then '其他' else year(pay_time) end  year_num,count(distinct order_id) '年销量',
cast(sum( orderamount + wallet_pay ) as UNSIGNED) '年流水',count(distinct member_id) '年购买人数' ,
cast(sum((orderamount + wallet_pay)*ifnull(b.proportion,0)) as unsigned) '收入分成'
 from edu_orders a left join edu_income_proportion b on a.product_id=b.product_id  
 where order_status='success' group by year(pay_time) ;"""

sql_edu_xiaoe="""select count(distinct order_id) sales_count,sum(price) sales_amount,count(distinct user_id)sales_buyers ,CAST(sum(price)*0.3 as UNSIGNED) 
from edu_orders_xiaoe where order_state=1 and date(created_at) between '2018-07-01' and '%s'"""

sql_edu_800vip="""select class_title,count(distinct order_id )'日销量',ifnull(sum( orderamount + wallet_pay ),0)'日流水' from  edu_orders 
where  order_status='success' and date(pay_time) between '{0}'and '{1}'and product_id='4391'"""

sql_edu_yesterday="""select 
count(distinct  order_id )'日销量',
ifnull(sum( orderamount + wallet_pay ),0)'日流水',
cast(sum((orderamount + wallet_pay)*ifnull(b.proportion,0)) as unsigned)  '收入分成'
 from edu_orders a left join edu_income_proportion b on a.product_id=b.product_id   where  order_status='success' and date(pay_time)= '{0}' ;"""
sql_edu_1day="""select 
count(distinct  order_id )'日销量',
ifnull(sum( orderamount + wallet_pay ),0)'日流水'
 from edu_orders a  where order_status='success' and  date(pay_time) = date_add('{0}',interval -1 day);"""
sql_edu_7day="""select 
count(distinct  order_id )'日销量',
ifnull(sum( orderamount+ wallet_pay),0)'日流水'
 from edu_orders a  where order_status='success' and  date(pay_time)= date_add('{0}',interval -7 day)  ;"""


sql_edu_7days="""select date(pay_time) date,count(distinct order_id) sales_count,round(sum(orderamount+wallet_pay),0) sales_amount
from edu_orders 
where order_status='success' and  date(pay_time) in %s group by date(pay_time) order by date(pay_time) desc """

sql_edu_week="""select concat(year(pay_time),'-',week(pay_time,1)) '周',count(1) '周销量',round(sum(orderamount)+sum(wallet_pay),0) '周流水',
cast(sum((orderamount + wallet_pay)*ifnull(b.proportion,0)) as unsigned)  '周收入分成'
from edu_orders a left join edu_income_proportion b on a.product_id=b.product_id  where  order_status='success' and  date(pay_time) between '%s'  and  '%s' """
sql_edu_month = """select month(pay_time)'月',count(1)'月销量',round(sum(orderamount+wallet_pay),0)'月流水'
,cast(sum((orderamount + wallet_pay)*ifnull(b.proportion,0)) as unsigned) '月收入分成'
 from edu_orders a left join edu_income_proportion b on a.product_id=b.product_id where  order_status='success' and date(pay_time) between '%s'and '%s' """

sql_edu_class="""select a.class_title,teacher_name,count(distinct order_id) class_sale_count,
(sum(orderamount)+sum(wallet_pay))/count(distinct order_id),sum(orderamount)+sum(wallet_pay),
ifnull((sum(orderamount + wallet_pay))*b.proportion,0)  income_proportion
from edu_orders a left join edu_income_proportion b on a.product_id=b.product_id 
where order_status='success' and date(pay_time) between '%s' and '%s' group by class_title order by class_sale_count desc,teacher_name desc"""

sql_edu_week_compared="""select concat(year(pay_time),'-',week(pay_time,1)) '周',count(1) '周销量',round(sum(orderamount)+sum(wallet_pay),0) '周流水'
from edu_orders where order_status='success' and date(pay_time)<='%s' group by year(pay_time),week(pay_time,1) order by year(pay_time) desc,week(pay_time,1) desc limit 12"""

sql_edu_month_compared = """select month(pay_time) '月份'
,count(distinct case when year(pay_time)=2016 then order_id else null end) '2016月销量' ,
cast(sum( case when year(pay_time)=2016 then orderamount +wallet_pay else null end) as UNSIGNED) '2016月流水'
,count(distinct case when year(pay_time)=2017 then order_id else null end) '2017月销量' ,
cast(sum( case when year(pay_time)=2017 then orderamount+wallet_pay else null end) as UNSIGNED) '2017月流水'
,count(distinct case when year(pay_time)=2018 then order_id else null end) '2018月销量' ,
cast(sum( case when year(pay_time)=2018 then orderamount+wallet_pay else null end) as UNSIGNED)  '2018月流水'
from edu_orders where  order_status='success'  group by month(pay_time) order by month(pay_time)  """

sql_edu_class_all="""select a.class_title,teacher_name,count(distinct order_id) class_sale_count,round((sum(orderamount)+sum(wallet_pay))/count(distinct order_id),0),
round(sum(orderamount)+sum(wallet_pay),0),cast((sum(orderamount)+sum(wallet_pay))*b.proportion as unsigned)
from edu_orders a left join edu_income_proportion b on a.product_id=b.product_id 
where order_status='success' group by class_title order by class_sale_count desc,sum(orderamount) desc,teacher_name desc"""

sql_littleclass_total="""select sum(login_uv) login_uv,sum(deblock_count) deblock_count,sum(b.homeworkers),sum(deblock_count)/sum(login_uv) cvr_deblock,
sum(b.homeworkers)/sum(login_uv) cvr_homework  from ( 
 select a.post_id,a.post_title,total_login_uv login_uv,count(distinct b.payment_id) deblock_count ,count(distinct c.member_id) homeworkers,
 count(distinct b.payment_id)/total_login_uv cvr_deblock,count(distinct c.member_id)/total_login_uv cvr_homework
 from posts_totality a 
 left join littleclass_orders b on a.post_id=b.post_id 
 left join littleclass_homework c on a.post_id=c.post_id 
 where b.post_id is not null or c.post_id is not null group by a.post_id)b order by login_uv desc;"""

sql_littleclass_post_total="""  
 select a.post_id,a.post_title,total_login_uv login_uv,count(distinct b.payment_id) deblock_count ,count(distinct c.member_id) homeworkers,
 count(distinct b.payment_id)/total_login_uv cvr_deblock,count(distinct c.member_id)/total_login_uv cvr_homework
 from posts_totality a 
 left join littleclass_orders b on a.post_id=b.post_id 
 left join littleclass_homework c on a.post_id=c.post_id 
 where b.post_id is not null or c.post_id is not null group by a.post_id order by login_uv desc;"""

sql_littleclass_day_total=""" select sum(a.login_uv) login_uv,count(distinct case when date(b.pay_time)='{0}' then payment_id else null end) deblock_count,
count(distinct case when date(c.answer_time)='{0}' then c.member_id else null end) homeworkers,
count(distinct case when date(b.pay_time)='{0}' then payment_id else null end)/sum(a.login_uv) cvr_deblock,
count(distinct case when date(c.answer_time)='{0}' then c.member_id else null end)/sum(a.login_uv) cvr_homework
 from posts_daily a left join littleclass_orders b on a.post_id=b.post_id 
 left join  littleclass_homework c on a.post_id=c.post_id 
where  (posts_date='{0}' and date(b.pay_time)='{0}' ) or (posts_date='{0}' and date(c.answer_time)='{0}') order by login_uv desc;"""

sql_littleclass_day_post="""select a.post_id,a.post_title,a.login_uv,count(distinct case when date(b.pay_time)='{0}' then payment_id else null end) deblock_count,
count(distinct case when date(c.answer_time)='{0}' then c.member_id else null end) homeworkers,
count(distinct case when date(b.pay_time)='{0}' then payment_id else null end) /a.login_uv cvr_deblock,
count(distinct case when date(c.answer_time)='{0}' then c.member_id else null end)/a.login_uv cvr_homework
from posts_daily a 
left join littleclass_orders b on a.post_id=b.post_id 
 left join  littleclass_homework c on a.post_id=c.post_id 
where posts_date='{0}' and ( b.post_id is not null or c.post_id is not null) group by a.post_id order by login_uv desc;"""

