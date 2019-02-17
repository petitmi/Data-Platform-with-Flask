sql_ec_year="""select a.year_num,
ifnull(a.sales_count,0)+ifnull(b.sales_count,0)+ifnull(c.sales_count,0),
 cast(ifnull(a.sales_amount,0)+ifnull(b.sales_amount,0)+ifnull(c.sales_amount,0)as unsigned),
 ifnull(a.sales_buyers,0)+ifnull(b.sales_buyers,0)
from
 (select year(pay_time) year_num,count(distinct order_id) sales_count,
sum( pay_amount ) sales_amount,count(distinct member_id) sales_buyers
 from ec_orders  where order_state=1 group by year(pay_time) )a 
 left join 
 ( select year(order_date) year_num,count(1) sales_count,sum(price_tax_sell*goods_num) sales_amount,count(distinct client) sales_buyers from ec_orders_chanjet where ec_type='consult' group by year(order_date))b  on a.year_num=b.year_num 
 left join 
 ( select year year_num,sum(sales_count) sales_count,sum(sales_amount) sales_amount from ec_month_offline group by year
)c on a.year_num=c.year_num
order by year_num;"""

sql_ec_99vip="""select '总数据',count(distinct uid) sales_uid_vip,count(distinct order_id) sales_count_vip,sum(pay_amount) sales_amount_vip
  from ec_orders where uid in(select uid from ec_99vip )  and order_state=1 and pay_time between '{0}'and '{1}'
;"""
# """  union
#   select case when b.ec_type='local' then '本地' else '影音店' end ,count(distinct b.member_id),count(distinct order_id),sum(pay_amount)
#   from ec_orders a left join ec_99vip b on a.member_id=b.member_id
# where   order_state=1  and b.member_id is not null  and pay_time between '{0}'and '{1}' group by b.ec_type """

sql_ec_99vip_sale="""select count(0) 
from ec_orders a left join ec_order_goods b on a.order_id=b.order_id 
where pay_time between '{0}'and '{1}' and goods_id in ('w_3979','m_748')  and a.order_state=1;"""

sql_ec_99vip_sale_all="""select count(distinct uid,member_id) from ec_99vip; """

sql_ec_offline_2018="""select sum(a.sales_amount) from ec_month_offline a  where a.year=2018;"""

sql_ec_yesterday="""select count(distinct  order_id )'日销量',ifnull(sum( pay_amount),0)'日流水'
 from ec_orders a  where pay_time between '{0}'and '{1}' and order_state= 1  ;"""

sql_ec_1day="""select count(distinct  order_id )'日销量',ifnull(sum( pay_amount ),0)'日流水'
 from ec_orders a  where pay_time  between '{0}'and '{1}'  and order_state= 1  ;"""

sql_ec_7day="""select count(distinct  order_id )'日销量',ifnull(sum(pay_amount),0)'日流水'
 from ec_orders a  where pay_time between '{0}'and '{1}' and order_state= 1  ;"""

sql_ec_7days="""select date(pay_time) date,count(distinct order_id) sales_count,cast(sum( pay_amount ) as UNSIGNED) sales_amount
from ec_orders 
where pay_time between '{0}' and '{1}' group by date(pay_time) order by date(pay_time) desc ;"""


sql_ec_week="""select concat(year(pay_time),'-',week(pay_time,1)) week_num,count(distinct order_id) '周销量',
cast(sum( pay_amount ) as UNSIGNED) '周流水'
 from ec_orders  where pay_time between '{0}' and '{1}' and order_state=1 ;"""



sql_ec_month = """select month(pay_time) '月份',count(distinct order_id)'月销量',cast(sum( pay_amount) as UNSIGNED)'月流水'
 from ec_orders where pay_time between '{0}' and '{1}' and order_state=1 """

sql_ec_chanjet_month="""select ifnull(count(distinct order_id),0) '月销量',
ifnull(cast(sum(price_tax_sell*goods_num)as UNSIGNED),0) '月流水'
 from ec_orders_chanjet where order_date between '{0}' and '{1}' and ec_type='consult'"""


sql_ec_type="""select case when ec_type='yydweb' then '影音店'  when ec_type='localweb' then '本地官网' else '本地小程序' end ,
count(distinct order_id),count(distinct case when post_id!=0 then order_id else null end)
,cast(sum( pay_amount ) as UNSIGNED),
cast(sum( pay_amount )/count(distinct  member_id) as UNSIGNED)
 from ec_orders a  where pay_time between '{0}' and '{1}' and order_state= 1  group by ec_type order by ec_type desc;"""


# sql_ec_week_compared="""select a.week_num'周',b.target_count'周目标',a.sales_count'周销量',a.sales_amount'周流水' from
# (select concat(year(pay_time),'-',week(pay_time,1)) week_num,count(distinct order_id) sales_count,
# cast(sum( pay_amount ) as UNSIGNED) sales_amount
#  from ec_orders  where date(pay_time) <= '%s' and order_state= 1
#  group by year(pay_time),week(pay_time,1) order by year(pay_time) desc,week(pay_time,1) desc limit 12)a
#  left join ec_week_target b on a.week_num=b.week order by week_num desc;"""

sql_ec_week_compared="""select a.week_num,ifnull(b.sales_count,0) sales_count,ifnull(b.sales_amount,0) sales_amount from 
(select concat(year(date),'-',week(date,1)) week_num from date_list group by year(date),week(date,1) order by date desc)a left join
(select concat(year(pay_time),'-',week(pay_time,1)) week_num,count(distinct order_id) sales_count,
cast(sum( pay_amount ) as UNSIGNED) sales_amount
 from ec_orders  where pay_time < '{0}' and order_state= 1 
 group by year(pay_time),week(pay_time,1) order by year(pay_time) desc,week(pay_time,1) desc limit 12)b
on a.week_num=b.week_num limit 12"""

# sql_ec_chanjet_week_compared="""select concat(year(order_date),'-',week(order_date,1)) week_num,count(distinct order_id) '周销量',
# cast(sum(price_tax_sell*goods_num)as UNSIGNED) '周流水'
#  from ec_orders_chanjet where date(order_date) <= '%s' and ec_type='consult'
#  group by year(order_date),week(order_date,1) order by year(order_date) desc,week(order_date,1) desc limit 12"""

sql_ec_chanjet_week_compared="""select a.week_num,ifnull(b.sales_count,0) sales_count,ifnull(b.sales_amount,0) sales_amount from 
(select concat(year(date),'-',week(date,1)) week_num from date_list group by year(date),week(date,1) order by date desc)a left join
(select concat(year(order_date),'-',week(order_date,1)) week_num,count(distinct order_id) sales_count,
cast(sum(price_tax_sell*goods_num)as UNSIGNED) sales_amount
 from ec_orders_chanjet where order_date <= '{0}' and ec_type='consult'
 group by year(order_date),week(order_date,1) order by year(order_date) desc,week(order_date,1) desc limit 12)b
on a.week_num=b.week_num limit 12"""

sql_ec_month_compared = """select month(pay_time) '月份'
,cast(count(distinct case when year(pay_time)=2016 then order_id else null end) as UNSIGNED) '2016月销量' ,
cast(sum( case when year(pay_time)=2016 then pay_amount else null end) as UNSIGNED) '2016月流水'
,cast(count(distinct case when year(pay_time)=2017 then order_id else null end) as UNSIGNED)  '2017月销量' ,
cast(sum( case when year(pay_time)=2017 then pay_amount else null end) as UNSIGNED)  '2017月流水'
,cast(count(distinct case when year(pay_time)=2018 then order_id else null end) as UNSIGNED)  '2018月销量' ,
cast(sum( case when year(pay_time)=2018 then pay_amount else null end) as UNSIGNED)   '2018月流水'
from ec_orders where order_state=1 group by month(pay_time) order by month(pay_time)  """
sql_ec_month_offline_2018="""select month,sales_count,sales_amount from ec_month_offline where year=2018 and month<7 order by month"""
sql_ec_month_offline_2017="""select month,sales_count,sales_amount from ec_month_offline where year=2017 order by month"""
sql_ec_month_chanjet_2018="""select month(order_date) month,count(distinct order_id) sales_count,sum(price_tax_sell*goods_num) sales_amount 
from ec_orders_chanjet where ec_type='consult' group by month(order_date)"""

sql_ec_goods="""select case when a.ec_type = 'yydweb' then '影音店' when a.ec_type = 'localweb' then '本地官网' else '本地小程序'  end platform,
goods_name,sum(goods_num),goods_price 
from ec_order_goods a left join ec_orders b on a.order_id=b.order_id 
where goods_id in ('m_939','m_918','w_4130','w_4143') and pay_time between '{0}' and '{1}' and a.order_state=1  
group by goods_id order by sum(goods_num) desc ; """

sql_ec_table_week="""  select concat(year(order_date),'-',week(order_date,1)) week_num,
ifnull(sum(price_tax_sell*goods_num),0) sales_amount,
ifnull(sum(amount_tax-amount_tax_purchase),0)  profit,
ifnull(sum(case when ec_type='consult'then price_tax_sell*goods_num else null end),0) sales_amount_consult,
ifnull(sum(case when ec_type='consult'then amount_tax-amount_tax_purchase else null end),0)  profit_consult,
ifnull(sum(case when ec_type='local' then price_tax_sell*goods_num else null end),0)  sales_amount_mina,
ifnull(sum(case when ec_type='local' then amount_tax-amount_tax_purchase else null end),0)  profit_mina,
ifnull(sum(case when ec_type='yyd' then price_tax_sell*goods_num else null end),0) sales_amount_web,
ifnull(sum(case when ec_type='yyd' then amount_tax-amount_tax_purchase else null end),0)  profit_web
 from ec_orders_chanjet where quantity_sales>0
 group by year(order_date),week(order_date,1) order by year(order_date) desc,week(order_date,1) desc limit 12;"""
sql_ec_table_month="""  select month(order_date) month,
ifnull(sum(price_tax_sell*goods_num),0)  sales_amount,
ifnull(sum(amount_tax-amount_tax_purchase),0)  profit,
ifnull(sum(case when ec_type='consult'then price_tax_sell*goods_num else null end),0)  sales_amount_consult,
ifnull(sum(case when ec_type='consult'then amount_tax-amount_tax_purchase else null end),0)  profit_consult,
ifnull(sum(case when ec_type='local' then price_tax_sell*goods_num else null end),0)  sales_amount_mina,
ifnull(sum(case when ec_type='local' then amount_tax-amount_tax_purchase else null end),0)  profit_mina,
ifnull(sum(case when ec_type='yyd' then price_tax_sell*goods_num else null end),0) sales_amount_web,
ifnull(sum(case when ec_type='yyd' then amount_tax-amount_tax_purchase else null end),0)  profit_web
 from ec_orders_chanjet  where quantity_sales>0
group by month(order_date) order by month(order_date) desc ;"""
