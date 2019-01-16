sql_department="""select a.department_name,ifnull(sum(sales_amount),0) sales_amount
from (select distinct department_id,department_name,order_id from finances_type) a 
left join (select * from finances_sum where year(received_time) ='%s'  or received_time is null 
) b on a.department_id=b.department_id   group by a.department_name order by a.order_id;"""


sql_class_month="""select mt.month,ifnull(dt.sales_amount_media,0) sales_amount_media,ifnull(dt.sales_amount_edu,0) sales_amount_edu,ifnull(dt.sales_amount_device,0) sales_amount_device,ifnull(dt.sales_amount_city,0) sales_amount_city
from month_list mt left join(
select month(b.received_time) month, 
sum(case when a.department_name='媒体' then sales_amount else 0 end) sales_amount_media,
sum(case when a.department_name='学习' then sales_amount else 0 end) sales_amount_edu,
sum(case when a.department_name='设备' then sales_amount else 0 end) sales_amount_device,
sum(case when a.department_name='城市' then sales_amount else 0 end) sales_amount_city
from 
(select distinct department_id,department_name from finances_type) a 
left join finances_sum b on a.department_id=b.department_id 
where sales_amount>0  and year(b.received_time) ='%s'
group by month(b.received_time) order by month)dt
on mt.month=dt.month
"""
sql_csc_month="""select a.month,ifnull(sales_amount_edu,0) sales_amount_edu
from month_list a left join
(select month(pay_time) month ,sum( orderamount + wallet_pay ) sales_amount_edu from  edu_orders 
where brand_name='拍片学院'  and year(pay_time)='%s' group by month)b on a.month=b.month """
sql_ppxy_month="""select a.month,ifnull(sales_amount_edu,0) sales_amount_edu
from month_list a left join
(select month(pay_time) month ,sum( orderamount + wallet_pay ) sales_amount_edu from  edu_orders 
where brand_name='CSC电影学院' and year(pay_time)='%s' group by month)b on a.month=b.month """
sql_ec_month_chanjet="""select a.month,ifnull(sales_amount_consult,0) sales_amount_consult
from month_list a left join
(select month(order_date) month,sum(price_tax_sell*goods_num) sales_amount_consult from ec_orders_chanjet 
where year(order_date)='%s' group by month(order_date))b on a.month=b.month"""
sql_ec_month_offline="""select a.month,ifnull(sales_amount_consult,0) sales_amount_consult
from month_list a left join
(select month,sales_amount sales_amount_consult from ec_month_offline 
where year='%s' and month<7 order by month)b on a.month=b.month"""
sql_ec_month_online="""select a.month,ifnull(sales_amount_online,0) sales_amount_online
from month_list a left join
(select month(pay_time) month,cast(sum(pay_amount) as UNSIGNED) sales_amount_online from ec_orders 
where order_state=1 and  year(pay_time)='%s'  group by month(pay_time) order by month(pay_time))b on a.month=b.month"""
sql_littleclass_month="""select a.month,ifnull(sales_amount_edu,0) sales_amount_edu
from month_list a left join
(select month(pay_time) month,sum(sales_amount) sales_amount_edu from littleclass_orders 
where year(pay_time)='%s' group by month(pay_time) order by month(pay_time))b on a.month=b.month"""


sql_ec_chanjet_thismonth="""select sum(price_tax_sell*goods_num) sales_amount  from ec_orders_chanjet where date(order_date) between '{0}' and '{1}'"""
sql_ec_online_thismonth="""select cast(sum(pay_amount) as UNSIGNED) sales_amount from ec_orders where order_state=1 and  date(pay_time) between '{0}' and '{1}' """
sql_csc_thismonth="""select sum( orderamount + wallet_pay ) sales_amount from edu_orders where brand_name='拍片学院' and order_status='success'  and date(pay_time) between '{0}' and '{1}'"""
sql_ppxy_thismonth="""select sum( orderamount + wallet_pay ) sales_amount from edu_orders where brand_name='CSC电影学院' and order_status='success'   and date(pay_time) between '{0}' and '{1}'"""
sql_littleclass_thismonth="""select sum(sales_amount) sales_amount from littleclass_orders where date(pay_time) between '{0}' and '{1}'"""
sql_project_thismonth="""select a.department_name,a.project_name,ifnull(sum(case when date(b.received_time) between '{0}' and '{1}' then sales_amount else null end),0) sales_amount
from finances_type a left join finances_sum b on a.project_id=b.project_id 
group by a.project_name 
order by a.order_id ,a.project_id desc;"""

sql_ec_chanjet="""select sum(price_tax_sell*goods_num) sales_amount from ec_orders_chanjet where year(order_date)='%s' """
sql_ec_offline="""select sum(sales_amount) sales_amount from ec_month_offline where year='%s' and month<7 """
sql_ec_online="""select cast(sum(pay_amount) as UNSIGNED) sales_amount from ec_orders where order_state=1 and  year(pay_time)='%s' """
sql_csc="""select sum( orderamount + wallet_pay ) sales_amount from edu_orders 
where brand_name='拍片学院' and order_status='success'  and year(pay_time)='%s'"""
sql_ppxy="""select sum( orderamount + wallet_pay ) sales_amount from edu_orders 
where brand_name='CSC电影学院' and order_status='success' and year(pay_time)='%s'"""
sql_littleclass="""select sum(sales_amount) sales_amount  from littleclass_orders where year(pay_time) ='%s'"""
sql_project="""select a.department_name,a.project_name,ifnull(sum(sales_amount),0) sales_amount
from finances_type a left join finances_sum b on a.project_id=b.project_id 
where  year(b.received_time) ='%s' or b.received_time is null 
group by a.project_name 
order by a.order_id ,a.project_id desc;"""

sql_ec_chanjet_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_ec_chanjet from month_list a left join 
(select month(order_date) month,sum(price_tax_sell*goods_num) sales_amount  from ec_orders_chanjet 
where year(order_date)='%s' group by month(order_date) )b on a.month=b.month;"""
sql_ec_offline_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_ec_offline from month_list a left join 
(select month,sales_amount from ec_month_offline where year='%s' and month<7)b on a.month=b.month;"""
sql_ec_online_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_ec_online from month_list a left join 
(select month(pay_time) month,cast(sum(pay_amount) as UNSIGNED) sales_amount from ec_orders 
where order_state=1 and  year(pay_time)='%s' 
group by month(pay_time) )b on a.month=b.month;"""
sql_csc_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_csc from month_list a left join 
(select month(pay_time) month,sum( orderamount + wallet_pay ) sales_amount from edu_orders 
where brand_name='拍片学院' and order_status='success'  and year(pay_time) ='%s' group by month(pay_time))b on a.month=b.month;"""
sql_ppxy_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_ppxy from month_list a left join 
(select month(pay_time) month,sum( orderamount + wallet_pay ) sales_amount from edu_orders 
where brand_name='CSC电影学院' and order_status='success'   and year(pay_time)='%s' group by month(pay_time))b on a.month=b.month;"""
sql_littleclass_month_compared="""select a.month,ifnull(b.sales_amount,0) sales_amount_littleclass from month_list a left join 
(select month(pay_time) month,sum(sales_amount) sales_amount from littleclass_orders 
where year(pay_time) ='%s' group by month(pay_time))b on a.month=b.month;"""

sql_project_month_compared="""select a.month,ifnull(sales_amount_yltx,0) sales_amount_yltx,ifnull(sales_amount_rs,0) sales_amount_rs,
ifnull(sales_amount_cjrh,0) sales_amount_cjrh,ifnull(sales_amount_cjk,0) sales_amount_cjk,ifnull(sales_amount_gg,0) sales_amount_gg,
ifnull(sales_amount_zl,0) sales_amount_zl,ifnull(sales_amount_wx,0) sales_amount_wx,ifnull(sales_amount_cq,0) sales_amount_cq, 
ifnull(sales_amount_dyz,0) sales_amount_dyz,ifnull(sales_amount_stz,0) sales_amount_stz
from month_list a left join 
(select month(received_time) month,
sum(case when `project_id`=314 then sales_amount else 0 end) sales_amount_cjrh, /*产教融合*/
sum(case when `project_id`=313 then sales_amount else 0 end) sales_amount_gg,/*广告*/
sum(case when `project_id`=312 then sales_amount else 0 end) sales_amount_yltx,/*一录同行*/
sum(case when `project_id`=315 then sales_amount else 0 end) sales_amount_wx,/*维修*/
sum(case when `project_id`=311 then sales_amount else 0 end) sales_amount_zl,/*租赁*/
sum(case when `project_id`=310 then sales_amount else 0 end) sales_amount_rs,/*二手*/
sum(case when `project_id`=309 then sales_amount else 0 end) sales_amount_cjk,/*场景库*/
sum(case when `project_id`=308 then sales_amount else 0 end) sales_amount_cq/*重庆*/,
sum(case when `project_id`=321 then sales_amount else 0 end) sales_amount_dyz/*电影周*/,
sum(case when `project_id`=323 then sales_amount else 0 end) sales_amount_stz/*师徒制*/

from finances_sum where year(received_time)='%s'
group by month(received_time))b on a.month=b.month ;"""



# **********************************************************************************************************************
# screen
sql_activate="""select count(distinct member_id) active_all from person_infos  
where  actived_sites like '%unsung_hero%' and hero_actived_at between '{0}' and '{1}';"""
sql_activate_all="""select count(distinct member_id) active_all from person_infos  
where  actived_sites like '%unsung_hero%';;"""