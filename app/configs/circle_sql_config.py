sql_read_boards_daily="select * from boards_daily where boards_date='%s'"
sql_read_posts_daily="select * from posts_daily where posts_date='%s'"
sql_read_data_daily="select * from data_daily where data_date='%s'"
sql_read_data_totality="select * from data_totality where data_date='%s'"
sql_read_boards_totality="select * from boards_totality"
sql_read_posts_totality="select * from posts_totality"
sql_read_boards_a="select * from boards_daily where board_id=1684 and boards_date='%s'"

sql_data_totality="""select total_boards,active_boards,total_login_uv,total_pv,total_article_posts,total_video_posts,total_publishers,total_marks,total_comments,total_messages
 from data_totality where data_date='%s'"""


sql_data_daily="""select data_date,login_uv,newly_login_uv,pv,cast(anony_pv*100/pv as UNSIGNED)
    from data_daily where data_date between date_add('%s',interval -7 day) and '%s' order by data_date desc"""

sql_data_monthly="""select data_date,login_uv,newly_login_uv,pv,cast(anony_pv*100/pv as UNSIGNED),boards,article_posts,video_posts,publishers,comments,marks,messages
    from data_monthly where data_date between date_add('%s',interval -7 month) and '%s' order by data_date desc"""


sql_posts_detail="""select a.post_title,a.board_name,b.total_login_uv,login_uv,newly_login_uv,pv,cast(anony_pv*100/pv as UNSIGNED) ,total_comments,comments,total_marks,marks
from posts_daily a left join posts_totality b on a.post_id=b.post_id where posts_date='%s' order by login_uv desc limit 15"""

sql_boards_detail="""select a.board_name,login_uv,newly_login_uv,pv,cast(anony_pv*100/pv as UNSIGNED),total_comments,comments,total_marks,marks,b.total_followers,followers
from boards_daily a left join boards_totality b on a.board_id=b.board_id where boards_date='%s'order by login_uv desc limit 15"""