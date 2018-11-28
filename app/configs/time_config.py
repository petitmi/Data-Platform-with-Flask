import datetime


# yesterday_sql = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
def get_monday(thatdate):
    thatdate=datetime.datetime.strptime(thatdate,'%Y-%m-%d')
    thatdate_weekday=thatdate.weekday()
    thatdate_monday=(thatdate-datetime.timedelta(thatdate_weekday)).strftime('%Y-%m-%d')
    return  thatdate_monday
def get_month_1st(thatdate):
    thatdate=datetime.datetime.strptime(thatdate,'%Y-%m-%d')
    get_month_1st = datetime.date(year=thatdate.year, month=thatdate.month, day=1).strftime('%Y-%m-%d')
    return  get_month_1st
class get_days_list:
    def __init__(self,thatdate,days):
        self.thedate=datetime.datetime.strptime(thatdate,'%Y-%m-%d')
        self.days=days
    def sql_list(self):
        sql_list_days = []
        for i in range(-1,self.days):
            daybefore = self.thedate - datetime.timedelta(i)
            sql_list_days.append(daybefore.strftime('%Y-%m-%d'))
        sql_list_days.reverse()
        return sql_list_days
    def es_list(self):
        es_list_days = []
        for i in range(-1,self.days):
            daybefore = self.thedate - datetime.timedelta(i)
            es_list_days.append(daybefore.strftime('%Y-%m-%dT00:00:00+0800'))
        es_list_days.reverse()
        return es_list_days






