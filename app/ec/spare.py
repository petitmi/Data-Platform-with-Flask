
def olp_month_offline():

    result_ec = pd.read_sql_query(sql_ec_month_compared, db.engine).fillna(0)
    result_ec_offline_2018 = pd.read_sql(sql_ec_month_offline_2018,  db.engine).fillna(0)
    result_ec_offline_2017= pd.read_sql(sql_ec_month_offline_2017,  db.engine).fillna(0)
    x = np.zeros((5, 2))
    result_ec_offline_2018 = result_ec_offline_2018.append(
        pd.DataFrame(x, index=range(len(result_ec_offline_2018), 12), columns=['sales_count', 'sales_amount']),sort=True)  # 补充6：11行数
    attr=result_ec['月份'].values.tolist()
    #小鹅通
    # result_ec.loc[7,['2018月流水']]=result_ec[(result_ec['月份']==8)]['2018月流水'].values[0]+227384
    # m1 = result_ec['2016月销量'].values.tolist()
    m2 =(result_ec['2017月销量']+result_ec_offline_2017['sales_count']).values.tolist()
    m3=(result_ec['2018月销量']+result_ec_offline_2018['sales_count']).values.tolist()
    # m4 = result_ec['2016月流水'].values.tolist()
    m5 = (result_ec['2017月流水']+result_ec_offline_2017['sales_amount']).values.tolist()
    m6 = (result_ec['2018月流水']+result_ec_offline_2018['sales_amount']).values.tolist()

    x=[(str(x)+'月') for x in attr]

    bar = Bar()
    # bar.add("2016月销量", x, m1)
    bar.add("2017月销量", x, m2)
    bar.add("2018月销量", x, m3)
    line = Line()
    # line.add("2016月流水", x, m4, yaxis_formatter=" ￥")
    line.add("2017月流水", x, m5, yaxis_formatter=" ￥")
    line.add("2018月流水", x, m6, yaxis_formatter=" ￥")

    overlap = Overlap(width=1100, height=400)
    overlap.add(bar)
    overlap.add(line, yaxis_index=1, is_add_yaxis=True)
    return overlap
