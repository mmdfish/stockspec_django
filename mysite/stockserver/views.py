from django.http import JsonResponse

import sqlite3
import pandas as pd
from datetime import datetime
import json 
from . import common

def index(request):
    sql_cmd = "SELECT * FROM allstock limit 0,10"
    cx = sqlite3.connect(common.db_path)
    result = pd.read_sql(sql=sql_cmd, con=cx)
    df_json = result.to_json(orient = 'table', force_ascii = False)
    data = json.loads(df_json)

    return JsonResponse(data, safe=False)

def spec(request):
    specname = request.GET.get('specname', 'amplitude_10')
    order = request.GET.get('order', 'desc')
    isAbs = request.GET.get('abs', 'false')
    orderbyvalue = specname
    if isAbs == 'true':
        orderbyvalue = "abs(" + specname + ")"
    print(specname, order, isAbs, orderbyvalue)
    cx = sqlite3.connect(common.db_path)
    sql_cmd = "SELECT * FROM stock_spec where date=(select max(date) from stock_spec) and (code like 'sh.60%' or code like 'sh.688%' or code like 'sz.00%' or code like 'sz.300%') and " + specname + " != 'NaN' order by " + orderbyvalue + " " + order + " limit 0,100"
    
    result = pd.read_sql(sql=sql_cmd, con=cx)
    df_json = result.to_json(orient = 'table', force_ascii = False)
    data = json.loads(df_json)

    return JsonResponse(data, safe=False)

def dayk(request):
    codename = request.GET.get('code', 'sh.000001')
    order = 'desc'
    print(codename, order)
    cx = sqlite3.connect(common.db_path)
    sql_cmd = "SELECT * FROM stock_day_k where code='" + codename +"' order by date desc limit 0,500"
    
    result = pd.read_sql(sql=sql_cmd, con=cx)

    df_json = result.to_json(orient = 'table', force_ascii = False)
    data = json.loads(df_json)

    return JsonResponse(data, safe=False)

def hs300spec(request):
    specname = request.GET.get('specname', 'trendgap_y')
    order = request.GET.get('order', 'desc')
    isAbs = request.GET.get('abs', 'false')
    orderbyvalue = specname
    if isAbs == 'true':
        orderbyvalue = "abs(" + specname + ")"
    print(specname, order, isAbs, orderbyvalue)
    cx = sqlite3.connect(common.db_path)
    sql_cmd = "SELECT * FROM stock_hs300_spec where date=(select max(date) from stock_hs300_spec) order by " + orderbyvalue  + " " + order
    
    result = pd.read_sql(sql=sql_cmd, con=cx)
    df_json = result.to_json(orient = 'table', force_ascii = False)
    data = json.loads(df_json)

    return JsonResponse(data, safe=False)

def qualification(request):
    specname = request.GET.get('specname', 'macd_cross_above')
    value = request.GET.get('value', 1)
    pageIndex = request.GET.get('page', 0)
    print(specname, value, pageIndex)
    cx = sqlite3.connect(common.db_path)

    if specname == "rebound":
        pageIndex = int(pageIndex)
        totalNum = -1
        sql_cmd = "select DISTINCT date from stock_qualification order by date desc limit 0,5"
        cursor = cx.execute(sql_cmd)
        result = cursor.fetchall()
        date1 = result[0][0]
        date2 = result[4][0]
        if pageIndex == 0:
            sql_cmd = "SELECT count(*) FROM stock_qualification where (dayk_desc_3 = '2' or cross_up_ma10 = '1') and date='" + date1 +"' and code in (SELECT code FROM stock_qualification where \
                (ma5_10 = '1' and (date>='" + date2 + "' and date<='" + date1 + "')) and (code like 'sh.6%' or code like 'sz.00%' or code like 'sz.300%'))"
            cursor = cx.execute(sql_cmd)
            result = cursor.fetchone()
            totalNum = result[0]

        startIndex = pageIndex * 100
        step = 99
        sql_cmd = "SELECT * FROM stock_qualification where (dayk_desc_3 = '2' or cross_up_ma10 = '1') and date='" + date1 +"' and code in (SELECT code FROM stock_qualification where \
                (ma5_10 = '1' and (date>='" + date2 + "' and date<='" + date1 + "')) and (code like 'sh.6%' or code like 'sz.00%' or code like 'sz.300%')) limit " + str(startIndex) + "," + str(step)
        result = pd.read_sql(sql=sql_cmd, con=cx)
        df_json = result.to_json(orient = 'table', force_ascii = False)
        data = json.loads(df_json)

        data["total"] = totalNum

        return JsonResponse(data, safe=False)

    pageIndex = int(pageIndex)
    totalNum = -1
    if pageIndex == 0:
        sql_cmd = "SELECT count(*) FROM stock_qualification where date=(select max(date) from stock_qualification) and (code like 'sh.6%' or code like 'sz.00%' or code like 'sz.300%') and " + specname + " = " + str(value)
        cursor = cx.execute(sql_cmd)
        result = cursor.fetchone()
        totalNum = result[0]

    startIndex = pageIndex * 100
    step = 99
    sql_cmd = "SELECT * FROM stock_qualification where date=(select max(date) from stock_qualification) and (code like 'sh.6%' or code like 'sz.00%' or code like 'sz.300%') and " + specname + " = " + str(value) + " limit " + str(startIndex) + "," + str(step)
    result = pd.read_sql(sql=sql_cmd, con=cx)
    df_json = result.to_json(orient = 'table', force_ascii = False)
    data = json.loads(df_json)

    data["total"] = totalNum

    return JsonResponse(data, safe=False)
