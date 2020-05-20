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