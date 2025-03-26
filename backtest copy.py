import requests
from datetime import date, timedelta
import asyncio
import json
import ssl
import websockets
import requests
import threading
from google.protobuf.json_format import MessageToDict
import MarketDataFeedV3_pb2 as pb
import requests
from datetime import date, timedelta
from multiprocessing import Process, Manager, Lock
import time
import random
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import pymysql
import os
import bcrypt

app = Flask(__name__)


access_token = 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2VUJUN1ciLCJqdGkiOiI2N2UyNDcxMTFkODkwODFmZWUxNTYyYTUiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaWF0IjoxNzQyODgyNTc3LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDI5NDAwMDB9.Tv574IOVdvVbbvGkMjx7aFjJvmpVY2g_i4D0-8fNT74'

twentystocks = []
toremove_from_allstocks = []
toremove_from_ten = []
toremove_from_fifteen = []
previous_close_data = {}


dict2 = {'NSE_EQ|INE585B01010': 'MARUTI', 'NSE_EQ|INE139A01034': 'NATIONALUM', 'NSE_EQ|INE947Q01028': 'LAURUSLABS', 'NSE_EQ|INE918I01026': 'BAJAJFINSV', 'NSE_EQ|INE758E01017': 'JIOFIN', 'NSE_EQ|INE522D01027': 'MANAPPURAM', 'NSE_EQ|INE089A01031': 'DRREDDY', 'NSE_EQ|INE00R701025': 'DALBHARAT', 'NSE_EQ|INE848E01016': 'NHPC', 'NSE_EQ|INE917I01010': 'BAJAJ-AUTO', 'NSE_EQ|INE070A01015': 'SHREECEM', 'NSE_EQ|INE982J01020': 'PAYTM', 'NSE_EQ|INE761H01022': 'PAGEIND', 'NSE_EQ|INE749A01030': 'JINDALSTEL', 'NSE_EQ|INE591G01017': 'COFORGE', 'NSE_EQ|INE494B01023': 'TVSMOTOR', 'NSE_EQ|INE160A01022': 'PNB', 'NSE_EQ|INE736A01011': 'CDSL', 'NSE_EQ|INE646L01027': 'INDIGO', 'NSE_EQ|INE010B01027': 'ZYDUSLIFE', 'NSE_EQ|INE102D01028': 'GODREJ', 'NSE_EQ|INE302A01020': 'EXIDEIND', 'NSE_EQ|INE134E01011': 'PFC', 'NSE_EQ|INE009A01021': 'INFY', 'NSE_EQ|INE376G01013': 'BIOCON', 'NSE_EQ|INE619A01035': 'PATANJALI', 'NSE_EQ|INE465A01025': 'BHARATFORG', 'NSE_EQ|INE463A01038': 'BERGEPAINT', 'NSE_EQ|INE397D01024': 'BHARTIARTL', 'NSE_EQ|INE192R01011': 'DMART', 'NSE_EQ|INE540L01014': 'ALKEM', 'NSE_EQ|INE775A01035': 'MOTHERSON', 'NSE_EQ|INE237A01028': 'KOTAKBANK', 'NSE_EQ|INE059A01026': 'CIPLA', 'NSE_EQ|INE732I01013': 'ANGELONE', 'NSE_EQ|INE361B01024': 'DIVISLAB', 'NSE_EQ|INE797F01020': 'JUBLFOOD', 'NSE_EQ|INE811K01011': 'PRESTIGE', 'NSE_EQ|INE180A01020': 'MFSL', 'NSE_EQ|INE949L01017': 'AUBANK', 'NSE_EQ|INE881D01027': 'OFSS', 'NSE_EQ|INE030A01027': 'HINDUNILVR', 'NSE_EQ|INE795G01014': 'HDFCLIFE', 'NSE_EQ|INE476A01022': 'CANBK', 'NSE_EQ|INE745G01035': 'MCX', 'NSE_EQ|INE531E01026': 'HINDCOPPER', 'NSE_EQ|INE823G01014': 'JKCEMENT', 'NSE_EQ|INE721A01047': 'SHRIRAMFIN', 'NSE_EQ|INE028A01039': 'BANKBARODA', 'NSE_EQ|INE670K01029': 'LODHA', 'NSE_EQ|INE280A01028': 'TITAN', 'NSE_EQ|INE158A01026': 'HEROMOTOCO', 'NSE_EQ|INE123W01016': 'SBILIFE', 'NSE_EQ|INE298A01020': 'CUMMINSIND', 'NSE_EQ|INE192A01025': 'TATACONSUM', 'NSE_EQ|INE769A01020': 'AARTIIND', 'NSE_EQ|INE398R01022': 'SYNGENE', 'NSE_EQ|INE155A01022': 'TATAMOTORS', 'NSE_EQ|INE674K01013': 'ABCAPITAL', 'NSE_EQ|INE094A01015': 'HINDPETRO', 'NSE_EQ|INE274J01014': 'OIL', 'NSE_EQ|INE528G01035': 'YESBANK', 'NSE_EQ|INE093I01010': 'OBEROIRLTY', 'NSE_EQ|INE726G01019': 'ICICIPRULI', 'NSE_EQ|INE012A01025': 'ACC', 'NSE_EQ|INE073K01018': 'SONACOMS', 'NSE_EQ|INE095A01012': 'INDUSINDBK', 'NSE_EQ|INE006I01046': 'ASTRAL', 'NSE_EQ|INE562A01011': 'INDIANB', 'NSE_EQ|INE195A01028': 'SUPREMEIND', 'NSE_EQ|INE142M01025': 'TATATECH', 'NSE_EQ|INE849A01020': 'TRENT', 'NSE_EQ|INE669C01036': 'TECHM', 'NSE_EQ|INE136B01020': 'CYIENT', 'NSE_EQ|INE216A01030': 'BRITANNIA', 'NSE_EQ|INE002S01010': 'MGL', 'NSE_EQ|INE111A01025': 'CONCOR', 'NSE_EQ|INE062A01020': 'SBIN', 'NSE_EQ|INE118H01025': 'BSE', 'NSE_EQ|INE364U01010': 'ADANIGREEN', 'NSE_EQ|INE238A01034': 'AXISBANK', 'NSE_EQ|INE081A01020': 'TATASTEEL', 'NSE_EQ|INE044A01036': 'SUNPHARMA', 'NSE_EQ|INE883A01011': 'MRF', 'NSE_EQ|INE075A01022': 'WIPRO', 'NSE_EQ|INE498L01015': 'LTF', 'NSE_EQ|INE935N01020': 'DIXON', 'NSE_EQ|INE002L01015': 'SJVN', 'NSE_EQ|INE038A01020': 'HINDALCO', 'NSE_EQ|INE484J01027': 'GODREJPROP', 'NSE_EQ|INE031A01017': 'HUDCO', 'NSE_EQ|INE242A01010': 'IOC', 'NSE_EQ|INE205A01025': 'VEDL', 'NSE_EQ|INE027H01010': 'MAXHEALTH', 'NSE_EQ|INE692A01016': 'UNIONBANK', 'NSE_EQ|INE04I401011': 'KPITTECH', 'NSE_EQ|INE101D01020': 'GRANULES', 'NSE_EQ|INE010V01017': 'LTTS', 'NSE_EQ|INE263A01024': 'BEL', 'NSE_EQ|INE020B01018': 'RECLTD', 'NSE_EQ|INE685A01028': 'TORNTPHARM', 'NSE_EQ|INE647A01010': 'SRF', 'NSE_EQ|INE121A08PJ0': 'CHOLAFIN', 'NSE_EQ|INE860A01027': 'HCLTECH', 'NSE_EQ|INE974X01010': 'TIINDIA', 'NSE_EQ|INE854D01024': 'UNITDSPR', 'NSE_EQ|INE220G01021': 'JSL', 'NSE_EQ|INE742F01042': 'ADANIPORTS', 'NSE_EQ|INE226A01021': 'VOLTAS', 'NSE_EQ|INE171A01029': 'FEDERALBNK', 'NSE_EQ|INE976G01028': 'RBLBANK', 'NSE_EQ|INE047A01021': 'GRASIM', 'NSE_EQ|INE326A01037': 'LUPIN', 'NSE_EQ|INE262H01021': 'PERSISTENT', 'NSE_EQ|INE584A01023': 'NMDC', 'NSE_EQ|INE084A01016': 'BANKINDIA', 'NSE_EQ|INE085A01013': 'CHAMBLFERT', 'NSE_EQ|INE878B01027': 'KEI', 'NSE_EQ|INE836A01035': 'BSOFT', 'NSE_EQ|INE548A01028': 'HFCL', 'NSE_EQ|INE414G01012': 'MUTHOOTFIN', 'NSE_EQ|INE018E01016': 'SBICARD', 'NSE_EQ|INE669E01016': 'IDEA', 'NSE_EQ|INE776C01039': 'GMRAIRPORT', 'NSE_EQ|INE211B01039': 'PHOENIXLTD', 'NSE_EQ|INE417T01026': 'POLICYBZR', 'NSE_EQ|INE813H01021': 'TORNTPOWER', 'NSE_EQ|INE868B01028': 'NCC', 'NSE_EQ|INE213A01029': 'ONGC', 'NSE_EQ|INE335Y01020': 'IRCTC', 'NSE_EQ|INE931S01010': 'ADANIENSOL', 'NSE_EQ|INE821I01022': 'IRB', 'NSE_EQ|INE053F01010': 'IRFC', 'NSE_EQ|INE323A01026': 'BOSCHLTD', 'NSE_EQ|INE127D01025': 'HDFCAMC', 'NSE_EQ|INE021A01026': 'ASIANPAINT', 'NSE_EQ|INE356A01018': 'MPHASIS', 'NSE_EQ|INE733E01010': 'NTPC', 'NSE_EQ|INE214T01019': 'LTIM', 'NSE_EQ|INE176B01034': 'HAVELLS', 'NSE_EQ|INE022Q01020': 'IEX', 'NSE_EQ|INE545U01014': 'BANDHANBNK', 'NSE_EQ|INE511C01022': 'POONAWALLA', 'NSE_EQ|INE115A01026': 'LICHSGFIN', 'NSE_EQ|INE596I01012': 'CAMS', 'NSE_EQ|INE702C01027': 'APLAPOLLO', 'NSE_EQ|INE343H01029': 'SOLARINDS', 'NSE_EQ|INE388Y01029': 'NYKAA', 'NSE_EQ|INE117A01022': 'ABB', 'NSE_EQ|INE530B01024': 'IIFL', 'NSE_EQ|INE239A01024': 'NESTLEIND', 'NSE_EQ|INE758T01015': 'ZOMATO', 'NSE_EQ|INE154A01025': 'ITC', 'NSE_EQ|INE455K01017': 'POLYCAB', 'NSE_EQ|INE406A01037': 'AUROPHARMA', 'NSE_EQ|INE101A01026': 'M&M', 'NSE_EQ|INE437A01024': 'APOLLOHOSP', 'NSE_EQ|INE208A01029': 'ASHOKLEY', 'NSE_EQ|INE303R01014': 'KALYANKJIL', 'NSE_EQ|INE245A01021': 'TATAPOWER', 'NSE_EQ|INE288B01029': 'DEEPAKNTR', 'NSE_EQ|INE148O01028': 'DELHIVERY', 'NSE_EQ|INE331A01037': 'RAMCOCEM', 'NSE_EQ|INE053A01029': 'INDHOTEL', 'NSE_EQ|INE090A01021': 'ICICIBANK', 'NSE_EQ|INE628A01036': 'UPL', 'NSE_EQ|INE196A01026': 'MARICO', 'NSE_EQ|INE787D01026': 'BALKRISIND', 'NSE_EQ|INE018A01030': 'LT', 'NSE_EQ|INE121J01017': 'INDUSTOWER', 'NSE_EQ|INE140A01024': 'PEL', 'NSE_EQ|INE399L01023': 'ATGL', 'NSE_EQ|INE092T01019': 'IDFCFIRSTB', 'NSE_EQ|INE347G01014': 'PETRONET', 'NSE_EQ|INE067A01029': 'CGPOWER', 'NSE_EQ|INE438A01022': 'APOLLOTYRE', 'NSE_EQ|INE615H01020': 'TITAGARH', 'NSE_EQ|INE423A01024': 'ADANIENT', 'NSE_EQ|INE121E01018': 'JSWENERGY', 'NSE_EQ|INE019A01038': 'JSWSTEEL', 'NSE_EQ|INE151A01013': 'TATACOMM', 'NSE_EQ|INE259A01022': 'COLPAL', 'NSE_EQ|INE522F01014': 'COALINDIA', 'NSE_EQ|INE095N01031': 'NBCC', 'NSE_EQ|INE296A01024': 'BAJFINANCE', 'NSE_EQ|INE765G01017': 'ICICIGI', 'NSE_EQ|INE066F01020': 'HAL', 'NSE_EQ|INE257A01026': 'BHEL', 'NSE_EQ|INE002A01018': 'RELIANCE', 'NSE_EQ|INE203G01027': 'IGL', 'NSE_EQ|INE467B01029': 'TCS', 'NSE_EQ|INE774D08MG3': 'M&MFIN', 'NSE_EQ|INE647O01011': 'ABFRL', 'NSE_EQ|INE079A01024': 'AMBUJACEM', 'NSE_EQ|INE129A01019': 'GAIL', 'NSE_EQ|INE0J1Y01017': 'LICI', 'NSE_EQ|INE481G01011': 'ULTRACEMCO', 'NSE_EQ|INE299U01018': 'CROMPTON', 'NSE_EQ|INE040A01034': 'HDFCBANK', 'NSE_EQ|INE114A01011': 'SAIL', 'NSE_EQ|INE486A01021': 'CESC', 'NSE_EQ|INE935A01035': 'GLENMARK', 'NSE_EQ|INE603J01030': 'PIIND', 'NSE_EQ|INE003A01024': 'SIEMENS', 'NSE_EQ|INE202E01016': 'IREDA', 'NSE_EQ|INE066A01021': 'EICHERMOT', 'NSE_EQ|INE029A01011': 'BPCL', 'NSE_EQ|INE670A01012': 'TATAELXSI', 'NSE_EQ|INE663F01024': 'NAUKRI', 'NSE_EQ|INE752E01010': 'POWERGRID', 'NSE_EQ|INE092A01019': 'TATACHEM', 'NSE_EQ|INE271C01023': 'DLF', 'NSE_EQ|INE318A01026': 'PIDILITIND', 'NSE_EQ|INE200M01039': 'VBL', 'NSE_EQ|INE016A01026': 'DABUR', 'NSE_EQ|INE042A01014': 'ESCORTS'}

allstock = ['MARUTI', 'NATIONALUM', 'LAURUSLABS', 'BAJAJFINSV', 'JIOFIN', 'MANAPPURAM', 'DRREDDY', 'DALBHARAT', 'NHPC', 'BAJAJ-AUTO', 'SHREECEM', 'PAYTM', 'PAGEIND', 'JINDALSTEL', 'COFORGE', 'TVSMOTOR', 'PNB', 'CDSL', 'INDIGO', 'ZYDUSLIFE', 'GODREJ', 'EXIDEIND', 'PFC', 'INFY', 'BIOCON', 'PATANJALI', 'BHARATFORG', 'BERGEPAINT', 'BHARTIARTL', 'DMART', 'ALKEM', 'MOTHERSON', 'KOTAKBANK', 'CIPLA', 'ANGELONE', 'DIVISLAB', 'JUBLFOOD', 'PRESTIGE', 'MFSL', 'AUBANK', 'OFSS', 'HINDUNILVR', 'HDFCLIFE', 'CANBK', 'MCX', 'HINDCOPPER', 'JKCEMENT', 'SHRIRAMFIN', 'BANKBARODA', 'LODHA', 'TITAN', 'HEROMOTOCO', 'SBILIFE', 'CUMMINSIND', 'TATACONSUM', 'AARTIIND', 'SYNGENE', 'TATAMOTORS', 'ABCAPITAL', 'HINDPETRO', 'OIL', 'YESBANK', 'OBEROIRLTY', 'ICICIPRULI', 'ACC', 'SONACOMS', 'INDUSINDBK', 'ASTRAL', 'INDIANB', 'SUPREMEIND', 'TATATECH', 'TRENT', 'TECHM', 'CYIENT', 'BRITANNIA', 'MGL', 'CONCOR', 'SBIN', 'BSE', 'ADANIGREEN', 'AXISBANK', 'TATASTEEL', 'SUNPHARMA', 'MRF', 'WIPRO', 'LTF', 'DIXON', 'SJVN', 'HINDALCO', 'GODREJPROP', 'HUDCO', 'IOC', 'VEDL', 'MAXHEALTH', 'UNIONBANK', 'KPITTECH', 'GRANULES', 'LTTS', 'BEL', 'RECLTD', 'TORNTPHARM', 'SRF', 'CHOLAFIN', 'HCLTECH', 'TIINDIA', 'UNITDSPR', 'JSL', 'ADANIPORTS', 'VOLTAS', 'FEDERALBNK', 'RBLBANK', 'GRASIM', 'LUPIN', 'PERSISTENT', 'NMDC', 'BANKINDIA', 'CHAMBLFERT', 'KEI', 'BSOFT', 'HFCL', 'MUTHOOTFIN', 'SBICARD', 'IDEA', 'GMRAIRPORT', 'PHOENIXLTD', 'POLICYBZR', 'TORNTPOWER', 'NCC', 'ONGC', 'IRCTC', 'ADANIENSOL', 'IRB', 'IRFC', 'BOSCHLTD', 'HDFCAMC', 'ASIANPAINT', 'MPHASIS', 'NTPC', 'LTIM', 'HAVELLS', 'IEX', 'BANDHANBNK', 'POONAWALLA', 'LICHSGFIN', 'CAMS', 'APLAPOLLO', 'SOLARINDS', 'NYKAA', 'ABB', 'IIFL', 'NESTLEIND', 'ZOMATO', 'ITC', 'POLYCAB', 'AUROPHARMA', 'M&M', 'APOLLOHOSP', 'ASHOKLEY', 'KALYANKJIL', 'TATAPOWER', 'DEEPAKNTR', 'DELHIVERY', 'RAMCOCEM', 'INDHOTEL', 'ICICIBANK', 'UPL', 'MARICO', 'BALKRISIND', 'LT', 'INDUSTOWER', 'PEL', 'ATGL', 'IDFCFIRSTB', 'PETRONET', 'CGPOWER', 'APOLLOTYRE', 'TITAGARH', 'ADANIENT', 'JSWENERGY', 'JSWSTEEL', 'TATACOMM', 'COLPAL', 'COALINDIA', 'NBCC', 'BAJFINANCE', 'ICICIGI', 'HAL', 'BHEL', 'RELIANCE', 'IGL', 'TCS', 'M&MFIN', 'ABFRL', 'AMBUJACEM', 'GAIL', 'LICI', 'ULTRACEMCO', 'CROMPTON', 'HDFCBANK', 'SAIL', 'CESC', 'GLENMARK', 'PIIND', 'SIEMENS', 'IREDA', 'EICHERMOT', 'BPCL', 'TATAELXSI', 'NAUKRI', 'POWERGRID', 'TATACHEM', 'DLF', 'PIDILITIND', 'VBL', 'DABUR', 'ESCORTS']

allstockkeys =['NSE_EQ|INE585B01010','NSE_EQ|INE139A01034','NSE_EQ|INE947Q01028', 'NSE_EQ|INE918I01026', 'NSE_EQ|INE758E01017', 'NSE_EQ|INE522D01027', 'NSE_EQ|INE089A01031', 'NSE_EQ|INE00R701025', 'NSE_EQ|INE848E01016', 'NSE_EQ|INE917I01010', 'NSE_EQ|INE070A01015', 'NSE_EQ|INE982J01020', 'NSE_EQ|INE761H01022', 'NSE_EQ|INE749A01030', 'NSE_EQ|INE591G01017', 'NSE_EQ|INE494B01023', 'NSE_EQ|INE160A01022', 'NSE_EQ|INE736A01011', 'NSE_EQ|INE646L01027', 'NSE_EQ|INE010B01027', 'NSE_EQ|INE102D01028', 'NSE_EQ|INE302A01020', 'NSE_EQ|INE134E01011', 'NSE_EQ|INE009A01021', 'NSE_EQ|INE376G01013', 'NSE_EQ|INE619A01035', 'NSE_EQ|INE465A01025', 'NSE_EQ|INE463A01038', 'NSE_EQ|INE397D01024', 'NSE_EQ|INE192R01011', 'NSE_EQ|INE540L01014', 'NSE_EQ|INE775A01035', 'NSE_EQ|INE237A01028', 'NSE_EQ|INE059A01026', 'NSE_EQ|INE732I01013', 'NSE_EQ|INE361B01024', 'NSE_EQ|INE797F01020', 'NSE_EQ|INE811K01011', 'NSE_EQ|INE180A01020', 'NSE_EQ|INE949L01017', 'NSE_EQ|INE881D01027', 'NSE_EQ|INE030A01027', 'NSE_EQ|INE795G01014', 'NSE_EQ|INE476A01022', 'NSE_EQ|INE745G01035', 'NSE_EQ|INE531E01026', 'NSE_EQ|INE823G01014', 'NSE_EQ|INE721A01047', 'NSE_EQ|INE028A01039', 'NSE_EQ|INE670K01029', 'NSE_EQ|INE280A01028', 'NSE_EQ|INE158A01026', 'NSE_EQ|INE123W01016', 'NSE_EQ|INE298A01020', 'NSE_EQ|INE192A01025', 'NSE_EQ|INE769A01020', 'NSE_EQ|INE398R01022', 'NSE_EQ|INE155A01022', 'NSE_EQ|INE674K01013', 'NSE_EQ|INE094A01015', 'NSE_EQ|INE274J01014', 'NSE_EQ|INE528G01035', 'NSE_EQ|INE093I01010', 'NSE_EQ|INE726G01019', 'NSE_EQ|INE012A01025', 'NSE_EQ|INE073K01018', 'NSE_EQ|INE095A01012', 'NSE_EQ|INE006I01046', 'NSE_EQ|INE562A01011', 'NSE_EQ|INE195A01028', 'NSE_EQ|INE142M01025', 'NSE_EQ|INE849A01020', 'NSE_EQ|INE669C01036', 'NSE_EQ|INE136B01020', 'NSE_EQ|INE216A01030', 'NSE_EQ|INE002S01010', 'NSE_EQ|INE111A01025', 'NSE_EQ|INE062A01020', 'NSE_EQ|INE118H01025', 'NSE_EQ|INE364U01010', 'NSE_EQ|INE238A01034', 'NSE_EQ|INE081A01020', 'NSE_EQ|INE044A01036', 'NSE_EQ|INE883A01011', 'NSE_EQ|INE075A01022', 'NSE_EQ|INE498L01015', 'NSE_EQ|INE935N01020', 'NSE_EQ|INE002L01015', 'NSE_EQ|INE038A01020', 'NSE_EQ|INE484J01027', 'NSE_EQ|INE031A01017', 'NSE_EQ|INE242A01010', 'NSE_EQ|INE205A01025', 'NSE_EQ|INE027H01010', 'NSE_EQ|INE692A01016', 'NSE_EQ|INE04I401011', 'NSE_EQ|INE101D01020', 'NSE_EQ|INE010V01017', 'NSE_EQ|INE263A01024', 'NSE_EQ|INE020B01018', 'NSE_EQ|INE685A01028', 'NSE_EQ|INE647A01010', 'NSE_EQ|INE121A08PJ0', 'NSE_EQ|INE860A01027', 'NSE_EQ|INE974X01010', 'NSE_EQ|INE854D01024', 'NSE_EQ|INE220G01021', 'NSE_EQ|INE742F01042', 'NSE_EQ|INE226A01021', 'NSE_EQ|INE171A01029', 'NSE_EQ|INE976G01028', 'NSE_EQ|INE047A01021', 'NSE_EQ|INE326A01037', 'NSE_EQ|INE262H01021', 'NSE_EQ|INE584A01023', 'NSE_EQ|INE084A01016', 'NSE_EQ|INE085A01013', 'NSE_EQ|INE878B01027', 'NSE_EQ|INE836A01035', 'NSE_EQ|INE548A01028', 'NSE_EQ|INE414G01012', 'NSE_EQ|INE018E01016', 'NSE_EQ|INE669E01016', 'NSE_EQ|INE776C01039', 'NSE_EQ|INE211B01039', 'NSE_EQ|INE417T01026', 'NSE_EQ|INE813H01021', 'NSE_EQ|INE868B01028', 'NSE_EQ|INE213A01029', 'NSE_EQ|INE335Y01020', 'NSE_EQ|INE931S01010', 'NSE_EQ|INE821I01022', 'NSE_EQ|INE053F01010', 'NSE_EQ|INE323A01026', 'NSE_EQ|INE127D01025', 'NSE_EQ|INE021A01026', 'NSE_EQ|INE356A01018', 'NSE_EQ|INE733E01010', 'NSE_EQ|INE214T01019', 'NSE_EQ|INE176B01034', 'NSE_EQ|INE022Q01020', 'NSE_EQ|INE545U01014', 'NSE_EQ|INE511C01022', 'NSE_EQ|INE115A01026', 'NSE_EQ|INE596I01012', 'NSE_EQ|INE702C01027', 'NSE_EQ|INE343H01029', 'NSE_EQ|INE388Y01029', 'NSE_EQ|INE117A01022', 'NSE_EQ|INE530B01024', 'NSE_EQ|INE239A01024', 'NSE_EQ|INE758T01015', 'NSE_EQ|INE154A01025', 'NSE_EQ|INE455K01017', 'NSE_EQ|INE406A01037', 'NSE_EQ|INE101A01026', 'NSE_EQ|INE437A01024', 'NSE_EQ|INE208A01029', 'NSE_EQ|INE303R01014', 'NSE_EQ|INE245A01021', 'NSE_EQ|INE288B01029', 'NSE_EQ|INE148O01028', 'NSE_EQ|INE331A01037', 'NSE_EQ|INE053A01029', 'NSE_EQ|INE090A01021', 'NSE_EQ|INE628A01036', 'NSE_EQ|INE196A01026', 'NSE_EQ|INE787D01026', 'NSE_EQ|INE018A01030', 'NSE_EQ|INE121J01017', 'NSE_EQ|INE140A01024', 'NSE_EQ|INE399L01023', 'NSE_EQ|INE092T01019', 'NSE_EQ|INE347G01014', 'NSE_EQ|INE067A01029', 'NSE_EQ|INE438A01022', 'NSE_EQ|INE615H01020', 'NSE_EQ|INE423A01024', 'NSE_EQ|INE121E01018', 'NSE_EQ|INE019A01038', 'NSE_EQ|INE151A01013', 'NSE_EQ|INE259A01022', 'NSE_EQ|INE522F01014', 'NSE_EQ|INE095N01031', 'NSE_EQ|INE296A01024', 'NSE_EQ|INE765G01017', 'NSE_EQ|INE066F01020', 'NSE_EQ|INE257A01026', 'NSE_EQ|INE002A01018', 'NSE_EQ|INE203G01027', 'NSE_EQ|INE467B01029', 'NSE_EQ|INE774D08MG3', 'NSE_EQ|INE647O01011', 'NSE_EQ|INE079A01024', 'NSE_EQ|INE129A01019', 'NSE_EQ|INE0J1Y01017', 'NSE_EQ|INE481G01011', 'NSE_EQ|INE299U01018', 'NSE_EQ|INE040A01034', 'NSE_EQ|INE114A01011', 'NSE_EQ|INE486A01021', 'NSE_EQ|INE935A01035', 'NSE_EQ|INE603J01030', 'NSE_EQ|INE003A01024', 'NSE_EQ|INE202E01016', 'NSE_EQ|INE066A01021', 'NSE_EQ|INE029A01011', 'NSE_EQ|INE670A01012', 'NSE_EQ|INE663F01024', 'NSE_EQ|INE752E01010', 'NSE_EQ|INE092A01019', 'NSE_EQ|INE271C01023', 'NSE_EQ|INE318A01026', 'NSE_EQ|INE200M01039', 'NSE_EQ|INE016A01026',
                                   'NSE_EQ|INE042A01014']


stop_flag = False
stop_flag1 = False
stop_flag2= False


def previous():
    global previous_close_data
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    for key in dict2:
    # Get Yesterday's Date

    # API URL (From Date & To Date are both yesterday to get single day's data)
        url = f"https://api.upstox.com/v2/historical-candle/{key}/day/{yesterday}/{yesterday}"

        # Set headers
        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        # Make the API request
        response = requests.get(url, headers=headers)
        data = response.json()
        if "data" in data and "candles" in data["data"] and len(data["data"]["candles"]) > 0:
            previous_close = data["data"]["candles"][0][4]
            ten = previous_close * 1.02
            fifteen = previous_close * 1.06
            twenty = previous_close * 1.08
            tensell = previous_close * .9
            fifteensell = previous_close * .85
            twentysell = previous_close * .8          
            previous_close_data[key]= {'pre_day':previous_close, 'ten':ten, 'fifteen':fifteen, 'twenty':twenty, 'tensell':tensell, 'fifteensell':fifteensell, 'twentysell':twentysell}
    print('previou',previous_close_data)        
        
        
real_time_data = {
    "ABB": {}, "ACC": {}, "APLAPOLLO": {}, "AUBANK": {}, "AARTIIND": {}, "ADANIENSOL": {}, "ADANIENT": {},
    "ADANIGREEN": {}, "ADANIPORTS": {}, "ATGL": {}, "ABCAPITAL": {}, "ABFRL": {}, "ALKEM": {}, "AMBUJACEM": {},
    "ANGELONE": {}, "APOLLOHOSP": {}, "APOLLOTYRE": {}, "ASHOKLEY": {}, "ASIANPAINT": {}, "ASTRAL": {},
    "AUROPHARMA": {}, "DMART": {}, "AXISBANK": {}, "BSOFT": {}, "BSE": {}, "BAJAJ-AUTO": {}, "BAJFINANCE": {},
    "BAJAJFINSV": {}, "BALKRISIND": {}, "BANDHANBNK": {}, "BANKBARODA": {}, "BANKINDIA": {},
    "BERGEPAINT": {}, "BEL": {}, "BHARATFORG": {}, "BHEL": {}, "BPCL": {}, "BHARTIARTL": {}, "BIOCON": {},
    "BOSCHLTD": {}, "BRITANNIA": {}, "CESC": {}, "CGPOWER": {}, "CANBK": {}, "CDSL": {}, "CHAMBLFERT": {},
    "CHOLAFIN": {}, "CIPLA": {}, "COALINDIA": {}, "COFORGE": {}, "COLPAL": {}, "CAMS": {}, "CONCOR": {},
    "CROMPTON": {}, "CUMMINSIND": {}, "CYIENT": {}, "DLF": {}, "DABUR": {}, "DALBHARAT": {}, "DEEPAKNTR": {},
    "DELHIVERY": {}, "DIVISLAB": {}, "DIXON": {}, "DRREDDY": {}, "EICHERMOT": {}, "ESCORTS": {},
    "EXIDEIND": {}, "NYKAA": {}, "GAIL": {}, "GMRAIRPORT": {}, "GLENMARK": {}, "GODREJ": {},
    "GODREJPROP": {}, "GRANULES": {}, "GRASIM": {}, "HCLTECH": {}, "HDFCAMC": {}, "HDFCBANK": {},
    "HDFCLIFE": {}, "HFCL": {}, "HAVELLS": {}, "HEROMOTOCO": {}, "HINDALCO": {}, "HAL": {}, "HINDCOPPER": {},
    "HINDPETRO": {}, "HINDUNILVR": {}, "HUDCO": {}, "ICICIBANK": {}, "ICICIGI": {}, "ICICIPRULI": {},
    "IDFCFIRSTB": {}, "IIFL": {}, "IRB": {}, "ITC": {}, "INDIANB": {}, "IEX": {}, "IOC": {}, "IRCTC": {}, "IRFC": {},
    "IREDA": {}, "IGL": {}, "INDUSTOWER": {}, "INDUSINDBK": {}, "NAUKRI": {}, "INFY": {}, "INDIGO": {},
    "JKCEMENT": {}, "JSWENERGY": {}, "JSWSTEEL": {}, "JSL": {}, "JINDALSTEL": {}, "JIOFIN": {},
    "JUBLFOOD": {}, "KEI": {}, "KPITTECH": {}, "KALYANKJIL": {}, "KOTAKBANK": {}, "LTF": {}, "LTTS": {},
    "LICHSGFIN": {}, "LTIM": {}, "LT": {}, "LAURUSLABS": {}, "LICI": {}, "LUPIN": {}, "MRF": {}, "LODHA": {},
    "MGL": {}, "M&MFIN": {}, "M&M": {}, "MANAPPURAM": {}, "MARICO": {}, "MARUTI": {}, "MFSL": {}, "MAXHEALTH": {},
    "MPHASIS": {}, "MCX": {}, "MUTHOOTFIN": {}, "NBCC": {}, "NCC": {}, "NHPC": {}, "NMDC": {}, "NTPC": {},
    "NATIONALUM": {}, "NESTLEIND": {}, "OBEROIRLTY": {}, "ONGC": {}, "OIL": {}, "PAYTM": {}, "OFSS": {},
    "POLICYBZR": {}, "PIIND": {}, "PAGEIND": {}, "PATANJALI": {}, "PERSISTENT": {}, "PETRONET": {},
    "PIDILITIND": {}, "PEL": {}, "POLYCAB": {}, "POONAWALLA": {}, "PFC": {}, "POWERGRID": {}, "PRESTIGE": {},
    "PNB": {}, "RBLBANK": {}, "RECLTD": {}, "RELIANCE": {}, "SBICARD": {}, "SBILIFE": {}, "SHREECEM": {},
    "SJVN": {}, "SRF": {}, "MOTHERSON": {}, "SHRIRAMFIN": {}, "SIEMENS": {}, "SOLARINDS": {}, "SONACOMS": {},
    "SBIN": {}, "SAIL": {}, "SUNPHARMA": {}, "SUPREMEIND": {}, "SYNGENE": {}, "TATACONSUM": {},
    "TITAGARH": {}, "TVSMOTOR": {}, "TATACHEM": {}, "TATACOMM": {}, "TCS": {}, "TATAELXSI": {},
    "TATAMOTORS": {}, "TATAPOWER": {}, "TATASTEEL": {}, "TATATECH": {}, "TECHM": {}, "FEDERALBNK": {},
    "INDHOTEL": {}, "PHOENIXLTD": {}, "RAMCOCEM": {}, "TITAN": {}, "TORNTPHARM": {}, "TORNTPOWER": {},
    "TRENT": {}, "TIINDIA": {}, "UPL": {}, "ULTRACEMCO": {}, "UNIONBANK": {}, "UNITDSPR": {}, "VBL": {},
    "VEDL": {}, "IDEA": {}, "VOLTAS": {}, "WIPRO": {}, "YESBANK": {}, "ZOMATO": {}, "ZYDUSLIFE": {}
}


def get_market_data_feed_authorize_v3():
    """Get authorization for market data feed."""
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    api_response = requests.get(url=url, headers=headers)
    response_json = api_response.json()
    # print("API Response:", response_json)  # Debugging line
    return api_response.json()


def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


async def fetch_market_data(real_time_data):
    await asyncio.sleep(1)  # Wait for 1 second

    """Fetch market data using WebSocket and print it."""   

    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Get market data feed authorization
    response = get_market_data_feed_authorize_v3()
    # Connect to the WebSocket with SSL context
    async with websockets.connect(response["data"]["authorized_redirect_uri"], ssl=ssl_context) as websocket:
        print('Connection established')


        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": ['NSE_EQ|INE585B01010', 
                                   'NSE_EQ|INE139A01034','NSE_EQ|INE947Q01028', 'NSE_EQ|INE918I01026', 'NSE_EQ|INE758E01017', 'NSE_EQ|INE522D01027', 'NSE_EQ|INE089A01031', 'NSE_EQ|INE00R701025', 'NSE_EQ|INE848E01016', 'NSE_EQ|INE917I01010', 'NSE_EQ|INE070A01015', 'NSE_EQ|INE982J01020', 'NSE_EQ|INE761H01022', 'NSE_EQ|INE749A01030', 'NSE_EQ|INE591G01017', 'NSE_EQ|INE494B01023', 'NSE_EQ|INE160A01022', 'NSE_EQ|INE736A01011', 'NSE_EQ|INE646L01027', 'NSE_EQ|INE010B01027', 'NSE_EQ|INE102D01028', 'NSE_EQ|INE302A01020', 'NSE_EQ|INE134E01011', 'NSE_EQ|INE009A01021', 'NSE_EQ|INE376G01013', 'NSE_EQ|INE619A01035', 'NSE_EQ|INE465A01025', 'NSE_EQ|INE463A01038', 'NSE_EQ|INE397D01024', 'NSE_EQ|INE192R01011', 'NSE_EQ|INE540L01014', 'NSE_EQ|INE775A01035', 'NSE_EQ|INE237A01028', 'NSE_EQ|INE059A01026', 'NSE_EQ|INE732I01013', 'NSE_EQ|INE361B01024', 'NSE_EQ|INE797F01020', 'NSE_EQ|INE811K01011', 'NSE_EQ|INE180A01020', 'NSE_EQ|INE949L01017', 'NSE_EQ|INE881D01027', 'NSE_EQ|INE030A01027', 'NSE_EQ|INE795G01014', 'NSE_EQ|INE476A01022', 'NSE_EQ|INE745G01035', 'NSE_EQ|INE531E01026', 'NSE_EQ|INE823G01014', 'NSE_EQ|INE721A01047', 'NSE_EQ|INE028A01039', 'NSE_EQ|INE670K01029', 'NSE_EQ|INE280A01028', 'NSE_EQ|INE158A01026', 'NSE_EQ|INE123W01016', 'NSE_EQ|INE298A01020', 'NSE_EQ|INE192A01025', 'NSE_EQ|INE769A01020', 'NSE_EQ|INE398R01022', 'NSE_EQ|INE155A01022', 'NSE_EQ|INE674K01013', 'NSE_EQ|INE094A01015', 'NSE_EQ|INE274J01014', 'NSE_EQ|INE528G01035', 'NSE_EQ|INE093I01010', 'NSE_EQ|INE726G01019', 'NSE_EQ|INE012A01025', 'NSE_EQ|INE073K01018', 'NSE_EQ|INE095A01012', 'NSE_EQ|INE006I01046', 'NSE_EQ|INE562A01011', 'NSE_EQ|INE195A01028', 'NSE_EQ|INE142M01025', 'NSE_EQ|INE849A01020', 'NSE_EQ|INE669C01036', 'NSE_EQ|INE136B01020', 'NSE_EQ|INE216A01030', 'NSE_EQ|INE002S01010', 'NSE_EQ|INE111A01025', 'NSE_EQ|INE062A01020', 'NSE_EQ|INE118H01025', 'NSE_EQ|INE364U01010', 'NSE_EQ|INE238A01034', 'NSE_EQ|INE081A01020', 'NSE_EQ|INE044A01036', 'NSE_EQ|INE883A01011', 'NSE_EQ|INE075A01022', 'NSE_EQ|INE498L01015', 'NSE_EQ|INE935N01020', 'NSE_EQ|INE002L01015', 'NSE_EQ|INE038A01020', 'NSE_EQ|INE484J01027', 'NSE_EQ|INE031A01017', 'NSE_EQ|INE242A01010', 'NSE_EQ|INE205A01025', 'NSE_EQ|INE027H01010', 'NSE_EQ|INE692A01016', 'NSE_EQ|INE04I401011', 'NSE_EQ|INE101D01020', 'NSE_EQ|INE010V01017', 'NSE_EQ|INE263A01024', 'NSE_EQ|INE020B01018', 'NSE_EQ|INE685A01028', 'NSE_EQ|INE647A01010', 'NSE_EQ|INE121A08PJ0', 'NSE_EQ|INE860A01027', 'NSE_EQ|INE974X01010', 'NSE_EQ|INE854D01024', 'NSE_EQ|INE220G01021', 'NSE_EQ|INE742F01042', 'NSE_EQ|INE226A01021', 'NSE_EQ|INE171A01029', 'NSE_EQ|INE976G01028', 'NSE_EQ|INE047A01021', 'NSE_EQ|INE326A01037', 'NSE_EQ|INE262H01021', 'NSE_EQ|INE584A01023', 'NSE_EQ|INE084A01016', 'NSE_EQ|INE085A01013', 'NSE_EQ|INE878B01027', 'NSE_EQ|INE836A01035', 'NSE_EQ|INE548A01028', 'NSE_EQ|INE414G01012', 'NSE_EQ|INE018E01016', 'NSE_EQ|INE669E01016', 'NSE_EQ|INE776C01039', 'NSE_EQ|INE211B01039', 'NSE_EQ|INE417T01026', 'NSE_EQ|INE813H01021', 'NSE_EQ|INE868B01028', 'NSE_EQ|INE213A01029', 'NSE_EQ|INE335Y01020', 'NSE_EQ|INE931S01010', 'NSE_EQ|INE821I01022', 'NSE_EQ|INE053F01010', 'NSE_EQ|INE323A01026', 'NSE_EQ|INE127D01025', 'NSE_EQ|INE021A01026', 'NSE_EQ|INE356A01018', 'NSE_EQ|INE733E01010', 'NSE_EQ|INE214T01019', 'NSE_EQ|INE176B01034', 'NSE_EQ|INE022Q01020', 'NSE_EQ|INE545U01014', 'NSE_EQ|INE511C01022', 'NSE_EQ|INE115A01026', 'NSE_EQ|INE596I01012', 'NSE_EQ|INE702C01027', 'NSE_EQ|INE343H01029', 'NSE_EQ|INE388Y01029', 'NSE_EQ|INE117A01022', 'NSE_EQ|INE530B01024', 'NSE_EQ|INE239A01024', 'NSE_EQ|INE758T01015', 'NSE_EQ|INE154A01025', 'NSE_EQ|INE455K01017', 'NSE_EQ|INE406A01037', 'NSE_EQ|INE101A01026', 'NSE_EQ|INE437A01024', 'NSE_EQ|INE208A01029', 'NSE_EQ|INE303R01014', 'NSE_EQ|INE245A01021', 'NSE_EQ|INE288B01029', 'NSE_EQ|INE148O01028', 'NSE_EQ|INE331A01037', 'NSE_EQ|INE053A01029', 'NSE_EQ|INE090A01021', 'NSE_EQ|INE628A01036', 'NSE_EQ|INE196A01026', 'NSE_EQ|INE787D01026', 'NSE_EQ|INE018A01030', 'NSE_EQ|INE121J01017', 'NSE_EQ|INE140A01024', 'NSE_EQ|INE399L01023', 'NSE_EQ|INE092T01019', 'NSE_EQ|INE347G01014', 'NSE_EQ|INE067A01029', 'NSE_EQ|INE438A01022', 'NSE_EQ|INE615H01020', 'NSE_EQ|INE423A01024', 'NSE_EQ|INE121E01018', 'NSE_EQ|INE019A01038', 'NSE_EQ|INE151A01013', 'NSE_EQ|INE259A01022', 'NSE_EQ|INE522F01014', 'NSE_EQ|INE095N01031', 'NSE_EQ|INE296A01024', 'NSE_EQ|INE765G01017', 'NSE_EQ|INE066F01020', 'NSE_EQ|INE257A01026', 'NSE_EQ|INE002A01018', 'NSE_EQ|INE203G01027', 'NSE_EQ|INE467B01029', 'NSE_EQ|INE774D08MG3', 'NSE_EQ|INE647O01011', 'NSE_EQ|INE079A01024', 'NSE_EQ|INE129A01019', 'NSE_EQ|INE0J1Y01017', 'NSE_EQ|INE481G01011', 'NSE_EQ|INE299U01018', 'NSE_EQ|INE040A01034', 'NSE_EQ|INE114A01011', 'NSE_EQ|INE486A01021', 'NSE_EQ|INE935A01035', 'NSE_EQ|INE603J01030', 'NSE_EQ|INE003A01024', 'NSE_EQ|INE202E01016', 'NSE_EQ|INE066A01021', 'NSE_EQ|INE029A01011', 'NSE_EQ|INE670A01012', 'NSE_EQ|INE663F01024', 'NSE_EQ|INE752E01010', 'NSE_EQ|INE092A01019', 'NSE_EQ|INE271C01023', 'NSE_EQ|INE318A01026', 'NSE_EQ|INE200M01039', 'NSE_EQ|INE016A01026',
                                   'NSE_EQ|INE042A01014']

            }
        }
#"NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50",
        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        # Continuously receive and decode data from WebSocket
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            # Convert the decoded data to a dictionary
            data_dict = MessageToDict(decoded_data)
            # print(data_dict)
              
            if 'feeds' in data_dict:
                feeds = data_dict['feeds']
                # print(data_dict['feeds'])
                current_timestamp = data_dict.get("currentTs")

                # Separate Data into Variables
                
                for stock, details in feeds.items():
                    
                    ltp = details['ltpc'].get('ltp')  # Last Traded Price
                    ltt = details['ltpc'].get('ltt')  # Last Traded Time
                    ltq = details['ltpc'].get('ltq')  # Last Traded Quantity
                    cp = details['ltpc'].get('cp')    # Closing Price

                    d = dict2[stock]
                    # print(d)
                    real_time_data[d] = {
                        "Stock Symbol": stock,
                        "Last Traded Price": ltp,
                        "Last Traded Time": ltt,
                        "Last Traded Quantity": ltq,
                        "Closing Price": cp,
                        "Current Timestamp": current_timestamp
                    }
                    print('real time data huihiuhiuhiuhiuhiuhiuhh')

                   

                # Output the separated data
                # for stock in stock_data:
                #     print(stock)

            else:
                print("No 'feeds' key found in the data.")
                # Print the dictionary representation
            # print(json.dumps(data_dict))
            await asyncio.sleep(3)
            
def fetch_market_data_proc(real_time_data):
    asyncio.run(fetch_market_data(real_time_data))



def call(real_time_data, allstock, tenstocks, previous_close_data, lock, stop_flag):
    while not stop_flag:
        print("checking ten percent")
        with lock:
            for j in allstock :
                # print('real real',real_time_data)
                stock_data = real_time_data.get(j)
                # print(stock_data)
                ltp = stock_data.get('Last Traded Price')
                symbol = stock_data.get('Stock Symbol')
                print(ltp,symbol)
                print(previous_close_data[symbol]['ten'])
                
                if ltp>previous_close_data[symbol]['ten']:
                    print('buy',ltp,previous_close_data[symbol]['ten'],j)
                    allstock.remove(j)
                    tenstocks.append(j)
                    
                    
                    
                elif ltp<previous_close_data[symbol]['tensell']:
                    print('sell',ltp,previous_close_data[symbol]['ten'],j)
                    allstock.remove(j)
                    tenstocks.append(j)
                

                

def call_ten(real_time_data, fifteenstocks, tenstocks, previous_close_data, lock, stop_flag1):
    print("checking fifteen percent")
    while not stop_flag1:
        with lock:
            if tenstocks:
                for i in tenstocks:
                    ltp = real_time_data[i]['Last Traded Price']
                    symbol = real_time_data[i]['Stock Symbol']
                    
                    
                    if ltp>previous_close_data[symbol]['fifteen']:
                        print('buy fifteen',ltp,previous_close_data[symbol]['ten'],i)
                        fifteenstocks.append(i)
                        tenstocks.remove(i)


                    elif ltp<previous_close_data[symbol]['fifteensell']:
                        print('sell fifteen',ltp,previous_close_data[symbol]['ten'],i)
                        fifteenstocks.append(i)
                        tenstocks.remove(i)


            
def call_fifteen(real_time_data, fifteenstocks, previous_close_data, lock, stop_flag2):
    print("checking 20 percent")
    while not stop_flag2:
        with lock:
            if fifteenstocks:
                for i in fifteenstocks:
                    ltp = real_time_data[i]['Last Traded Price']
                    symbol = real_time_data[i]['Stock Symbol']
                
                    
                    if ltp>previous_close_data[symbol]['twenty']:
                        print('here dear we can buy free fries here dear we can buy free fries here dear we can buy free fries here dear we can buy free fries',ltp,previous_close_data[symbol]['ten'],i)
                        fifteenstocks.remove(i)
                        
                    elif ltp<previous_close_data[symbol]['twentysell']:
                        print('sell well tujhe hogi jail sell well tujhe hogi jailsell well tujhe hogi jail sell well tujhe hogi jail sell well tujhe hogi jail',ltp,previous_close_data[symbol]['ten'],i)
                        fifteenstocks.remove(i)







if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    manager = Manager() 
    previous_close_data = manager.dict()

    previous()
    allstock = manager.list(allstock)
    tenstocks = manager.list()
    fifteenstocks = manager.list()
    real_time_data = manager.dict()
    previous_close_data = manager.dict()
    lock = Lock()

    processes = [Process(target=fetch_market_data_proc, args=(real_time_data,)),
    Process(target=call, args=(real_time_data, allstock, tenstocks, previous_close_data, lock, stop_flag)),
    Process(target=call_ten, args=(real_time_data, fifteenstocks, tenstocks, previous_close_data, lock, stop_flag1)),
    Process(target=call_fifteen, args=(real_time_data, fifteenstocks, previous_close_data, lock, stop_flag2))]

    for p in processes:
        time.sleep(20)
        p.start()

    # Start Flask in a separate process
    flask_process = Process(target=app.run, kwargs={'debug': False, 'use_reloader': False})
    flask_process.start()

    try:
        # Wait for processes
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Shutting down...")
        for p in processes:
            p.terminate()
        flask_process.terminate()
    
    app.run(debug=True)
     

# how to stop a thread 
# small_loop.call_soon_threadsafe(loop.stop)