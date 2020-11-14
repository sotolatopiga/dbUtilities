from pymongo import MongoClient
import requests, pandas as pd, numpy as np, urllib, json
from bokeh.models import ColumnDataSource
from urllib.request import  urlopen
from commonOLD import tik, tok, time_format, threading_func_wrapper
from datetime import datetime, timedelta
from hoseParser import parseHose
from hoseParser import computeIndicatorSingleDataPoint
from requests import post

DEBUG = True
DC_DATABASE = 'dc_data'

HOSE_2020_11_02 = 'hose_2020_11_02'
HOSE_2020_11_06 = 'hose_2020_11_06'
PS_2020_11_06   = 'ps_2020_11_06'

HOSE_2020_11_10 = 'hose_2020_11_10'
PS_2020_11_10   = 'ps_2020_11_10'
SUU_2020_11_10 = 'suu_2020_11_10'

HOSE_2020_11_09 = 'hose_2020_11_09'
PS_2020_11_09   = 'ps_2020_11_09'
SUU_2020_11_09 = 'suu_2020_11_09'
VN30 = ['BID', 'CTG', 'EIB', 'FPT', 'GAS', 'HDB', 'HPG', 'KDH', 'MBB', 'MSN', 'MWG', 'NVL', 'PLX', 'PNJ', 'POW',
        'REE', 'ROS', 'SAB', 'SBT', 'SSI', 'STB', 'TCB', 'TCH', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']


client = MongoClient('localhost', 27017, maxPoolSize=28)
db = client[DC_DATABASE]
last_ps_request, last_hose_request, last_suu_request = None, None, None

BOKEH_PROD_PORT = 5009
BOKEH_DEV_PORT = 5008

HOSE_ENDPOINT_PORT = 5000
PS_ENDPOINT_PORT = 5001
SUU_ENDPOINT_PORT = 5010

ps_url   = f'http://localhost:{PS_ENDPOINT_PORT}/ps-pressure-out'
hose_url = f'http://localhost:{HOSE_ENDPOINT_PORT}/api/hose-indicators-outbound'
suu_url  = f"http://localhost:{SUU_ENDPOINT_PORT}/suu-data-out"

hose_in_url = f'http://localhost:{HOSE_ENDPOINT_PORT}/api/hose-snapshot-inbound'
#####################################################################################################################

def requestPSData():
    global last_ps_request
    res = requests.post(ps_url, json={})
    last_ps_request = res.json() # ['ohlcDataDic', 'orders', 'psPressure']
    return last_ps_request['orders'], ColumnDataSource(last_ps_request['ohlcDataDic']), last_ps_request['psPressure']


def requestHoseData():
    global last_hose_request
    res = requests.post(hose_url, json={})
    last_hose_request = res.json()
    dicBS = {key: last_hose_request['buySell'][key] for key in ['buyPressure', 'index', 'sellPressure']}
    sourceBuySell = ColumnDataSource(dicBS)  # ['buyPressure', 'index', 'sellPressure', 'time']

    dicVol = {key: last_hose_request['volumes'][key] for key in ['index', 'nnBuy', 'nnSell', 'totalValue']}
    sourceVolume = ColumnDataSource(dicVol)  # ['index', 'nnBuy', 'nnSell', 'time', 'totalValue']

    return sourceBuySell, sourceVolume


def fetchSuuData():
    global last_suu_request
    with urlopen(suu_url) as url:
        last_suu_request = json.loads(url.read().decode())
    return last_suu_request

#####################################################################################################################

def loadPSfromDB(day=PS_2020_11_06, tohour=False):
    global psdf
    psdf = pd.DataFrame(db[day].find({}, {'_id':0}))

    def psDataPoint_to_hour(ps_dp):
        o = ps_dp['orders'][0]
        return (o['hour']*3600 + o['minute']*60 + o['second']) /3600

    if tohour: psdf['hour'] = psdf['raws'].map(psDataPoint_to_hour)


def savePStoDB(day=PS_2020_11_06):
    db[day].delete_many({})
    db[day].insert_many(psdf.to_dict('records'))


def parseVN30(hoseDataPoint):
    req = {key:parseHose(hoseDataPoint['raws']['req'])[key] for key in VN30}
    return {'time': hoseDataPoint['raws']['time'], 'req': req}

def filterRaws(dp, filter=VN30):
    req = [row for row in dp['raws']['req'] if row[0] in filter]
    dp['raws']['req'] = req
    return dp

def computeIndicatorsFiltered(hoseDataSet, filter=VN30):
    dataVn30 = list(map(lambda dp: filterRaws(dp, filter=filter), hoseDataSet))
    times = list(map(lambda x: x['time'], dataVn30))
    df = pd.DataFrame(dataVn30)
    reqs = list(map(lambda x: x['req'], list(df['raws'].values)))
    parsed = list(map(parseHose, reqs))
    indicators = [computeIndicatorSingleDataPoint(hose, time) for hose ,time in zip(parsed, times)]
    return indicators


def iterateThrough(collection, TIME='hour'):
    global db
    lst = []
    cursor = db[collection].find_one()
    while cursor is not None:
        lst.append(cursor)
        print(f"\r#{len(lst)}", end="")
        last = cursor[TIME]
        cursor = db[collection].find_one({TIME:{'$gt':last}}, sort=[(TIME, 1)])
    if DEBUG: print('\n', len(lst))

# iterateThrough(PS_2020_11_06, 'hour')
# iterateThrough(HOSE_2020_11_06, 'timeObj')

######### Fake scraper: pulling from database at a regular interval #########

def fakeScrapers(hose_day=HOSE_2020_11_02, suu_day=None, ps_day=None, step=60,
                 hour=9, minute=0, second=0, interval=2, append=False,
                 fHose=None, fPs=None, fSuu=None):
    from datetime import datetime, timedelta
    global isRunning_vn30Scraper
    dbTime = datetime.fromtimestamp(db[hose_day].find_one()['raws']['time']['stamp'] / 1000)
    if suu_day is None: suu_day = hose_day.replace('hose', 'suu')
    if ps_day is None:  ps_day  = hose_day.replace('hose',  'ps')
    step = timedelta(seconds=step);  pad  = timedelta(seconds=0.01)
    marketCloseTime = datetime(year=dbTime.year, month=dbTime.month,
                               day=dbTime.day, hour=14, minute=45, second=59) + pad

    ###### Start the main loop ######
    currentTime = datetime.strptime(f"{dbTime.year}-{dbTime.month}-{dbTime.day} {hour}:{minute}:{second}", '%Y-%m-%d  %H:%M:%S')
    cursor = None;  resHose = []; cur_suu = None ; res_suu=[]; res_ps = [];  cur_ps = None; tik()
    countHose, countPS, countSuu = 0, 0, 0
    interval = timedelta(seconds=interval);  lastRun = datetime.now() - interval
    while currentTime < marketCloseTime:
        if not isRunning_vn30Scraper: break

        if cursor is not None:
            hose_time = cursor['time']
            cursor = cursor['raws']
            countHose += 1
            if append: resHose.append(cursor)
            if fHose is not None: fHose(cursor)
            print('\r hose', f'#{countHose}:', hose_time , end='')
        else: print('\r hose None' , end='')

        if cur_ps is not None:
            countPS += 1
            if append: res_ps.append(cur_ps['raws'])
            print('        ps:', f'#{countPS}:', cur_ps['time'], end='')
            if fPs is not None: fPs(cur_ps['raws'])
        else: print('        ps: None', end='')

        if cur_suu is not None:
            countSuu += 1
            if append: res_suu.append(cur_suu['raws'])
            print('        suu:', f'#{countSuu}:', cur_suu['time'], end='')
            if fSuu is not None: fSuu(cur_suu['raws'])
        else: print('        suu: None', end='')


        cursor = db[hose_day].find_one({'time': {'$gt': currentTime + pad, '$lt': currentTime + step + pad}},
                                       sort=[('time', -1)])
        cur_suu = db[suu_day].find_one({'time': {'$gt': currentTime + pad, '$lt': currentTime + step + pad}},
                                       sort=[('time', -1)])
        cur_ps  = db[ps_day ].find_one({'time': {'$gt': currentTime + pad, '$lt': currentTime + step + pad}},
                                       sort=[('time', -1)])
        while datetime.now() - lastRun <= interval: pass
        lastRun = datetime.now()
        currentTime += step

    print(f'\n hose:{len(resHose)},    ps:{len(res_ps)},    suu:{len(res_suu)}  \n')
    tok()
    return resHose, res_ps, res_suu
#%%
# hoseDataSet[0]['raws'].keys() = ['time', 'req']
# ps[0].keys() = ['psPressure', 'orders', 'time']
# suu[0].keys() = ['time', 'date'...]

# computeIndicatorsFiltered(hoseDataSet, filter=VN30)
# strToTime(suu[0]['date']+suu[0]['time'], format='%Y%m%d%H:%M:%S')
isRunning_vn30Scraper = True
def stop():
    global isRunning_vn30Scraper
    isRunning_vn30Scraper = False

def fooHose(raw):
    try:
        requests.post(hose_in_url, data=json.dumps(raw))
    finally:
        return

def foo():
    threading_func_wrapper(lambda: fakeScrapers(HOSE_2020_11_09, step=2, hour=9, minute=5, interval=3,
                                                fHose=fooHose))
foo()

