from pymongo import MongoClient
from commonOLD import threading_func_wrapper, Tc
from requests import post
import json

DEBUG = True
BOKEH_PROD_PORT = 5009
BOKEH_DEV_PORT = 5008

HOSE_ENDPOINT_PORT = 5000
PS_ENDPOINT_PORT = 5001
SUU_ENDPOINT_PORT = 5010

ps_url   = f'http://localhost:{PS_ENDPOINT_PORT}/ps-pressure-out'
hose_url = f'http://localhost:{HOSE_ENDPOINT_PORT}/api/hose-indicators-outbound'
suu_url  = f"http://localhost:{SUU_ENDPOINT_PORT}/suu-data-out"

hose_in_url = f'http://localhost:{HOSE_ENDPOINT_PORT}/api/hose-snapshot-inbound'
ps_in_url = f'http://localhost:{PS_ENDPOINT_PORT}/ps-pressure-in'
suu_in_url  = f"http://localhost:{SUU_ENDPOINT_PORT}/ingest-data"
suu2_in_url  = f"http://localhost:{SUU_ENDPOINT_PORT}/python-data"

client = MongoClient('localhost', 27017)
db = client['dc_data']
cursor_vn30 = db['temp_vn30_2020_11_12'].find({}, {'_id': 0})
cursor_ps   = db['temp_ps_2020_11_12'].find({}, {'_id': 0})
cursor_suu  = db['temp_suu_2020_11_12'].find({}, {'_id': 0})
cursor_suu2 = db['temp_suu_2020_11_12'+'busd'].find({}, {'_id': 0})

isRunning_vn30Scraper = True
isRunning_psScraper = True
isRunning_suu = True
isRunning_suu2 = True
#%%
def stopVn30():
    global isRunning_vn30Scraper
    isRunning_vn30Scraper = False

def startVn30():
    global isRunning_vn30Scraper
    isRunning_vn30Scraper = True

def send_vn30(data):
    return post(hose_in_url, data=json.dumps(data))


def processVN30(cursor):
    if not isRunning_vn30Scraper: return
    try:
        data = cursor.next()
        # if DEBUG: print("temp_vn30_2020_11_12: ", data['time']['date'].replace('_', '/'), data['time']['time'].replace('_', ':') )
        res = send_vn30(data).json()
        if DEBUG: print(Tc.CGREEN, 'VN30', res, Tc.CEND)
    except Exception as e:
        print('fake.py > processVN30(cursor) > error =', Tc.CREDBG2, e, Tc.CEND)
    finally:
        threading_func_wrapper(lambda: processVN30(cursor), 2)

startVn30()
processVN30(cursor_vn30)
#%%
stopVn30()

#%%

def stopPs():
    global isRunning_psScraper
    isRunning_psScraper = False

def startPs():
    global isRunning_psScraper
    isRunning_psScraper = True

def send_ps(data):
    return post(ps_in_url, data=json.dumps(data))


def processPs(cursor):
    if not isRunning_psScraper: return
    try:
        data = cursor.next()
        # if DEBUG: print("temp_ps_2020_11_12: ", data['time']['date'].replace('_', '/'), data['time']['time'].replace('_', ':') )
        res = send_ps(data).json()
        if DEBUG: print(Tc.CYELLOW, 'PS', res, Tc.CEND)
    except Exception as e:
        print('fake.py > processPS(cursor) > error =', Tc.CREDBG2, e, Tc.CEND)
    finally:
        threading_func_wrapper(lambda: processPs(cursor), 2)

startPs()
processPs(cursor_ps)

#%%
stopPs()

#%%
isRunning_suu = True

def stopSuu():
    global isRunning_suu
    isRunning_suu = False

def startSuu():
    global isRunning_suu
    isRunning_suu = True

def send_Suu(data):
    return post(suu_in_url, data=json.dumps(data))


def processSuu(cursor):
    if not isRunning_suu: return
    try:
        data = cursor.next()
        res = send_Suu(data).json()
        if DEBUG: print(Tc.CVIOLET, 'SUU',res, Tc.CEND)
    except Exception as e:
        print('fake.py > processSuu(cursor) > error =', Tc.CREDBG2, e, Tc.CEND)
    finally:
        threading_func_wrapper(lambda: processSuu(cursor), 2)

startSuu()
processSuu(cursor_suu)

#%%

stopSuu()

#%%

isRunning_suu2 = True

def stopSuu2():
    global isRunning_suu2
    isRunning_suu2 = False

def startSuu2():
    global isRunning_suu2
    isRunning_suu2 = True

def send_Suu2(data):
    return post(suu2_in_url, data=json.dumps(data))


def processSuu2(cursor):
    if not isRunning_suu2: return
    try:
        data = cursor.next()
        res = send_Suu2(data).json()
        if DEBUG: print(Tc.CBEIGE, 'SUU2',res, Tc.CEND)
    except Exception as e:
        print('fake.py > processSuu2(cursor) > error =', Tc.CREDBG2, e, Tc.CEND)
    finally:
        threading_func_wrapper(lambda: processSuu2(cursor), 2)

startSuu2()
processSuu2(cursor_suu2)
#%%
stopSuu2()