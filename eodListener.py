from werkzeug.serving import run_simple
from pymongo import MongoClient
from bokeh.util.browser import view
from copy import copy, deepcopy
from commonOLD import Tc, threading_func_wrapper
from flask import Flask, jsonify, request
from datetime import datetime, timedelta
from flask_cors import CORS
import json, pandas as pd, numpy as np
from random import randint

if "DEBUG" not in globals():
    DEBUG = True

    client = MongoClient('localhost', 27017)
    DC_DATABASE = 'dc_data'
    db = client[DC_DATABASE]
    EOD_COLLECTION_NAME = 'eod_hose_temp'
    CONSTANT_COLLECTION = 'constants'
    EOD_HOSE_LISTEN_PORT = 5003
    last = None
    VN30 = ['BID', 'CTG', 'EIB', 'FPT', 'GAS', 'HDB', 'HPG', 'KDH', 'MBB', 'MSN', 'MWG', 'NVL', 'PLX', 'PNJ', 'POW',
            'REE', 'ROS', 'SAB', 'SBT', 'SSI', 'STB', 'TCB', 'TCH', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
    HOSE_STOCKS = db[CONSTANT_COLLECTION].find_one({'name': 'hose_stock_list'})['value']
    HOSE_ID = {HOSE_STOCKS[i]: i for i in range(len(HOSE_STOCKS))}
    INITIAL_DF = pd.DataFrame({
        'raws':[],
    })
    df = INITIAL_DF
    numConflicts = 0
    conflicts = set([])

app = Flask(__name__)
CORS(app)

def checkDb(): print(db[dbName().replace('_off_peak', '')].count_documents({}))

def dbName(prefix = EOD_COLLECTION_NAME):
    n = datetime.now()
    return prefix + n.strftime( "_%Y_%m_%d" )

################################################## SERVER PAGES ###################################################

def stop(PORT = EOD_HOSE_LISTEN_PORT): view(f"http://localhost:{PORT}/shutdown")

# Shut down via HTTP
@app.route("/shutdown", methods=['GET', 'POST', 'DELETE', 'PUT'])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    return jsonify("Successfully closed server")


################################################# DATA INGESTION ##################################################

# Ingest Hose EoD data
@app.route('/eod-inbound', methods=['GET', 'POST', 'DELETE', 'PUT'])
def eod_inbound():
    global df, last, numConflicts
    try:
        raw = json.loads(request.data)
        df.loc[len(df)] = [raw]
        exists = findObjectsByName(raw['name'])
        st = f'{raw["name"]} (#{HOSE_ID[raw["name"]]})'
        if len(exists) > 0:
            lens = list(map(lambda x: x[1], exists))
            for l in lens:
                if l != raw["length"]:
                    numConflicts += 1
                    if raw["name"] not in conflicts: conflicts.add(raw["name"])
            print(f'Found {len(exists)} records for stock {st} with '
                  f'length: {lens} vs {raw["length"]}'
                  f' ({numConflicts} conflicts)')
        else:
            print(f'Inserting {st} for the first time '
                  f'(length = {raw["length"]}) ({numConflicts} conflicts)')

        db[dbName()].insert_one(deepcopy(raw))

    except Exception as e:
        print(f'Exception encontered while running {Tc.CGREEN}eod_inbound{Tc.CEND}: ',
              f'{Tc.CREDBG}{e}{Tc.CEND}')
        return jsonify("Listener received data but failed parsing or saving")

    return jsonify("Successfully received EoD Data!")


################################################# DB Stuff #########################################################

def countNullRecords():
    res = [0] * len(HOSE_STOCKS)
    allDone = True
    for i in range(len(HOSE_STOCKS)):
        cursor = list(db[dbName()].find({'name': HOSE_STOCKS[i]}))
        res[i] = len(cursor)
        if len(cursor) == 0: allDone = False
    return res, allDone


def findObjectsByName(name):
    return list(map(lambda x: (x['_id'], x['length']), db[dbName()].find({'name': name})))


def resolveDbConflicts(db_name=None):
    if db_name is None: db_name = dbName()

    dic = {}
    for stock in HOSE_STOCKS:
        cur = list(db[db_name].find({'name': stock}, {'_id': 0}))
        dicCount = {}; max = -1; res = None
        for data in cur:
           n = data['length']
           if n not in dicCount: dicCount[n] = 1
           else: dicCount[n] += 1
           if max < dicCount[n]:
                max = dicCount[n]
                res = data
        dic[stock] = res
        if len(dicCount) > 1: print('found 1 potential conflict for stock', stock, dicCount)
    return dic              # dic[stock].keys = ['name', 'length', 'first', 'last', 'data']


def writeDayToDb(data, db_name=None):
    if db_name is None: db_name = dbName().replace('_temp', '')
    db[db_name].delete_many({})
    for stock in HOSE_STOCKS:
        db[db_name].insert_one(data[stock])


def loadDayfromDb(db_name=None):
    if db_name is None: db_name = dbName().replace('_temp', '')
    dic = {}
    lst = list(db[db_name].find({}, {'_id':0}))
    for data in lst: dic[data['name']] = pd.DataFrame(data['data'])
    return dic

################################################## Main #########################################################

def starts(): threading_func_wrapper(
    lambda: run_simple('localhost', EOD_HOSE_LISTEN_PORT, app))

if __name__ == '__main__':
    isRunning = True #
    starts()

d = loadDayfromDb('eod_hose_2020_11_13')
#%%
print([ f'{stock}:{len(d[stock])}' for stock in VN30])

# countNullRecords() <== Check to see if there's any missing stock data
# data = resolveDbConflicts('eod_hose_temp_2020_11_13')   # This will delete the existing data
# writeDayToDb(data, 'eod_hose_2020_11_13')                  # Write all result here

