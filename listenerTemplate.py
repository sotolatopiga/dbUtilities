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
    last = None
    client = MongoClient('localhost', 27017)
    DC_DATABASE = 'dc_data'
    db = client[DC_DATABASE]
    FAKE_LISTENER_PORT = 5005

app = Flask(__name__)
CORS(app)

################################################## SERVER PAGES ###################################################

def stop(PORT = FAKE_LISTENER_PORT): view(f"http://localhost:{PORT}/shutdown")

# Shut down via HTTP
@app.route("/shutdown", methods=['GET', 'POST', 'DELETE', 'PUT'])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    return jsonify("Successfully closed server")


################################################# DATA INGESTION ##################################################

# Ingest data
@app.route('/data-in', methods=['GET', 'POST', 'DELETE', 'PUT'])
def eod_inbound(verbose=True):
    global last
    try:
        raw = json.loads(request.data)
        last = raw
        if verbose: print(f"{type(raw)}, keys={raw.keys()}" if type(raw) == dict else type(raw))
    except Exception as e:
        print(f'Exception encontered while running {Tc.CGREEN}eod_inbound{Tc.CEND}: ',
              f'{Tc.CREDBG}{e}{Tc.CEND}')
        return jsonify("Listener received data but failed parsing or saving")

    return jsonify("Successfully received <== from Fake Listener ")


##################################################### MAIN ########################################################

def starts(): threading_func_wrapper(lambda: run_simple('localhost', FAKE_LISTENER_PORT, app))

if __name__ == '__main__':
    isRunning = True #
    starts()

