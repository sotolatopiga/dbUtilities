import time


class Tc:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    CEND = '\33[0m'
    CBOLD = '\33[1m'
    CITALIC = '\33[3m'
    CURL = '\33[4m'
    CBLINK = '\33[5m'
    CBLINK2 = '\33[6m'
    CSELECTED = '\33[7m'

    CBLACK = '\33[30m'
    CRED = '\33[31m'
    CGREEN = '\33[32m'
    CYELLOW = '\33[33m'
    CBLUE = '\33[34m'
    CVIOLET = '\33[35m'
    CBEIGE = '\33[36m'
    CWHITE = '\33[37m'

    CBLACKBG = '\33[40m'
    CREDBG = '\33[41m'
    CGREENBG = '\33[42m'
    CYELLOWBG = '\33[43m'
    CBLUEBG = '\33[44m'
    CVIOLETBG = '\33[45m'
    CBEIGEBG = '\33[46m'
    CWHITEBG = '\33[47m'

    CGREY = '\33[90m'
    CRED2 = '\33[91m'
    CGREEN2 = '\33[92m'
    CYELLOW2 = '\33[93m'
    CBLUE2 = '\33[94m'
    CVIOLET2 = '\33[95m'
    CBEIGE2 = '\33[96m'
    CWHITE2 = '\33[97m'

    CGREYBG = '\33[100m'
    CREDBG2 = '\33[101m'
    CGREENBG2 = '\33[102m'
    CYELLOWBG2 = '\33[103m'
    CBLUEBG2 = '\33[104m'
    CVIOLETBG2 = '\33[105m'
    CBEIGEBG2 = '\33[106m'
    CWHITEBG2 = '\33[107m'

class Tc2:
    SOURCE_ANSI_COLOR = Tc.CEND
    TIME_ANSI_COLOR = Tc.CEND
    INCIDENT_ANSI_COLOR = Tc.CEND
    LOCATION_ANSI_COLOR = Tc.CEND
    OPERATOR_ANSI_COLOR = Tc.CEND
    OPERATOR_TICKET_COUNT = Tc.CEND
    TICKET_ID_ANSI_COLOR = Tc.CEND
    TICKET_COUNT_ANSI_COLOR = Tc.CEND

class My_module:
    def __init__(self, module_name,  compile_time=None, pprint=False):
        if compile_time is None: compile_time = time.time()
        self.compile_time = compile_time
        self.module_name = module_name

    def banner(self):
        return ' (msg from %s%s%s.py compiled at %s%s%s)' % \
               (Tc.CYELLOW, self.module_name, Tc.CEND,
                Tc.CBLACK, time_format(self.compile_time)[-11:-3],Tc.CEND)

    def __repr__(self):
        return '***%s%s%s*** compiled at %s' % \
               (Tc.CYELLOW, self.module_name, Tc.CEND,
                time_format(self.compile_time))

_module = My_module('commonOld')

def time_format(t, time_only=False):
    st = time.strftime('%Y/%m/%d  %I:%M:%S %p',time.localtime(t))
    if time_only: st = st[-11:-3]
    return st


def tik():
    global _tik
    _tik = time.time()
    print('%sStarted%s '%(Tc.CVIOLET, Tc.CEND) + 'timing... at %s%s%s.'%
          (Tc.CBLACK, time_format(_tik, time_only=True), Tc.CEND))

def tok():
    global _tik
    print('Elapsed time: %s%.5f%s seconds' % (Tc.CVIOLET, time.time()-_tik, Tc.CEND) + _module.banner())


def threading_func_wrapper(func, delay=0.001, args=None, start=True):
    import threading
    if args is None:
        func_thread = threading.Timer(delay, func)
    else:
        func_thread = threading.Timer(delay, func, (args,))
    if start: func_thread.start()
    return func_thread


def strToTime(st=None, pad='-', format=None):
    if format is None:
        format = f"%Y{pad}%m{pad}%d  %H:%M:%S"
    from datetime import datetime
    if st is None:
        n = datetime.now()
        st = n.strftime(format)
    if len(st) <= 8:
        n = datetime.now()
        st = n.strftime(format.split(' ')[0] + ' ') + st
    return datetime.strptime(st,format)


MARKET_START  = strToTime("09:00:00")
MARKET_LUNCH  = strToTime("11:30:57")
MARKET_NOON   = strToTime("13:00:00")
MARKET_END    = strToTime("14:45:57")

def isTradingHours(time = None):
    from datetime import datetime
    if time is None: time = datetime.now()
    if type(time) == str: time = strToTime(time)
    return (MARKET_START <= time <= MARKET_LUNCH) or (MARKET_NOON <= time <= MARKET_END)


def timeObjToTime(obj):
    from datetime import datetime
    time = datetime.fromtimestamp(int(obj['stamp']/1000))
    return time


def timeObjToHours(obj):
    t = timeObjToTime(obj)
    return (t.hour*3600 + t.minute*60 + t.second)/3600.0


def dateTimeToHours(t):
    return (t.hour * 3600 + t.minute * 60 + t.second) / 3600.0


def strToHour(st, pad='-'):
    t = strToTime(st, pad=pad)
    return dateTimeToHours(t)

