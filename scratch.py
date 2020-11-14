from selenium.webdriver import Firefox, DesiredCapabilities, FirefoxProfile
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

import time

profile = FirefoxProfile()
# Allow autoplay
profile.set_preference("media.autoplay.default", 0)
cap = DesiredCapabilities.FIREFOX
options = Options()
# options.headless = True
driver = Firefox(firefox_profile=profile, capabilities=cap, options=options)
driver.get('https://banggia.vps.com.vn/#PhaiSinh/VN30')
try:
    time.sleep(3)
    with driver.context(driver.CONTEXT_CHROME):
        console = driver.find_element(By.ID, "tabbrowser-tabs")
        console.send_keys(Keys.LEFT_CONTROL + Keys.LEFT_SHIFT + 'k')
        time.sleep(3)
        console.send_keys('$x("/html/body/div[2]/div/div/div[2]/div/div/table/tbody/tr[1]/td[1]/a")[0].click()'  + Keys.ENTER )
        time.sleep(3)
        console.send_keys(':screenshot --full-page' + Keys.ENTER)
        console.send_keys(Keys.LEFT_CONTROL + Keys.LEFT_SHIFT + 'k')

except:
    pass

driver.get_screenshot_as_file("/home/sotola/Desktop/screenshot.png")

#%%
# install chromium, its driver, and selenium


# set options to be headless, ..
from selenium import webdriver
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
# open it, go to a website, and get results
wd = webdriver.Chrome(options=options)
wd.get("https://banggia.vps.com.vn/#PhaiSinh/VN30")
print(wd.page_source)  # results
# divs = wd.find_elements_by_css_selector('div')

try:
    time.sleep(3)
    with driver.context(driver.CONTEXT_CHROME):
        console = driver.find_element(By.ID, "tabbrowser-tabs")
        console.send_keys(Keys.LEFT_CONTROL + Keys.LEFT_SHIFT + 'i')
        time.sleep(3)
        console.send_keys(
            '$x("/html/body/div[2]/div/div/div[2]/div/div/table/tbody/tr[1]/td[1]/a")[0].click()' + Keys.ENTER)
        time.sleep(3)
except:
    pass

#%%  https://stackoverflow.com/questions/20907180/getting-console-log-output-from-chrome-with-selenium-python-api-bindings
#
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from time import sleep

# enable browser logging
d = DesiredCapabilities.CHROME
d['goog:loggingPrefs'] = { 'browser':'ALL' }
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
# options.add_argument("start-maximized")
options.add_argument("--window-size=1920,1080")
options.add_argument("--auto-open-devtools-for-tabs")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
driver = webdriver.Chrome(desired_capabilities=d,options=options)

# load the desired webpage
driver.get('https://banggia.vps.com.vn/#PhaiSinh/VN30')
sleep(3)
driver.execute_script("""document.querySelector("#sym_VN30F2011 > a").click()""")
sleep(1)
driver.get_screenshot_as_file("/home/sotola/Desktop/screenshot.png")
# https://stackoverflow.com/questions/20907180/getting-console-log-output-from-chrome-with-selenium-python-api-bindings
sleep(1)
driver.execute_script("""// Scrape phai sinh 1s
function grabVn30List() {
    data = grabData("/html/body/div[2]/div/div/div[2]/div/div/table/tbody");
    const lst = data.map(x => x[0]);
    return lst;
}

function grabTodayPS() {
    const data = grabData("/html/body/div[2]/div/div/div[2]/div/div/table/tbody/tr[2]/td/div/div[3]/table/tbody")
    console.save(data, FN_GRAB_TODAY );
}


function betweenTimes(startTime, endTime) {
    var dt = new Date();//current Date that gives us current Time also


    var s =  startTime.split(':');
    var dt1 = new Date(dt.getFullYear(), dt.getMonth(), dt.getDate(),
        parseInt(s[0]), parseInt(s[1]), parseInt(s[2]));

    var e =  endTime.split(':');
    var dt2 = new Date(dt.getFullYear(), dt.getMonth(),
        dt.getDate(),parseInt(e[0]), parseInt(e[1]), parseInt(e[2]));

    return dt >= dt1 && dt <= dt2
}


function isTradingTime() {
    return betweenTimes('08:59:50','11:30:50') || betweenTimes('12:59:50','14:45:50')
}

function today() {
    var today = new Date();
    var dd = String(today.getDate()).padStart(2, '0');
    var mm = String(today.getMonth() + 1).padStart(2, '0'); //January is 0!
    var yyyy = today.getFullYear();
    var minute = today.getMinutes()
    var second = today.getSeconds()
    var hour = today.getHours()
    const f = x => x < 10 ? "0"+x : ""+x
    return {
                'stamp':today.getTime(),
                'date':yyyy+"_"+f(mm)+"_"+f(dd),
                'time':f(hour)+"_"+f(minute)+"_"+f(second),
            }
}

function Arr(x) { return Array.prototype.slice.call(x.children)}

function getTableBody(st) {return Arr($x(st)[0])}

function parseRows(body) {return body.map(row => Arr(row).map(cell => cell.textContent))}

function grabData(s) {return parseRows(getTableBody(s))}

(function(console){

    console.save = function(data, filename){

        if(!data) {
            console.error('Console.save: No data')
            return;
        }

        if(!filename) filename = 'console.json'

        if(typeof data === "object"){
            data = JSON.stringify(data, undefined, 4)
        }

        var blob = new Blob([data], {type: 'text/json'}),
            e    = document.createEvent('MouseEvents'),
            a    = document.createElement('a')

        a.download = filename
        a.href = window.URL.createObjectURL(blob)
        a.dataset.downloadurl =  ['text/json', a.download, a.href].join(':')
        e.initMouseEvent('click', true, false, window, 0, 0, 0, 0, 0, false, false, false, false, 0, null)
        a.dispatchEvent(e)
    }
})(console)

// showChart("ACB")
function pullDataFromChart() {
    // console.log(Arr(document.querySelector("#historyOrder")));
    return parseRows(Arr(document.querySelector("#historyOrder")))
}


// https://trade-hn.vndirect.com.vn/chung-khoan/phai-sinh
function scrapeVNDirect() { // No dependency
    var tbl = Array.prototype.slice.call(document.querySelector("#derivative-info-table-scroll-area-VN30F2011 > div.derivative-info-table.header-fixed").children)[1].children[0]

    var foo = r => {
        txt = Array.prototype.slice.call(r.children).map(x => x.innerText)
        return [parseInt(txt[0]), parseFloat(txt[1]), parseFloat(txt[2]), parseInt(txt[3]) ]
    }
    tbl.children
    return Array.prototype.slice.call(tbl.children).map(foo)
}

function pullRandomData () {
    showChart(rd())
    setTimeout(function(){
        pullDataFromChart()
    }, CHART_DELAY);
}

function report() {
    console.log(`The dictionary has ${Object.keys(dataDic).length} entries. The last entry has ${dataDic[Object.keys(dataDic)[Object.keys(dataDic).length -1]].length} data points`)
}

function pullData() {
    const stock = vn30List[count]
    showChart(stock)
    setTimeout(function() {
        const data = pullDataFromChart()
        dataDic[stock] = data
        report()

    }, CHART_DELAY);

    count = next(count)
}
let rd = () => vn30List[Math.floor(Math.random() * vn30List.length)]    // Randomize a Mã chứng khoán

function saveit() {
    console.save(dataDic, `HOSE-${start}-${count}-${FN_SAVEIT}`)
}

function grabHoseTable() {
    return  Array.prototype.slice.call(document.querySelector("#sortable-banggia").children)
        .map(x => Array.prototype.slice.call(x.children).map(x => x.textContent))
}

function grabVPS_psMarketDepth() {

    return  [Array.prototype.slice.call(document.querySelector("#tbodyPhaisinhContent > tr.detail > td > div > div.center-panel > div > table:nth-child(1)").children)
        .map(x => Array.prototype.slice.call(x.children).map(x => x.textContent)),Array.prototype.slice.call(document.querySelector("#tbodyPhaisinhContent > tr.detail > td > div > div.center-panel > div > table:nth-child(2)").children)
        .map(x => Array.prototype.slice.call(x.children).map(x => x.textContent)) ]
}
function next(count) { return (count+1) % vn30List.length}

var NOW = () => document.querySelector("#main-wrapper > footer > span").innerText.replaceAll(":", "_")

function scrapePsVPS_MarketDepth() {

    // console.log(grabVPS_psMarketDepth())
    // console.save(grabVPS_psMarketDepth(), FN_SCRAPE + NOW() + ".json")
}

var printInterval = 0
//fav
function scrapePsPressureAndOrders() {
    var pressure = psPressure()
    var orders = grabVPS_PhaiSinhOrderTable()
    var req = {
        psPressure: pressure,
        orders: orders,
        time: today(),
    }

    printInterval += 1
    if (printInterval % 10 === 3) {
        console.log("ps-pressure-request (from browser): ")
        console.log(req)
    }

    if (! isTradingTime()) {
        console.log('not Trading Time')
        //return
    }

    fetch('http://127.0.0.1:'+ 5005 +'/data-in', {
        method: 'POST',
        body: JSON.stringify(req),
        headers: {
            'Content-type': 'application/json; charset=UTF-8'
        }
    })
        .then(res => res.json())
        .then(console.log)
}



function psPressure() {
    var buys = Array.prototype.slice.call(document.querySelector("#ngiaIndexBContent").children)
    var prowb = r => parseFloat(r.children[1].innerText)*parseFloat(r.children[0].innerText)*1000000/100000000
    var totalBuys = buys.map(prowb).reduce((a, b) => a + b, 0)

    var prows = r => parseFloat(r.children[0].innerText)*parseFloat(r.children[1].innerText)*1000000/100000000
    var sells = Array.prototype.slice.call(document.querySelector("#ngiaIndexSContent").children)
    var totalSells = sells.map(prows).reduce((a, b) => a + b, 0)


    var prb = rr => parseInt(rr.children[0].innerText)
    var volBuys = buys.map(prb)
    var prs = rr => parseInt(rr.children[1].innerText)
    var volSells = sells.map(prs);
    var spread = parseFloat(sells[0].children[0].innerText) - parseFloat(buys[0].children[1].innerText)
    data = {
        psBuyPressure: totalBuys,
        psSellPressure: totalSells,
        volBuys: volBuys,
        volSells: volSells,
        totalVolBuys: volBuys.reduce((a, b) => a + b, 0),
        totalVolSells: volSells.reduce((a, b) => a + b, 0),
        spread: spread,}

    return data
}


// https://banggia.vps.com.vn/#PhaiSinh/VN30
function grabVPS_PhaiSinhOrderTable() {
    arr = Array.prototype.slice.call(document.querySelector("#historyOrder > tbody").children)
    var n = arr.length - 1
    function parseRow(r) {
        var t = r.children[0].innerText.split(":").map(x => parseInt(x))
        var hour = t[0]
        var minute = t[1]
        var second = t [2]
        var volume = parseInt(r.children[1].innerText.replace(",", ""))
        var price = parseFloat(r.children[2].innerText)
        return {hour:hour, minute:minute, second:second, volume:volume, price:price}
    }
    res = arr.map(parseRow)
    for (i = 0; i < arr.length; i++) {
        res[i]['index'] = n - i
    }
    return res
}

function scrapeVPS_PhaiSinhOrderTable() {
    marketDepth = grabVPS_psMarketDepth()
    //orders = grabVPS_PhaiSinhOrderTable().slice(0, NUM_PS_ORDERS_PER_PUSH) console.save({orders: orders, marketDepth: marketDepth},      FN_SCRAPE + "_" + NOW() + ".json")
    fetch('http://127.0.0.1:'+ FLASK_PORT +'//api/ps-orders-inbound', {
        method: 'POST',
        body: JSON.stringify({
            orders: orders,
            psPressure: marketDepth}),
        headers: {
            'Content-type': 'application/json; charset=UTF-8'
        }
    })
        .then(res => res.json())
        .then(x => x)
    // console.log(data)
}

function stop() {
    clearInterval(myInterval)
}

FN_SCRAPE = "PhaiSinh_04_11_2020"
FN_GRAB_TODAY = "VN30F2010_2020_11_04"
FN_SAVE_IT = "2020_11_02"
FLASK_PORT = 5000

NUM_PS_ORDERS_PER_PUSH = 4000 // 30kb for 200 dataPoint

// psMarketDepthInterval = setInterval(scrapePsVPS_MarketDepth, 1500)
// psOrderTableInterval = setInterval(scrapeVPS_PhaiSinhOrderTable, 2000)

clrInterval = setInterval(console.clear, 60*1000)
setInterval(scrapePsPressureAndOrders, 1*1000)

""")


#%%
for entry in driver.get_log('browser'):
    print(entry)
