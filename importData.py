from pymongo import MongoClient
import csv
import urllib3
from datetime import datetime
import time

#mongo_client = MongoClient('mongodb://127.0.0.1:27017')

mongo_client = MongoClient('127.0.0.1:27017',
                            username='ruser',
                            password='flzx3qc',
                            authSource='COVID19-DB',
                            authMechanism='SCRAM-SHA-256')

db = mongo_client["COVID19-DB"]

cdc_ts = db["CDC-TimeSeries"]
dxy_ts = db["DXY-TimeSeries"]

manager = urllib3.PoolManager(10)

def getMonthAndDay(titleInfo, index):
    month = 0
    day = 0
    
    if titleInfo is None or len(titleInfo) == 0:
        return (0, 0)
    if index < 0 or len(titleInfo) <= index:
        return (0, 0)

    date = titleInfo[index].split('/')
    if date is None or len(date) < 2:
        return (0, 0)
    month = int(date[0])
    day = int(date[1])
    return (month, day)


def importGlobalConfirmedData():
    confirmedUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    
    response = manager.request('GET', confirmedUrl)
    time.sleep(10)
    reader = csv.reader(response.data.decode('utf-8').splitlines())
    if reader is None:
        print("CSV File Error")
        return
    # Read the title and the date 
    titleInfo = next(reader)
    print("Import global confirmed data is start")
    for row in reader:
        if len(row) <= 6:
            print("CSV File Structure Error")
            break
        for i in range(4, len(row)):
            month, day = getMonthAndDay(titleInfo, i)
            insertConfirmedData(row[0], row[1], row[2], row[3], month, day, row[i])
    
    print("Import confirmed data is completed.")



def insertConfirmedData(province, country, latitude, longitude, month, day, count):
    data = {
        "Province/State": province,
        "Country/Region": country,
        "Latitude": latitude,
        "Longitude": longitude,
        "Confirmed": count,
        "Date": datetime(2020, month, day, 0, 0, 0),
    }
    start = datetime(2020, month, day, 0, 0, 0)
    end = datetime(2020, month, day, 23, 59, 59)
    doc = cdc_ts.find_one_and_update({"$and": [{"Date": {'$gte': start, '$lt': end}},
                                    {"Province/State": {"$eq": province}},
                                    {"Country/Region": {"$eq": country}}]}, 
                                    {"$set": {"Confirmed": count}})
    if doc is None:
        cdc_ts.insert_one(data)

def importGlobalDeathData():
    dataUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
    response = manager.request('GET', dataUrl)
    time.sleep(10)
    reader = csv.reader(response.data.decode('utf-8').splitlines())
    if reader is None:
        print("CSV File Error")
        return
    titleInfo = next(reader)
    print("Import global deaths data is starting")
    for row in reader:
        if len(row) <= 6:
            print("CSV File Structure Error")
            break
        for i in range(4, len(row)):
            month, day = getMonthAndDay(titleInfo, i)
            updateDeathData(row[0], row[1], row[2], row[3], month, day, row[i])
    print("Import global deaths data is completed.")
    

def updateDeathData(province, country, latitude, longitude, month, day, count):
    # Create an new node
    data = {
        "Province/State": province,
        "Country/Region": country,
        "Latitude": latitude,
        "Longitude": longitude,
        "Death": count,
        "Date": datetime(2020, month, day, 0, 0, 0),
    }

    start = datetime(2020, month, day, 0, 0, 0)
    end = datetime(2020, month, day, 23, 59, 59)
    doc = cdc_ts.find_one_and_update({"$and": [{"Date": {'$gte': start, '$lt': end}},
                                    {"Province/State": {"$eq": province}},
                                    {"Country/Region": {"$eq": country}}]}, 
                                    {"$set": {"Death": count}})
    if doc is None:
        cdc_ts.insert_one(data)
    

def importGlobalRecoveryData():
    dataUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"
    response = manager.request('GET', dataUrl)
    time.sleep(10)
    reader = csv.reader(response.data.decode('utf-8').splitlines())
    if reader is None:
        print("CSV File Error")
        return
    titleInfo = next(reader)

    for row in reader:
        if len(row) <= 6:
            print("CSV File Structure Error")
            break
        for i in range(4, len(row)):
            month, day = getMonthAndDay(titleInfo, i)
            updateRecoveryData(row[0], row[1], row[2], row[3], month, day, row[i])
    print("Import Global recovered data is completed.")



def updateRecoveryData(province, country, latitude, longitude, month, day, count):
    # Create an new node
    data = {
        "Province/State": province,
        "Country/Region": country,
        "Latitude": latitude,
        "Longitude": longitude,
        "Recovery": count,
        "Date": datetime(2020, month, day, 0, 0, 0),
    }

    start = datetime(2020, month, day, 0, 0, 0)
    end = datetime(2020, month, day, 23, 59, 59)
    doc = cdc_ts.find_one_and_update({"$and": [{"Date": {'$gte': start, '$lt': end}},
                                    {"Province/State": {"$eq": province}},
                                    {"Country/Region": {"$eq": country}}]}, 
                                    {"$set": {"Recovery": count}})
    if doc is None:
        cdc_ts.insert_one(data)

def importUSConfirmedData():
    dataUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
    response = manager.request('GET', dataUrl)
    reader = csv.reader(response.data.decode('utf-8').splitlines())
    if reader is None:
        print("CSV File Error")
        return
    titleInfo = next(reader)
    print("Import US confirmed data is starting")
    for row in reader:
        if len(row) <= 6:
            print("CSV File Structure Error")
            break
        for i in range(11, len(row)):
            month, day = getMonthAndDay(titleInfo, i)
            insertConfirmedData(row[6], row[7], row[8], row[9], month, day, row[i])
    
    print("Import US confirmed data is completed.")

def importUSDeathsData():
    dataUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
    response = manager.request('GET', dataUrl)
    reader = csv.reader(response.data.decode('utf-8').splitlines())
    if reader is None:
        print("CSV File Error")
        return
    titleInfo = next(reader)
    print("Import US deaths data is starting")
    for row in reader:
        if len(row) <= 6:
            print("CSV File Structure Error")
            break
        for i in range(12, len(row)):
            month, day = getMonthAndDay(titleInfo, i)
            updateDeathData(row[6], row[7], row[8], row[9], month, day, row[i])
    
    print("Import US deaths data is completed.")

def dropTimeSeries():
    cdc_ts.drop()

def dropDXYTimeSeries():
    dxy_ts.drop()

def importDXYData():
    url = "https://raw.githubusercontent.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYArea.csv"
    response = manager.request('GET', url)
    time.sleep(10)
    reader = csv.reader(response.data.decode('utf-8').splitlines())
    if reader is None:
        print("DingXiangYuan CSV File Error")
        return
    title = next(reader)
    fieldDict = getFieldIndex(title)
    print(fieldDict)
    print("Import DXY data is starting")
    for row in reader:
        data = parseData(row, fieldDict)
        insertDXYData(data)

    print("Import DXY data is completed.")

# 由于网上的丁香园数据的字段位置一直在变化，编写一个字段和位置的解析器
def getFieldIndex(titleRow):
    fieldDict = {}
    for i in range(len(titleRow)):
        if titleRow[i] == 'countryEnglishName':
            fieldDict['countryName'] = i
        elif titleRow[i] == 'provinceEnglishName':
            fieldDict['provinceName'] = i
        elif titleRow[i] == 'province_zipCode':
            fieldDict['provinceZipCode'] = i
        elif titleRow[i] == 'province_confirmedCount':
            fieldDict['provinceConfirmed'] = i
        elif titleRow[i] == 'province_suspectedCount':
            fieldDict['provinceSuspected'] = i
        elif titleRow[i] == 'province_curedCount':
            fieldDict['provinceRecoveryed'] = i
        elif titleRow[i] == 'province_deadCount':
            fieldDict['provinceDeaths'] = i
        elif titleRow[i] == 'updateTime':
            fieldDict['updateTime'] = i
        elif titleRow[i] == 'cityEnglishName':
            fieldDict['cityName'] = i
        elif titleRow[i] == 'city_zipCode':
            fieldDict['cityZipCode'] = i
        elif titleRow[i] == 'city_confirmedCount':
            fieldDict['cityConfirmed'] = i
        elif titleRow[i] == 'city_suspectedCount':
            fieldDict['citySuspected'] = i
        elif titleRow[i] == 'city_curedCount':
            fieldDict['cityRecoveryed'] = i
        elif titleRow[i] == 'city_deadCount':
            fieldDict['cityDeaths'] = i
    return fieldDict

# Parse the csv row line to data
def parseData(csvRow, fieldDict):
    COUNTRY_NAME = fieldDict.get("countryName", 0)
    PROVINCE_NAME = fieldDict.get("provinceName", 0)
    PROVINCE_ZIPCODE = fieldDict.get("provinceZipCode", 0)
    PROVINCE_CONFIRMED = fieldDict.get("provinceConfirmed", 0)
    PROVINCE_SUSPECTED = fieldDict.get("provinceSuspected", 0)
    PROVINCE_RECOVERYED = fieldDict.get("provinceRecoveryed", 0)
    PROVINCE_DEATHS = fieldDict.get("provinceDeaths", 0)
    CITY_NAME = fieldDict.get("cityName", 0)
    CITY_ZIPCODE = fieldDict.get("cityZipCode", 0)
    CITY_CONFIRMED = fieldDict.get("cityConfirmed", 0)
    CITY_SUSPECTED = fieldDict.get("citySuspected", 0)
    CITY_RECOVERYED = fieldDict.get("cityRecoveryed", 0)
    CITY_DEATHS = fieldDict.get("cityDeaths", 0)
    UPDATE_TIME = fieldDict.get("updateTime", 0)
    if csvRow is None or len(csvRow) < 19:
        return None
    if csvRow[PROVINCE_CONFIRMED] == '':
        csvRow[PROVINCE_CONFIRMED] = 0
    if csvRow[PROVINCE_SUSPECTED] == '':
        csvRow[PROVINCE_SUSPECTED] = 0
    if csvRow[PROVINCE_RECOVERYED] == '':
        csvRow[PROVINCE_RECOVERYED] = 0
    if csvRow[PROVINCE_DEATHS] == '':
        csvRow[PROVINCE_DEATHS] = 0
    if csvRow[CITY_CONFIRMED] == '':
        csvRow[CITY_CONFIRMED] = 0
    if csvRow[CITY_SUSPECTED] == '':
        csvRow[CITY_SUSPECTED] = 0
    if csvRow[CITY_RECOVERYED] == '':
        csvRow[CITY_RECOVERYED] = 0
    if csvRow[CITY_DEATHS] == '':
        csvRow[CITY_DEATHS] = 0
    
    updateDate = ''
    try:
        if csvRow[UPDATE_TIME] != '':
            updateDate = datetime.fromisoformat(csvRow[UPDATE_TIME])
        else:
            updateDate = datetime.utcnow()
    except ValueError:
        updateDate = datetime.utcnow()
    
    data = {
        "country": csvRow[COUNTRY_NAME],
        "province": csvRow[PROVINCE_NAME],
        "provinceZipCode": csvRow[PROVINCE_ZIPCODE],
        "provinceConfirmed": csvRow[PROVINCE_CONFIRMED],
        "provinceSuspected": csvRow[PROVINCE_SUSPECTED],
        "provinceRecoveryed": csvRow[PROVINCE_RECOVERYED],
        "provinceDeaths": csvRow[PROVINCE_DEATHS],
        "city": csvRow[CITY_NAME],
        "cityZipCode": csvRow[CITY_ZIPCODE],
        "cityConfirmed": csvRow[CITY_CONFIRMED],
        "citySuspected": csvRow[CITY_SUSPECTED],
        "cityRecoveryed": csvRow[CITY_RECOVERYED],
        "cityDeaths": csvRow[CITY_DEATHS],
        "updateDate": updateDate,
    }
    return data

def insertDXYData(data):
    if data is None:
        return False
    date = data.get("updateDate", "")
    
    start = datetime(date.year, date.month, date.day, 0, 0, 0)
    end = datetime(date.year, date.month, date.day, 23, 59, 59)

    doc = dxy_ts.find_one({"$and": [{"updateDate": {'$gte': start, '$lt': end}},
                                {"province": {"$eq": data.get("province", "")}},
                                {"country": {"$eq": data.get("country", "")}},
                                {"provinceZipCode": {"$eq": data.get("provinceZipCode", "")}},
                                {"city": {"$eq": data.get("city", "")}},
                                {"cityZipCode": {"$eq": data.get("cityZipCode", "")}},
                                ]})
    if doc is None:
        dxy_ts.insert_one(data)
        return True
    else:
        
        return False

if __name__ == '__main__':
    dropTimeSeries()
    importGlobalConfirmedData()
    importGlobalDeathData()
    importGlobalRecoveryData()
    importUSConfirmedData()
    importUSDeathsData()
    dropDXYTimeSeries()
    importDXYData()
    
    
