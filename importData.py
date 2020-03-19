from pymongo import MongoClient
import csv
import urllib3
from datetime import datetime

mongo_client = MongoClient('mongodb://127.0.0.1:27017')
# mongo_client = MongoClient('mongodb://%s:%s@127.0.0.1:27017' % ("root", "1234zlyc"))

db = mongo_client["COVID19-DB"]

cdc_ts = db["CDC-TimeSeries"]
dxy_ts = db["DXY-TimeSeries"]

def importConfirmedData():
    confirmedUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"
    manager = urllib3.PoolManager(3)
    response = manager.request('GET', confirmedUrl)

    reader = csv.reader(response.data.decode('utf-8').splitlines())
    if reader is None:
        print("CSV File Error")
        return
    # Read the title and the date 
    titleInfo = next(reader)
    #print(titleInfo)
    
    for row in reader:
        if len(row) <= 6:
            print("CSV File Structure Error")
            break
        for i in range(4, len(row)):
            month, day = getMonthAndDay(titleInfo, i)
            insertConfirmedData(row[0], row[1], row[2], row[3], month, day, row[i])
    
    print("Import data successful")

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
        print("Insert confirm data")
    

def importDeathData():
    dataUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"
    manager = urllib3.PoolManager(3)
    response = manager.request('GET', dataUrl)

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
            updateDeathData(row[0], row[1], row[2], row[3], month, day, row[i])

    

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

def importRecoveryData():
    dataUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"
    manager = urllib3.PoolManager(3)
    response = manager.request('GET', dataUrl)

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

def dropTimeSeries():
    # cdc_ts.drop()
    dxy_ts.drop()

def importDXYData():
    url = "https://raw.githubusercontent.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYArea.csv"
    manager = urllib3.PoolManager(3)
    response = manager.request('GET', url)

    reader = csv.reader(response.data.decode('utf-8').splitlines())
    if reader is None:
        print("DingXiangYuan CSV File Error")
        return
    title = next(reader)
    
    for row in reader:
        data = parseData(row)
        insertDXYData(data)
    
# Parse the csv row line to data
def parseData(csvRow):
    if csvRow is None or len(csvRow) < 19:
        return None
    if csvRow[7] == '':
        csvRow[7] = 0
    if csvRow[8] == '':
        csvRow[8] = 0
    if csvRow[9] == '':
        csvRow[9] = 0
    if csvRow[10] == '':
        csvRow[10] = 0
    if csvRow[15] == '':
        csvRow[15] = 0
    if csvRow[16] == '':
        csvRow[16] = 0
    if csvRow[17] == '':
        csvRow[17] = 0
    if csvRow[18] == '':
        csvRow[18] = 0
    updateDate = datetime.strptime(csvRow[11], "YYYY-MM-DD HH:MM:SS")
    #updateDate = datetime.fromisoformat(csvRow[11])
    data = {
        "country": csvRow[3],
        "province": csvRow[5],
        "provinceZipCode": csvRow[6],
        "provinceConfirmed": csvRow[7],
        "provinceSuspected": csvRow[8],
        "provinceRecoveryed": csvRow[9],
        "provinceDeaths": csvRow[10],
        "updateDate": updateDate,
        "city": csvRow[13],
        "cityZipCode": csvRow[14],
        "cityConfirmed": csvRow[15],
        "citySuspected": csvRow[16],
        "cityRecoveryed": csvRow[17],
        "cityDeaths": csvRow[18]
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
        print("dxy have the record already")
        return False

if __name__ == '__main__':
    dropTimeSeries()
    #importConfirmedData()
    #importDeathData()
    #importRecoveryData()
    importDXYData()
    
    
