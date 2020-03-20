from flask import Flask, jsonify, request
from flask_cors import CORS


from datetime import datetime, date
import pymongo
from pymongo import MongoClient

mongo_client = MongoClient('127.0.0.1:27017',
                            username='ruser',
                            password='flzx3qc',
                            authSource='COVID19-DB',
                            authMechanism='SCRAM-SHA-256')

db = mongo_client["COVID19-DB"]

cdc_ts = db["CDC-TimeSeries"]
dxy_ts = db["DXY-TimeSeries"]

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return "Corona Data Server"

@app.route('/data', methods=['GET', 'POST'])
def importCSV():
    content = request.get_json()
    country = content.get("country", "")
    province = content.get("province", "")
    print("Country: " + country + " Province: " + province)
    dateList = []
    confirmedList = []
    deathsList = []
    recoveredList = []
    if country == 'China':
        nameList = province.split(' ', 1)
        city = ''
        provinceName = ''
        if len(nameList) >= 2:
            provinceName = nameList[0]
            city = nameList[1]
        
        docs = dxy_ts.find({"$and": [{"city": {"$eq": city}},
                                {"province": {"$eq": provinceName}}]}).sort("updateDate", pymongo.ASCENDING)
        for doc in docs:
            update_date = doc.get("updateDate")
            date_str = update_date.date().isoformat()
            dateList.append(date_str)
            confirmedList.append(doc.get("cityConfirmed", 0))
            deathsList.append(doc.get("cityDeaths", 0))
            recoveredList.append(doc.get("cityRecoveryed", 0))
    else:
        docs = cdc_ts.find({"$and": [{"Country/Region": {"$eq": country}},
                                    {"Province/State": {"$eq": province}}]}).sort("Date", pymongo.ASCENDING)
        for doc in docs:
            update_date = doc.get("Date")
            date_obj = update_date.date()
            date_str = date_obj.isoformat()
            dateList.append(date_str)
            confirmedList.append(doc.get("Confirmed"))
            deathsList.append(doc.get("Death"))
            recoveredList.append(doc.get("Recovery"))
    
    node = {
        'country': country,
        'province': province,
        'confirmed': confirmedList,
        'deaths': deathsList,
        'recovered': recoveredList,
        'date': dateList,
    }
    return jsonify(node)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')