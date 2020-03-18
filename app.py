from flask import Flask

from pymongo import MongoClient

import csv
from urllib3 import PoolManager

app = Flask(__name__)

@app.route('/')
def home():
    return "Corona Data Server"

@app.route('/csv')
def importCSV():
    return "csv module Test"

if __name__ == '__main__':
    app.run(debug=True)