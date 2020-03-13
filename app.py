from flask import Flask

from pymongo import MongoClient

app = Flask(__name__)

@app.route('/')
def hello():
    return "Corona Data Server starting..."

if __name__ == '__main__':
    app.run()