from flask import Flask, send_file
import flask_httpauth as fhtp
from dotenv import load_dotenv
import os
from requests.auth import HTTPBasicAuth
import requests
import datetime

load_dotenv()

basic = HTTPBasicAuth(os.getenv('METRA_USERNAME'), os.getenv('METRA_PASSWORD'))
basic_username = os.getenv('BASIC_AUTH_USERNAME')
basic_password = os.getenv('BASIC_AUTH_PASSWORD')

app = Flask(__name__)
auth = fhtp.HTTPBasicAuth()

def call_endpoint(endpoint):
    return requests.get(f'https://gtfsapi.metrarail.com/gtfs/raw{endpoint}', auth=basic)

def parse_date(date_str):
    return datetime.datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")

def pull_file():
    r = call_endpoint('/schedule.zip')
    with open('schedule.zip', "wb") as file:
        file.write(bytearray(r.content))

    r = call_endpoint('/published.txt')
    with open('last_update.txt', "w") as file:
        file.write(r.text)

def refresh_file():
    with open("last_update.txt", "r") as file:
        local_date = parse_date(file.read())
    
    r = call_endpoint('/published.txt')
    server_date = parse_date(r.text)

    if local_date < server_date:
        pull_file()
        return "Updated from server"
    else:
        return "No update found, returning..."


@auth.verify_password
def verify_password(username, password):
    if (username == basic_username) and (password == basic_password):
        return True

@app.route('/bundled_data')
@auth.login_required
def zipped_data():
    return send_file('schedule.zip')

@app.route('/refresh')
@auth.login_required
def refresh():
    return refresh_file()
 
if __name__ == '__main__':
    pull_file()
    app.run()
