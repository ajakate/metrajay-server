# from IPython import embed
from flask import Flask, send_file, jsonify, request
from flask_caching import Cache
import flask_httpauth as fhtp
from dotenv import load_dotenv
import os
from requests.auth import HTTPBasicAuth
import requests
import datetime
import database

load_dotenv()

basic = HTTPBasicAuth(os.getenv('METRA_USERNAME'), os.getenv('METRA_PASSWORD'))
basic_username = os.getenv('BASIC_AUTH_USERNAME')
basic_password = os.getenv('BASIC_AUTH_PASSWORD')
fly_app_name = os.getenv('FLY_APP_NAME')

app = Flask(__name__)
cache = Cache(app,config={'CACHE_TYPE': 'simple'})

auth = fhtp.HTTPBasicAuth()

def call_endpoint(endpoint):
    return requests.get(f'https://gtfsapi.metrarail.com/gtfs/raw{endpoint}', auth=basic)

def parse_date(date_str):
    return datetime.datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")

@cache.cached(timeout=0)
def grab_path_data():
    return jsonify(database.get_paths())

def pull_file():
    r = call_endpoint('/schedule.zip')
    with open('data/schedule.zip', "wb") as file:
        file.write(bytearray(r.content))

    database.load_data('data/schedule.zip')
    cache.clear()
    with app.app_context():
        grab_path_data()

    r = call_endpoint('/published.txt')
    with open('data/last_update.txt', "w") as file:
        file.write(r.text)

def refresh_file():
    with open("data/last_update.txt", "r") as file:
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
    return send_file('data/schedule.zip')

@app.route('/refresh')
@auth.login_required
def refresh():
    return refresh_file()

@app.route('/')
def home():
    return "OK"

@app.route('/paths')
@auth.login_required
def paths():
    return grab_path_data()

# TODO: memoize
@app.route('/stops', methods=['GET'])
def stops():
    args = request.args
    d = database.get_stops(args['stop1'],args['stop2'])
    return jsonify(d)

def register_cron():
    refresh_url = f"https://{fly_app_name}.fly.dev/refresh"
    basic_auth = f"{basic_username}:{basic_password}"
    os.system(f"bash set_keepalive.sh {refresh_url} {basic_auth}")
    
if __name__ == '__main__':
    if fly_app_name:
        register_cron()
    pull_file()
    app.run(host='0.0.0.0', port=8080)
