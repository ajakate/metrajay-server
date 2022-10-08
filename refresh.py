from dotenv import load_dotenv
import os
from requests.auth import HTTPBasicAuth
import requests

load_dotenv()

basic = HTTPBasicAuth(os.getenv('BASIC_AUTH_USERNAME'), os.getenv('BASIC_AUTH_PASSWORD'))
fly_app_name = os.getenv('FLY_APP_NAME')

requests.get(f'https://{fly_app_name}.fly.dev/refresh', auth=basic)
