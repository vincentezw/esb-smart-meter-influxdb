# #!/usr/bin/env python3

# Script to read smart meter data from ESB Networks
# https://gist.github.com/schlan/f72d823dd5c1c1d19dfd784eb392dded

# Modified by badger707
# https://github.com/badger707/esb-smart-meter-reading-automation

# forked by vincentezw to directly write to influxdb
# https://github.com/vincentezw/esb-smart-meter-influxdb

import argparse
import configparser
import csv
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
from influxdb_client_3 import InfluxDBClient3, Point

parser = argparse.ArgumentParser()
parser.add_argument("--debug", action="store_true", help="Enable debug mode")
args = parser.parse_args()
logging.basicConfig(format="%(message)s", level=logging.INFO, stream=sys.stdout)

if args.debug:
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Debug mode enabled.")

logging.debug("reading config")
config = configparser.ConfigParser()
config.read(".secrets")

esb_meter_mprn = config.get("ESB", "meter_mprn")
esb_username = config.get("ESB", "username")
esb_password = config.get("ESB", "password")
influx_host = config.get("InfluxDB", "host")
influx_bucket = config.get("InfluxDB", "bucket")
influx_organisation = config.get("InfluxDB", "organisation")
influx_token = config.get("InfluxDB", "token")


def load_esb_data(user, password, mpnr, start_date):
    logging.info("Loading ESB data for MPRN %s", mpnr)
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
        }
    )
    logging.debug("[+] calling login page. ..")
    login_page = s.get("https://myaccount.esbnetworks.ie/", allow_redirects=True)
    result = re.findall(r"(?<=var SETTINGS = )\S*;", str(login_page.content))
    settings = json.loads(result[0][:-1])
    logging.debug("[+] sending credentials ...")
    s.post(
        "https://login.esbnetworks.ie/esbntwkscustportalprdb2c01.onmicrosoft.com/B2C_1A_signup_signin/SelfAsserted?tx="
        + settings["transId"]
        + "&p=B2C_1A_signup_signin",
        data={"signInName": user, "password": password, "request_type": "RESPONSE"},
        headers={
            "x-csrf-token": settings["csrf"],
        },
        allow_redirects=False,
    )
    logging.debug("[+] passing AUTH ...")
    confirm_login = s.get(
        "https://login.esbnetworks.ie/esbntwkscustportalprdb2c01.onmicrosoft.com/B2C_1A_signup_signin/api/CombinedSigninAndSignup/confirmed",
        params={
            "rememberMe": False,
            "csrf_token": settings["csrf"],
            "tx": settings["transId"],
            "p": "B2C_1A_signup_signin",
        },
    )
    logging.debug("[+] confirm_login: ", confirm_login)
    logging.debug("[+] doing some BeautifulSoup ...")
    soup = BeautifulSoup(confirm_login.content, "html.parser")
    form = soup.find("form", {"id": "auto"})
    s.post(
        form["action"],
        allow_redirects=False,
        data={
            "state": form.find("input", {"name": "state"})["value"],
            "client_info": form.find("input", {"name": "client_info"})["value"],
            "code": form.find("input", {"name": "code"})["value"],
        },
    )

    # data = s.get('https://myaccount.esbnetworks.ie/datadub/GetHdfContent?mprn=' + mpnr + '&startDate=' + start_date.strftime('%Y-%m-%d'))
    logging.debug("[+] getting CSV file for MPRN ...")
    data = s.get(
        "https://myaccount.esbnetworks.ie/DataHub/DownloadHdf?mprn="
        + mpnr
        + "&startDate="
        + start_date.strftime("%Y-%m-%d")
    )

    logging.debug("[+] CSV file received !!!")
    data_decoded = data.content.decode("utf-8").splitlines()
    logging.debug("[+] data decoded from Binary format")
    json_data = csv_response_to_json(data_decoded)
    return json_data


def csv_response_to_json(csv_file):
    logging.debug("[+] creating JSON file from CSV ...")
    my_json = []
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        my_json.append(row)
    with open("json_data.json", "w", encoding="utf-8") as jsonf:
        json_out = json.dumps(my_json, indent=2)
        jsonf.write(json_out)
    logging.debug("[+] end of JSON OUT, returning value ...")
    return my_json


def parse_date(date_str):
    logging.debug("[+] parsing some data fields ...")
    if len(date_str) == 19:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    else:
        dt = datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S")
        tz_offset = int(date_str[-6:-3])
        tz = timezone(timedelta(hours=tz_offset))
        return dt.replace(tzinfo=tz)


def load_smart_meter_stats_v2(user, password, mpnr):
    # last_month = datetime.today() - timedelta(days=30)
    today_ = datetime.today()
    # smart_meter_data = load_esb_data(user, password, mpnr, last_month)
    smart_meter_data = load_esb_data(user, password, mpnr, today_)
    return smart_meter_data


client = InfluxDBClient3(
    token=influx_token,
    host=influx_host,
    org=influx_organisation,
    database=influx_bucket,
)

data = load_smart_meter_stats_v2(esb_username, esb_password, esb_meter_mprn)
total_entries = len(data)

for i, entry in enumerate(data, 1):
    timestamp = datetime.strptime(entry["Read Date and End Time"], "%d-%m-%Y %H:%M")
    point = (
        Point("meter_reading")
        .tag("MPRN", entry["MPRN"])
        .tag("MeterSerialNumber", entry["Meter Serial Number"])
        .tag("ReadType", entry["Read Type"])
        .field("reading", float(entry["Read Value"]))
        .time(timestamp)
    )
    client.write(point)
    sys.stdout.write(f"\rProcessing entry {i}/{total_entries}")
    sys.stdout.flush()

client.close()
print()
logging.info("Import completed and JSON file generated")
