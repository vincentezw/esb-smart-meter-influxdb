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
import os
import re
import sys
from datetime import datetime

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


class ESBSmartMeterReader:
    json_file_path = "esb_readings.json"

    def __init__(
        self,
        config_file_path,
    ):
        logging.debug("reading config")
        config = configparser.ConfigParser()
        config.read(config_file_path)
        self.esb_meter_mprn = config.get("ESB", "meter_mprn")
        self.esb_username = config.get("ESB", "username")
        self.esb_password = config.get("ESB", "password")
        self.influx_host = config.get("InfluxDB", "host")
        self.influx_bucket = config.get("InfluxDB", "bucket")
        self.influx_organisation = config.get("InfluxDB", "organisation")
        self.influx_token = config.get("InfluxDB", "token")

    def load_smart_meter_stats_v2(self):
        # this parameter looks to be ignored by the ESB endpoint, which always returns *all* the data
        today_ = datetime.today()
        smart_meter_data = self.__load_esb_data(today_)
        if len(smart_meter_data) == 0:
            logging.info("No new data found")
            return

        self.__export_to_influx(smart_meter_data)
        logging.info("Import completed and JSON file generated")

    def __load_esb_data(self, start_date):
        logging.info("Loading ESB data for MPRN %s", self.esb_meter_mprn)
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
            data={
                "signInName": self.esb_username,
                "password": self.esb_password,
                "request_type": "RESPONSE",
            },
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

        logging.debug("[+] getting CSV file for MPRN ...")
        data = s.get(
            "https://myaccount.esbnetworks.ie/DataHub/DownloadHdf?mprn="
            + self.esb_meter_mprn
            + "&startDate="
            + start_date.strftime("%Y-%m-%d")
        )

        logging.debug("[+] CSV file received !!!")
        data_decoded = data.content.decode("utf-8").splitlines()
        logging.debug("[+] data decoded from Binary format")
        json_data = self.__csv_response_to_json(data_decoded)
        return json_data

    def __csv_response_to_json(self, csv_file):
        logging.debug("[+] creating JSON file from CSV ...")
        output_json = []
        existing_data = self.__get_previous_data()
        new_entries = []
        existing_entries = set(
            (entry["Read Date and End Time"] for entry in existing_data)
        )
        logging.debug("Found %s existing records in JSON file", len(existing_entries))

        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            output_json.append(row)
            unique_identifier = row["Read Date and End Time"]
            if unique_identifier not in existing_entries:
                new_entries.append(row)
                existing_entries.add(unique_identifier)
            else:
                print(row)

        logging.info(
            "Found %s new entries (%s entries downloaded)",
            len(new_entries),
            len(output_json),
        )

        with open(self.json_file_path, "w", encoding="utf-8") as jsonf:
            json_out = json.dumps(output_json, indent=2)
            jsonf.write(json_out)
            logging.debug("[+] JSON file created")
        return new_entries

    def __get_previous_data(self):
        existing_json_data = []

        if os.path.exists(self.json_file_path):
            with open(self.json_file_path, "r", encoding="utf-8") as existing_json_file:
                existing_json_data = json.load(existing_json_file)

        return existing_json_data

    def __export_to_influx(self, data):
        client = InfluxDBClient3(
            token=self.influx_token,
            host=self.influx_host,
            org=self.influx_organisation,
            database=self.influx_bucket,
        )

        total_entries = len(data)

        for i, entry in enumerate(data, 1):
            timestamp = datetime.strptime(
                entry["Read Date and End Time"], "%d-%m-%Y %H:%M"
            )
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


ESBSmartMeterReader(".secrets").load_smart_meter_stats_v2()
