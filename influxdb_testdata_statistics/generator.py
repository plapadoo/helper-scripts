#!/usr/bin/env python

import httplib
from time import time
import random

influxdb_host = "localhost"
influxdb_port = 8086
influxdb_user = ""
influxdb_password = "root"
influxdb_database = "opencast"
number_of_lines = 100000
# one year before
start_date = int(time()) - 86400 * 365
end_date = int(time())

input_data = {
    "tenant1": {"series1": ["event1", "event2"], "series2": ["event3", "event4"]},
    "tenant2": {"series3": ["event5", "event6"], "series4": ["event7", "event8"]},
}

http = httplib.HTTPConnection(influxdb_host, influxdb_port)


def generate_line(event, series, organization, time, count):
    return (
        "impressions_daily,episodeId="
        + event
        + ",organizationId="
        + str(organization)
        + ",seriesId="
        + series
        + " value="
        + str(count)
        + " "
        + str(time)
    )


def write_lines(line):
    auth_string = "&u=" + influxdb_user + "&p=" + influxdb_password

    if influxdb_user == "":
        auth_string = ""

    http.request(
        "POST", "/write?db=" + influxdb_database + "&precision=s" + auth_string, line
    )

    response = http.getresponse()
    response.read()

    if response.status / 200 != 1:
        raise Exception("HTTP status " + str(response.status))


def random_line(d, start_date, end_date):
    tenant = random.choice(d.keys())
    series = random.choice(d[tenant].keys())
    event = random.choice(d[tenant][series])
    t = int(random.uniform(start_date, end_date))
    count = random.uniform(1, 50)
    return generate_line(event, series, tenant, t, count)


def write_random_lines(d, start_date, end_date, bucket_size):
    lines = ""
    for i in range(0, bucket_size):
        lines += random_line(d, start_date, end_date) + "\n"
    write_lines(lines.strip())


random.seed()

bucket_size = 1000
write_random_lines(input_data, start_date, end_date, bucket_size)
# for i in range(0, number_of_lines // bucket_size):
#     write_random_lines(input_data, start_date, end_date, bucket_size)
