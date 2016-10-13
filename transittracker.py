import requests
import subway_pb2
import gtfs_realtime_pb2
import sqlite3
import apiKey
import datetime 
from tarfile import TarFile
import io
import time




def download_historical_data(start_date, end_date, dbconn):
    date_to_fetch = start_date
    while date_to_fetch < end_date:
        data = fetch_data_from_archive(date_to_fetch, dbconn)
        if data is not None:
            save_gtfs_from_tar(data, dbconn)
        date_to_fetch = date_to_fetch + datetime.timedelta(days = 1)

def fetch_data_from_archive(date, dbconn):
    try:
        url = "http://data.mytransit.nyc.s3.amazonaws.com/subway_time/{year}/{year}-{month:0>2}/subway_time_{year}{month:0>2}{day:0>2}.tar.xz"
        urlf = url.format(year = date.year, month = date.month, day = date.day)
        r = requests.get(urlf)
        print(urlf, r.status_code)
        return r.content
    except:
        print("error Fetching GTFS feed for ", date)
        return None

def save_gtfs_from_tar(data, dbconn):
    io_bytes = io.BytesIO(data)
    tar = TarFile.open(fileobj=io_bytes, mode='r')
    count = len(tar.getmembers())
    for i, member in enumerate(tar.getmembers()):
        print(member, " out of ", count)
        iobuf = tar.extractfile(member=member)
        try:

            t0 = time.time()
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(iobuf.read())
            t1 = time.time()
            save_feed_data(feed_message, dbconn)
            t2 = time.time()
            print (t1-t0, t2-t1)

        except DecodeError:
            print("Could not decode feed message")

def save_feed_data(feed_message, dbconn):
    for entity in feed_message.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            for stu in trip_update.stop_time_update:
                if stu.HasField("arrival") and stu.HasField("departure"):
                    statement = "INSERT OR IGNORE INTO stops VALUES (\"{routeid}\", \"{trainid}\", \"{direction}\", \"{tripid}\", \"{stopid}\", {arrivalTime}, {departureTime});"
                    fstatement = statement.format(routeid=trip_update.trip.route_id,
                                                  trainid=trip_update.trip.Extensions[subway_pb2.nyct_trip_descriptor].train_id, 
                                                  direction=trip_update.trip.Extensions[subway_pb2.nyct_trip_descriptor].direction,
                                                  tripid=trip_update.trip.trip_id,
                                                  stopid=stu.stop_id,
                                                  arrivalTime=stu.arrival.time,
                                                  departureTime=stu.departure.time)
                    dbconn.execute(fstatement)
                dbconn.commit()

#nop if exists, returns connection
def create_database():
    c = sqlite3.connect('stops.db')
    cursor = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stops';")
    f = cursor.fetchone()
    if f is None:
        c.execute('''CREATE TABLE stops (routeid text, trainid text, direction text, tripid text, stopid text, arrivalTime int, departureTime int, PRIMARY KEY(routeid, trainid, direction, tripid, stopid))''')
        c.commit()
    return c


if __name__ == "__main__":
    dbconn = create_database() 
    start_date = datetime.date(2016, 1, 31)
    end_date = datetime.date(2016, 2, 1)
    download_historical_data(start_date, end_date, dbconn)  
