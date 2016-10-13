import subway_pb2
import gtfs_realtime_pb2
import os
from datetime import datetime, timedelta
import sqlite3
import multiprocessing as mp
from multiprocessing import Process
from pprint import pprint as pp
from google.protobuf.message import DecodeError

def read_proto_list(dir):
    list = os.listdir(dir)
    file_list = [os.path.join(dir,f) for f in list]
    return file_list
      
def parse_proto(file):
    with open(file,'rb') as f:
        try:
            print("parsing", f)
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(f.read())
            return feed_message
        except DecodeError as e:
            return None

        
def multiprocess_parse(file_list):
    #read all the files into the queue
    q = mp.Queue()
    for p in plist:
        q.put(p)
      
    p = mp.Pool(8)
    result = p.map(parse_proto, file_list)
    p.terminate()
    return result
    
def save_feed_data(feed_message, dbconn):
    for entity in feed_message.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            for stu in trip_update.stop_time_update:
                if stu.HasField("arrival") and stu.HasField("departure"):
                    statement = "INSERT OR IGNORE INTO stops VALUES (\"{routeid}\", \"{direction}\", \"{tripid}\", \"{stopid}\", {arrivalTime}, {departureTime});"
                    fstatement = statement.format(routeid=trip_update.trip.route_id,
                                                  direction=trip_update.trip.Extensions[subway_pb2.nyct_trip_descriptor].direction,
                                                  tripid=trip_update.trip.trip_id,
                                                  stopid=stu.stop_id,
                                                  arrivalTime=stu.arrival.time,
                                                  departureTime=stu.departure.time)
                    dbconn.execute(fstatement)
                
def parse_and_save(dbconn, file_list):
    feed_messages = multiprocess_parse(file_list)
    for i,message in enumerate(feed_messages):
        if message is not None:
            save_feed_data(message, dbconn)
            print("write file", i)
    dbconn.commit()
    print("done")

        
#nop if exists, returns connection
def create_database():
    c = sqlite3.connect('stops.db')
    cursor = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stops';")
    f = cursor.fetchone()
    if f is None:
        c.execute('''CREATE TABLE trips (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                         tripid TEXT)''')
        c.execute('''CREATE TABLE stations (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            stationid TEXT)''')
        c.execute('''CREATE TABLE stops (routeid f,
                                        direction TEXT,
                                        tripid INTEGER,
                                        stationid INTEGER,
                                        arrivalTime INTEGER,
                                        departureTime INTEGER,
                                        FOREIGN KEY(tripid) REFERENCES trips(id),
                                        FOREIGN KEY(stationid) REFERENCES stations(id),
                                        PRIMARY KEY(routeid, direction, stationid))''')
        c.commit()
    return c
            
            
if __name__ == "__main__":
    #calculate data over time
    conn = create_database()
    dir  = "E:\\subway data\\sep\\"
    exit()
    plist = read_proto_list(dir)[:100]
    parse_and_save(conn, plist)
    

