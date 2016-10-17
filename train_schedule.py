import subway_pb2
import gtfs_realtime_pb2
import os
from datetime import datetime, timedelta
import sqlite3
import multiprocessing as mp
from multiprocessing import Process
from pprint import pprint as pp
import queue
from google.protobuf.message import DecodeError
import re
      
def parse_proto(file):
    with open(file,'rb') as f:
        try:
            print("parsing", f)
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(f.read())
            return feed_message
        except DecodeError as e:
            return None
        except UnicodeDecodeError:
            return None

#take a protobuf, returns a tuple of  
#(routeid, direction, tripid, stationid, arrivaltime, departuretime)
#for each of the stops in the message
#these are the only things we care about right now
def proto_to_tuple_list(protobuf):
    tuple_list= []
    for entity in protobuf.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            for stu in trip_update.stop_time_update:
                if stu.HasField("arrival") and stu.HasField("departure"):
                    strip_id =  strip_stop_pattern_from_tripid(trip_update.trip.trip_id)
                    tup = (trip_update.trip.route_id,
                              trip_update.trip.Extensions[subway_pb2.nyct_trip_descriptor].direction,
                              strip_id,
                              stu.stop_id,
                              stu.arrival.time,
                              stu.departure.time)
                    tuple_list.append(tup)
    return tuple_list

#nyct gtfs realtime trip ids have the following general format (via regex)
#(?P<origin_time>\d+)_(?P<route_id>\w)\.\.(?P<direction>\w)(?P<path_identifier>\w+)
# origin time, route id and direction are enough to uniquely idenfiy a trip
# the path identifier is extraneous, and may dissapear during a trip
# to prevent this from making a single trip appear as many in our db, we will strip it out
def strip_stop_pattern_from_tripid(trip_id):
    try:
        regex = r"(\d+_\w?\.\.\w)\w*"
        m = re.match(regex, trip_id)
        return m.group(1)
    except:
        print(trip_id)

def parse_protoQ(protoQ):
    dbconn = get_db_conn()
    while True:
        try:
            protobuf = protoQ.get(block=False)
            p = parse_proto(protobuf)
            if p is not None:
                list_of_stop_tuples = proto_to_tuple_list(p)
                save_to_db(list_of_stop_tuples, dbconn)
            protoQ.task_done()
        except queue.Empty:
            #if we ever fail to get here it means the whole queue is empty
            #because all of the items are pre-populated
            return

def save_to_db(list_of_stop_tuples, dbconn):
    stations = set([tup[3] for tup in list_of_stop_tuples])
    trips = set([tup[2] for tup in list_of_stop_tuples])
    insert_new_trips(trips, dbconn)
    insert_new_stations(stations, dbconn)
    insert_new_stops(list_of_stop_tuples, dbconn)
    dbconn.commit()

def insert_new_stops(stops, dbconn):
    statement = '''INSERT OR IGNORE INTO stops (routeid,
                                                direction,
                                                tripid,
                                                stationid,
                                                arrivaltime,
                                                departuretime) 
                                            VALUES (?, ?, ?, ?, ?, ?);'''
    dbconn.executemany(statement, stops)

def insert_new_trips(new_trips, dbconn):
    insert_trips = [(val,) for val in new_trips]
    dbconn.executemany("INSERT OR IGNORE INTO trips (tripid) VALUES (?)", insert_trips)

def insert_new_stations(new_stations, dbconn):
    insert_stations = [(val,) for val in new_stations]
    dbconn.executemany("INSERT OR IGNORE INTO stations (stationid) VALUES (?)", insert_stations)


def parse_and_save(dbconn, file_list):
    #read all the files into the queue
    protoQ = mp.JoinableQueue()
    for p in plist:
        protoQ.put(p)
    procs = []
    for i in range(10):
        proc = Process(target = parse_protoQ, args=(protoQ,))
        procs.append(proc)   
        proc.start()
    protoQ.join()
    for proc in procs:
        proc.join()

def get_db_conn():
    return sqlite3.connect('stops.db')

#nop if exists
def create_database():
    c = get_db_conn()
    cursor = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stops';")
    f = cursor.fetchone()
    if f is None:
        c.execute('''CREATE TABLE trips (id INTEGER PRIMARY KEY,
                                          tripid TEXT UNIQUE)''')
        c.execute('''CREATE TABLE stations (id INTEGER PRIMARY KEY ,
                                             stationid TEXT UNIQUE)''')
        c.execute('''CREATE TABLE stops (routeid f,
                                        direction TEXT,
                                        tripid INTEGER,
                                        stationid INTEGER,
                                        arrivalTime INTEGER,
                                        departureTime INTEGER,
                                        FOREIGN KEY(tripid) REFERENCES trips(id),
                                        FOREIGN KEY(stationid) REFERENCES stations(id),
                                        PRIMARY KEY(routeid, direction, stationid, tripid))''')
        c.commit()
            
def read_proto_list(dir):
    list = sorted(os.listdir(dir))
    file_list = [os.path.join(dir,f) for f in list]
    return file_list 

if __name__ == "__main__":
    #calculate data over time
    conn = create_database()
    dir  = "E:\\subway data\\sep\\"
    plist = read_proto_list(dir)
    parse_and_save(conn, plist)
    

