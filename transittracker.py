import requests
import subway_pb2
import gtfs_realtime_pb2
import sqlite3
import apiKey

def fetch_data():
    try:
        print("Fetching GTFS feed")
        api_key = apiKey.api_key

        endpoint = "http://datamine.mta.info/mta_esi.php?key={api_key}&feed_id=1"
        endpoint_format = endpoint.format(api_key = api_key)
        print(endpoint_format)
        
        r = requests.get(endpoint_format)
        print(r.status_code, len(r.text))
        
        feed_message = gtfs_realtime_pb2.FeedMessage()
        feed_message.ParseFromString(r.content)
        print("GTFS feed fetched succesfully")
        return feed_message
    except:
        print("error Fetching GTFS feed")
        return None
    
def save_feed_data(dbconn, feed_message):
    for entity in feed_message.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            if len(trip_update.stop_time_update) != 0:
                for stu in trip_update.stop_time_update:
                    if stu.HasField("arrival") and stu.HasField("departure"):
                        statement = "INSERT OR REPLACE INTO stops VALUES (\"{routeid}\", \"{trainid}\", \"{direction}\", \"{tripid}\", \"{stopid}\", {arrivalTime}, {departureTime});"
                        fstatement = statement.format(routeid=trip_update.trip.route_id,
                                                      trainid=trip_update.trip.Extensions[subway_pb2.nyct_trip_descriptor].train_id, 
                                                      direction=trip_update.trip.Extensions[subway_pb2.nyct_trip_descriptor].direction,
                                                      tripid=trip_update.trip.trip_id,
                                                      stopid=stu.stop_id,
                                                      arrivalTime=stu.arrival.time,
                                                      departureTime=stu.departure.time)
                        dbconn.execute(fstatement)
                dbconn.commit()

                

def poll_and_record(dbconn):
    feed_message = fetch_data()
    if feed_message is not None:
        save_feed_data(dbconn, feed_message)

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
    poll_and_record(dbconn)
