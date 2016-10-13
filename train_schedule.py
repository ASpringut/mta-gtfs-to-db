import subway_pb2
import gtfs_realtime_pb2
import os
from datetime import datetime, timedelta

def read_proto(dir):
    list = os.listdir(dir)[:10]
    for f in list:
        with open(os.path.join(dir, f),'rb') as file:
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(file.read())
            print(datetime.now())

if __name__ == "__main__":
    #calculate data over time
        
    dir  = "E:\\subway data\\sep\\"
    time_size = read_proto(dir)
    
