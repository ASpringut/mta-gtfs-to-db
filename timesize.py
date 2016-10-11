import os
from datetime import datetime, timedelta
import pytz
import matplotlib.pyplot as plt
from collections import defaultdict

#size in kilobytes
def time_size(dir):
    list = os.listdir(dir)
    return [(file_time(f), file_size(f, dir)/1000.0) for f in list]
    
#size in kilobytes grouped by hour
def time_size_by_hour(dir):
    list = os.listdir(dir)
    d = defaultdict(lambda: 0)
    for f in list:
        time = file_time(f)
        time = time.replace(minute = 0, second = 0)
        size = file_size(f, dir)/1000.0
        d[time] = d[time] + size
        
    return [(k,v) for k,v in sorted(d.items())]
    

def file_time(file_name):
    return datetime.strptime(file_name, "gtfs-%Y%m%dT%H%M%SZ")
    
def file_size(file_name, dir):
    return os.stat(dir+file_name).st_size

def get_start_of_day_labels():
    #we are in est
    tz = pytz.timezone('US/Eastern')
    start = datetime(2016,9,1,0,tzinfo = tz)
    end = datetime(2016,10,1,0,tzinfo = tz)
    dates = []
    labels = []
    while start < end:
        dates.append(start)
        labels.append(start.strftime("%b %d"))
        start = start + timedelta(days = 1)
    return dates, labels

    
if __name__ == "__main__":
    #calculate data over time
        
    dir  = "E:\\subway data\\sep\\"
    time_size = time_size(dir)
    #time_size = time_size_by_hour(dir)
    plt.plot([ts[0] for ts in time_size],[ts[1] for ts in time_size])
    plt.axes().xaxis_date(tz="EST")
    plt.axes().set_ylabel("kilobytes of data recorded")
    dates, labels = get_start_of_day_labels()
    plt.xticks(dates, labels, rotation=45)
    plt.subplots_adjust(bottom=0.15)
    
    plt.show()