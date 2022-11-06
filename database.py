from IPython import embed
import sqlite3
import zipfile
import csv
import re
import pandas as pd
from datetime import timedelta, date

recreate_db_q = """
DROP TABLE IF EXISTS calendar_dates;
CREATE TABLE calendar_dates (
    service_id text,
    date text,
    exception_type INTEGER
);
CREATE INDEX cd_sid
ON calendar_dates(service_id);

DROP TABLE IF EXISTS calendar;
CREATE TABLE calendar (
    service_id text,
    monday boolean,
    tuesday boolean,
    wednesday boolean,
    thursday boolean,
    friday boolean,
    saturday boolean,
    sunday boolean,
    start_date text,
    end_date text
);
CREATE INDEX c_sid
ON calendar(service_id);

DROP TABLE IF EXISTS routes;
CREATE TABLE routes (
    route_id text,
    route_short_name text,
    route_long_name text
);
CREATE INDEX r_rid
ON routes(route_id);

DROP TABLE IF EXISTS stop_times;
CREATE TABLE stop_times (
    trip_id text,
    arrival_time text,
    departure_time text,
    stop_id text,
    stop_sequence INTEGER
);
CREATE INDEX st_tid
ON stop_times(trip_id);
CREATE INDEX st_sid
ON stop_times(stop_id);

DROP TABLE IF EXISTS stops;
CREATE TABLE stops (
    stop_id text,
    stop_name text
);
CREATE INDEX s_sid
ON stops(stop_id);
CREATE INDEX s_sn
ON stops(stop_name);

DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
    route_id text,
    service_id text,
    trip_id text,
    trip_headsign text,
    direction_id boolean
);
CREATE INDEX t_rid
ON trips(route_id);
CREATE INDEX t_sid
ON trips(service_id);
CREATE INDEX t_tid
ON trips(trip_id);
"""

get_paths_q = """
select s.stop_id, s.stop_name, t.route_id, group_concat(distinct stop_sequence) as stop_sequence
from stop_times st
join stops s on st.stop_id=s.stop_id
join trips t on t.trip_id=st.trip_id
where t.direction_id = 0
group by s.stop_id, t.route_id;
"""

def get_sched_q(stop1, stop2):
    return f"""
select

st.arrival_time time1,
st.stop_id station1,
st.stop_sequence seq1,
t.route_id,
t.direction_id,
st2.arrival_time time2,
st2.stop_id station2,
st.stop_sequence seq2,
c.*,
cd.date ex_date,
cd.exception_type ex_type

from stop_times st
join trips t on t.trip_id = st.trip_id
join stop_times st2 on st2.trip_id = t.trip_id
join calendar c on c.service_id=t.service_id
left join calendar_dates cd on cd.service_id=t.service_id
where st.stop_id = '{stop1}'
and st2.stop_id = '{stop2}'
and c.end_date >= Date('now')
and c.start_date < Date('now', '+40 days') -- TODO: maybe fix this line
order by t.service_id, direction_id, st.departure_time
;
    """


database_name = 'data/metra.db'
date_matcher = r'^(\d{4})(\d{2})(\d{2})$'

file_vals = {
    'calendar_dates': ['service_id', 'date', 'exception_type'],
    'calendar': ['service_id','monday','tuesday','wednesday','thursday','friday','saturday','sunday','start_date','end_date'],
    'routes': ['route_id', 'route_short_name','route_long_name'],
    'stop_times': ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence'],
    'stops': ['stop_id', 'stop_name'],
    'trips': ['route_id','service_id','trip_id','trip_headsign','direction_id']
}

def get_paths():
    con = sqlite3.connect(database_name)
    cur = con.cursor()
    cur.execute(get_paths_q)
    rows = cur.fetchall()
    cur.close()
    con.close()
    paths = {}
    for row in rows:
        stop_id = row[0]
        stop_name = row[1]
        route_id = row[2]
        stop_sequence = row[3]

        if stop_id in paths.keys():
            paths[stop_id]['routes'].append({'id': route_id, 'stop_sequence': stop_sequence})
        else:
            paths[stop_id] = {
                'name': stop_name,
                'routes': [{'id': route_id, 'stop_sequence': stop_sequence}]
            }
    return paths

def filter_date(v):
    match = re.search(date_matcher, v)
    if match:
        return f"{match[1]}-{match[2]}-{match[3]}"
    else:
        return v

def get_data_for_date(d, df):
    weekday = d.strftime('%A').lower()
    today = pd.Timestamp(d)
    valid_rows = df[df['start_date'] <= today][df['end_date'] >= today]

    return_data = []

    for i, row in valid_rows.iterrows():
        if (row['ex_type'] == 2) and (row['ex_date'] == today):
            continue
        if (row[weekday] == 1) or (row['ex_type'] == 1 and row['ex_date'] == today):
            return_data.append([row['time1'], row['time2'], row['direction_id']])

    if today in df['ex_date'].unique():
        sched_type = 'special'
    else:
        sched_type = 'standard'

    return [[d, d.strftime('%A').lower(), sched_type], sorted(return_data)]

def create_response(group):
    dates = group[0]
    parsed_dates = [ [d[0].strftime("%Y-%m-%d"), d[2] ] for d in dates ]
    schedule = group[1]
    inbound = []
    outbound = []
    for s in schedule:
        if s[2] == 0:
            outbound.append(sorted([s[0],s[1]]))
        else:
            inbound.append(sorted([s[1],s[0]]))
    
    return {
        'dates': parsed_dates,
        'inbound': inbound,
        'outbound': outbound
    }

def get_stops(stop1, stop2):
    dates =  [date.today() + timedelta(days=d) for d in range(7)]
    con = sqlite3.connect(database_name)
    df = pd.read_sql_query(get_sched_q(stop1,stop2), con, parse_dates=['start_date','end_date','ex_date'])
    con.close()

    data_by_date = [get_data_for_date(d, df) for d in dates]

    final = []

    for day,sched in data_by_date:
        if final == []:
            final.append([[day], sched])
            continue
        
        last_schedule = final[-1][1]
        if last_schedule == sched:
            final[-1][0].append(day)
            continue
        final.append([[day], sched])
    
    return_data = [create_response(sched_group) for sched_group in final]
    return return_data

def load_data(zip_path):
    with zipfile.ZipFile(zip_path,"r") as zip_ref:
        zip_ref.extractall("data/schedule")

    con = sqlite3.connect(database_name)
    cur = con.cursor()
    cur.executescript(recreate_db_q)
    con.commit()

    for k,v in file_vals.items():
        filename = f"data/schedule/{k}.txt"
        with open(filename) as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            data = []
            for row in reader:
                vals = [ row[val] for val in v]
                vals = [ filter_date(v) for v in vals ]
                data.append(vals)
            qs = ['?'] * len(v)
            cur.executemany(f"INSERT INTO {k} ({','.join(v)}) VALUES({','.join(qs)})", data)
            con.commit()
    cur.close()
    con.close()
