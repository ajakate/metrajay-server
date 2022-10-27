from multiprocessing.resource_sharer import stop
import sqlite3
import zipfile
import csv

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

database_name = 'data/metra.db'

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
                data.append(vals)
            qs = ['?'] * len(v)
            cur.executemany(f"INSERT INTO {k} ({','.join(v)}) VALUES({','.join(qs)})", data)
            con.commit()
    cur.close()
    con.close()
    get_paths()
