#!/usr/bin/env python3
# problem1/load_data.py
import argparse, csv, os, sys
import psycopg2

def connect(args):
    conn = psycopg2.connect(
        host=args.host, dbname=args.dbname, user=args.user, password=args.password, port=args.port
    )
    conn.autocommit = False
    return conn

def run_sql(conn, sql_text):
    with conn.cursor() as cur:
        cur.execute(sql_text)

def load_lines(conn, path):
    with conn.cursor() as cur, open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        count=0
        for row in rdr:
            cur.execute(
                "INSERT INTO lines(line_name, vehicle_type) VALUES (%s,%s) ON CONFLICT (line_name) DO NOTHING",
                (row['line_name'], row['vehicle_type'])
            )
            count += 1
    return count

def load_stops(conn, path):
    with conn.cursor() as cur, open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        seen=set()
        count=0
        for row in rdr:
            name = row['stop_name']
            if name in seen:
                continue
            seen.add(name)
            cur.execute(
                "INSERT INTO stops(stop_name, latitude, longitude) VALUES (%s,%s,%s) ON CONFLICT (stop_name) DO NOTHING",
                (name, row.get('latitude'), row.get('longitude'))
            )
            count += 1
    return count

def fetch_map(conn, table, key_col, val_col):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {key_col}, {val_col} FROM {table}")
        return {row[0]: row[1] for row in cur.fetchall()}

def load_line_stops(conn, path, line_map, stop_map):
    with conn.cursor() as cur, open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        count=0
        for row in rdr:
            line_id = line_map.get(row['line_name'])
            stop_id = stop_map.get(row['stop_name'])
            if line_id is None or stop_id is None:
                raise ValueError(f"Missing FK for line={row['line_name']} stop={row['stop_name']}")
            cur.execute(
                """
                INSERT INTO line_stops(line_id, stop_id, sequence, time_offset_minutes)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (line_id, sequence) DO NOTHING
                """,
                (line_id, stop_id, int(row['sequence']), int(row['time_offset']))
            )
            count += 1
    return count

def load_trips(conn, path, line_map):
    with conn.cursor() as cur, open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        count=0
        for row in rdr:
            line_id = line_map.get(row['line_name'])
            if line_id is None:
                raise ValueError(f"Missing line_id for {row['line_name']}")
            cur.execute(
                "INSERT INTO trips(trip_id, line_id, scheduled_departure, vehicle_id) VALUES (%s,%s,%s,%s) ON CONFLICT (trip_id) DO NOTHING",
                (row['trip_id'], line_id, row['scheduled_departure'], row['vehicle_id'])
            )
            count += 1
    return count

def load_stop_events(conn, path, stop_map):
    with conn.cursor() as cur, open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        count=0
        for row in rdr:
            stop_id = stop_map.get(row['stop_name'])
            if stop_id is None:
                raise ValueError(f"Missing stop_id for {row['stop_name']}")
            cur.execute(
                """
                INSERT INTO stop_events(trip_id, stop_id, scheduled, actual, passengers_on, passengers_off)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (row['trip_id'], stop_id, row['scheduled'], row['actual'], int(row['passengers_on']), int(row['passengers_off']))
            )
            count += 1
    return count

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', required=True)
    ap.add_argument('--dbname', required=True)
    ap.add_argument('--user', required=True)
    ap.add_argument('--password', required=True)
    ap.add_argument('--port', default=5432, type=int)
    ap.add_argument('--datadir', required=True, help='Path to folder with CSVs (lines.csv, stops.csv, line_stops.csv, trips.csv, stop_events.csv)')
    args = ap.parse_args()

    print(f"Connected target: {args.dbname}@{args.host}:{args.port}")
    conn = connect(args)
    try:
        with open('schema.sql','r', encoding='utf-8') as f:
            print("Creating schema...")
            run_sql(conn, f.read())

        print("Loading data...")
        n_lines = load_lines(conn, os.path.join(args.datadir, 'lines.csv'))
        n_stops = load_stops(conn, os.path.join(args.datadir, 'stops.csv'))
        line_map = fetch_map(conn, 'lines', 'line_name', 'line_id')
        stop_map = fetch_map(conn, 'stops', 'stop_name', 'stop_id')

        n_line_stops = load_line_stops(conn, os.path.join(args.datadir, 'line_stops.csv'), line_map, stop_map)
        n_trips = load_trips(conn, os.path.join(args.datadir, 'trips.csv'), line_map)
        n_events = load_stop_events(conn, os.path.join(args.datadir, 'stop_events.csv'), stop_map)

        conn.commit()
        total = n_lines + n_stops + n_line_stops + n_trips + n_events
        print(f"Loaded rows: lines={n_lines}, stops={n_stops}, line_stops={n_line_stops}, trips={n_trips}, stop_events={n_events}")
        print(f"Total: {total} rows")
    except Exception as e:
        conn.rollback()
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
