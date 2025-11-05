#!/usr/bin/env python3
# problem1/queries.py
# Pretty-prints JSON by default (indent=2). Use --compact to output a single line.
import argparse, json, os, sys, time
import psycopg2

def connect(dbname):
    return psycopg2.connect(host=os.getenv('PGHOST','localhost'),
                            dbname=dbname,
                            user=os.getenv('PGUSER','postgres'),
                            password=os.getenv('PGPASSWORD','postgres'),
                            port=int(os.getenv('PGPORT','5432')))

def rows(cur):
    cols=[d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def q1(conn):
    sql = """
    SELECT s.stop_name, ls.sequence, ls.time_offset_minutes AS time_offset
    FROM line_stops ls
    JOIN lines l ON l.line_id = ls.line_id
    JOIN stops s ON s.stop_id = ls.stop_id
    WHERE l.line_name = 'Route 20'
    ORDER BY ls.sequence
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return rows(cur)

def q2(conn):
    sql = """
    SELECT t.trip_id, l.line_name, t.scheduled_departure
    FROM trips t
    JOIN lines l ON l.line_id = t.line_id
    WHERE (t.scheduled_departure::time) >= TIME '07:00' AND (t.scheduled_departure::time) < TIME '09:00'
    ORDER BY t.scheduled_departure
    """
    with conn.cursor() as cur:
        cur.execute(sql); return rows(cur)

def q3(conn):
    sql = """
    SELECT s.stop_name, COUNT(DISTINCT l.line_id) AS line_count
    FROM line_stops ls
    JOIN lines l ON l.line_id = ls.line_id
    JOIN stops s ON s.stop_id = ls.stop_id
    GROUP BY s.stop_name
    HAVING COUNT(DISTINCT l.line_id) >= 2
    ORDER BY line_count DESC, s.stop_name
    """
    with conn.cursor() as cur:
        cur.execute(sql); return rows(cur)

def q4(conn):
    trip = os.getenv('TRIP_ID','T0001')
    sql = """
    SELECT se.trip_id, s.stop_name, se.scheduled, se.actual
    FROM stop_events se
    JOIN stops s ON s.stop_id = se.stop_id
    WHERE se.trip_id = %s
    ORDER BY se.scheduled
    """
    with conn.cursor() as cur:
        cur.execute(sql, (trip,)); return rows(cur)

def q5(conn):
    sql = """
    SELECT DISTINCT l.line_name
    FROM lines l
    JOIN line_stops ls1 ON ls1.line_id = l.line_id
    JOIN stops s1 ON s1.stop_id = ls1.stop_id AND s1.stop_name = 'Wilshire / Veteran'
    WHERE EXISTS (
        SELECT 1
        FROM line_stops ls2
        JOIN stops s2 ON s2.stop_id = ls2.stop_id
        WHERE ls2.line_id = l.line_id AND s2.stop_name = 'Le Conte / Broxton'
    )
    ORDER BY l.line_name
    """
    with conn.cursor() as cur:
        cur.execute(sql); return rows(cur)

def q6(conn):
    sql = """
    SELECT l.line_name, ROUND(AVG(se.passengers_on)::numeric, 2) AS avg_passengers
    FROM stop_events se
    JOIN trips t ON t.trip_id = se.trip_id
    JOIN lines l ON l.line_id = t.line_id
    GROUP BY l.line_name
    ORDER BY avg_passengers DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql); return rows(cur)

def q7(conn):
    sql = """
    SELECT s.stop_name,
           SUM(se.passengers_on + se.passengers_off) AS total_activity
    FROM stop_events se
    JOIN stops s ON s.stop_id = se.stop_id
    GROUP BY s.stop_name
    ORDER BY total_activity DESC, s.stop_name
    LIMIT 10
    """
    with conn.cursor() as cur:
        cur.execute(sql); return rows(cur)

def q8(conn):
    sql = """
    SELECT l.line_name, COUNT(*) AS delay_count
    FROM stop_events se
    JOIN trips t ON t.trip_id = se.trip_id
    JOIN lines l ON l.line_id = t.line_id
    WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
    GROUP BY l.line_name
    ORDER BY delay_count DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql); return rows(cur)

def q9(conn):
    sql = """
    SELECT se.trip_id, COUNT(*) AS delayed_stop_count
    FROM stop_events se
    WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
    GROUP BY se.trip_id
    HAVING COUNT(*) >= 3
    ORDER BY delayed_stop_count DESC, se.trip_id
    """
    with conn.cursor() as cur:
        cur.execute(sql); return rows(cur)

def q10(conn):
    sql = """
    WITH totals AS (
      SELECT s.stop_name, SUM(se.passengers_on) AS total_boardings
      FROM stop_events se
      JOIN stops s ON s.stop_id = se.stop_id
      GROUP BY s.stop_name
    ), avgv AS (
      SELECT AVG(total_boardings) AS avg_board FROM totals
    )
    SELECT t.stop_name, t.total_boardings
    FROM totals t, avgv a
    WHERE t.total_boardings > a.avg_board
    ORDER BY t.total_boardings DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql); return rows(cur)

QUERIES = {
    "Q1": (q1, "List all stops on Route 20 in order"),
    "Q2": (q2, "Trips during morning rush (7-9 AM)"),
    "Q3": (q3, "Transfer stops (stops on 2+ routes)"),
    "Q4": (q4, "Complete route for trip (uses stop_events)"),
    "Q5": (q5, "Routes serving both Wilshire/Veteran and Le Conte/Broxton"),
    "Q6": (q6, "Average ridership by line"),
    "Q7": (q7, "Top 10 busiest stops"),
    "Q8": (q8, "Count delays by line (> 2 min)"),
    "Q9": (q9, "Trips with 3+ delayed stops"),
    "Q10": (q10, "Stops with above-average ridership (boardings)"),
}

def emit(obj, compact=False):
    if compact:
        print(json.dumps(obj, default=str))
    else:
        print(json.dumps(obj, default=str, indent=2, ensure_ascii=False))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--query', choices=QUERIES.keys())
    ap.add_argument('--all', action='store_true')
    ap.add_argument('--dbname', required=True)
    # Output control: pretty by default; use --compact to disable indentation
    ap.add_argument('--compact', action='store_true', help='Emit compact single-line JSON')
    args = ap.parse_args()

    conn = connect(args.dbname)
    try:
        if args.all:
            for key in QUERIES:
                func, desc = QUERIES[key]
                t0=time.time()
                data = func(conn)
                out = {
                    "query": key, "description": desc, "results": data, "count": len(data),
                    "execution_time_ms": round((time.time()-t0)*1000,2)
                }
                emit(out, compact=args.compact)
        else:
            func, desc = QUERIES[args.query]
            t0=time.time()
            data = func(conn)
            out = {
                "query": args.query, "description": desc, "results": data, "count": len(data),
                "execution_time_ms": round((time.time()-t0)*1000,2)
            }
            emit(out, compact=args.compact)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
