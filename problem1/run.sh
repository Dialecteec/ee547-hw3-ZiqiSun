#!/bin/bash
set -e
echo "Starting PostgreSQL..."
docker-compose up -d db
echo "Waiting for database to come up..."
sleep 5
echo "Loading data..."
docker-compose run --rm app python load_data.py --host $PGHOST --dbname transit --user $PGUSER --password $PGPASSWORD --datadir /app/data
echo ""
echo "Running sample queries..."
docker-compose run --rm app python queries.py --query Q1 --dbname transit
docker-compose run --rm app python queries.py --query Q3 --dbname transit
