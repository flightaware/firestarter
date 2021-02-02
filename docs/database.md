# Database support
Firestarter offers flexibility in your choice of database. Its db-updater
component is written on top of SQLAlchemy, a python library which supports many
popular SQL databases (full listing at
https://docs.sqlalchemy.org/en/13/dialects/index.html). By default, db-updater
uses a sqlite database located on a Docker named volume. This allows the
database file (located at `/home/firestarter/db/flights.db`) to persist between
container restarts and allows sharing the database file with the fids
component. Db-updater uses TimescaleDB for positions. TimescaleDB is a
time-series database, so it is very efficient in handling time-series data like
positions. It is also an extension of PostgreSQL, which has been fully tested
in db-updater.

When starting db-updater, it checks the database it's connected to ensure that
the "flights" or "positions" table exists (depending on what it is intended to
update). If no such table exists, it is created with the schema found
[here](../db-updater/main.py).

## Customizing the database connection
To use a different database than db-updater's default sqlite file, you just
need to set the DB_URL environment variable. The syntax for this variable is
described at https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls.
Here are a few examples of potential values for the variable:
* `sqlite:///db/flights.db`  
  This is the default DB_URL. It uses the sqlite dialect, and it opens the
  database located at `./db/flights.db`
* `postgresql://postgres:password@10.1.1.1/flightdata`  
  This is a sample PostgreSQL connection URL. It will connect to a database
  named "flightdata" running at the host with IP 10.1.1.1 with username
  "postgres" and password "password". The database does not need to be running
  in Docker; it just needs to be reachable from the container host.
