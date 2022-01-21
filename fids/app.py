"""Read flight information from database and display it on a webpage"""

from datetime import datetime, timezone
import os
import time
from typing import Optional
from flask import Flask, request, jsonify, abort, Response
import sqlalchemy as sa  # type: ignore
from sqlalchemy.sql import union, select, func, and_, or_  # type: ignore
from sqlalchemy.sql.expression import text # type: ignore

# pylint: disable=invalid-name
flights_engine = sa.create_engine(os.environ["FLIGHTS_DB_URL"], echo=True)
flights_meta = sa.MetaData()
flights_insp = sa.inspect(flights_engine)
while "flights" not in flights_insp.get_table_names():
    print("Waiting for flights table to exist before starting")
    flights_insp.info_cache.clear()
    time.sleep(3)
flights = sa.Table("flights", flights_meta, autoload_with=flights_engine)

positions_engine = sa.create_engine(os.environ["POSITIONS_DB_URL"], echo=True)

while True:
    try:
        positions_engine.connect()
        break
    except sa.exc.OperationalError as error:
        print(f"Can't connect to the database ({error}), trying again in a few seconds")
        time.sleep(3)

positions_meta = sa.MetaData()
positions_insp = sa.inspect(positions_engine)
while "positions" not in positions_insp.get_table_names():
    print("Waiting for positions table to exist before starting")
    positions_insp.info_cache.clear()
    time.sleep(3)
positions = sa.Table("positions", positions_meta, autoload_with=positions_engine)

google_maps_api_key = os.environ["GOOGLE_MAPS_API_KEY"]

app = Flask(__name__)

UTC = timezone.utc


@app.route("/positions/<flight_id>")
def get_positions(flight_id: str) -> dict:
    """Get positions for a specific flight_id"""
    result = positions_engine.execute(
        positions.select().where(positions.c.id == flight_id).order_by(positions.c.time.desc())
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@app.route("/flights/")
@app.route("/flights/<flight_id>")
def get_flight(flight_id: Optional[str] = None) -> dict:
    """Get info for a specific flight_id"""
    if flight_id is None:
        # get random flight
        whereclause = flights.c.id.in_(
            select([flights.c.id])
            .where(flights.c.flight_number != "BLOCKED")
            .order_by(func.random())
            .limit(1)
        )
    else:
        whereclause = flights.c.id == flight_id
    result = flights_engine.execute(flights.select().where(whereclause)).first()
    if result is None:
        abort(404)
    return dict(result)


@app.route("/airports/")
def get_busiest_airports() -> Response:
    """Get the busiest airport"""
    limit = request.args.get("limit", 10)
    query = request.args.get("query")
    since = int(request.args.get("since", 0))
    if query:
        result = flights_engine.execute(
            union(
                select([flights.c.origin]).distinct().where(flights.c.origin.like(f"%{query}%")),
                select([flights.c.destination])
                .distinct()
                .where(flights.c.destination.like(f"%{query}%")),
            )
        )
        if result is None:
            abort(404)
        return jsonify([row[0] for row in result])

    return jsonify(
        [
            row.origin
            for row in flights_engine.execute(
                select([flights.c.origin])
                .where(
                    func.coalesce(flights.c.actual_off, flights.c.actual_out)
                    > (
                        select(
                            [
                                func.datetime(
                                    func.max(flights.c.actual_off), text(f"'-{since} hours'")
                                )
                            ]
                        )
                    )
                )
                .group_by(flights.c.origin)
                .order_by(func.count().desc(), flights.c.origin)
                .limit(limit)
            )
        ]
    )


@app.route("/airports/<airport>/arrivals")
def airport_arrivals(airport: str) -> Response:
    """Get a list of arrivals for a certain airport"""
    airport = airport.upper()
    result = flights_engine.execute(
        flights.select().where(
            and_(
                flights.c.destination == airport,
                flights.c.flight_number != "BLOCKED",
                func.coalesce(flights.c.actual_out, flights.c.actual_off) is not None,
                func.coalesce(flights.c.actual_in, flights.c.actual_on, flights.c.cancelled)
                > (select([func.datetime(func.max(flights.c.actual_off), text(f"'-5 hours'"))])),
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@app.route("/airports/<airport>/departures")
def airport_departures(airport: str) -> Response:
    """Get a list of departures for a certain airport"""
    airport = airport.upper()
    result = flights_engine.execute(
        flights.select().where(
            and_(
                flights.c.origin == airport,
                flights.c.flight_number != "BLOCKED",
                func.coalesce(flights.c.actual_out, flights.c.actual_off)
                > (select([func.datetime(func.max(flights.c.actual_off), text(f"'-5 hours'"))])),
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


# pylint: disable=singleton-comparison
@app.route("/airports/<airport>/enroute")
@app.route("/airports/<airport>/scheduledto")
def airport_enroute(airport: str) -> Response:
    """Get a list of flights enroute to a certain airport"""
    airport = airport.upper()
    result = flights_engine.execute(
        flights.select().where(
            and_(
                flights.c.destination == airport,
                flights.c.flight_number != "BLOCKED",
                func.coalesce(flights.c.actual_in, flights.c.actual_on, flights.c.cancelled)
                == None,
                flights.c.estimated_on.between(
                    select([func.datetime(func.max(flights.c.actual_off), text(f"'-5 hours'"))]),
                    select([func.datetime(func.max(flights.c.actual_off), text(f"'+6 hours'"))]),
                ),
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@app.route("/airports/<airport>/scheduled")
@app.route("/airports/<airport>/scheduledfrom")
def airport_scheduled(airport: str) -> Response:
    """Get a list of scheduled flights from a certain airport"""
    airport = airport.upper()
    result = flights_engine.execute(
        flights.select().where(
            and_(
                flights.c.origin == airport,
                flights.c.flight_number != "BLOCKED",
                or_(
                    func.coalesce(flights.c.actual_out, flights.c.actual_off) == None,
                    # You can actually get true_cancel'ed flights with an actual_out/off. Weird?
                    flights.c.true_cancel,
                ),
                flights.c.scheduled_off.between(
                    select([func.datetime(func.max(flights.c.actual_off), text(f"'-5 hours'"))]),
                    select([func.datetime(func.max(flights.c.actual_off), text(f"'+6 hours'"))]),
                ),
                or_(
                    flights.c.cancelled == None,
                    and_(
                        flights.c.true_cancel,
                        flights.c.cancelled
                        > (
                            select(
                                [func.datetime(func.max(flights.c.actual_off), text(f"'-5 hours'"))]
                            )
                        ),
                    ),
                ),
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@app.route("/mapskey")
def get_map_api_key() -> Response:
    """Get the google maps api key"""
    return google_maps_api_key

app.run(host="0.0.0.0", port=5000)
