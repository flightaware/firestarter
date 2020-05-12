"""Read flight information from database and display it on a webpage"""

from datetime import datetime, timedelta, timezone
import os
import time
from typing import Optional
from flask import Flask, request, jsonify, abort, render_template, Response
import sqlalchemy as sa  # type: ignore
from sqlalchemy.sql import union, select, func, and_, or_  # type: ignore

ENGINE = sa.create_engine(os.getenv("DB_URL"), echo=True)
META = sa.MetaData()
INSP = sa.inspect(ENGINE)
while "flights" not in INSP.get_table_names():
    print("Waiting for flights table to exist before starting")
    INSP.info_cache.clear()
    time.sleep(3)
FLIGHTS = sa.Table("flights", META, autoload_with=ENGINE)

APP = Flask(__name__, template_folder="frontend/build", static_folder="frontend/build/static")
# Uncomment to enable serving the frontend separately (when testing, perhaps)
# CORS(app)

UTC = timezone.utc


# Let frontend handle any unknown routes
@APP.route("/", defaults={"path": ""})
@APP.route("/<path:path>")
def catch_all(path):
    """Render HTML"""
    # pylint: disable=unused-argument
    return render_template("index.html")


@APP.route("/flights/")
@APP.route("/flights/<flight_id>")
def get_flight(flight_id: Optional[str] = None) -> dict:
    """Get info for a specific flight_id"""
    if flight_id is None:
        # get random flight
        whereclause = FLIGHTS.c.id.in_(
            select([FLIGHTS.c.id])
            .where(FLIGHTS.c.flight_number != "BLOCKED")
            .order_by(func.random())
            .limit(1)
        )
    else:
        whereclause = FLIGHTS.c.id == flight_id
    result = ENGINE.execute(FLIGHTS.select().where(whereclause)).first()
    if result is None:
        abort(404)
    return dict(result)


@APP.route("/airports/")
def get_busiest_airports() -> Response:
    """Get the busiest airport"""
    limit = request.args.get("limit", 10)
    since = datetime.fromtimestamp(int(request.args.get("since", 0)), tz=UTC)
    query = request.args.get("query")
    if query:
        result = ENGINE.execute(
            union(
                select([FLIGHTS.c.origin]).distinct().where(FLIGHTS.c.origin.like(f"%{query}%")),
                select([FLIGHTS.c.destination])
                .distinct()
                .where(FLIGHTS.c.destination.like(f"%{query}%")),
            )
        )
        if result is None:
            abort(404)
        return jsonify([row[0] for row in result])

    return jsonify(
        [
            row.origin
            for row in ENGINE.execute(
                select([FLIGHTS.c.origin])
                .where(func.coalesce(FLIGHTS.c.actual_off, FLIGHTS.c.actual_out) > since)
                .group_by(FLIGHTS.c.origin)
                .order_by(func.count().desc())
                .limit(limit)
            )
        ]
    )


@APP.route("/airports/<airport>/arrivals")
def airport_arrivals(airport: str) -> Response:
    """Get a list of arrivals for a certain airport"""
    airport = airport.upper()
    dropoff = datetime.now(tz=UTC) - timedelta(hours=5)
    result = ENGINE.execute(
        FLIGHTS.select().where(
            and_(
                FLIGHTS.c.destination == airport,
                FLIGHTS.c.flight_number != "BLOCKED",
                func.coalesce(FLIGHTS.c.actual_out, FLIGHTS.c.actual_off) is not None,
                func.coalesce(FLIGHTS.c.actual_in, FLIGHTS.c.actual_on, FLIGHTS.c.cancelled)
                > dropoff,
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@APP.route("/airports/<airport>/departures")
def airport_departures(airport: str) -> Response:
    """Get a list of departures for a certain airport"""
    airport = airport.upper()
    dropoff = datetime.now(tz=UTC) - timedelta(hours=5)
    result = ENGINE.execute(
        FLIGHTS.select().where(
            and_(
                FLIGHTS.c.origin == airport,
                FLIGHTS.c.flight_number != "BLOCKED",
                func.coalesce(FLIGHTS.c.actual_out, FLIGHTS.c.actual_off) > dropoff,
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


# pylint: disable=singleton-comparison
@APP.route("/airports/<airport>/enroute")
@APP.route("/airports/<airport>/scheduledto")
def airport_enroute(airport: str) -> Response:
    """Get a list of flights enroute to a certain airport"""
    airport = airport.upper()
    past_dropoff = datetime.now(tz=UTC) - timedelta(hours=5)
    future_dropoff = datetime.now(tz=UTC) + timedelta(hours=6)
    result = ENGINE.execute(
        FLIGHTS.select().where(
            and_(
                FLIGHTS.c.destination == airport,
                FLIGHTS.c.flight_number != "BLOCKED",
                func.coalesce(FLIGHTS.c.actual_in, FLIGHTS.c.actual_on, FLIGHTS.c.cancelled)
                == None,
                FLIGHTS.c.estimated_on.between(past_dropoff, future_dropoff),
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@APP.route("/airports/<airport>/scheduled")
@APP.route("/airports/<airport>/scheduledfrom")
def airport_scheduled(airport: str) -> Response:
    """Get a list of scheduled flights from a certain airport"""
    airport = airport.upper()
    past_dropoff = datetime.now(tz=UTC) - timedelta(hours=5)
    future_dropoff = datetime.now(tz=UTC) + timedelta(hours=6)
    result = ENGINE.execute(
        FLIGHTS.select().where(
            and_(
                FLIGHTS.c.origin == airport,
                FLIGHTS.c.flight_number != "BLOCKED",
                or_(
                    func.coalesce(FLIGHTS.c.actual_out, FLIGHTS.c.actual_off) == None,
                    # You can actually get true_cancel'ed flights with an actual_out/off. Weird?
                    FLIGHTS.c.true_cancel,
                ),
                FLIGHTS.c.filed_off.between(past_dropoff, future_dropoff),
                or_(
                    FLIGHTS.c.cancelled == None,
                    and_(FLIGHTS.c.true_cancel, FLIGHTS.c.cancelled > past_dropoff),
                ),
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


APP.run(host="0.0.0.0", port=5000, debug=True)
