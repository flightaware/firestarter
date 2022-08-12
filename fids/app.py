"""Read flight information from database and display it on a webpage"""

from base64 import b64encode
from datetime import datetime, timezone
import os
import time
from typing import Optional, Iterable
from flask import Flask, request, jsonify, abort, Response
import requests
import sqlalchemy as sa  # type: ignore
from sqlalchemy.sql import union, select, func, and_, or_  # type: ignore
from sqlalchemy.sql.expression import text # type: ignore

import trig

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


def _get_ground_positions(flight_id: str) -> Iterable:
    return (
        positions_engine.execute(
            positions.select().where(and_(positions.c.id == flight_id, positions.c.air_ground == "G")).order_by(positions.c.time.desc())
        )
        or []
    )


def _get_positions(flight_id: str) -> Iterable:
    return (
        positions_engine.execute(
            positions.select().where(positions.c.id == flight_id).order_by(positions.c.time.desc())
        )
        or []
    )


@app.route("/ground_positions/<flight_id>")
def get_ground_positions(flight_id: str) -> Response:
    """Get ground positions for a specific flight_id"""
    result = _get_ground_positions(flight_id)
    if not result:
        abort(404)
    return jsonify([dict(e) for e in result])


@app.route("/positions/<flight_id>")
def get_positions(flight_id: str) -> Response:
    """Get positions for a specific flight_id"""
    result = _get_positions(flight_id)
    if not result:
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


@app.route("/map/<flight_id>")
def get_map(flight_id: str) -> bytes:
    """Get a static map image of the specified flight. Returned as a
    base64-encoded image"""
    def get_first_4_coords(a1, a2, b1, b2):
        """
        Small function to get 4 earliest coordinates by time and return top 2
        earliest coordinates (for bearing)
        """
        holder = sorted([a1, a2, b1, b2], reverse=True, key=lambda x: x.time)
        return holder[:2]
    positions = list(_get_positions(flight_id))
    if not positions:
        abort(404)

    # Do ground positions
    ground_positions = list(_get_ground_positions(flight_id))
    do_gp = True
    if not ground_positions:
        do_gp = False
    bearing = 0
    if len(positions) > 1:
        if do_gp:
            result = get_first_4_coords(positions[0], positions[1], ground_positions[0], ground_positions[1])
            coord1 = (float(result[1].latitude), float(result[1].longitude))
            coord2 = (float(result[0].latitude), float(result[0].longitude))
        else:
            coord1 = (float(positions[1].latitude), float(positions[1].longitude))
            coord2 = (float(positions[0].latitude), float(positions[0].longitude))
        bearing = trig.get_cardinal_for_angle(trig.get_bearing_degrees(coord1, coord2))
    coords = "|".join(f"{pos.latitude},{pos.longitude}" for pos in positions)


    coords_gp = list(_get_ground_positions(flight_id))

    google_maps_url = "https://maps.googleapis.com/maps/api/staticmap"
    google_maps_params = {
        "size": "640x400",
        "markers": [
            f"anchor:center|icon:https://github.com/flightaware/fids_frontend/raw/master/images/aircraft_{bearing}.png|{positions[0].latitude},{positions[0].longitude}",
        ],
        "path": [
            f"color:0x0000ff|weight:5|{coords}",
            f"color:0xff0000|weight:5|{coords_gp}"
            ],
        "key": google_maps_api_key,
    }
    response = requests.get(google_maps_url, google_maps_params)
    response.raise_for_status()
    image = b64encode(response.content)
    return image


app.run(host="0.0.0.0", port=5000)
