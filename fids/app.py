from datetime import datetime, timedelta, timezone
import os
import time

from flask import Flask, request, jsonify, abort, render_template
from flask_cors import CORS
import sqlalchemy as sa
from sqlalchemy.sql import union, select, func, and_, or_

engine = sa.create_engine(os.getenv("DB_URL"), echo=True)
meta = sa.MetaData()
insp = sa.inspect(engine)
while "flights" not in insp.get_table_names():
    print("Waiting for flights table to exist before starting")
    insp.info_cache.clear()
    time.sleep(3)
flights = sa.Table("flights", meta, autoload_with=engine)

app = Flask(
    __name__, template_folder="frontend/build", static_folder="frontend/build/static"
)
# Uncomment to enable serving the frontend separately (when testing, perhaps)
# CORS(app)

UTC = timezone.utc


# Let frontend handle any unknown routes
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def catch_all(path):
    return render_template("index.html")


@app.route("/flights/")
@app.route("/flights/<flight_id>")
def get_flight(flight_id=None):
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
    result = engine.execute(flights.select().where(whereclause)).first()
    if result is None:
        abort(404)
    return dict(result)


@app.route("/airports/")
def get_busiest_airports():
    limit = request.args.get("limit", 10)
    since = datetime.fromtimestamp(int(request.args.get("since", 0)), tz=UTC)
    query = request.args.get("query")
    if query:
        result = engine.execute(
            union(
                select([flights.c.origin])
                .distinct()
                .where(flights.c.origin.like(f"%{query}%")),
                select([flights.c.destination])
                .distinct()
                .where(flights.c.destination.like(f"%{query}%")),
            )
        )
        if result is None:
            abort(404)
        return jsonify([row[0] for row in result])
    else:
        return jsonify(
            [
                row.origin
                for row in engine.execute(
                    select([flights.c.origin])
                    .where(
                        func.coalesce(flights.c.actual_off, flights.c.actual_out)
                        > since
                    )
                    .group_by(flights.c.origin)
                    .order_by(func.count().desc())
                    .limit(limit)
                )
            ]
        )


@app.route("/airports/<airport>/arrivals")
def airport_arrivals(airport):
    airport = airport.upper()
    dropoff = datetime.now(tz=UTC) - timedelta(hours=5)
    result = engine.execute(
        flights.select().where(
            and_(
                flights.c.destination == airport,
                flights.c.flight_number != "BLOCKED",
                func.coalesce(flights.c.actual_out, flights.c.actual_off) != None,
                func.coalesce(
                    flights.c.actual_in, flights.c.actual_on, flights.c.cancelled
                )
                > dropoff,
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@app.route("/airports/<airport>/departures")
def airport_departures(airport):
    airport = airport.upper()
    dropoff = datetime.now(tz=UTC) - timedelta(hours=5)
    result = engine.execute(
        flights.select().where(
            and_(
                flights.c.origin == airport,
                flights.c.flight_number != "BLOCKED",
                func.coalesce(flights.c.actual_out, flights.c.actual_off) > dropoff,
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@app.route("/airports/<airport>/enroute")
@app.route("/airports/<airport>/scheduledto")
def airport_enroute(airport):
    airport = airport.upper()
    past_dropoff = datetime.now(tz=UTC) - timedelta(hours=5)
    future_dropoff = datetime.now(tz=UTC) + timedelta(hours=6)
    result = engine.execute(
        flights.select().where(
            and_(
                flights.c.destination == airport,
                flights.c.flight_number != "BLOCKED",
                func.coalesce(
                    flights.c.actual_in, flights.c.actual_on, flights.c.cancelled
                )
                == None,
                flights.c.estimated_on.between(past_dropoff, future_dropoff),
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


@app.route("/airports/<airport>/scheduled")
@app.route("/airports/<airport>/scheduledfrom")
def airport_scheduled(airport):
    airport = airport.upper()
    past_dropoff = datetime.now(tz=UTC) - timedelta(hours=5)
    future_dropoff = datetime.now(tz=UTC) + timedelta(hours=6)
    result = engine.execute(
        flights.select().where(
            and_(
                flights.c.origin == airport,
                flights.c.flight_number != "BLOCKED",
                or_(
                    func.coalesce(flights.c.actual_out, flights.c.actual_off) == None,
                    # You can actually get true_cancel'ed flights with an actual_out/off. Weird?
                    flights.c.true_cancel,
                ),
                flights.c.filed_off.between(past_dropoff, future_dropoff),
                or_(
                    flights.c.cancelled == None,
                    and_(flights.c.true_cancel, flights.c.cancelled > past_dropoff),
                ),
            )
        )
    )
    if result is None:
        abort(404)
    return jsonify([dict(e) for e in result])


app.run(host="0.0.0.0", port=5000, debug=True)
