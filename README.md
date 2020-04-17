# Firestarter - Getting started with FlightAware Firehose
Firestarter is a small collection of services and sample applications to help
you get started using [FlightAware's Firehose Flight Data Feed](https://flightaware.com/commercial/firehose/documentation).

Firestarter is structured as a group of Docker containers managed by
Docker Compose. Currently, 2 core services and 1 sample application are
included, with more being developed (see [the roadmap](./ROADMAP.md) for
details).

## Quickstart
You must set the following variables (in your environment or a .env file)
before you can start using Firestarter.
* FH_USERNAME - Your FlightAware Firehose account username
* FH_APIKEY - The key to your FlightAware Firehose account
* INIT_CMD_ARGS - Firehose initiation command; more information about this is
available at https://flightaware.com/commercial/firehose/documentation/commands
and in the env section of [docker-compose.yml](./docker-compose.yml). Its value
will vary based on your account configuration, but a very basic example that
should work for most users is `events "flightplan departure arrival
cancellation"`.

There are a number of other environment variables that can be set to tune the
behavior of Firestarter. They are documented in
[docker-compose.yml](./docker-compose.yml).

You'll also need to install Docker (18.06+) and Docker Compose (1.22.0+)\
Details available at Docker's site: https://docs.docker.com/get-docker/

The usual Docker Compose incantation run in the root of this repo will get you
up and running:
```
docker-compose up --build
```

After running the above command and letting the 3 containers build, you should
be greeted with log output from each container. The services will log
periodically as Firehose messages are received, while the sample webapp will
produce some initial log output and then only log as requests are made to it.

You can test out the sample application by visiting http://localhost:5000 in
your web browser (if not running Docker locally, use the Docker host's
address).


## Firestarter Components

### connector
The connector service handles connecting to Firehose over an SSL socket. This
involves building and sending the initiation command, handling compression, and
reconnecting to Firehose without data loss if the connection is interrupted.
The connector then forwards Firehose messages to its own clients.

### db-updater
The db-updater service receives Firehose messages from the connector and
maintains a database table of flights based on their contents. The service is
only capable of handling so-called "flifo" (flight info) messages currently; in
the future, position messages will also be handled. The default database
configuration writes to a sqlite database in a named volume, but PostgreSQL is
also supported. Other databases could potentially be supported with little
effort. To prevent bloat, flights older than 48 hours are automatically
dropped from the table.

### fids
The sample application is a webapp backed by the flights database. You can use
it to browse flight data by airport, presenting flights similarly to how you'd
see them on a flight information display system (FIDS). Detailed information
for individual flights can also be viewed. While the 2 services are intended to
be used in a production environment, this sample application should only be
considered a demonstration of what can be built using the data from Firehose.
It should *not* be used in a production environment.


Check out [the roadmap](./ROADMAP.md) to see what components are coming in the
future!
