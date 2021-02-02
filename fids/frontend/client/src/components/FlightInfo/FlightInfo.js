import React, { Component } from 'react';
import { Container, Row, Col, Card } from 'react-bootstrap';
import { Redirect } from 'react-router-dom';
import axios from 'axios';
import * as FS from '../../controller.js';
import * as helpers from '../../helpers.js';

export default class FlightInfo extends Component {
    constructor(props) {
        super(props);

        this.state = {
            data: {},
            positions: {},
            flight_id: props.match.flight,
            loading_flight: true,
            loading_positions: true,
        }
    }

    componentDidMount() {

        const { match: { params } } = this.props;
        this.fetchFlightInfo(params.flight);
        this.fetchPositions(params.flight);

    }

    fetchFlightInfo(flightID) {
        axios.get(`/flights/${(flightID === 'random') ? '' : flightID}`)
            .then(response => {
                console.log(response.data)
                this.setState({
                    loading_flight: false,
                    data: response.data,
                    flight_id: response.data.id
                });
                if (flightID === 'random') {
                    // Original position fetch will have been skipped since we
                    // didn't initially have a flight ID. Try again.
                    this.fetchPositions(response.data.id);
                }
            });
    }

    fetchPositions(flightID) {
        if (flightID !== 'random') {
            axios.get(`/positions/${flightID}`)
                .then(response => {
                    console.log(response)
                    this.setState({
                        loading_positions: false,
                        positions: response.data
                    });
                });
        }
    }

    render() {
        
        const { data, positions, loading_flight, loading_positions } = this.state;

        

        const hasDepartureGate= () => {

            const gateIsEstimated = () => {
                return (data.estimated_departure_gate && !data.actual_departure_gate)
            }

            if (data.actual_departure_gate || data.estimated_departure_gate) {
                if ((data.actual_departure_gate !== data.estimated_departure_gate) && (data.estimated_departure_gate && data.actual_departure_gate)) {
                return <div className="d-flex justify-content-center gate-number">Gate changed to {data.actual_departure_gate}</div>
                } else return <div className={`d-flex justify-content-center align-items-center ${gateIsEstimated() ? "estimated-gate" : "gate-number"}`}>Gate {data.actual_departure_gate || data.estimated_departure_gate}</div>
            }
            return <div className="d-flex justify-content-center align-items-center no-gate">No gate information available</div>
        }

        const hasArrivalGate= () => {

            const gateIsEstimated = () => {
                return (data.estimated_arrival_gate && !data.actual_arrival_gate)
            }

            if (data.actual_arrival_gate || data.estimated_arrival_gate) {
                if ((data.actual_arrival_gate !== data.estimated_arrival_gate) && (data.estimated_arrival_gate && data.actual_arrival_gate)) {
                return <div className="d-flex justify-content-center gate-number">Gate changed to {data.actual_arrival_gate}</div>
                } else return <div className={`d-flex justify-content-center align-items-center ${gateIsEstimated() ? "estimated-gate" : "gate-number"}`}>Gate {data.actual_arrival_gate || data.estimated_arrival_gate}</div>
            }
            return <div className="d-flex justify-content-center align-items-center no-gate">No gate information available</div>
        }

        const hasBaggage = () => {
            if (data.baggage_claim) {
            return <div className="d-flex justify-content-center baggage-claim">Baggage claim {data.baggage_claim}</div>
            }
            return <div style={{height: '21px'}} />
        }

        const getGateDeparture = () => {
            if (data.actual_out) {
                return <div className="mb-1 actual-time">{data.actual_out}</div>
            } else {
            return <div className="mb-1 estimated-time">{data.predicted_out || data.estimated_out || data.scheduled_out}</div>
            }
        }

        const getTakeoffTime = () => {
            if (data.actual_off) {
                return <div className="mb-1 actual-time">{data.actual_off}</div>
            } else {
            return <div className="mb-1 estimated-time">{data.predicted_off || data.estimated_off}</div>
            }
        }

        const getLandingTime = () => {
            if (data.actual_on) {
                return <div className="mb-1 actual-time">{data.actual_on}</div>
            } else {
            return <div className="mb-1 estimated-time">{data.predicted_on || data.estimated_on}</div>
            }
        }

        const getGateArrival = () => {
            if (data.actual_in) {
                return <div className="mb-1 actual-time">{data.actual_in}</div>
            } else {
            return <div className="mb-1 estimated-time">{data.predicted_in || data.estimated_in || data.scheduled_in}</div>
            }
        }

        const getMapImage = () => {
            // do not display a map if there are no positions
            if (positions.length == 0) {
                return <div></div>
            }

            // Get aircraft image bearing
            let image_bearing = 0;
            if (positions.length > 5) {
                image_bearing = helpers.getClosestDegreeAngle(helpers.getBearingDegrees(positions[4].latitude, positions[4].longitude, positions[0].latitude, positions[0].longitude));
            }

            // build latlon list
            let latlon = "";
            for(let i = 0; i < positions.length; i++) {
                let obj = positions[i];
                latlon +=  "|" + obj.latitude + "," + obj.longitude;
            }

            return <div className="d-flex align-items-center"><img src={`https://maps.googleapis.com/maps/api/staticmap?size=640x400&markers=anchor:center|icon:https://github.com/flightaware/firestarter/raw/master/fids/images/aircraft_${image_bearing}.png|${positions[0].latitude},${positions[0].longitude}&path=color:0x0000ff|weight:5${latlon}&key=${window.GOOGLE_MAPS_API_KEY}`}/></div>
        }

        return (
            <Container className="flight-info-wrapper">
                {(data.id && this.props.match.flight !== data.id) ? <Redirect to={`/flightinfo/${data.id}`}/> : ""}
                {
                !loading_flight ?
                    <>
                    <Container className="p-3 flight-number">
                        <Row lg={3}>
                            {data.flight_number}
                        </Row>
                        <Row className="flight-numbers-row" lg={1}>
                            {data.registration &&
                            <Col className="p-0">
                                {data.registration}
                            </Col>}
                        </Row>
                        <Row className="flight-status-row d-flex align-items-center">
                            {(FS.isCancelled(data) === "ec") 
                                ? 
                                <span className="estimated-cancel">CANCELLED</span> 
                                : 
                                (FS.isCancelled(data) === "tc") 
                                ? 
                                <span className="flight-cancel">CANCELLED</span> 
                                : 
                                FS.flightStatus(data)
                            }
                        </Row>
                    </Container>
                    <Container className="origin-dest-info">
                        <Row>
                            <Col className="origin-dest-wrapper">
                                <Card className="origin-dest-card">
                                    <div className="origin-dest-title d-flex justify-content-center mb-2">Origin</div>
                                    <div className="d-flex justify-content-center origin-dest-data">{data.origin}</div>
                                    {hasDepartureGate()}
                                    {(data.actual_departure_terminal || data.estimated_departure_terminal) ? 
                                    <div>Terminal {data.actual_departure_terminal || data.estimated_departure_terminal}</div>
                                    : <div style={{height: '21px'}} />}
                                    <div style={{height: '21px'}} />
                                </Card>
                            </Col>
                            <Col className="origin-dest-wrapper">
                                <Card className="origin-dest-card">
                                    <div className="origin-dest-title d-flex justify-content-center mb-2">Destination</div>
                                    <div className="d-flex justify-content-center origin-dest-data">{data.destination}</div>
                                    {hasArrivalGate()}
                                    {(data.actual_arrival_terminal || data.estimated_arrival_terminal) ? 
                                    <div>Terminal {data.actual_arrival_terminal || data.estimated_arrival_terminal}</div>
                                    : <div style={{height: '21px'}} />}
                                    {hasBaggage()}
                                </Card>
                            </Col>
                        </Row>
                    </Container>
                    {!loading_positions && window.GOOGLE_MAPS_API_KEY ?
                    <div className="d-flex justify-content-center">{getMapImage()}</div>
                    : <Container></Container>}
                    <Container>
                        <Card className="detail-card">
                            <Row className="detail-card-title">
                                <Col className="mb-1 mt-1">
                                    <b>Departure Times</b>
                                </Col>
                            </Row>
                            <Row className="m-0">
                                <Col>
                                    <div className="mb-1 detail-card-subtitle">
                                        Gate Departure
                                    </div>
                                    {getGateDeparture()}
                                    {data.scheduled_out && 
                                    <div className="mb-1 scheduled-time">
                                        Scheduled: {data.scheduled_out}
                                    </div>}
                                </Col>
                                <Col>
                                    <div className="mb-1 detail-card-subtitle">
                                        Takeoff
                                    </div>
                                    {getTakeoffTime()}
                                    {data.scheduled_off &&
                                    <div className="mb-1 scheduled-time">
                                        Scheduled: {data.scheduled_off}
                                    </div>}
                                </Col>
                            </Row>
                        </Card>
                    </Container>
                    <Container>
                        <Card className="detail-card">
                            <Row className="detail-card-title">
                                <Col className="mb-1 mt-1">
                                    <b>Arrival Times</b>
                                </Col>
                            </Row>
                            <Row className="m-0">
                                <Col>
                                    <div className="mb-1 detail-card-subtitle">
                                        Landing
                                    </div>
                                    {getLandingTime()}
                                    {(data.predicted_on || data.estimated_on) && 
                                    <div className="mb-1 scheduled-time">
                                        Scheduled: {data.predicted_on || data.estimated_on}
                                    </div>}
                                </Col>
                                <Col>
                                    <div className="mb-1 detail-card-subtitle">
                                        Gate Arrival
                                    </div>
                                    {getGateArrival()}
                                    {data.scheduled_in && 
                                    <div className="mb-1 scheduled-time">
                                        Scheduled: {data.scheduled_in}
                                    </div>}
                                </Col>
                            </Row>
                        </Card>
                    </Container>
                    <Container>
                        <Card className="detail-card">
                            <Row className="detail-card-title">
                                <Col className="mb-1 mt-1">
                                    <b>Flight Details</b>
                                </Col>
                            </Row>
                            <Row className="m-0">
                                <Col>
                                    <div className="mb-1 detail-card-subtitle">
                                        Aircraft type
                                    </div>
                                    {data.aircraft_type ? 
                                    <div className="mb-1">{data.aircraft_type}</div>
                                    :
                                    <div className="mb-1 not-available">Not available</div>}
                                    <div className="mb-1 detail-card-subtitle">
                                        Filed airspeed / Ground speed
                                    </div>
                                    {data.filed_speed ? 
                                    <div className="mb-1">{data.filed_speed} KIAS / {data.filed_ground_speed && data.filed_ground_speed} knots</div>
                                    :
                                    <div className="mb-1 not-available">Not available</div>}
                                    <div className="mb-1 detail-card-subtitle">
                                        Filed Altitude
                                    </div>
                                    {data.filed_altitude ? 
                                    <div className="mb-1">{data.filed_altitude}</div>
                                    :
                                    <div className="mb-1 not-available">Not available</div>}
                                    <div className="mb-1 detail-card-subtitle">
                                        Filed Route
                                    </div>
                                    {data.route ? 
                                    <div className="mb-1">{data.route}</div>
                                    :
                                    <div className="mb-1 not-available">Not available</div>}
                                </Col>
                            </Row>
                        </Card>
                    </Container>
                    </>
                : <Container></Container>
                }
            </Container>
        )
    }
}
