import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import MaterialTable from 'material-table';
import axios from 'axios';
import { Spinner } from 'react-bootstrap';
import * as FS from '../../controller.js'

export default class ArrivalsTable extends Component {
    constructor(props) {
        super(props);

        this.state = {
            data: [],
            loading: true
        }
    }

    componentDidMount() {
        this.fetchData();
    }

    fetchData() {
        axios.get(`/airports/${this.props.code}/arrivals`)
            .then(response => {
                this.setState({
                    data: response.data,
                    loading: false
                });
            });
    }


    render() {
        const { data, loading } = this.state;

        return (
            <div className="table-wrapper">
                <div className="airport-page-table-inner">
                    {!loading ? 
                    <MaterialTable 
                        title="Arrivals"
                        options={{
                            search: false,
                            pageSize: 10,
                            headerStyle: {
                                fontFamily: 'Helvetica',
                                backgroundColor: '#002F5D',
                                color: '#FFF',
                            },
                            cellStyle: {
                                fontFamily: 'Helvetica-Light',
                                padding: '10px',
                            }
                        }}
                        columns={[
                            {
                                title: "Ident", 
                                field: "ident", 
                                render: rowData => <Link to={`/flightinfo/${rowData.id}`}>{rowData.ident}</Link>
                            },
                            {
                                title: "Type", 
                                field: "type"
                            },
                            {
                                title: "From", 
                                field: "from"
                            },
                            {
                                title: "Depart", 
                                field: "depart",
                                render: rowData => rowData.departEstimated ? <div className="estimated-time">{FS.makeTime(rowData.depart)}</div> : FS.makeTime(rowData.depart),
                            },
                            {
                                title: "Arrive", 
                                field: "arrive",
                                render: rowData => rowData.arriveEstimated ? <div className="estimated-time">{FS.makeTime(rowData.arrive)}</div> : FS.makeTime(rowData.arrive),
                            },
                            {
                                title: "Gate",
                                field: "gate"
                            },
                            {
                                title: "Baggage",
                                field: "baggage"
                            }
                        ]}
                        data={data.map(flight => (
                        {
                                ident: flight.flight_number,
                                id: flight.id,
                                arriveEstimated: FS.getArrivalTime(flight).estimated,
                                departEstimated: FS.getDepartureTime(flight).estimated,
                                type: flight.aircraft_type, 
                                from: flight.origin, 
                                depart: FS.getDepartureTime(flight).time, 
                                arrive: FS.getArrivalTime(flight).time,
                                baggage: flight.baggage_claim,
                                gate: FS.getArrivalGate(flight)
                            } 
                        ))}
                    />
                    :
                    <div className="airport-table-spinner">
                        <Spinner animation="border" variant="primary" />
                    </div> 
                    }
                </div>
            </div>
        )
    }
}
