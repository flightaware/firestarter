import React, { Component } from 'react'
import axios from 'axios';
import MaterialTable from 'material-table';
import { Spinner } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import * as FS from '../../controller.js';

export default class ScheduledTable extends Component {
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
        axios.get(`/airports/${this.props.code}/scheduled`)
            .then(response => {
                this.setState({
                    data: response.data,
                    loading: false
                });
            });
    }

    render() {
        const { data, loading } = this.state
        return (
            <div className="table-wrapper">
                <div className="airport-page-table-inner">
                    {!loading ?
                    <>
                    <MaterialTable 
                        title="Scheduled"
                        options={{
                            search: false,
                            pageSize: 10,
                            headerStyle: {
                                backgroundColor: '#002F5D',
                                color: '#FFF',
                            },
                            cellStyle: {
                                fontFamily: 'Helvetica-Light',
                                padding: '10px'
                            },
                            rowStyle: rowData => ({
                                backgroundColor: rowData.cancelled ? '#FF000055' : '#00000000'
                            })
                        }}
                        columns={[
                            {title: "Ident", field: "ident", render: rowData => <Link to={`/flightinfo/${rowData.id}`}>{rowData.ident}</Link>},
                            {title: "Type", field: "type"},
                            {title: "To", field: "to"},
                            {
                                title: "Depart", 
                                field: "depart",
                                render: rowData => rowData.departEstimated ? <div className="estimated-time">{FS.makeTime(rowData.depart)}</div> : FS.makeTime(rowData.depart)
                            },
                            {title: "Gate", field: "gate"},
                            {
                                title: "Arrive", 
                                field: "arrive",
                                render: rowData => rowData.arriveEstimated ? <div className="estimated-time">{FS.makeTime(rowData.arrive)}</div> : FS.makeTime(rowData.arrive),
                            }
                        ]}
                        data={data.map(flight => (
                            {
                                ident: flight.flight_number,
                                id: flight.id,
                                departEstimated: FS.getDepartureTime(flight).estimated,
                                arriveEstimated: FS.getArrivalTime(flight).estimated,
                                type: flight.aircraft_type, 
                                to: flight.destination, 
                                depart: FS.getDepartureTime(flight).time, 
                                gate: FS.getDepartureGate(flight),
                                arrive: FS.getArrivalTime(flight).time,
                                cancelled: flight.cancelled
                            } 
                         ))}
                    />
                    </>
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
