import React, { Component } from 'react';
import { Container, ListGroup, Spinner, ButtonGroup, ToggleButton } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import axios from 'axios'

class AirportList extends Component {

    state = {
        topAirports: [],
        loading: true,
        since: 1
    }

    componentDidMount() {
        this.fetchTopAirports(this.state.since);
    }

    fetchTopAirports(time) {
        axios.get(`/airports/?limit=10&since=${time}`)
            .then(response => {
                this.setState({
                    topAirports: response.data,
                    loading: false
                });
                console.log(response.data);
            });
    }

    onRadioClick(val) {
        this.setState({
            loading: true,
            since: val
        });
        this.fetchTopAirports(val);
    }

    render() {

        const { topAirports, loading, since } = this.state;

        const timeIntervalText = (since) => {
            switch (since) {
                case 1:
                    return "1 hour"
                case 12:
                    return "12 hours"
                case 24:
                    return "24 hours"
                default:
                    return since
            }
        }

        return (
            <>
            {loading 
                ?
                <div className="loading-spinner">
                    <Spinner animation="border" variant="primary" />
                </div> 
                :
                <>
                <Container className="topAirport-list-container mt-3 d-flex justify-content-center align-items-center flex-column">
                    <div className="d-flex justify-content-start flex-column w-100">
                        <span className="topAirport-list-container-title">Busiest Airports</span>
            <span className="topAirport-list-container-subtitle">*Departures in the last {timeIntervalText(since)}</span>
                    </div>
                    <ListGroup className="topAirport-list-group">
                        {topAirports.map((airport, index) => (
                            <ListGroup.Item className="top-airport-item d-flex justify-content-center" key={index}>
                                <Link className="h-100 w-100 airport-list-link" to={`/airport/${airport}`}>{airport}</Link>
                            </ListGroup.Item>
                        ))}
                    </ListGroup>
                </Container>
                <Container className="radio-button-container">
                        <ButtonGroup className="w-100" toggle>
                            <ToggleButton onChange={() => this.onRadioClick(1)} type="radio" name="since" defaultChecked value={1}>1 hour</ToggleButton>
                            <ToggleButton onChange={() => this.onRadioClick(12)} type="radio" name="since" value={12}>12 hours</ToggleButton>
                            <ToggleButton onChange={() => this.onRadioClick(24)} type="radio" name="since" value={24}>24 hours</ToggleButton>
                        </ButtonGroup>
                </Container>
                </>
                } 
            </>
        )
    }
}

export default AirportList
