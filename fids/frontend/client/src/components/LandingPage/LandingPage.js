import React, { Component } from 'react';
import AirportSearch from './AirportSearch';
import AirportList from './AirportList';
import { Container } from 'react-bootstrap';

export default class LandingPage extends Component {

    render() {
        return (
            <div className="landing-page-wrapper">
                <Container className="d-flex justify-content-center flex-column">
                    <AirportSearch />
                    <div className="busy-airport-wrapper d-flex justify-content-center align-items-center flex-column">
                        <AirportList />
                    </div>
                </Container>
            </div>
        );
    }
} 
