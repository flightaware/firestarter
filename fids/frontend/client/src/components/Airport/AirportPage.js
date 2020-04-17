import React, { Component } from 'react';
import ArrivalsTable from './ArrivalsTable';
import DeparturesTable from './DeparturesTable';
import EnrouteTable from './EnrouteTable';
import ScheduledTable from './ScheduledTable';

export default class AirportPage extends Component {

    state = {
        icao: '',
        loading: true,
        arrivalData: [],
        departureData: [],
        enrouteData: [],
        scheduledData: []
    }

    componentDidMount() {
        const { match: { params } } = this.props;

        this.setState({
            icao: params.code,
            loading: false
        });
    }

    render() {
        const { loading, icao } = this.state;

        return (
            <div className="airport-page-wrapper">
                {!loading && 
                <>
                <div className="airport-page-title">{icao} Information</div>
                <div className="airport-page-tables-wrapper">
                    <ArrivalsTable code={icao} />
                    <DeparturesTable code={icao} />
                    <EnrouteTable code={icao} />
                    <ScheduledTable code={icao} />
                </div>
                </>
                }
            </div>
        );
    }
} 