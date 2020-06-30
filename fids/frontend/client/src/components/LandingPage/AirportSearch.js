import React, { Component } from 'react';
import { NavLink, withRouter } from 'react-router-dom';

class AirportSearch extends Component {
    state = {
        airport: '',
        origin: '',
        destination: '',
        validated: false,
        hasText: false,
        errorMessage: ''
    }

    submitForm (e) {
        e.preventDefault();
        this.props.history.push(`/airport/${this.state.airport}`);
    }

    handleChange = (e) => {

        // Make sure the user is only entering search data for a single airport OR a
        // city pair.
        if (e.target.name === "airport" && (this.state.origin !== '' || this.state.destination !== '')) {
            this.setState({
                origin: '',
                destination: ''
            });
        } else if ((e.target.name === 'origin' || e.target.name === 'destination') && this.state.airport !== '') {
            this.setState({
                airport: ''
            });
        }
        // Set the state to what we're searching for
        this.setState({
            [e.target.name]: e.target.value.toUpperCase()
        });

        if (this.state.airport !== "" || (this.state.origin !== "" && this.state.destination !== "")) {
            this.setState({
                validated: true
            });
        }

        if (e.target.value === "") {
            this.setState({
                validated: false
            });
        }
    }

    render() {
        return (
            <div className="search-wrapper">
                <div className="single-airport-search-wrapper">
                    <div className="search-row">
                        <div className="airport-input-group">
                            <label htmlFor="airport">
                                <b>Airport</b>
                            </label>
                            <form onSubmit={this.submitForm.bind(this)}>
                                <input value={this.state.airport} onChange={this.handleChange} id="airport" type="text" name="airport" placeholder="e.g. KIAH"/>
                            </form>
                        </div>
                        <NavLink to={`/airport/${this.state.airport}`} className="submit-button">GO</NavLink>
                    </div>
                </div>
            </div>
        );
    }
}

export default withRouter(AirportSearch)
