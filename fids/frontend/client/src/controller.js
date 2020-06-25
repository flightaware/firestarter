import React from 'react';

export const minuteZeroes = (num) => {
    return (num < 10 ? '0' : '') + num;
}

export const makeTime = (time) => {
    if (isNaN(time)) {
        return "";
    }
    let millis = new Date(time);
    let hours = millis.getUTCHours();
    let minutes = millis.getMinutes();
    return `${hours}:${minuteZeroes(minutes)} GMT`
}

export const getDepartureTime = (flight) => {
    if (flight.actual_off) {
        let time = {
            estimated: false,
            time: Date.parse(flight.actual_off)
        }
        return time;
    }
    let time = {
        estimated: true,
        time: Date.parse(flight.predicted_off || flight.estimated_off)
    }
    return time;
}

export const getArrivalTime = (flight) => {
    if (flight.actual_on) {
        let time = {
            estimated: false,
            time: Date.parse(flight.actual_on)
        }
        return time;
    }
    let time = {
        estimated: true,
        time: Date.parse(flight.predicted_on || flight.estimated_on)
    }
    return time;
}

export const getDepartureGate = (flight) => {
    if (flight.actual_departure_gate) {
        return <div className="actual-time">{flight.actual_departure_gate}</div>
    } else {
        return <div className="estimated-time">{flight.estimated_departure_gate}</div>
    }
}

export const getArrivalGate = (flight) => {
    if (flight.actual_arrival_gate) {
        return <div className="actual-time">{flight.actual_arrival_gate}</div>
    } else {
        return <div className="estimated-time">{flight.estimated_arrival_gate}</div>
    }
}

export const isCancelled = (flight) => {
    if (flight.cancelled) {
        if (flight.true_cancel) {
            // true cancel
            return "tc"
        }
        // estimated cancel
        return "ec"
    }
    return false
}

export const hasDeparted= (flight) => {

    const now = Date.now();

    const departureTime = (flight.actual_off || flight.predicted_off || flight.estimated_off);

    return ((flight.actual_off && !hasLanded(flight)) || ((now - Date.parse(departureTime) > 1800000)));
}

const hasLanded = (flight) => {
    return flight.actual_on;
}

const isAirborne = (flight) => {
    return (hasDeparted(flight) && !hasLanded(flight) && !flight.true_cancel);
}

export const flightStatus = (flight) => {

            const now = Date.now();

            const arrivalTime = (flight.predicted_in || flight.scheduled_in || flight.estimated_in);

            if (hasLanded(flight)) {
                if (flight.actual_in) {
                    return <span className="actual">Parked</span>
                } else if (hasDeparted(flight) && (now > Date.parse(arrivalTime))) {
                    return <span className="estimated">Parked (estimated)</span>
                }
                return <span className="actual">Landed</span>
            } else if (isAirborne(flight)) {
                return <span className="actual">En Route</span>
            }
            
            return <span className="actual">Scheduled</span>
        }