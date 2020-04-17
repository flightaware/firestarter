import React from 'react';
import LandingPage from './components/LandingPage/LandingPage';
import FindFlight from './components/FindFlight/FindFlight';
import AirportPage from './components/Airport/AirportPage';
import FlightInfo from './components/FlightInfo/FlightInfo'
import { BrowserRouter as Router, Route, Switch } from 'react-router-dom';
import AppNavbar from './components/AppNavbar';
import { ThemeProvider, createMuiTheme } from '@material-ui/core/styles';


const App = () => {
    const tableTheme = createMuiTheme({
        overrides: {
            MuiTableSortLabel: {
                root: {
                    color: '#fff',
                    '&:hover': {
                        color: 'whitesmoke'
                    },
                    '&:focus': {
                        color: 'whitesmoke'
                    }
                },
                active: {
                    color: 'whitesmoke !important'
                },
                icon: {
                    color: 'whitesmoke !important'
                }
            }
        }
    });
    return (
        <ThemeProvider theme={tableTheme}>
            <Router>
                <div className="app">
                    <AppNavbar />
                    <Switch>
                        <Route path={`/`} component={LandingPage} exact />
                        <Route path={`/airport/:code`} component={AirportPage} />
                        <Route path={`/findflight/:origin/:destination`} component={FindFlight} />
                        <Route path={`/flightinfo/:flight`} component={FlightInfo} ></Route>
                        <Route path={`/flightinfo/`} component={FlightInfo} ></Route>
                    </Switch>
                </div>
            </Router>
        </ThemeProvider>
    );
} 

export default App; 
