{/* <h3><span>- OR -</span></h3>
                <div className="search-wrapper">
                    <div className="city-pair-search-wrapper">
                        <div className="search-row">
                            <div className="input-group input-group-origin">
                                <label htmlFor="airport">
                                    <b>Origin</b>
                                </label>
                                <input value={this.state.origin} onChange={this.handleChange} id="origin" type="text" name="origin" placeholder="e.g. KIAH"/>
                            </div>
                            <div className="input-group">
                                <label htmlFor="destination">
                                    <b>Destination</b>
                                </label>
                                <input value={this.state.destination} onChange={this.handleChange} id="destination" type="text" name="destination" placeholder="e.g. KIAH"/>
                            </div>
                        </div>
                        {this.state.validated 
                            ? 
                            <div className="search-row">
                                <NavLink to={this.state.airport !== '' ? `/airport/${this.state.airport}` : `/findflight/${this.state.origin}/${this.state.destination}`} className="submit-button">GO</NavLink>
                            </div> 
                            : 
                            <div/>}
                    </div>
                </div> */}