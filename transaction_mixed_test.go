// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import "testing"

func TestTransactionGeography(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	// objective:
	// create node types: Countries, Cities
	// create 3 edge types: CountryHasCity, CityInCountry

	// create 4 Country nodes, 4 City nodes
	// make the edge relations
	// setup indexes on country.code, city.airport
	// setup meta in country.history, city.bio
	// verify records

	it.tearDown()
}
