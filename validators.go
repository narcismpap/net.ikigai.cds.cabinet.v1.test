// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	"errors"
	"fmt"
)

func validateErrorNotFound(object interface{}, response interface{}, it *CabinetTest, err error) {
	if err != nil {
		it.test.Logf("[I] Object %v was deleted as expected", object)
	} else {
		it.logThing(response, errors.New("object should have been deleted"), "ValidateGetNotFound")
	}
}

func validatePayload(ob interface{}, it *CabinetTest, expect []byte, receive []byte) {
	if string(receive) != string(expect) {
		it.logThing(ob, errors.New(fmt.Sprintf("Payload mismath. Received [%s] expected [%s]", receive, expect)), "PayloadVerify")
	} else {
		it.test.Logf("validatePayload(%v) = Match [%s]", ob, receive)
	}
}
