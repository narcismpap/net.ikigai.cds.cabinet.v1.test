// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"errors"
	"fmt"
	"testing"
)

func edgeWithPayload(e *pb.Edge, payload []byte) *pb.Edge{
	return &pb.Edge{
		Subject: e.Subject,
		Predicate: e.Predicate,
		Target: e.Target,
		Properties: payload,
	}
}

func TestTransactionEdgeComplexCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	payload1 := []byte("Harry Potter")
	payload2 := []byte("Hermione Granger")

	payload3 := []byte("Professor Albus Dumbledore")
	payload4 := []byte("Professor Minerva McGonagall")

	e1 := &pb.Edge{Subject: "1EKkY0eMD7bVu4jenaz6skyzbt1", Predicate: 100, Target: "1EKkY1T6y4G3Xf2jtlaM39VucSX"}
	e2 := &pb.Edge{Subject: "1EKkY0p9MGb3kAl9TO0dkOkHdQv", Predicate: 84, Target: "1EKkXz3CjX9vALVvgyayPfECq6I"}

	t1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edgeWithPayload(e1, payload1)}},
		{ActionId: 2, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edgeWithPayload(e2, payload3)}},
	}

	_ = transactionRunner(&t1, &it)

	// check edges
	r1, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e1})
	it.logThing(r1, err, "EdgeGet")
	validatePayload(r1, &it, payload1, r1.Properties)

	r2, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e2})
	it.logThing(r2, err, "EdgeGet")
	validatePayload(r2, &it, payload3, r2.Properties)

	// update edges 1 -> 4, 2 -> 2
	t2 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edgeWithPayload(e1, payload4)}},
		{ActionId: 2, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edgeWithPayload(e2, payload2)}},
	}

	_ = transactionRunner(&t2, &it)

	r3, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e1})
	it.logThing(r3, err, "EdgeGet")
	validatePayload(r3, &it, payload4, r3.Properties)

	r4, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e2})
	it.logThing(r4, err, "EdgeGet")
	validatePayload(r4, &it, payload2, r4.Properties)
	
	// remove payload one one
	t3 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edgeWithPayload(e1, nil)}},
	}

	_ = transactionRunner(&t3, &it)
	
	r5, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e1})
	it.logThing(r5, err, "EdgeGet")
	validatePayload(r5, &it, []byte(""), r5.Properties)
	
	it.tearDown()
}

func TestTransactionEdgeEmptyPayload(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	e1 := &pb.Edge{Subject: "1EKkY0eMD7bVu4jenaz6skyzbzp", Predicate: 134, Target: "1EKkY1T6y4G3Xf2jtlaM39Vuc31"}

	t1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: e1}},
	}

	_ = transactionRunner(&t1, &it)

	r1, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e1})
	it.logThing(r1, err, "EdgeGet")
	validatePayload(r1, &it, []byte(""), r1.Properties)

	it.tearDown()
}


func validatePayload(ob interface{}, it *CabinetTest, expect []byte, receive []byte){
	if string(receive) != string(expect){
		it.logThing(ob, errors.New(fmt.Sprintf("Payload mismath. Received [%s] expected [%s]", receive, expect)), "PayloadVerify")
	}else{
		it.test.Logf("payload ok: %s", receive)
	}
}