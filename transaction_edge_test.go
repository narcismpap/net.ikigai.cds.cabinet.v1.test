// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"testing"
)

// edgeSimpleCRUD
// edgeComplexCRUD
// edgeClear
// edgeMultiClear

func edgeWithPayload(e *pb.Edge, payload []byte) *pb.Edge{
	return &pb.Edge{
		Subject: e.Subject,
		Predicate: e.Predicate,
		Target: e.Target,
		Properties: payload,
	}
}

func edgeWithoutPayload(e *pb.Edge) *pb.Edge{
	return &pb.Edge{
		Subject: e.Subject,
		Predicate: e.Predicate,
		Target: e.Target,
	}
}

func TestTransactionEdgeComplexCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	payload1, payload2, payload3, payload4 := MockRandomPayload(), MockRandomPayload(), MockRandomPayload(), MockRandomPayload()
	e1, e2 := MockRandomEdge(), MockRandomEdge()

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

	r1, r2 = nil, nil

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

	r3, r4 = nil, nil

	// remove payload one
	t3 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edgeWithPayload(e1, nil)}},
	}

	_ = transactionRunner(&t3, &it)
	
	r5, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e1})
	it.logThing(r5, err, "EdgeGet")
	validatePayload(r5, &it, []byte(""), r5.Properties)

	r6, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e2})
	it.logThing(r6, err, "EdgeGet")
	validatePayload(r6, &it, payload2, r6.Properties)

	r5, r6 = nil, nil

	// delete one edge, check the other
	t4 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeDelete{EdgeDelete: e1}},
	}

	_ = transactionRunner(&t4, &it)

	null1, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e1})
	validateErrorNotFound(e1, null1, &it, err)

	r7, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e2})
	it.logThing(r7, err, "EdgeGet")
	validatePayload(r7, &it, payload2, r7.Properties)

	null1, r7 = nil, nil

	// delete other edge
	t5 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeDelete{EdgeDelete: e2}},
	}

	_ = transactionRunner(&t5, &it)

	null2, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e2})
	validateErrorNotFound(e2, null2, &it, err)

	it.tearDown()
}

func TestTransactionEdgeEmptyPayload(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	e1 := MockRandomEdge()

	t1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: e1}},
	}

	_ = transactionRunner(&t1, &it)

	r1, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: e1})
	it.logThing(r1, err, "EdgeGet")
	validatePayload(r1, &it, []byte(""), r1.Properties)

	it.tearDown()
}


func TestTransactionEdgeClear(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	edges := make([]*pb.Edge, 0)
	trx := make([]pb.TransactionAction, 0)
	pos := uint32(0)

	// create edges
	for pos < TestSequentialSize{
		edge := &pb.Edge{Subject: "0EKkESgUWIlAgoqLguCtqEESgUW", Predicate: 2018, Target: MockRandomNodeID(), Properties: MockRandomPayload()}

		edges = append(edges, edge)
		trx = append(trx, pb.TransactionAction{
			ActionId: pos, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edge},
		})

		pos += 1
	}

	_ = transactionRunner(&trx, &it)

	// get edges
	pos = 0

	for edgeIdx := range edges{
		eRsp, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: edgeWithoutPayload(edges[edgeIdx])})
		it.logThing(eRsp, err, "EdgeGet")
		validatePayload(eRsp, &it, edges[edgeIdx].Properties, eRsp.Properties)
	}

	// clear all edges
	t2 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeClear{EdgeClear: &pb.Edge{Subject: "0EKkESgUWIlAgoqLguCtqEESgUW", Predicate: 2018}}},
	}

	_ = transactionRunner(&t2, &it)

	// attempt to get any of the previous edges
	for edgeIdx := range edges{
		eNull, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: edgeWithoutPayload(edges[edgeIdx])})
		validateErrorNotFound(edges[edgeIdx], eNull, &it, err)
	}

	it.tearDown()
}

func TestTransactionEdgeMultiClear(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	edges := make([]*pb.Edge, 0)
	trx := make([]pb.TransactionAction, 0)
	pos := uint32(0)

	// create edges
	for pos < TestSequentialSize{
		predicate := uint32(2019)

		if pos > 50{
			predicate = uint32(2020)
		}

		edge := &pb.Edge{Subject: "0EKkESgUWIlAgoqLguCtqEESYYY", Predicate: predicate, Target: MockRandomNodeID(), Properties: MockRandomPayload()}

		edges = append(edges, edge)
		trx = append(trx, pb.TransactionAction{
			ActionId: pos, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edge},
		})

		pos += 1
	}

	_ = transactionRunner(&trx, &it)

	// clear P:2019 edges
	t2 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeClear{EdgeClear: &pb.Edge{Subject: "0EKkESgUWIlAgoqLguCtqEESYYY", Predicate: 2019}}},
	}

	_ = transactionRunner(&t2, &it)

	// attempt to get any of the previous edges
	for edgeIdx := range edges{
		eEX, err := it.client.EdgeGet(it.ctx, &pb.EdgeGetRequest{Edge: edgeWithoutPayload(edges[edgeIdx])})

		if edges[edgeIdx].Predicate == 2020{
			validatePayload(eEX, &it, edges[edgeIdx].Properties, eEX.Properties)
		}else{
			validateErrorNotFound(edges[edgeIdx], eEX, &it, err)
		}
	}

	it.tearDown()
}
