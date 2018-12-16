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

func nodeWithPayload(n *pb.Node, payload []byte) *pb.Node{
	n2 := n
	n.Properties = payload
	return n2
}

func nodeWithoutPayload(n *pb.Node) *pb.Node{
	n2 := n
	n.Properties = nil
	return n2
}

func TestTransactionNodeSimpleCRUD(t *testing.T) {
	t.Parallel()

	itMutation := CabinetTest{test: t}
	itMutation.setup(2)

	n1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: 1, Version: 1, Id: "tmp:1"}}},
		{ActionId: 2, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: 1, Id: "tmp:1"}}},
		{ActionId: 3, Action: &pb.TransactionAction_NodeDelete{NodeDelete: &pb.Node{Type: 1, Id: "tmp:1"}}},
	}

	mapIDs := CDSTransactionRunner(&n1, &itMutation)
	itMutation.tearDown()

	itRead := CabinetTest{test: t}
	itRead.setup(2)

	expectedNull, err := itRead.client.NodeGet(itRead.ctx, &pb.NodeGetRequest{NodeType: 1, Id: mapIDs["tmp:1"]})
	validateErrorNotFound(mapIDs["tmp:1"], expectedNull, &itRead, err)

	itRead.tearDown()
}

func TestTransactionNodeMultiCRUD(t *testing.T) {
	t.Parallel()

	it := CabinetTest{test: t}
	it.setup(4)

	payload1, payload2, payload3, payload4 := MockRandomPayload(), MockRandomPayload(), MockRandomPayload(), MockRandomPayload()
	nodeType := uint32(1)

	n1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: nodeType, Version: 1, Id: "tmp:1", Properties: payload1}}},
		{ActionId: 2, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: nodeType, Version: 1, Id: "tmp:2", Properties: payload2}}},
	}

	mapIDs := CDSTransactionRunner(&n1, &it)
	it.test.Logf("mapIDs is %v", mapIDs)

	el1, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: mapIDs["tmp:1"]})
	it.logThing(el1, err, "NodeGet")
	validatePayload(el1, &it, payload1, el1.Properties)

	el2, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: mapIDs["tmp:2"]})
	it.logThing(el2, err, "NodeGet")
	validatePayload(el2, &it, payload2, el2.Properties)

	el1, el2 = nil, nil

	// new payloads and out-of-order ActionId
	n2 := []pb.TransactionAction{
		{ActionId: 2, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: nodeType, Id: mapIDs["tmp:2"], Properties: payload3}}},
		{ActionId: 8, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: nodeType, Id: mapIDs["tmp:1"], Properties: payload4}}},
	}

	_ = CDSTransactionRunner(&n2, &it)

	el3, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: mapIDs["tmp:1"]})
	it.logThing(el3, err, "NodeGet")
	validatePayload(el3, &it, payload4, el3.Properties)

	el4, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: mapIDs["tmp:2"]})
	it.logThing(el4, err, "NodeGet")
	validatePayload(el4, &it, payload3, el4.Properties)

	el3, el4 = nil, nil

	// delete one of the payloads
	n3 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeDelete{NodeDelete: &pb.Node{Type: nodeType, Id: mapIDs["tmp:2"]}}},
	}

	_ = CDSTransactionRunner(&n3, &it)

	expectedNull, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: 1, Id: mapIDs["tmp:2"]})
	validateErrorNotFound(mapIDs["tmp:2"], expectedNull, &it, err)

	// Create and Alter in one go
	n4 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: 1, Version: 1, Id: "tmp:5"}}},
		{ActionId: 2, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: 1, Id: "tmp:5", Properties: payload4}}},
	}

	cNodeIDs := CDSTransactionRunner(&n4, &it)

	el5, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: cNodeIDs["tmp:5"]})
	it.logThing(el1, err, "NodeGet")
	validatePayload(el5, &it, payload4, el5.Properties)

	// delete other nodes
	n5 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeDelete{NodeDelete: &pb.Node{Type: nodeType, Id: mapIDs["tmp:1"]}}},
		{ActionId: 2, Action: &pb.TransactionAction_NodeDelete{NodeDelete: &pb.Node{Type: nodeType, Id: cNodeIDs["tmp:5"]}}},
	}

	_ = CDSTransactionRunner(&n5, &it)

	it.tearDown()
}
