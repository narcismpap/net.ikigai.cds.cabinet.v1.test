// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"errors"
	"testing"
)

func TestTransactionNodeSimpleCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	n1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: 1, Version: 1, Id: "tmp:1"}}},
		{ActionId: 2, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: 1, Id: "tmp:1"}}},
		{ActionId: 3, Action: &pb.TransactionAction_NodeDelete{NodeDelete: &pb.Node{Type: 1, Id: "tmp:1"}}},
	}

	mapIDs := transactionRunner(&n1, &it)

	expectedNull, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: 1, Id: mapIDs["tmp:1"]})

	if err != nil{
		it.test.Logf("[I] Node %s was deleted as expected", mapIDs["tmp:1"])
	}else{
		it.logThing(expectedNull, errors.New("node was supposed to be deleted"), "NodeGet")
	}

	it.tearDown()
}

func TestTransactionNodeMultiCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	payload1 := []byte("Harry Potter")
	payload2 := []byte("Hermione Granger")

	payload3 := []byte("Professor Albus Dumbledore")
	payload4 := []byte("Professor Minerva McGonagall")

	nodeType := uint32(1)

	n1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: nodeType, Version: 1, Id: "tmp:1", Properties: payload1}}},
		{ActionId: 2, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: nodeType, Version: 1, Id: "tmp:2", Properties: payload2}}},
	}

	mapIDs := transactionRunner(&n1, &it)
	it.test.Logf("mapIDs is %v", mapIDs)

	el1, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: mapIDs["tmp:1"]})
	it.logThing(el1, err, "NodeGet")

	if err == nil && string(el1.Properties) != string(payload1){
		it.test.Errorf("NodeGet(%d, %s) Unexpected payload, wanted [%v] got [%v]", nodeType, mapIDs["tmp:1"], payload1, el1.Properties)
	}

	el2, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: mapIDs["tmp:2"]})
	it.logThing(el2, err, "NodeGet")

	if err == nil && string(el2.Properties) != string(payload2){
		it.test.Errorf("NodeGet(%d, %s) Unexpected payload, wanted [%v] got [%v]", nodeType, mapIDs["tmp:2"], payload1, el1.Properties)
	}

	el1, el2 = nil, nil

	// new payloads and out-of-order ActionId
	n2 := []pb.TransactionAction{
		{ActionId: 2, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: nodeType, Id: mapIDs["tmp:2"], Properties: payload3}}},
		{ActionId: 8, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: nodeType, Id: mapIDs["tmp:1"], Properties: payload4}}},
	}

	_ = transactionRunner(&n2, &it)

	el3, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: mapIDs["tmp:1"]})
	it.logThing(el3, err, "NodeGet")

	if err == nil && string(el3.Properties) != string(payload4){
		it.test.Errorf("NodeGet(%d, %s) Unexpected payload, wanted [%v] got [%v]", nodeType, mapIDs["tmp:1"], payload4, el3.Properties)
	}

	el4, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: mapIDs["tmp:2"]})
	it.logThing(el4, err, "NodeGet")

	if err == nil && string(el4.Properties) != string(payload3){
		it.test.Errorf("NodeGet(%d, %s) Unexpected payload, wanted [%v] got [%v]", nodeType, mapIDs["tmp:2"], payload3, el4.Properties)
	}

	// delete one of the payloads
	n3 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeDelete{NodeDelete: &pb.Node{Type: nodeType, Id: mapIDs["tmp:2"]}}},
	}

	_ = transactionRunner(&n3, &it)

	expectedNull, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: 1, Id: mapIDs["tmp:2"]})

	if err != nil{
		it.test.Logf("[I] Node %s was deleted as expected", mapIDs["tmp:1"])
	}else{
		it.logThing(expectedNull, errors.New("node was supposed to be deleted"), "NodeGet")
	}

	// Create and Alter in one go
	n4 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: 1, Version: 1, Id: "tmp:5"}}},
		{ActionId: 2, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: 1, Id: "tmp:5", Properties: payload4}}},
	}

	cNodeIDs := transactionRunner(&n4, &it)

	el5, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: nodeType, Id: cNodeIDs["tmp:5"]})
	it.logThing(el1, err, "NodeGet")

	if err == nil && string(el5.Properties) != string(payload4){
		it.test.Errorf("NodeGet(%d, %s) Unexpected payload, wanted [%v] got [%v]", nodeType, cNodeIDs["tmp:5"], payload4, el5.Properties)
	}


	it.tearDown()
}

