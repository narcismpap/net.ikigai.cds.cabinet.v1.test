// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	"cds.ikigai.net/cabinet.v1.test/cabinet"
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"fmt"
	"strings"
	"testing"
)

// No need to do very comprehensive T.ReadCheck tests as that is taken care of by read_check.go file
// T.ReadCheck is just a wrapper around using an existing transaction for live integrity testing

func TestTransactionReadCheckNode(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(2)

	p1, p2 := MockRandomPayload(), MockRandomPayload()
	n1 := &pb.Node{Type: uint32(MockRandomInt(1, 65000)), Version: 1, Id: "tmp:1", Properties: p1}

	mapIDs := CDSTransactionRunner(&([]pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: n1}},
	}), &it)

	n1.Id = mapIDs["tmp:1"]
	n1IRI := fmt.Sprintf("n/%d/%s", n1.Type, n1.Id)

	cds := cabinet.Transaction{}
	cds.Setup(it.ctx, it.client)

	cds.O(&pb.TransactionAction{
		Action: &pb.TransactionAction_ReadCheck{ReadCheck: &pb.ReadCheckRequest{
			Source: n1IRI, Operator: pb.CheckOperators_EQUAL, Target: &pb.CheckTarget{Target: &pb.CheckTarget_Val{Val: "not good"}},
		}}})

	cds.O(&pb.TransactionAction{
		Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: nodeWithPayload(n1, p2)},
	})

	checkTransactionFailed(&it, cds.Commit(), "E(0x013)")

	// should still have p1
	el1, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: n1.Type, Id: n1.Id})
	it.logThing(el1, err, "NodeGet")
	validatePayload(el1, &it, p1, el1.Properties)

	// update payload with valid R/C, now payload is p2
	cds2 := cabinet.Transaction{}
	cds2.Setup(it.ctx, it.client)

	cds2.O(&pb.TransactionAction{
		Action: &pb.TransactionAction_ReadCheck{ReadCheck: &pb.ReadCheckRequest{
			Source: n1IRI, Operator: pb.CheckOperators_EQUAL, Target: &pb.CheckTarget{Target: &pb.CheckTarget_Val{Val:string(p1)}},
		}}})

	cds2.O(&pb.TransactionAction{
		Action: &pb.TransactionAction_ReadCheck{ReadCheck: &pb.ReadCheckRequest{
			Source: n1IRI, Operator: pb.CheckOperators_EXISTS, Target: &pb.CheckTarget{Target: &pb.CheckTarget_Val{Val:"*"}},
		}}})

	cds2.O(&pb.TransactionAction{Action: &pb.TransactionAction_ReadCheck{ReadCheck: &pb.ReadCheckRequest{
		Source: n1IRI, Operator: pb.CheckOperators_TOUCH, Target: &pb.CheckTarget{Target: &pb.CheckTarget_Val{Val:"*"}},
	}}})

	cds2.O(&pb.TransactionAction{Action: &pb.TransactionAction_NodeUpdate{
		NodeUpdate: nodeWithPayload(n1, p2),
	}})

	checkTransactionSuccess(&it, cds2.Commit())

	el2, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: n1.Type, Id: n1.Id})
	it.logThing(el2, err, "NodeGet")
	validatePayload(el2, &it, p2, el2.Properties)

	it.tearDown()
}

func checkTransactionFailed(it *CabinetTest, err error, code string){
	if err != nil && strings.Contains(err.Error(), code){
		it.test.Logf("Transaction was rejected with code: %s", code)
	}else{
		it.test.Errorf("Transaction was not rejected with %s, got %v", code, err)
	}
}

func checkTransactionSuccess(it *CabinetTest, err error){
	if err != nil{
		it.test.Errorf("Transaction failed with error: %s", err)
	}else{
		it.test.Log("Transaction was committed successfully")
	}
}