// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"errors"
	"io"
	"sync"
	"testing"
)

func TestTransactionNodeCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup()

	n1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{Type: 1, Version: 1, Id: "tmp:1"}}},
		{ActionId: 2, Action: &pb.TransactionAction_NodeUpdate{NodeUpdate: &pb.Node{Type: 1, Id: "tmp:1"}}},
		{ActionId: 3, Action: &pb.TransactionAction_NodeDelete{NodeDelete: &pb.Node{Type: 1, Id: "tmp:1"}}},
	}

	mapN1 := transactionRunner(&n1, &it)

	expectedNull, err := it.client.NodeGet(it.ctx, &pb.NodeGetRequest{NodeType: 1, Id: mapN1["tmp:1"]})

	if err != nil{
		it.test.Logf("[I] Node %s was deleted as expected", mapN1["tmp:1"])
	}else{
		it.logThing(expectedNull, errors.New("node was supposed to be deleted"), "NodeGet")
	}

	it.tearDown()
}

func transactionRunner(actions *[]pb.TransactionAction, it *CabinetTest) map[string]string{
	stream, err := it.client.Transaction(it.ctx)
	tempMap := make(map[uint32]string)
	idMap := make(map[string]string)
	var tMutex sync.Mutex

	if err != nil {
		it.test.Errorf("%v.Transaction(_) = _, %v", it.client, err)
	}

	waitc := make(chan struct{})

	go func() {
		for {
			in, err := stream.Recv()

			if err == io.EOF {
				close(waitc)
				return
			}

			if err != nil {
				it.test.Errorf("Failed to receive TransactionActionResponse in %v.Transaction(_) = _, %v", it.client, err)
			}else {
				it.test.Logf("For [%d] got response %v", in.ActionId, in)

				switch tReq := in.Response.(type) {
					case *pb.TransactionActionResponse_NodeCreate:
						tMutex.Lock()
						idMap[tempMap[in.ActionId]] = tReq.NodeCreate.Id
						tMutex.Unlock()
				}
			}
		}
	}()

	for _, action := range *actions {
		if err := stream.Send(&action); err != nil {
			it.test.Errorf("Failed to send TransactionAction to %v.Transaction(_) = _, %v", it.client, err)
		}

		switch tReq := action.Action.(type) {
			case *pb.TransactionAction_NodeCreate:
				tempMap[action.ActionId] = tReq.NodeCreate.Id
			}
	}

	stream.CloseSend()
	<-waitc

	return idMap
}

