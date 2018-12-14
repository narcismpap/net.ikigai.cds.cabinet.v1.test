// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"io"
	"sync"
)

func transactionRunner(actions *[]pb.TransactionAction, it *CabinetTest) map[string]string{
	stream, err := it.client.Transaction(it.ctx)
	tempMap := make(map[uint32]string)
	idMap := make(map[string]string)

	var tMutex sync.Mutex

	if err != nil {
		it.test.Errorf("%v.Transaction(_) = _, %v", it.client, err)
	}

	wc := make(chan struct{})

	go func() {
		for {
			in, err := stream.Recv()

			if err == io.EOF {
				close(wc)
				return
			}

			if err != nil {
				it.test.Errorf("Failed to receive TransactionActionResponse in %v.Transaction(_) = _, %v", it.client, err)
				close(wc)
				return
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
	<-wc

	return idMap
}

