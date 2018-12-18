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

func CDSTransactionRunner(actions *[]pb.TransactionAction, it *CabinetTest) map[string]string{
	stream, err := it.client.Transaction(it.ctx)
	tempMap 	:= make(map[uint32]string)
	idMap 		:= make(map[string]string)
	idMux 		:= sync.Mutex{}
	tmpMux 		:= sync.Mutex{}


	if err != nil {
		it.test.Errorf("%v.Transaction(_) = _, %v", it.client, err)
		return idMap
	}

	if len(*actions) == 0{
		it.test.Errorf("%v.Transaction(_) = _, %v", it.client, "No actions supplied")
		return idMap
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
				it.test.Errorf("TransactionActionResponse(_) = _, %v", err)
				close(wc)
				return
			}else {
				it.test.Logf("TransactionActionResponse(%d) = %v", in.ActionId, in)

				switch tReq := in.Response.(type) {
				case *pb.TransactionActionResponse_NodeCreate:
					idMux.Lock()
					tmpMux.Lock()

					idMap[tempMap[in.ActionId]] = tReq.NodeCreate.Id

					tmpMux.Unlock()
					idMux.Unlock()
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
				tmpMux.Lock()
				tempMap[action.ActionId] = tReq.NodeCreate.Id
				tmpMux.Unlock()
			}
	}

	err = stream.CloseSend()

	if err != nil{
		it.test.Errorf("Failed to close stream to %v.Transaction(_) = _, %v", it.client, err)
	}

	<-wc

	return idMap
}

