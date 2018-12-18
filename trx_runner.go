// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	"cds.ikigai.net/cabinet.v1.test/cabinet"
	pb "cds.ikigai.net/cabinet.v1/rpc"
)

func CDSTransactionRunner(actions *[]pb.TransactionAction, it *CabinetTest) map[string]string{
	cds := &cabinet.Transaction{}
	cds.Setup(it.ctx, it.client)

	for _, action := range *actions {
		cds.O(&action)
	}

	err := cds.Commit()

	if err != nil{
		it.test.Errorf("%v.Transaction(_) = _, %v", it.client, err)
		return nil
	}

	return cds.GetIdMap()
}
