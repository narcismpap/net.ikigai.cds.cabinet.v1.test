// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"fmt"
	"io"
	"testing"
)

func TestNodeCreateListAll(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	nPayloads := make(map[string][]byte)
	trx := make([]pb.TransactionAction, 0)
	pos := uint32(0)

	nSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: MockRandomUUID()})
	it.logThing("SeqCreate", err, "SequentialCreate")
	nType := nSeq.Seqid

	// create nodes
	for pos < TestSequentialSize {
		tPos := fmt.Sprintf("tmp:%d", pos)
		nPayloads[tPos] = MockRandomPayload()

		trx = append(trx, pb.TransactionAction{
			ActionId: pos, Action: &pb.TransactionAction_NodeCreate{NodeCreate: &pb.Node{
				Type: nType, Version: 1, Id: tPos, Properties: nPayloads[tPos],
			}}})

		pos += 1
	}

	mapIDs := CDSTransactionRunner(&trx, &it)
	rMap := make(map[string]string)
	nReceived := uint32(0)

	for tmp, aID := range mapIDs {
		rMap[aID] = tmp
	}

	// check list
	lStr, err := it.client.NodeList(it.ctx, &pb.NodeListRequest{
		NodeType:    nType,
		IncludeType: true, IncludeId: true, IncludeProp: true,
		Opt: &pb.ListOptions{
			Mode: pb.ListRange_ALL, PageSize: TestSequentialSize * 5,
		}})

	if err != nil {
		it.test.Errorf("[E] %v.NodeList(%d) = _. %v", it.client, nType, err)
	} else {
		for {
			node, err := lStr.Recv()

			if err == io.EOF {
				break
			} else if err != nil {
				it.test.Errorf("[E] %v.NodeList(_) = _, %v", it.client, err)
				break
			} else {
				tmpPos := rMap[node.Id]
				nReceived += 1

				if node.Type != nType {
					it.test.Errorf("[E] node.type got %d expected %d", node.Type, nType)
				} else if node.Id != mapIDs[rMap[node.Id]] {
					it.test.Errorf("[E] node.id got %s expected %s", node.Id, mapIDs[rMap[node.Id]])
				} else if string(node.Properties) != string(nPayloads[tmpPos]) {
					it.test.Errorf("[E] node.properties got %v expected %v", string(node.Properties), string(nPayloads[tmpPos]))
				} else {
					it.test.Logf("[I] %v.NodeList(%d) got %v", it.client, nType, node)
				}
			}
		}
	}

	if nReceived != pos {
		it.test.Errorf("[E] Received %d nodes, expected %d results", nReceived, pos)
	}

	// clear records
	trxDelete := make([]pb.TransactionAction, 0)
	dPos := uint32(0)

	for _, nID := range mapIDs {
		trxDelete = append(trxDelete, pb.TransactionAction{
			ActionId: dPos, Action: &pb.TransactionAction_NodeDelete{NodeDelete: &pb.Node{
				Type: nType, Id: nID,
			}}})
		dPos += 1
	}

	_ = CDSTransactionRunner(&trxDelete, &it)

	// try loading records again
	nullList, err := it.client.NodeList(it.ctx, &pb.NodeListRequest{
		NodeType:    nType,
		IncludeType: true, IncludeId: true, IncludeProp: true,
		Opt: &pb.ListOptions{
			Mode: pb.ListRange_ALL, PageSize: TestSequentialSize * 5,
		}})

	if err != nil {
		it.test.Errorf("[E] %v.NodeList(%d) = _. %v", it.client, nType, err)
	} else {
		for {
			node, err := nullList.Recv()

			if err == io.EOF {
				break
			} else if err != nil {
				it.test.Errorf("[E] %v.NodeList(_) = _, %v", it.client, err)
				break
			} else {
				it.test.Errorf("[E] Unexpected node received: %v", node)
			}
		}
	}

	it.tearDown()
}
