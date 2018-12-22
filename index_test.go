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

func TestIndexCreateListAll(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	tmpNodes := make(map[string]*pb.Node)
	iNodes := make(map[string]*pb.Node)

	trx := make([]pb.TransactionAction, 0)
	pos := uint32(0)
	npos := uint32(0)

	// setup Prop ID & Node Type
	iSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "i", Uuid: MockRandomUUID()})
	it.logThing("SeqCreate", err, "SequentialCreate")
	indexType := iSeq.Seqid

	inSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: MockRandomUUID()})
	it.logThing("SeqCreate", err, "SequentialCreate")
	nodeType := inSeq.Seqid

	i2Seq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "i", Uuid: MockRandomUUID()})
	it.logThing("SeqCreate", err, "SequentialCreate")
	index2Type := i2Seq.Seqid

	// setup nodes
	for npos < TestSequentialSize {
		tPos := fmt.Sprintf("tmp:%d", pos)
		tmpNodes[tPos] = &pb.Node{
			Type: nodeType, Version: 1, Id: tPos, Properties: []byte(MockRandomAlpha(10)),
		}

		trx = append(trx, pb.TransactionAction{
			ActionId: pos, Action: &pb.TransactionAction_NodeCreate{NodeCreate: tmpNodes[tPos]}})

		if npos%2 == 0 {
			trx = append(trx, pb.TransactionAction{
				ActionId: pos + 1, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: &pb.Index{
					Type: indexType, Node: tPos, Value: "cats", Properties: tmpNodes[tPos].Properties,
				}}})
		} else {
			trx = append(trx, pb.TransactionAction{
				ActionId: pos + 2, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: &pb.Index{
					Type: index2Type, Node: tPos, Value: "dogs", Properties: tmpNodes[tPos].Properties,
				}}})
		}

		pos += 3
		npos += 1
	}

	mapIDs := CDSTransactionRunner(&trx, &it)

	for tmp, aID := range mapIDs {
		iNodes[aID] = tmpNodes[tmp]
		iNodes[aID].Id = aID
	}
	tmpNodes = nil

	// check list
	indexCheckList(&it, iNodes, indexType, "cats", TestSequentialSize/2)
	indexCheckList(&it, iNodes, index2Type, "dogs", TestSequentialSize/2)

	// make sure there is no contamination
	indexCheckList(&it, iNodes, indexType, "dogs", 0)
	indexCheckList(&it, iNodes, index2Type, "cats", 0)

	// remove indexes
	delTrx := make([]pb.TransactionAction, 0)
	tPos := uint32(0)

	for i := range iNodes {
		delTrx = append(delTrx, pb.TransactionAction{
			ActionId: tPos, Action: &pb.TransactionAction_IndexDelete{IndexDelete: &pb.Index{
				Type: indexType, Node: i, Value: "cats",
			}}})

		delTrx = append(delTrx, pb.TransactionAction{
			ActionId: tPos + 1, Action: &pb.TransactionAction_IndexDelete{IndexDelete: &pb.Index{
				Type: index2Type, Node: i, Value: "dogs",
			}}})

		tPos += 2
	}

	_ = CDSTransactionRunner(&delTrx, &it)

	// check indexes again
	indexCheckList(&it, iNodes, indexType, "cats", 0)
	indexCheckList(&it, iNodes, index2Type, "dogs", 0)

	it.tearDown()
}

func indexCheckList(it *CabinetTest, nodes map[string]*pb.Node, indexType uint32, val string, size uint16) {
	rCount := uint16(0)

	lStr, err := it.client.IndexList(it.ctx, &pb.IndexListRequest{
		Index: indexType, Value: val,
		IncludeIndex: true, IncludeValue: true, IncludeProp: true, IncludeNode: true,
		Opt: &pb.ListOptions{
			Mode: pb.ListRange_ALL, PageSize: TestSequentialSize * 5,
		}})

	if err != nil {
		it.test.Errorf("[E] %v.IndexList(%d,%s) = _. %v", it.client, indexType, val, err)
	} else {
		for {
			index, err := lStr.Recv()

			if err == io.EOF {
				break
			} else if err != nil {
				it.test.Errorf("[E] %v.IndexList(%d,%s) = _, %v", it.client, indexType, val, err)
				break
			} else {
				rCount += 1

				if index.Type != indexType {
					it.test.Errorf("[E] index.type got %d expected %d", index.Type, nodes[index.Node].Type)
				} else if index.Node != nodes[index.Node].Id {
					it.test.Errorf("[E] index.node got %s expected %s", index.Node, nodes[index.Node].Id)
				} else if index.Value != val {
					it.test.Errorf("[E] index.value got %v expected %v", index.Value, val)
				} else if string(index.Properties) != string(nodes[index.Node].Properties) {
					it.test.Errorf("[E] index.properties got %v expected %v", string(index.Properties), string(nodes[index.Node].Properties))
				} else {
					it.test.Logf("[I] %v.IndexList(%d,%s) got %v", it.client, indexType, val, index)
				}
			}
		}
	}

	if size != rCount {
		it.test.Errorf("expected %d records, got %d", size, rCount)
	}
}
