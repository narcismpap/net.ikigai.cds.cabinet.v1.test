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

type indexTestExpectedCount struct{
	key string
	results uint32
}

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
				ActionId: pos + 1, Action: &pb.TransactionAction_IndexCreate{IndexCreate: &pb.Index{
					Type: indexType, Node: tPos, Value: "cats", Properties: tmpNodes[tPos].Properties,
				}}})
		} else {
			trx = append(trx, pb.TransactionAction{
				ActionId: pos + 2, Action: &pb.TransactionAction_IndexCreate{IndexCreate: &pb.Index{
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

func TestIndexChoices(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	trx := make([]pb.TransactionAction, 0)
	indexes := make(map[string][]string)
	expect := make([]indexTestExpectedCount, 0)
	pos := uint32(0)

	iType := uint32(8465)

	// create indexes
	for pos < TestSequentialSize {
		var iKey string

		if pos % 2 == 0 {
			iKey = "cats"
		}else if pos % 3 == 0{
			iKey = "dogs"
		}else{
			iKey = "ikigai.net"
		}

		nID := MockRandomNodeID()
		indexes[iKey] = append(indexes[iKey], nID)

		trx = append(trx, pb.TransactionAction{
			ActionId: pos + 1, Action: &pb.TransactionAction_IndexCreate{IndexCreate: &pb.Index{
				Type: iType, Node: nID, Value: iKey,
			}}})

		pos += 1
	}

	for iKey := range indexes{
		expect = append(expect, indexTestExpectedCount{key: iKey, results: uint32(len(indexes[iKey]))})
	}

	_ = CDSTransactionRunner(&trx, &it)
	trx = make([]pb.TransactionAction, 0)

	// check choices
	indexCheckChoices(&it, expect, iType)

	// change nodes (cats -> alpha, dogs -> beta, drop half of ikigai.net)
	indexes2 := make(map[string][]string)
	pos2 := uint32(1)

	for _, nodeId := range indexes["cats"]{
		trx = append(trx, pb.TransactionAction{
			ActionId: pos2, Action: &pb.TransactionAction_IndexDelete{IndexDelete: &pb.Index{
				Type: iType, Node: nodeId, Value: "cats",
			}}})

		trx = append(trx, pb.TransactionAction{
			ActionId: pos2 + 1, Action: &pb.TransactionAction_IndexCreate{IndexCreate: &pb.Index{
				Type: iType, Node: nodeId, Value: "alpha",
			}}})

		pos2 += 2
		indexes2["alpha"] = append(indexes2["alpha"], nodeId)
	}

	for _, nodeId := range indexes["dogs"]{
		trx = append(trx, pb.TransactionAction{
			ActionId: pos2, Action: &pb.TransactionAction_IndexDelete{IndexDelete: &pb.Index{
				Type: iType, Node: nodeId, Value: "dogs",
			}}})

		trx = append(trx, pb.TransactionAction{
			ActionId: pos2 + 1, Action: &pb.TransactionAction_IndexCreate{IndexCreate: &pb.Index{
				Type: iType, Node: nodeId, Value: "beta",
			}}})

		pos2 += 2
		indexes2["beta"] = append(indexes2["beta"], nodeId)
	}

	for iPos, nodeId := range indexes["ikigai.net"]{
		if iPos % 2 == 0 {
			trx = append(trx, pb.TransactionAction{
				ActionId: pos2, Action: &pb.TransactionAction_IndexDelete{IndexDelete: &pb.Index{
					Type: iType, Node: nodeId, Value: "ikigai.net",
				}}})
			pos2 += 1
		}else{
			indexes2["ikigai.net"] = append(indexes2["ikigai.net"], nodeId)
		}
	}

	_ = CDSTransactionRunner(&trx, &it)
	trx = make([]pb.TransactionAction, 0)

	// check updates
	expect2 := []indexTestExpectedCount{{key: "cats", results: 0}, {key: "dogs", results: 0}}
	for iKey2 := range indexes2{
		expect2 = append(expect2, indexTestExpectedCount{key: iKey2, results: uint32(len(indexes2[iKey2]))})
	}

	indexCheckChoices(&it, expect2, iType)

	// drop indexes
	rsp, err := it.client.IndexDrop(it.ctx, &pb.IndexDropRequest{Index: iType})

	if err != nil{
		it.test.Errorf("IndexDrop(): _, %v", err)
	}else if rsp.Status != pb.MutationStatus_SUCCESS{
		it.test.Errorf("IndexDrop(): got response: %v", rsp)
	}

	// check cleared results
	expect3 := make([]indexTestExpectedCount, 0)
	indexCheckChoices(&it, expect3, iType)

	it.tearDown()
}

func indexCheckChoices(it *CabinetTest, expect []indexTestExpectedCount, iType uint32){
	res := make(map[string]uint32)
	iChoices, err := it.client.IndexChoices(it.ctx, &pb.IndexChoiceRequest{
		Index: iType,
		Opt: &pb.ListOptions{
			Mode: pb.ListRange_ALL, PageSize: TestSequentialSize * 5,
		}})

	if err != nil {
		it.test.Errorf("[E] %v.IndexChoices(%d) = _. %v", it.client, iType, err)
	} else {
		for {
			idx, err := iChoices.Recv()

			if err == io.EOF {
				break
			} else if err != nil {
				it.test.Errorf("[E] %v.IndexChoices(%d) = _. %v", it.client, iType, err)
				break
			} else {
				res[idx.Value] = idx.Count
			}
		}
	}

	for e := range expect{
		if res[expect[e].key] == expect[e].results{
			it.test.Logf("%s: got %d results", expect[e].key, expect[e].results)
		}else{
			it.test.Errorf("%s: got %d, expected %d results", expect[e].key, res[expect[e].key], expect[e].results)
		}
	}

	for e2 := range res{
		found := false

		for e3 := range expect{
			if expect[e3].key == e2{
				found = true
				break
			}
		}

		if found == false{
			it.test.Errorf("indexCheckChoices: Got unexpected index: %s (%d)", e2, res[e2])
		}
	}

	if len(res) != len(expect){
		it.test.Errorf("expected %d records, got %d", len(expect), len(res))
	}
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
