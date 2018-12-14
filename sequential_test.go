// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestSequenceBadSignatureCreate(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(4)

	mt1, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n"}) // missing Node
	tester.logRejection(mt1, err, "SequentialCreate(Type, Node=nil)")

	mt2, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Node: "XXXX"}) // missing Type
	tester.logRejection(mt2, err, "SequentialCreate(Type=nil, Node)")

	mt3, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{}) // missing Type & None
	tester.logRejection(mt3, err, "SequentialCreate(Type=nil, Node=nil)")

	mt4, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Node: "XXXX", Type: "n", Seqid: uint32(100)}) // Unexpected Seqid
	tester.logRejection(mt4, err, "SequentialCreate(Type, Node, +Seqid)")

	tester.tearDown()
}

func TestSequenceBadSignatureUpdate(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(4)

	// Bad Signature (Update)
	up1, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{}) // missing Type, Node, Seqid
	tester.logRejection(up1, err, "SequentialUpdate(Type=nil, Node=nil, Seq=nil)")

	up2, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Type: "n", Node: "XXX"}) // missing SeqID
	tester.logRejection(up2, err, "SequentialUpdate(Type, Node, Seq=nil)")

	up3, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Type: "n", Seqid: uint32(100)}) // missing Node
	tester.logRejection(up3, err, "SequentialUpdate(Type, Node=nil, Seq)")

	up4, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Node: "XXX", Seqid: uint32(100)}) // missing Type
	tester.logRejection(up4, err, "SequentialUpdate(Type=nil, Node, Seq)")

	up5, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Seqid: uint32(100)}) // missing Type & Node
	tester.logRejection(up5, err, "SequentialUpdate(Type=nil, Node=nil, Seq)")

	up6, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Node: "XXX"}) // missing Type & Sequence
	tester.logRejection(up6, err, "SequentialUpdate(Type=nil, Node, Seq=nil)")

	up7, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Type: "n"}) // missing Node & Sequence
	tester.logRejection(up7, err, "SequentialUpdate(Type, Node=nil, Seq=nil)")

	tester.tearDown()
}

func TestSequenceBadSignatureDelete(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(4)

	dl1, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n"}) // missing SeqID
	tester.logRejection(dl1, err, "SequentialDelete(Type, Seq=nil)")

	dl2, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Seqid: uint32(100)}) // missing Type
	tester.logRejection(dl2, err, "SequentialDelete(Type=nil, Seq)")

	dl3, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{}) // missing Type & SeqId
	tester.logRejection(dl3, err, "SequentialDelete(Type=nil, Seq=nil)")

	dl4, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n", Seqid: uint32(100), Node: "xxx"}) // extra Node
	tester.logRejection(dl4, err, "SequentialDelete(Type, Seq, +Node)")

	tester.tearDown()
}

func TestSequenceBadSignatureGet(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(4)

	gr1, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{}) // Missing Type & Seq
	tester.logRejection(gr1, err, "SequentialGet(Type=nil, Seq=nil)")

	gr2, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Seqid: uint32(100)}) // Missing Type
	tester.logRejection(gr2, err, "SequentialGet(Type=nil, Seq)")

	gr3, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n"}) // Missing Seq
	tester.logRejection(gr3, err, "SequentialGet(Type, Seq=nil)")

	gr4, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: uint32(100), Node: "XXXX"}) // Extra Node
	tester.logRejection(gr4, err, "SequentialGet(Type, Seq, +Node)")

	tester.tearDown()
}

func TestSequenceBadSignatureList(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(4)

	ls1, err1 := tester.client.SequentialList(tester.ctx, &pb.SequentialListRequest{Opt: &pb.ListOptions{PageSize: 100}})

	if err1 != nil{
		tester.test.Errorf("[E] SequentialList(Type=nil, opt.Mode, opt.PageSize=100) got %v", err1)
	}else{
		for{
			ls1S, err := ls1.Recv()
			if err == io.EOF {}else{
				tester.logRejection(ls1S, err, "SequentialList(Type=nil, opt.Mode, opt.PageSize=100)")
			}
			break
		}
	}

	ls2, err2 := tester.client.SequentialList(tester.ctx, &pb.SequentialListRequest{Type: "n", Opt: &pb.ListOptions{Mode: pb.RetrieveMode_ALL}})

	if err2 != nil{
		tester.test.Errorf("[E] SequentialList(Type=nil, opt.Mode, opt.PageSize=100) got %v", err2)
	}else {
		for {
			ls2S, err := ls2.Recv()
			if err == io.EOF {} else {
				tester.logRejection(ls2S, err, "SequentialList(Type=nil, opt.Mode, opt.PageSize=nil)")
			}
			break
		}
	}

	tester.tearDown()
}

func TestSequenceCRUD(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(4)

	lastSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Node: "XXXXX"})
	tester.logThing(lastSeq, err, "SequentialCreate")

	updStatus, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Type: "n", Node: "YYYY", Seqid: lastSeq.GetSeqid()})
	tester.logThing(updStatus, err, "SequentialUpdate")

	if updStatus.GetStatus() != pb.MutationStatus_SUCCESS{
		tester.test.Errorf("SequentialUpdate(%d): expected %s, actual %s", lastSeq.GetSeqid(), pb.MutationStatus_SUCCESS, updStatus)
	}

	updValue, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: lastSeq.GetSeqid()})
	tester.logThing(updValue, err, "SequentialGet")

	if updValue.GetNode() != "YYYY"{
		tester.test.Errorf("SequentialGet(%d): expected %s, actual %s", lastSeq.GetSeqid(), "YYYY", updValue)
	}

	delStatus, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n", Seqid: lastSeq.GetSeqid()})
	tester.logThing(delStatus, err, "SequentialDelete")

	if delStatus.GetStatus() != pb.MutationStatus_SUCCESS{
		tester.test.Errorf("SequentialDelete(%d): expected %s, actual %s", lastSeq.GetSeqid(), pb.MutationStatus_SUCCESS, delStatus)
	}

	expectedNull, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: lastSeq.GetSeqid()})

	if err != nil{
		tester.test.Logf("[I] Object %d was deleted as expected", lastSeq.GetSeqid())
	}else{
		tester.logThing(expectedNull, errors.New("object should have been deleted"), "SequentialGet")
	}

	tester.tearDown()
}

func TestSequenceNumberInit(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(3)

	var randType = fmt.Sprintf("test_%s", tester.randomAlpha(5))

	newSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: randType, Node: "XXXXX"})
	tester.logThing(newSeq, err, fmt.Sprintf("SequentialCreate(%s)", randType) )

	if newSeq.GetSeqid() != 1{
		tester.test.Errorf("Excepected newly created sequence to be 1, got %d", newSeq.GetSeqid())
	}

	tester.tearDown()
}

func TestSequenceNumberSeries(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(uint32(float64(TestSequentialSize) * 0.15))

	var randType = fmt.Sprintf("test_%s", tester.randomAlpha(5))
	expected := uint32(1)

	for expected < TestSequentialSize{
		var rNode = "XXXX" + tester.randomAlpha(10)

		serialSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: randType, Node: rNode})
		tester.logThing(serialSeq, err, fmt.Sprintf("%d * SequentialCreate(%s)", expected, randType) )

		if err == nil && serialSeq.GetSeqid() != expected{
			tester.test.Errorf("[E] Serial number test, got %d expected %d", serialSeq.GetSeqid(), expected)
			break
		}

		expected += 1
	}

	tester.tearDown()
}

func TestSequenceConflicts(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(uint32(float64(TestParallelSize) * 0.15))

	var wg sync.WaitGroup

	var pc = 1
	var randPar = fmt.Sprintf("testp_%s", tester.randomAlpha(5))

	tester.test.Logf("[I] Attempting to simulate live Sequential conflicts (%d) under %s", TestParallelSize, randPar)

	for pc <= TestParallelSize{
		parallelSequenceInsert(&tester, randPar, &wg)
		pc += 1
	}

	wg.Wait()

	// ensure there are no clashes
	for i := range tester.parallelIDs{
		for j := range tester.parallelIDs{
			if i != j && tester.parallelIDs[i] == tester.parallelIDs[j]{
				tester.test.Errorf("[E] Parallel %d ID generation resulted in duplicate Sequence: [%d, %d]", TestParallelSize, i, j)
			}

		}
	}

	// sort SeqIDs and check if as-expected (sequential)
	var ex = uint32(1)
	sort.Slice(tester.parallelIDs, func(i, j int) bool {
		return tester.parallelIDs[i] < tester.parallelIDs[j]
	})

	for q := range tester.parallelIDs {
		if tester.parallelIDs[q] != ex{
			tester.test.Errorf("[E] Parallel %d expected sorted SeqID of %d, got %d", TestParallelSize, ex, tester.parallelIDs[q])
		}

		ex += 1
	}

	tester.tearDown()
}

func TestSequenceList(t *testing.T) {
	tester := CabinetTest{test: t}
	tester.setup(uint32(float64(TestSequentialSize) * 0.15)) // dynamic ctx applies just to create

	var nodeRandMap = make(map[uint32]string)
	var randType = fmt.Sprintf("test_%s", tester.randomAlpha(5))
	var i = 0

	// create nodes
	for i < TestSequentialSize{
		var rNode = "XXXX" + tester.randomAlpha(10)
		serialSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: randType, Node: rNode})

		if err != nil{
			tester.test.Errorf("[E] Unexpected %v.SequentialCreate(%s) = _. %v", tester.client, randType, err)
			break
		}else{
			nodeRandMap[serialSeq.GetSeqid()] = rNode
		}

		i += 1
	}

	// test list
	listCtx, listCancel := context.WithTimeout(context.Background(), 3 * time.Second) // 3s, reads must be fast
	defer listCancel()

	listOpt := &pb.ListOptions{Mode: pb.RetrieveMode_ALL, PageSize: 100}
	lStr, err := tester.client.SequentialList(listCtx, &pb.SequentialListRequest{Type: randType, Opt: listOpt})
	expID := uint32(1)

	if err != nil{
		tester.test.Errorf("[E] %v.SequentialList(%s) = _. %v", tester.client, randType, err)
	}else{
		for {
			sequence, err := lStr.Recv()

			if err == io.EOF {
				break
			}else if err != nil {
				tester.test.Errorf("[E] %v.SequentialList(_) = _, %v", tester.client, err)
				break
			}else {
				isError := false

				if sequence.GetType() != randType {
					tester.test.Errorf("[E] sequence.type got %s expected %s", sequence.GetType(), randType)
					isError = true
				}

				if sequence.GetSeqid() != expID {
					tester.test.Errorf("[E] sequence.seqId got %d expected %d", sequence.GetSeqid(), expID)
					isError = true
				}

				if sequence.GetNode() != nodeRandMap[sequence.GetSeqid()] {
					tester.test.Errorf("[E] sequence.node got %s expected %s", sequence.GetNode(), nodeRandMap[sequence.GetSeqid()])
					isError = true
				}

				if !isError{
					tester.test.Logf("[I] %v.SequentialList(%s) got %v", tester.client, randType, sequence)
				}
			}

			expID += 1
		}
	}

	tester.tearDown()
}

func parallelSequenceInsert(tester *CabinetTest, sType string, wg *sync.WaitGroup) {
	wg.Add(1)

	go func() {
		newSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: sType, Node: "XXXXXX"})
		tester.logThing(newSeq, err, fmt.Sprintf("SequentialCreate(%s)", sType) )

		if err == nil{
			tester.parallelMux.Lock()
			tester.parallelIDs = append(tester.parallelIDs, newSeq.GetSeqid())
			tester.parallelMux.Unlock()
		}

		wg.Done()
	}()
}
