// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestSequenceBadSignatureCreate(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(4)

	mt1, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n"}) // missing UUID
	tester.logRejection(mt1, err, "SequentialCreate(Type, UUID=nil)")

	mt2, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Uuid: MockRandomUUID()}) // missing Type
	tester.logRejection(mt2, err, "SequentialCreate(Type=nil, UUID)")

	mt3, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{}) // missing Type & UUID
	tester.logRejection(mt3, err, "SequentialCreate(Type=nil, UUID=nil)")

	mt4, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Uuid: MockRandomUUID(), Type: "n", Seqid: uint32(100)}) // Unexpected Seqid
	tester.logRejection(mt4, err, "SequentialCreate(Type, UUID, +Seqid)")

	tester.tearDown()
}

func TestSequenceBadSignatureDelete(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(4)

	dl1, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n"}) // missing SeqID
	tester.logRejection(dl1, err, "SequentialDelete(Type, Seq=nil)")

	dl2, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Seqid: uint32(100)}) // missing Type
	tester.logRejection(dl2, err, "SequentialDelete(Type=nil, Seq)")

	dl3, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{}) // missing Type & SeqId
	tester.logRejection(dl3, err, "SequentialDelete(Type=nil, Seq=nil)")

	dl4, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n", Seqid: uint32(100), Uuid: MockRandomUUID()}) // extra UUID
	tester.logRejection(dl4, err, "SequentialDelete(Type, Seq, +UUID)")

	tester.tearDown()
}

func TestSequenceBadSignatureGet(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(4)

	gr1, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{}) // Missing Type & Seq
	tester.logRejection(gr1, err, "SequentialGet(Type=nil, Seq=nil)")

	gr2, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Seqid: uint32(100)}) // Missing Type
	tester.logRejection(gr2, err, "SequentialGet(Type=nil, Seq)")

	gr3, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n"}) // Missing Seq
	tester.logRejection(gr3, err, "SequentialGet(Type, Seq=nil)")

	gr4, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: uint32(100), Uuid: MockRandomUUID()}) // Extra UUID
	tester.logRejection(gr4, err, "SequentialGet(Type, Seq, +UUID)")

	tester.tearDown()
}

func TestSequenceBadSignatureList(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(4)

	ls1, err1 := tester.client.SequentialList(tester.ctx, &pb.SequentialListRequest{Opt: &pb.ListOptions{PageSize: 100}, IncludeUuid:true})

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

	ls2, err2 := tester.client.SequentialList(tester.ctx, &pb.SequentialListRequest{Type: "n", Opt: &pb.ListOptions{Mode: pb.ListRange_ALL}, IncludeUuid:true})

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

func TestSequenceRepeatUUID(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(4)

	seqUUID := MockRandomUUID()

	mt1, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(mt1, err, "SequentialCreate(Type, UUID)")

	mt2, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logRejection(mt2, err, "SequentialCreate(Type, UUID)")

	mt3, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logRejection(mt3, err, "SequentialCreate(Type, UUID)")

	valByUUID, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(valByUUID, err, "SequentialGetByUUID")

	if valByUUID.Seqid != mt1.Seqid{
		tester.test.Errorf("SeqID mismatch, expected %d got %d", mt1.Seqid, valByUUID.Seqid)
	}

	tester.tearDown()
}

func TestSequenceRepeatClearUUID(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(4)

	seqUUID := MockRandomUUID()

	// seq1
	mt1, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(mt1, err, "SequentialCreate(Type, UUID)")

	mt2, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logRejection(mt2, err, "SequentialCreate(Type, UUID)")

	mt3, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(mt3, err, "SequentialDelete(Type, UUID)")

	//seq2
	mt4, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(mt4, err, "SequentialCreate(Type, UUID)")

	mt5, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logRejection(mt5, err, "SequentialCreate(Type, UUID)")

	// check record
	valByUUID, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(valByUUID, err, "SequentialGetByUUID")

	if valByUUID.Seqid != mt4.Seqid{
		tester.test.Errorf("SeqID mismatch, expected %d got %d", mt4.Seqid, valByUUID.Seqid)
	}

	// check deleted record
	nullBySeqID, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: mt1.Seqid})
	validateErrorNotFound(mt1.Seqid, nullBySeqID, &tester, err)

	tester.tearDown()
}

func TestSequenceCRUD(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(4)

	seqUUID := MockRandomUUID()

	// new seq
	lastSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(lastSeq, err, "SequentialCreate")

	// verify loading by SeqID & UUID
	valByUUID, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(valByUUID, err, "SequentialGetByUUID")

	valBySeqID, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: lastSeq.Seqid})
	tester.logThing(valBySeqID, err, "SequentialGetById")

	if valByUUID.GetSeqid() != lastSeq.Seqid{
		tester.test.Errorf("SequentialGet(%d): expected SeqID %d, actual %v", lastSeq.GetSeqid(), lastSeq.Seqid, valByUUID)
	}

	if valBySeqID.Seqid != valByUUID.Seqid || valBySeqID.Uuid != valByUUID.Uuid{
		tester.test.Errorf("SequentialGet(UUID & SeqID): results mismatch, [%v] and [%v]", valByUUID, valBySeqID)
	}

	// remove seq
	delStatus, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	tester.logThing(delStatus, err, "SequentialDelete")

	if delStatus.GetStatus() != pb.MutationStatus_SUCCESS{
		tester.test.Errorf("SequentialDelete(%d): expected %s, actual %s", lastSeq.GetSeqid(), pb.MutationStatus_SUCCESS, delStatus)
	}

	// verify access
	nullBySeqID, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: lastSeq.GetSeqid()})
	validateErrorNotFound(lastSeq, nullBySeqID, &tester, err)

	nullByUUID, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	validateErrorNotFound(seqUUID, nullByUUID, &tester, err)

	tester.tearDown()
}

func TestSequenceNumberInit(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(3)

	var randType = fmt.Sprintf("test_%s", tester.randomAlpha(5))

	newSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: randType, Uuid: MockRandomUUID()})
	tester.logThing(newSeq, err, fmt.Sprintf("SequentialCreate(%s)", randType) )

	if newSeq.GetSeqid() != 1{
		tester.test.Errorf("Excepected newly created sequence to be 1, got %d", newSeq.GetSeqid())
	}

	tester.tearDown()
}

func TestSequenceNumberSeries(t *testing.T) {
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(uint32(float64(TestSequentialSize) * 0.15))

	var randType = fmt.Sprintf("test_%s", tester.randomAlpha(5))
	expected := uint32(1)

	for expected < TestSequentialSize{
		var rUUID = MockRandomUUID()

		serialSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: randType, Uuid: rUUID})
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
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(10)

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
	t.Parallel()

	tester := CabinetTest{test: t}
	tester.setup(uint32(float64(TestSequentialSize) * 0.15)) // dynamic ctx applies just to create

	var UUIDMap = make(map[uint32]string)
	var randType = fmt.Sprintf("test_%s", tester.randomAlpha(5))
	var i = 0

	// request new seq
	for i < TestSequentialSize{
		var rUUID = MockRandomUUID()
		serialSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: randType, Uuid: rUUID})

		if err != nil{
			tester.test.Errorf("[E] Unexpected %v.SequentialCreate(%s) = _. %v", tester.client, randType, err)
			break
		}else{
			UUIDMap[serialSeq.GetSeqid()] = rUUID
		}

		i += 1
	}

	// test list
	listCtx, listCancel := context.WithTimeout(context.Background(), 3 * time.Second) // 3s, reads must be fast
	defer listCancel()

	listOpt := &pb.ListOptions{Mode: pb.ListRange_ALL, PageSize: TestSequentialSize}
	lStr, err := tester.client.SequentialList(listCtx, &pb.SequentialListRequest{
		Type: randType, Opt: listOpt,
		IncludeUuid: true, IncludeSeqid: true,
	})

	expID := uint32(1)
	sReceived := 0

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
				sReceived += 1
				isError := false

				if sequence.GetSeqid() != expID {
					tester.test.Errorf("[E] sequence.seqId got %d expected %d", sequence.GetSeqid(), expID)
					isError = true
				}

				if sequence.GetUuid() != UUIDMap[sequence.GetSeqid()] {
					tester.test.Errorf("[E] sequence.UUID got %s expected %s", sequence.GetUuid(), UUIDMap[sequence.GetSeqid()])
					isError = true
				}

				if !isError{
					tester.test.Logf("[I] %v.SequentialList(%s) got %v", tester.client, randType, sequence)
				}
			}

			expID += 1
		}
	}

	if sReceived != i{
		tester.test.Errorf("[E] Expected %d sequences, got %d", i, sReceived)
	}

	tester.tearDown()
}

func parallelSequenceInsert(tester *CabinetTest, sType string, wg *sync.WaitGroup) {
	wg.Add(1)

	go func() {
		newSeq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: sType, Uuid: MockRandomUUID()})
		tester.logThing(newSeq, err, fmt.Sprintf("SequentialCreate(%s)", sType) )

		if err == nil{
			tester.parallelMux.Lock()
			tester.parallelIDs = append(tester.parallelIDs, newSeq.GetSeqid())
			tester.parallelMux.Unlock()
		}

		wg.Done()
	}()
}
