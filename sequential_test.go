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
	it := CabinetTest{test: t}
	it.setup(4)

	mt1, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n"}) // missing UUID
	it.logRejection(mt1, err, "SequentialCreate(Type, UUID=nil)")

	mt2, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Uuid: MockRandomUUID()}) // missing Type
	it.logRejection(mt2, err, "SequentialCreate(Type=nil, UUID)")

	mt3, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{}) // missing Type & UUID
	it.logRejection(mt3, err, "SequentialCreate(Type=nil, UUID=nil)")

	mt4, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Uuid: MockRandomUUID(), Type: "n", Seqid: uint32(100)}) // Unexpected Seqid
	it.logRejection(mt4, err, "SequentialCreate(Type, UUID, +Seqid)")

	it.tearDown()
}

func TestSequenceBadSignatureDelete(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	dl1, err := it.client.SequentialDelete(it.ctx, &pb.Sequential{Type: "n"}) // missing SeqID
	it.logRejection(dl1, err, "SequentialDelete(Type, Seq=nil)")

	dl2, err := it.client.SequentialDelete(it.ctx, &pb.Sequential{Seqid: uint32(100)}) // missing Type
	it.logRejection(dl2, err, "SequentialDelete(Type=nil, Seq)")

	dl3, err := it.client.SequentialDelete(it.ctx, &pb.Sequential{}) // missing Type & SeqId
	it.logRejection(dl3, err, "SequentialDelete(Type=nil, Seq=nil)")

	dl4, err := it.client.SequentialDelete(it.ctx, &pb.Sequential{Type: "n", Seqid: uint32(100), Uuid: MockRandomUUID()}) // extra UUID
	it.logRejection(dl4, err, "SequentialDelete(Type, Seq, +UUID)")

	it.tearDown()
}

func TestSequenceBadSignatureGet(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	gr1, err := it.client.SequentialGet(it.ctx, &pb.Sequential{}) // Missing Type & Seq
	it.logRejection(gr1, err, "SequentialGet(Type=nil, Seq=nil)")

	gr2, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Seqid: uint32(100)}) // Missing Type
	it.logRejection(gr2, err, "SequentialGet(Type=nil, Seq)")

	gr3, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n"}) // Missing Seq
	it.logRejection(gr3, err, "SequentialGet(Type, Seq=nil)")

	gr4, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Seqid: uint32(100), Uuid: MockRandomUUID()}) // Extra UUID
	it.logRejection(gr4, err, "SequentialGet(Type, Seq, +UUID)")

	it.tearDown()
}

func TestSequenceBadSignatureList(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	ls1, err1 := it.client.SequentialList(it.ctx, &pb.SequentialListRequest{Opt: &pb.ListOptions{PageSize: 100}, IncludeUuid:true})

	if err1 != nil{
		it.test.Errorf("[E] SequentialList(Type=nil, opt.Mode, opt.PageSize=100) got %v", err1)
	}else{
		for{
			ls1S, err := ls1.Recv()
			if err == io.EOF {}else{
				it.logRejection(ls1S, err, "SequentialList(Type=nil, opt.Mode, opt.PageSize=100)")
			}
			break
		}
	}

	ls2, err2 := it.client.SequentialList(it.ctx, &pb.SequentialListRequest{Type: "n", Opt: &pb.ListOptions{Mode: pb.ListRange_ALL}, IncludeUuid:true})

	if err2 != nil{
		it.test.Errorf("[E] SequentialList(Type=nil, opt.Mode, opt.PageSize=100) got %v", err2)
	}else {
		for {
			ls2S, err := ls2.Recv()
			if err == io.EOF {} else {
				it.logRejection(ls2S, err, "SequentialList(Type=nil, opt.Mode, opt.PageSize=nil)")
			}
			break
		}
	}

	it.tearDown()
}

func TestSequenceRepeatUUID(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	seqUUID := MockRandomUUID()

	mt1, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(mt1, err, "SequentialCreate(Type, UUID)")

	mt2, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logRejection(mt2, err, "SequentialCreate(Type, UUID)")

	mt3, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logRejection(mt3, err, "SequentialCreate(Type, UUID)")

	valByUUID, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(valByUUID, err, "SequentialGetByUUID")

	if valByUUID.Seqid != mt1.Seqid{
		it.test.Errorf("SeqID mismatch, expected %d got %d", mt1.Seqid, valByUUID.Seqid)
	}

	it.tearDown()
}

func TestSequenceRepeatClearUUID(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	seqUUID := MockRandomUUID()

	// seq1
	mt1, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(mt1, err, "SequentialCreate(Type, UUID)")

	mt2, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logRejection(mt2, err, "SequentialCreate(Type, UUID)")

	mt3, err := it.client.SequentialDelete(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(mt3, err, "SequentialDelete(Type, UUID)")

	//seq2
	mt4, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(mt4, err, "SequentialCreate(Type, UUID)")

	mt5, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logRejection(mt5, err, "SequentialCreate(Type, UUID)")

	// check record
	valByUUID, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(valByUUID, err, "SequentialGetByUUID")

	if valByUUID.Seqid != mt4.Seqid{
		it.test.Errorf("SeqID mismatch, expected %d got %d", mt4.Seqid, valByUUID.Seqid)
	}

	// check deleted record
	nullBySeqID, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Seqid: mt1.Seqid})
	validateErrorNotFound(mt1.Seqid, nullBySeqID, &it, err)

	it.tearDown()
}

func TestSequenceCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	seqUUID := MockRandomUUID()

	// new seq
	lastSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(lastSeq, err, "SequentialCreate")

	// verify loading by SeqID & UUID
	valByUUID, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(valByUUID, err, "SequentialGetByUUID")

	valBySeqID, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Seqid: lastSeq.Seqid})
	it.logThing(valBySeqID, err, "SequentialGetById")

	if valByUUID.GetSeqid() != lastSeq.Seqid{
		it.test.Errorf("SequentialGet(%d): expected SeqID %d, actual %v", lastSeq.GetSeqid(), lastSeq.Seqid, valByUUID)
	}

	if valBySeqID.Seqid != valByUUID.Seqid || valBySeqID.Uuid != valByUUID.Uuid{
		it.test.Errorf("SequentialGet(UUID & SeqID): results mismatch, [%v] and [%v]", valByUUID, valBySeqID)
	}

	// remove seq
	delStatus, err := it.client.SequentialDelete(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	it.logThing(delStatus, err, "SequentialDelete")

	if delStatus.GetStatus() != pb.MutationStatus_SUCCESS{
		it.test.Errorf("SequentialDelete(%d): expected %s, actual %s", lastSeq.GetSeqid(), pb.MutationStatus_SUCCESS, delStatus)
	}

	// verify access
	nullBySeqID, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Seqid: lastSeq.GetSeqid()})
	validateErrorNotFound(lastSeq, nullBySeqID, &it, err)

	nullByUUID, err := it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Uuid: seqUUID})
	validateErrorNotFound(seqUUID, nullByUUID, &it, err)

	it.tearDown()
}

func TestSequenceNumberInit(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(3)

	var randType = fmt.Sprintf("test_%s", it.randomAlpha(5))

	newSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: randType, Uuid: MockRandomUUID()})
	it.logThing(newSeq, err, fmt.Sprintf("SequentialCreate(%s)", randType) )

	if newSeq.GetSeqid() != 1{
		it.test.Errorf("Excepected newly created sequence to be 1, got %d", newSeq.GetSeqid())
	}

	it.tearDown()
}

func TestSequenceNumberSeries(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(uint32(float64(TestSequentialSize) * 0.15))

	var randType = fmt.Sprintf("test_%s", it.randomAlpha(5))
	expected := uint32(1)

	for expected < TestSequentialSize{
		var rUUID = MockRandomUUID()

		serialSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: randType, Uuid: rUUID})
		it.logThing(serialSeq, err, fmt.Sprintf("%d * SequentialCreate(%s)", expected, randType) )

		if err == nil && serialSeq.GetSeqid() != expected{
			it.test.Errorf("[E] Serial number test, got %d expected %d", serialSeq.GetSeqid(), expected)
			break
		}

		expected += 1
	}

	it.tearDown()
}

func TestSequenceConflicts(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(10)

	var wg sync.WaitGroup

	var pc = 1
	var randPar = fmt.Sprintf("testp_%s", it.randomAlpha(5))

	it.test.Logf("[I] Attempting to simulate live Sequential conflicts (%d) under %s", TestParallelSize, randPar)

	for pc <= TestParallelSize{
		parallelSequenceInsert(&it, randPar, &wg)
		pc += 1
	}

	wg.Wait()

	// ensure there are no clashes
	for i := range it.parallelIDs{
		for j := range it.parallelIDs{
			if i != j && it.parallelIDs[i] == it.parallelIDs[j]{
				it.test.Errorf("[E] Parallel %d ID generation resulted in duplicate Sequence: [%d, %d]", TestParallelSize, i, j)
			}

		}
	}

	// sort SeqIDs and check if as-expected (sequential)
	var ex = uint32(1)
	sort.Slice(it.parallelIDs, func(i, j int) bool {
		return it.parallelIDs[i] < it.parallelIDs[j]
	})

	for q := range it.parallelIDs {
		if it.parallelIDs[q] != ex{
			it.test.Errorf("[E] Parallel %d expected sorted SeqID of %d, got %d", TestParallelSize, ex, it.parallelIDs[q])
		}

		ex += 1
	}

	it.tearDown()
}

func TestSequenceList(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(uint32(float64(TestSequentialSize) * 0.15)) // dynamic ctx applies just to create

	var UUIDMap = make(map[uint32]string)
	var randType = fmt.Sprintf("test_%s", it.randomAlpha(5))
	var i = 0

	// request new seq
	for i < TestSequentialSize{
		var rUUID = MockRandomUUID()
		serialSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: randType, Uuid: rUUID})

		if err != nil{
			it.test.Errorf("[E] Unexpected %v.SequentialCreate(%s) = _. %v", it.client, randType, err)
			break
		}else{
			UUIDMap[serialSeq.GetSeqid()] = rUUID
		}

		i += 1
	}

	// test list
	listCtx, listCancel := context.WithTimeout(context.Background(), 3 * time.Second) // 3s, reads must be fast
	defer listCancel()

	listOpt := &pb.ListOptions{Mode: pb.ListRange_ALL, PageSize: TestSequentialSize * 5}
	lStr, err := it.client.SequentialList(listCtx, &pb.SequentialListRequest{
		Type: randType, Opt: listOpt,
		IncludeUuid: true, IncludeSeqid: true,
	})

	expID := uint32(1)
	sReceived := 0

	if err != nil{
		it.test.Errorf("[E] %v.SequentialList(%s) = _. %v", it.client, randType, err)
	}else{
		for {
			sequence, err := lStr.Recv()

			if err == io.EOF {
				break
			}else if err != nil {
				it.test.Errorf("[E] %v.SequentialList(_) = _, %v", it.client, err)
				break
			}else {
				sReceived += 1
				isError := false

				if sequence.GetSeqid() != expID {
					it.test.Errorf("[E] sequence.seqId got %d expected %d", sequence.GetSeqid(), expID)
					isError = true
				}

				if sequence.GetUuid() != UUIDMap[sequence.GetSeqid()] {
					it.test.Errorf("[E] sequence.UUID got %s expected %s", sequence.GetUuid(), UUIDMap[sequence.GetSeqid()])
					isError = true
				}

				if !isError{
					it.test.Logf("[I] %v.SequentialList(%s) got %v", it.client, randType, sequence)
				}
			}

			expID += 1
		}
	}

	if sReceived != i{
		it.test.Errorf("[E] Expected %d sequences, got %d", i, sReceived)
	}

	it.tearDown()
}

func parallelSequenceInsert(it *CabinetTest, sType string, wg *sync.WaitGroup) {
	wg.Add(1)

	go func() {
		newSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: sType, Uuid: MockRandomUUID()})
		it.logThing(newSeq, err, fmt.Sprintf("SequentialCreate(%s)", sType) )

		if err == nil{
			it.parallelMux.Lock()
			it.parallelIDs = append(it.parallelIDs, newSeq.GetSeqid())
			it.parallelMux.Unlock()
		}

		wg.Done()
	}()
}
