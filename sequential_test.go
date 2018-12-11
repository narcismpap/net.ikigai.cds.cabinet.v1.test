// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	"cds.ikigai.net/cabinet.v1.test/test_helpers"
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"sort"
	"sync"
	"testing"
	"time"
)

const (
	TestParallelSize = 100
	TestSequentialSize = 100
	TestGRPCService = "127.0.0.1:8888"
)

func TestCRUD(t *testing.T) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(TestGRPCService, opts...)

	if err != nil {
		t.Errorf("fail to dial: %v", err)
	}

	defer conn.Close()

	client := pb.NewCDSCabinetClient(conn)

	tester := SequenceTest{
		client: client,
		test: t,
	}

	tester.ctx, tester.cancel = context.WithTimeout(context.Background(), 10 * time.Second)
	defer tester.cancel()

	// CRUD basic testing
	lastSeq, err := tester.doCreate("n", "XXXXX")
	tester.logThing(lastSeq, err, "doCreate")

	updStatus, err := tester.doUpdate("n", "YYYY", lastSeq.GetSeqid())
	tester.logThing(updStatus, err, "doUpdate")

	if updStatus.GetStatus() != pb.MutationStatus_SUCCESS{
		tester.test.Errorf("QUpdate(%d): expected %s, actual %s", lastSeq.GetSeqid(), pb.MutationStatus_SUCCESS, updStatus)
	}

	updValue, err := tester.doGet("n", lastSeq.GetSeqid())
	tester.logThing(updValue, err, "doGet")

	if updValue.GetNode() != "YYYY"{
		tester.test.Errorf("QGet(%d): expected %s, actual %s", lastSeq.GetSeqid(), "YYYY", updValue)
	}

	delStatus, err := tester.doDelete("n", lastSeq.GetSeqid())
	tester.logThing(delStatus, err, "doDelete")

	if delStatus.GetStatus() != pb.MutationStatus_SUCCESS{
		tester.test.Errorf("QDelete(%d): expected %s, actual %s", lastSeq.GetSeqid(), pb.MutationStatus_SUCCESS, delStatus)
	}

	expectedNull, err := tester.doGet("n", lastSeq.GetSeqid())

	if err != nil{
		tester.test.Logf("[I] Object %d was deleted as expected", lastSeq.GetSeqid())
	}else{
		tester.logThing(expectedNull, err, "doGet")
	}

	// new sequential types should also start at 1
	var randType = fmt.Sprintf("test_%s", test_helpers.SecureRandomAlphaString(5))

	newSeq, err := tester.doCreate(randType, "XXXXXX")
	tester.logThing(newSeq, err, fmt.Sprintf("doCreate(%s)", randType) )

	if newSeq.GetSeqid() != 1{
		tester.test.Errorf("Excepected newly created sequence  to be 1, got %d", newSeq.GetSeqid())
	}

	// try Y in sequence and ensure that IDs are continuous
	x := 1
	expected := uint32(2)

	for x < TestSequentialSize{
		serialSeq, err := tester.doCreate(randType, "XXXXX")
		tester.logThing(serialSeq, err, fmt.Sprintf("%d * doCreate(%s)", x, randType) )

		if err == nil && serialSeq.GetSeqid() != expected{
			tester.test.Errorf("[E] Serial number test, got %d expected %d", serialSeq.GetSeqid(), expected)
			break
		}

		x += 1
		expected += 1
	}

	// Bad Signature (Create)
	mt1, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n"}) // missing Node
	tester.logRejection(mt1, err, "SequentialCreate(Type, Node=nil)")

	mt2, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Node: "XXXX"}) // missing Type
	tester.logRejection(mt2, err, "SequentialCreate(Type=nil, Node)")

	mt3, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{}) // missing Type & None
	tester.logRejection(mt3, err, "SequentialCreate(Type=nil, Node=nil)")

	mt4, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Node: "XXXX", Type: "n", Seqid: 100}) // Unexpected Seqid
	tester.logRejection(mt4, err, "SequentialCreate(Type, Node, +Seqid)")

	// Bad Signature (Update)
	up1, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{}) // missing Type, Node, Seqid
	tester.logRejection(up1, err, "SequentialUpdate(Type=nil, Node=nil, Seq=nil)")

	up2, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Type: randType, Node: "XXX"}) // missing SeqID
	tester.logRejection(up2, err, "SequentialUpdate(Type, Node, Seq=nil)")

	up3, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Type: randType, Seqid: newSeq.GetSeqid()}) // missing Node
	tester.logRejection(up3, err, "SequentialUpdate(Type, Node=nil, Seq)")

	up4, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Node: "XXX", Seqid: newSeq.GetSeqid()}) // missing Type
	tester.logRejection(up4, err, "SequentialUpdate(Type=nil, Node, Seq)")

	up5, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Seqid: newSeq.GetSeqid()}) // missing Type & Node
	tester.logRejection(up5, err, "SequentialUpdate(Type=nil, Node=nil, Seq)")

	up6, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Node: "XXX"}) // missing Type & Sequence
	tester.logRejection(up6, err, "SequentialUpdate(Type=nil, Node, Seq=nil)")

	up7, err := tester.client.SequentialUpdate(tester.ctx, &pb.Sequential{Type: randType}) // missing Node & Sequence
	tester.logRejection(up7, err, "SequentialUpdate(Type, Node=nil, Seq=nil)")

	// Bad Signature (Delete)
	dl1, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n"}) // missing SeqID
	tester.logRejection(dl1, err, "SequentialDelete(Type, Seq=nil)")

	dl2, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Seqid: newSeq.GetSeqid()}) // missing Type
	tester.logRejection(dl2, err, "SequentialDelete(Type=nil, Seq)")

	dl3, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{}) // missing Type & SeqId
	tester.logRejection(dl3, err, "SequentialDelete(Type=nil, Seq=nil)")

	dl4, err := tester.client.SequentialDelete(tester.ctx, &pb.Sequential{Type: "n", Seqid: newSeq.GetSeqid(), Node: "xxx"}) // extra Node
	tester.logRejection(dl4, err, "SequentialDelete(Type, Seq, +Node)")

	// Bad Signature (Get)
	gr1, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{}) // Missing Type & Seq
	tester.logRejection(gr1, err, "SequentialGet(Type=nil, Seq=nil)")

	gr2, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Seqid: newSeq.GetSeqid()}) // Missing Type
	tester.logRejection(gr2, err, "SequentialGet(Type=nil, Seq)")

	gr3, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n"}) // Missing Seq
	tester.logRejection(gr3, err, "SequentialGet(Type, Seq=nil)")

	gr4, err := tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: newSeq.GetSeqid(), Node: "XXXX"}) // Extra None
	tester.logRejection(gr4, err, "SequentialGet(Type, Seq, +Node)")

	// attempt to simulate conflicts (cds.cabinet should resolve them automatically)
	var wg sync.WaitGroup

	var pc = 1
	var randPar = fmt.Sprintf("testp_%s", test_helpers.SecureRandomAlphaString(5))

	tester.test.Logf("[I] Attempting to simulate live Sequential conflicts (%d) under %s", TestParallelSize, randPar)

	for pc < TestParallelSize{
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
	sort.Slice(tester.parallelIDs, func(i, j int) bool { return tester.parallelIDs[i] < tester.parallelIDs[j] })

	for q := range tester.parallelIDs {
		if tester.parallelIDs[q] != ex{
			tester.test.Errorf("[E] Parallel %d expected sorted SeqID of %d, got %d", TestParallelSize, ex, tester.parallelIDs[q])
		}

		ex += 1
	}
}

func parallelSequenceInsert(tester *SequenceTest, sType string, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		newSeq, err := tester.doCreate(sType, "XXXXXX")
		tester.logThing(newSeq, err, fmt.Sprintf("doCreate(%s)", sType) )

		if err == nil{
			tester.parallelMux.Lock()
			tester.parallelIDs = append(tester.parallelIDs, newSeq.GetSeqid())
			tester.parallelMux.Unlock()
		}

		wg.Done()
	}()
}

type SequenceTest struct{
	client pb.CDSCabinetClient
	test *testing.T

	ctx context.Context
	cancel context.CancelFunc

	parallelIDs []uint32
	parallelMux sync.Mutex
}

func (s *SequenceTest) doCreate(sType string, sNode string) (*pb.Sequential, error){
	return s.client.SequentialCreate(s.ctx, &pb.Sequential{Type: sType, Node: sNode})
}

func (s *SequenceTest) doUpdate(sType string, sNode string, sID uint32) (*pb.MutationResponse, error){
	return s.client.SequentialUpdate(s.ctx, &pb.Sequential{Type: sType, Node: sNode, Seqid: sID})
}

func (s *SequenceTest) doDelete(sType string, sID uint32) (*pb.MutationResponse, error){
	return s.client.SequentialDelete(s.ctx, &pb.Sequential{Type: sType, Seqid: sID})
}

func (s *SequenceTest) doGet(sType string, sID uint32) (*pb.Sequential, error){
	return s.client.SequentialGet(s.ctx, &pb.Sequential{Type: sType, Seqid: sID})
}

func (s *SequenceTest) logThing(object interface{}, err error, method string) (bool, interface{}){
	if err != nil{
		s.test.Errorf("[E] %v.%s(): %v", s.client, method, err)
		return true, object
	}else{
		s.test.Logf("[I] %v.%s(): %v", s.client, method, object)
		return false, object
	}
}


func (s *SequenceTest) logRejection(object interface{}, err error, method string){
	if err == nil{
		s.test.Errorf("[E] %s was allowed; should be rejected", method)
	}else{
		s.test.Logf("[I] Rejected %s: %v", method, err)
	}
}
