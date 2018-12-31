// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"io"
	"testing"
)

func TestEdgeCreateListAll(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	edges := make(map[string]*pb.Edge)
	trx := make([]pb.TransactionAction, 0)
	pos := uint32(0)

	predSeq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "p", Uuid: MockRandomUUID()})
	it.logThing("SeqCreate", err, "SequentialCreate")
	eSubject := MockRandomNodeID()

	// create nodes
	for pos < TestSequentialSize {
		edge := &pb.Edge{Subject: eSubject, Predicate: predSeq.Seqid, Target: MockRandomNodeID(), Properties: MockRandomPayload()}
		edges[edge.Target] = edge

		trx = append(trx, pb.TransactionAction{
			ActionId: pos, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: edge},
		})

		pos += 1
	}

	_ = CDSTransactionRunner(&trx, &it)

	// check list
	lStr, err := it.client.EdgeList(it.ctx, &pb.EdgeListRequest{
		Subject: eSubject, Predicate: predSeq.Seqid,
		IncludeSubject: true, IncludePredicate: true, IncludeProp: true, IncludeTarget: true,
		Opt: &pb.ListOptions{
			Mode: pb.ListRange_ALL, PageSize: TestSequentialSize * 5,
		}})

	eReceived := uint32(0)

	if err != nil {
		it.test.Errorf("[E] %v.EdgeList(%s, %d) = _. %v", it.client, eSubject, predSeq.Seqid, err)
	} else {
		for {
			edge, err := lStr.Recv()

			if err == io.EOF {
				break
			} else if err != nil {
				it.test.Errorf("[E] %v.EdgeList(_) = _, %v", it.client, err)
				break
			} else {
				eReceived += 1

				if edge.Subject != edges[edge.Target].Subject {
					it.test.Errorf("[E] edge.subject got %s expected %s", edge.Subject, edges[edge.Target].Subject)
				} else if edge.Predicate != edges[edge.Target].Predicate {
					it.test.Errorf("[E] edge.predicate got %d expected %d", edge.Predicate, edges[edge.Target].Predicate)
				} else if edge.Target != edges[edge.Target].Target {
					it.test.Errorf("[E] edge.target got %s expected %s", edge.Target, edges[edge.Target].Target)
				} else if string(edge.Properties) != string(edges[edge.Target].Properties) {
					it.test.Errorf("[E] edge.properties got %v expected %v", string(edge.Properties), string(edges[edge.Target].Properties))
				} else {
					it.test.Logf("[I] %v.EdgeList(%s, %d) got %v", it.client, eSubject, predSeq.Seqid, edge)
				}
			}
		}
	}

	lStr = nil
	if eReceived != pos {
		it.test.Errorf("[E] Received %d edges, expected %d results", eReceived, pos)
	}

	// clear records
	t2 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeClear{EdgeClear: &pb.Edge{Subject: eSubject, Predicate: predSeq.Seqid, Target: "*"}}},
	}

	_ = CDSTransactionRunner(&t2, &it)

	// try to list again
	nullList, err := it.client.EdgeList(it.ctx, &pb.EdgeListRequest{
		Subject: eSubject, Predicate: predSeq.Seqid,
		IncludeSubject: true, IncludePredicate: true, IncludeProp: true, IncludeTarget: true,
		Opt: &pb.ListOptions{
			Mode: pb.ListRange_ALL, PageSize: TestSequentialSize * 5,
		}})

	if err != nil {
		it.test.Errorf("[E] %v.EdgeList(%s, %d) = _. %v", it.client, eSubject, predSeq.Seqid, err)
	} else {
		for {
			edge, err := nullList.Recv()

			if err == io.EOF {
				break
			} else if err != nil {
				it.test.Errorf("[E] %v.EdgeList(_) = _, %v", it.client, err)
				break
			} else {
				it.test.Errorf("[E] Unexpected edge: %v", edge)
			}
		}
	}

	it.tearDown()
}
