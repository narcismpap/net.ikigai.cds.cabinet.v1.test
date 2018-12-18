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

func TestMetaNodeCreateListAll(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	m1 := &pb.Meta{
		Object: &pb.Meta_Node{Node: MockRandomNodeID()},
	}

	SharedMetaNodeCreateListAll(&it, m1)

	it.tearDown()
}

func TestMetaEdgeCreateListAll(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	m1 := &pb.Meta{
		Object: &pb.Meta_Edge{Edge: &pb.Edge{
			Subject: MockRandomNodeID(),
			Predicate: uint32(MockRandomInt(1000, 8000)),
			Target: MockRandomNodeID(),
	}}}

	SharedMetaNodeCreateListAll(&it, m1)

	it.tearDown()
}

func SharedMetaNodeCreateListAll(it *CabinetTest, m *pb.Meta) {
	pos := uint32(0)
	metas := make(map[uint32]*pb.Meta)
	trx := make([]pb.TransactionAction, 0)

	randSeq := &RandomSequence{min: 1000, max: 65000, sequence: make(map[uint32]bool)}

	for pos < TestSequentialSize{
		tmpMeta := &pb.Meta{Object: m.Object}
		tmpMeta.Key = randSeq.new()
		tmpMeta.Val = MockRandomPayload()

		metas[tmpMeta.Key] = tmpMeta
		trx = append(trx, pb.TransactionAction{
			ActionId: pos + 1, Action: &pb.TransactionAction_MetaUpdate{MetaUpdate: tmpMeta},
		})

		pos += 1
	}

	_ = CDSTransactionRunner(&trx, it)

	// check stored metas
	metaCheckList(it, metas, m, TestSequentialSize)

	// update payloads on all records
	updateTrx := make([]pb.TransactionAction, 0)
	pos = 0

	for mKey := range metas{
		metas[mKey].Val = MockRandomPayload()

		updateTrx = append(updateTrx, pb.TransactionAction{
			ActionId: pos + 1, Action: &pb.TransactionAction_MetaUpdate{MetaUpdate: metas[mKey]},
		})

		pos += 1
	}

	_ = CDSTransactionRunner(&updateTrx, it)
	metaCheckList(it, metas, m, uint16(len(metas)))

	// delete every second payload
	messTrx := make([]pb.TransactionAction, 0)
	pos = 0

	for mKey := range metas{
		if pos % 2 == 0 {
			messTrx = append(messTrx, pb.TransactionAction{
				ActionId: pos + 1, Action: &pb.TransactionAction_MetaDelete{MetaDelete: metaWithoutPayload(metas[mKey])},
			})
		}

		pos += 1
	}

	_ = CDSTransactionRunner(&messTrx, it)
	metaCheckList(it, metas, m, uint16(len(metas)/2))

	// delete all metas
	clearTrx := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_MetaClear{MetaClear: m}},
	}

	_ = CDSTransactionRunner(&clearTrx, it)
	metaCheckList(it, metas, m, 0)
}

func metaCheckList(it *CabinetTest, metas map[uint32]*pb.Meta, m *pb.Meta, size uint16){
	metaList, err := it.client.MetaList(it.ctx, &pb.MetaListRequest{
		Meta: m,
		IncludeNode: true, IncludeProperty: true, IncludeValue: true,
		IncludeSubject: true, IncludePredicate: true, IncludeTarget: true,

		Opt: &pb.ListOptions{
			Mode: pb.ListRange_ALL, PageSize: TestSequentialSize * 5,
		},})

	lCnt := uint16(0)

	if err != nil{
		it.test.Errorf("[E] %v.MetaList(%v) = _. %v", it.client, m, err)
	}else{
		for {
			meta, err := metaList.Recv()

			if err == io.EOF {
				break
			}else if err != nil {
				it.test.Errorf("[E] %v.MetaList(%s) = _, %v", it.client, m, err)
				break
			}else {
				it.test.Logf("[I] %v.MetaList(%v) got %v", it.client, m, meta)

				lCnt += 1

				// a bit verbose but the only way to properly read two oneof fields that I can think of
				switch mType := meta.Object.(type) {
				case *pb.Meta_Node:
					switch sType := m.Object.(type) {
					case *pb.Meta_Node:
						if sType.Node != mType.Node{
							it.test.Errorf("[E] meta.object.node got %s expected %s", mType.Node, sType.Node)
						}
					default:
						panic("should not trigger")
					}
				case *pb.Meta_Edge:
					switch sType := m.Object.(type) {
					case *pb.Meta_Edge:
						if mType.Edge.Subject != sType.Edge.Subject{
							it.test.Errorf("[E] meta.object.edge.subject got %s expected %s", mType.Edge.Subject, sType.Edge.Subject)
						}

						if mType.Edge.Predicate != sType.Edge.Predicate{
							it.test.Errorf("[E] meta.object.edge.predicate got %d expected %d ", mType.Edge.Predicate, sType.Edge.Predicate)
						}

						if mType.Edge.Target != sType.Edge.Target{
							it.test.Errorf("[E] meta.object.edge.target got %s expected %s", mType.Edge.Target, sType.Edge.Target)
						}
					default:
						panic("should not trigger")
					}
				default:
					it.test.Errorf("Received invalid meta.object type: %v", mType)
				}

				if meta.Key != metas[meta.Key].Key{
					it.test.Errorf("[E] meta.key got %d expected %d", meta.Key, metas[meta.Key].Key)
				}

				if string(meta.Val) != string(metas[meta.Key].Val){
					it.test.Errorf("[E] meta.val got %s expected %s", meta.Val, metas[meta.Key].Val)
				}

			}
		}
	}

	if lCnt != size{
		it.test.Errorf("Expected %d records, got %d", size, lCnt)
	}else{
		it.test.Logf("Got %d expected Meta records", size)
	}
}
