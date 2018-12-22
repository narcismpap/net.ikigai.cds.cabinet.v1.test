// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"testing"
)

func metaWithPayload(m *pb.Meta, payload []byte) *pb.Meta {
	return &pb.Meta{
		Object: m.Object,
		Key:    m.Key,
		Val:    payload,
	}
}

func metaWithoutPayload(m *pb.Meta) *pb.Meta {
	return &pb.Meta{
		Object: m.Object,
		Key:    m.Key,
	}
}

func TestTransactionMetaEdgeCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(2)

	m1 := &pb.Meta{Object: &pb.Meta_Edge{Edge: MockRandomEdge()}, Key: uint32(10)}
	m2 := &pb.Meta{Object: &pb.Meta_Edge{Edge: MockRandomEdge()}, Key: uint32(13)}

	MetaRunCRUD(&it, m1, m2)

	it.tearDown()
}

func TestTransactionMetaNodeCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(2)

	m1 := &pb.Meta{Object: &pb.Meta_Node{Node: MockRandomNodeID()}, Key: uint32(20)}
	m2 := &pb.Meta{Object: &pb.Meta_Node{Node: MockRandomNodeID()}, Key: uint32(21)}

	MetaRunCRUD(&it, m1, m2)

	it.tearDown()
}

func MetaRunCRUD(it *CabinetTest, m1 *pb.Meta, m2 *pb.Meta) {
	p1, p2 := MockRandomPayload(), MockRandomPayload()

	// new payload
	n1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_MetaUpdate{MetaUpdate: metaWithPayload(m1, p1)}},
		{ActionId: 2, Action: &pb.TransactionAction_MetaUpdate{MetaUpdate: metaWithPayload(m2, p2)}},
	}

	_ = CDSTransactionRunner(&n1, it)

	// check payload
	r1, err := it.client.MetaGet(it.ctx, m1)
	it.logThing(r1, err, "MetaGet")
	validatePayload(m1, it, p1, r1.Val)

	r2, err := it.client.MetaGet(it.ctx, m2)
	it.logThing(r2, err, "MetaGet")
	validatePayload(m2, it, p2, r2.Val)

	r1, r2 = nil, nil

	// delete first payload
	n2 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_MetaDelete{MetaDelete: metaWithPayload(m1, p1)}},
	}

	_ = CDSTransactionRunner(&n2, it)

	r3, err := it.client.MetaGet(it.ctx, m1)
	validateErrorNotFound(m1, r3, it, err)

	r4, err := it.client.MetaGet(it.ctx, m2)
	it.logThing(r4, err, "MetaGet")
	validatePayload(m2, it, p2, r4.Val)

	r3, r4 = nil, nil

	// create a batch of new payloads
	pos := uint32(0)
	metas := make([]*pb.Meta, 0)
	trx := make([]pb.TransactionAction, 0)

	for pos < TestSequentialSize {
		tmpMeta := m2
		tmpMeta.Key = pos
		tmpMeta.Val = MockRandomPayload()

		metas = append(metas, tmpMeta)
		trx = append(trx, pb.TransactionAction{
			ActionId: pos, Action: &pb.TransactionAction_MetaUpdate{MetaUpdate: tmpMeta},
		})

		pos += 1
	}

	_ = CDSTransactionRunner(&trx, it)

	// check results
	for i := range metas {
		iMeta, err := it.client.MetaGet(it.ctx, metaWithoutPayload(metas[i]))
		it.logThing(iMeta, err, "MetaGet")
		validatePayload(metas[i], it, metas[i].Val, iMeta.Val)
	}

	// clear payload batch
	n4 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_MetaClear{MetaClear: m2}},
	}

	_ = CDSTransactionRunner(&n4, it)

	// verify cleared batches
	for i := range metas {
		nullMeta, err := it.client.MetaGet(it.ctx, metaWithoutPayload(metas[i]))
		validateErrorNotFound(metas[i], nullMeta, it, err)
	}
}
