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

func IndexWithPayload(i pb.Index, payload []byte) *pb.Index{
	i2 := i
	i2.Properties = payload
	return &i2
}

func IndexWithoutPayload(i pb.Index) (r *pb.Index){
	i2 := i
	i2.Properties = nil
	return &i2
}

func IndexWithValuePayload(i pb.Index, value string, payload []byte) (r *pb.Index){
	i2 := i
	i2.Properties = payload
	i2.Value = value
	return &i2
}

func TestTransactionIndexSimpleCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	p1, p2 := MockRandomPayload(), MockRandomPayload()

	i1 := &pb.Index{Type: 10, Node: MockRandomNodeID(), Value: "Japan", Properties: nil}
	i2 := &pb.Index{Type: 10, Node: MockRandomNodeID(), Value: "South Korea", Properties: nil}

	t1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: i1}},
		{ActionId: 2, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: IndexWithPayload(*i1, p1)}},
		{ActionId: 3, Action: &pb.TransactionAction_IndexDelete{IndexDelete: IndexWithoutPayload(*i1)}},

		{ActionId: 4, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: IndexWithPayload(*i2, p1)}},
		{ActionId: 5, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: IndexWithPayload(*i2, p2)}},
	}

	_ = transactionRunner(&t1, &it)

	// check records
	r1, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i1)})
	validateErrorNotFound(i1, r1, &it, err)

	r2, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i2)})
	it.logThing(r2, err, "IndexGet")
	validatePayload(i2, &it, p2, r2.Properties)

	// remove record
	t2 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_IndexDelete{IndexDelete: IndexWithoutPayload(*i2)}},
	}

	_ = transactionRunner(&t2, &it)

	r3, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i2)})
	validateErrorNotFound(i2, r3, &it, err)

	it.tearDown()
}

func TestTransactionIndexComplexCRUD(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	p1, p2, p3, p4 := MockRandomPayload(), MockRandomPayload(), MockRandomPayload(), MockRandomPayload()

	i1 := &pb.Index{Type: 10, Node: MockRandomNodeID(), Value: "Brazil", Properties: nil}
	i2 := &pb.Index{Type: 10, Node: MockRandomNodeID(), Value: "Argentina", Properties: nil}

	t1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: i1}},
		{ActionId: 2, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: IndexWithPayload(*i2, p2)}},
	}

	_ = transactionRunner(&t1, &it)

	// check payloads
	r1, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i1)})
	it.logThing(r1, err, "IndexGet")
	validatePayload(i1, &it, []byte(""), r1.Properties)

	r2, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i2)})
	it.logThing(r2, err, "IndexGet")
	validatePayload(i2, &it, p2, r2.Properties)

	r1, r2 = nil, nil

	// update payloads
	t2 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: IndexWithPayload(*i1, p3)}},
		{ActionId: 2, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: IndexWithPayload(*i2, p4)}},
	}

	_ = transactionRunner(&t2, &it)

	r3, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i1)})
	it.logThing(r3, err, "IndexGet")
	validatePayload(i1, &it, p3, r3.Properties)

	r4, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i2)})
	it.logThing(r4, err, "IndexGet")
	validatePayload(i2, &it, p4, r4.Properties)

	r3, r4 = nil, nil

	// delete first, update second
	t3 := []pb.TransactionAction{
		{ActionId: 100, Action: &pb.TransactionAction_IndexDelete{IndexDelete: IndexWithoutPayload(*i1)}},
		{ActionId: 4122, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: IndexWithPayload(*i2, p1)}},
	}

	_ = transactionRunner(&t3, &it)

	r5, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i1)})
	validateErrorNotFound(i1, r5, &it, err)

	r6, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i2)})
	it.logThing(r6, err, "IndexGet")
	validatePayload(i2, &it, p1, r6.Properties)

	r5, r6 = nil, nil

	// delete second
	t4 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_IndexDelete{IndexDelete: IndexWithoutPayload(*i2)}},
	}

	_ = transactionRunner(&t4, &it)

	r7, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*i2)})
	validateErrorNotFound(i2, r7, &it, err)

	it.tearDown()
}

func TestTransactionIndexBatch(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)

	i1 := &pb.Index{Type: 13, Node: MockRandomNodeID(), Properties: nil}
	i2 := &pb.Index{Type: 51, Node: MockRandomNodeID(), Properties: nil}

	pos := uint32(0)
	indexes := make([]*pb.Index, TestSequentialSize)
	trx := make([]pb.TransactionAction, TestSequentialSize)

	// create all indexes
	for pos < TestSequentialSize{
		var tmpI *pb.Index

		if pos % 2 == 0 {
			tmpI = IndexWithValuePayload(*i1, MockRandomAlpha(10), MockRandomPayload())
		}else{
			tmpI = IndexWithValuePayload(*i2, MockRandomAlpha(10), MockRandomPayload())
		}

		indexes[pos] = tmpI
		trx[pos] = pb.TransactionAction{
			ActionId: pos, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: tmpI},
		}

		pos += 1
	}

	_ = transactionRunner(&trx, &it)

	// read indexes
	for i := range indexes{
		uIndex, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*indexes[i])})
		it.logThing(uIndex, err, "IndexGet")
		validatePayload(indexes[i], &it, indexes[i].Properties, uIndex.Properties)
	}

	// drop every even index
	dropPos := uint32(0)
	dropTrx := make([]pb.TransactionAction, 0)

	for i := range indexes{
		if i % 2 == 0 {
			dropTrx = append(dropTrx, pb.TransactionAction{
				ActionId: dropPos, Action: &pb.TransactionAction_IndexDelete{IndexDelete: IndexWithoutPayload(*indexes[i])},
			})
			dropPos += 1
		}
	}

	_ = transactionRunner(&dropTrx, &it)

	// read all indexes
	for i := range indexes{
		hmIndex, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: indexes[i]})

		if i % 2 == 0 {
			validateErrorNotFound(indexes[i], hmIndex, &it, err)
		}else {
			it.logThing(hmIndex, err, "IndexGet")
			validatePayload(indexes[i], &it, indexes[i].Properties, hmIndex.Properties)
		}
	}

	// delete all indexes
	delPos := uint32(0)
	delTrx := make([]pb.TransactionAction, TestSequentialSize)

	for i := range indexes{
		delTrx[delPos] = pb.TransactionAction{
			ActionId: delPos, Action: &pb.TransactionAction_IndexDelete{IndexDelete: IndexWithoutPayload(*indexes[i])},
		}
		delPos += 1
	}

	_ = transactionRunner(&delTrx, &it)

	// make sure all are removed
	for i := range indexes{
		nullIndex, err := it.client.IndexGet(it.ctx, &pb.IndexGetRequest{Index: IndexWithoutPayload(*indexes[i])})
		validateErrorNotFound(indexes[i], nullIndex, &it, err)
	}

	it.tearDown()
}
