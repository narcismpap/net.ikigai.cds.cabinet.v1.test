// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"errors"
	"testing"
	"time"
)

const (
	// renders tests slow, as must way 2*60s to ensure atomic cache is flushed
	PerformAtomicReads = false
)

func TestTransactionCounterNodeSimpleCRUD(t *testing.T) {
	s1 := &pb.Counter{
		Counter: uint32(MockRandomInt(1000, 65000)),
		Object: &pb.Counter_Node{Node: "1EKkY0eMD7bVu4jenaz6skyzbt1"},
	}

	s2 := &pb.Counter{
		Counter: uint32(MockRandomInt(1000, 65000)),
		Object: &pb.Counter_Node{Node: "1EKkY0eMD7bVu4jenaz6skyq8up"},
	}

	CounterSharedOrchestrate(t, s1, s2)
}

func TestTransactionCounterEdgeSimpleCRUD(t *testing.T) {
	s1 := &pb.Counter{
		Counter: uint32(MockRandomInt(1000, 65000)),
		Object: &pb.Counter_Edge{Edge: &pb.Edge{Subject: "1EKkY0eMD7bVu4jenaz6skyzbt1", Predicate: 1, Target: "1EKkY1T6y4G3Xf2jtlaM39VucSX"}},
	}

	s2 := &pb.Counter{
		Counter: uint32(MockRandomInt(1000, 65000)),
		Object: &pb.Counter_Edge{Edge: &pb.Edge{Subject: "1EKkY0p9MGb3kAl9TO0dkOkHdQv", Predicate: 2000, Target: "1EKkXz3CjX9vALVvgyayPfECq6I"}},
	}

	CounterSharedOrchestrate(t, s1, s2)
}

func counterWithValue(c *pb.Counter, v int64) (r *pb.Counter){
	return &pb.Counter{
		Object: c.Object,
		Counter: c.Counter,
		Value: v,
	}
}

func CounterSharedOrchestrate(t *testing.T, s1 *pb.Counter, s2 *pb.Counter) {
	it1 := CabinetTest{test: t}
	it1.setup(4)
	CounterSharedS1(it1, s1, s2)
	it1.tearDown()

	// must wait to hopefully flush the atomic cache. Can yield false results if requested too soon
	if PerformAtomicReads {
		time.Sleep(30 * time.Second)
	}

	it2 := CabinetTest{test: t}
	it2.setup(4)
	CounterSharedS2(it2, s1, s2)
	it2.tearDown()
}

func CounterSharedS1(it CabinetTest, s1 *pb.Counter, s2 *pb.Counter) {
	// new counter
	c1 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_CounterRegister{CounterRegister: s1}},
		{ActionId: 2, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s1, 10)}}, // this one is atomic

		{ActionId: 3, Action: &pb.TransactionAction_CounterRegister{CounterRegister: s2}},
	}

	_ = transactionRunner(&c1, &it)

	newCounterVal, err := it.client.CounterGet(it.ctx, s2)
	it.logThing(newCounterVal, err, "CounterGet")

	if newCounterVal.Value != int64(0){
		it.test.Errorf("Newly initiated counter expected to be %d, is %d", 0, newCounterVal.Value)
	}

	// do a few increments
	c2 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s2, 3)}},
		{ActionId: 2, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s1, 4)}},

		{ActionId: 3, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s2, -200)}},
		{ActionId: 4, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s1, -7)}},
	}

	_ = transactionRunner(&c2, &it)

	c3 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s2, 12)}},
		{ActionId: 2, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s1, 92)}},

		{ActionId: 3, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s1, 5)}},
		{ActionId: 4, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s2, -33)}},
	}

	_ = transactionRunner(&c3, &it)

	c4 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s2, 3)}},
		{ActionId: 2, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s2, 4)}},

		{ActionId: 3, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s1, -12)}},
		{ActionId: 4, Action: &pb.TransactionAction_CounterIncrement{CounterIncrement: counterWithValue(s1, 1)}},
	}

	_ = transactionRunner(&c4, &it)

}

func CounterSharedS2(it CabinetTest, s1 *pb.Counter, s2 *pb.Counter) {
	// Atomic Operations & Unit Tests Do Not Mix
	//
	// There is quite a problem in reading these values
	// By design, CDS Cabinet allows infinite operations that do not affect in any way performance
	// For that to work, the operations are queue, rather than executed, for future processing
	// this works great when you don't need immediate exact values (as it is the point), yet for testing to work,
	// we need to enforce a best-guess 60s sleep window to ensure the atomic queue is cleared.

	if PerformAtomicReads {
		// now attempt to read
		// WARNING! THIS TEST CAN YIELD FALSE POSITIVE RESULTS
		// attempt to use increasingly longer wait windows to ensure that it is an actual bug
		expectedC1 := int64(0 + 10 + 4 + (-7) + 92 + 5 + (-12) + 1)
		expectedC2 := int64(0 + 3 + (-200) + 12 + (-33) + 3 + 4)

		c1Val, err := it.client.CounterGet(it.ctx, s1)
		it.logThing(c1Val, err, "CounterGet")

		if c1Val.Value != expectedC1 {
			it.test.Errorf("Counter expected to be %d, is %d", expectedC1, c1Val.Value)
		}

		c2Val, err := it.client.CounterGet(it.ctx, s2)
		it.logThing(c2Val, err, "CounterGet")

		if c2Val.Value != expectedC2 {
			it.test.Errorf("Counter expected to be %d, is %d", expectedC2, c2Val.Value)
		}
	}

	// now delete counters
	c5 := []pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_CounterDelete{CounterDelete: s1}},
		{ActionId: 2, Action: &pb.TransactionAction_CounterDelete{CounterDelete: s2}},
	}

	_ = transactionRunner(&c5, &it)

	// attempt to read counters again
	c1Null, err1 := it.client.CounterGet(it.ctx, s1)
	c2Null, err2 := it.client.CounterGet(it.ctx, s2)

	if err1 != nil{
		it.test.Logf("[I] Counter %d on %v was deleted as expected", s1.Counter, s1.Object)
	}else{
		it.logThing(c1Null, errors.New("counter was supposed to be deleted"), "CounterGet")
	}

	if err2 != nil{
		it.test.Logf("[I] Counter %d on %v was deleted as expected", s2.Counter, s2.Object)
	}else{
		it.logThing(c2Null, errors.New("counter was supposed to be deleted"), "CounterGet")
	}
}