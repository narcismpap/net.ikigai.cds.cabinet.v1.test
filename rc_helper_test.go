// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

// see read_check_test.go for implementation
package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"fmt"
	"testing"
)

const (
	RC_STRING    = 0
	RC_NODE      = 1
	RC_EDGE      = 2
	RC_INDEX     = 3
	RC_META_EDGE = 4
	RC_META_NODE = 5
)

type ReadCheckValidator struct {
	it        *CabinetTest
	sourceIRI string

	p1 []byte // == source
	p2 []byte // != source

	cNode []*pb.Node
	cEdge []*pb.Edge
	cIndx []*pb.Index
	cMeta []*pb.Meta
}

func doReadCheck(t *testing.T, from int, to int) {
	it := CabinetTest{test: t}
	it.setup(4)

	r := ReadCheckValidator{
		it: &it,
		p1: MockRandomPayload(),
		p2: MockRandomPayload(),
	}

	switch from {
	case RC_NODE:
		r.sourceIRI = r.newNodeIRI(r.p1)
	case RC_EDGE:
		r.sourceIRI = r.newEdgeIRI(r.p1)
	case RC_INDEX:
		r.sourceIRI = r.newIndexIRI(r.p1)
	case RC_META_NODE:
		r.sourceIRI = r.newMetaNodeIRI(r.p1)
	case RC_META_EDGE:
		r.sourceIRI = r.newMetaEdgeIRI(r.p1)
	default:
		panic("unknown R/C from")
	}

	r.touchExists()

	switch to {
	case RC_STRING:
		r.eqString()
	case RC_NODE:
		r.eqNode()
	case RC_INDEX:
		r.eqIndex()
	case RC_EDGE:
		r.eqEdge()
	case RC_META_EDGE:
		r.eqMetaEdge()
	case RC_META_NODE:
		r.eqMetaNode()
	default:
		panic("unknown R/C to")
	}

	r.cleanUp()

	it.tearDown()
}

func (r *ReadCheckValidator) newNode(payload []byte) *pb.Node {
	n := &pb.Node{Type: uint32(MockRandomInt(10, 10000)), Version: 1, Id: "tmp:1", Properties: payload}

	mapIDs := CDSTransactionRunner(&([]pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_NodeCreate{NodeCreate: n}},
	}), r.it)
	n.Id = mapIDs["tmp:1"]

	r.cNode = append(r.cNode, n)
	return n
}

func (r *ReadCheckValidator) newNodeIRI(payload []byte) string {
	n := r.newNode(payload)
	return fmt.Sprintf("n/%d/%s", n.Type, n.Id)
}

func (r *ReadCheckValidator) newIndex(payload []byte) *pb.Index {
	i := &pb.Index{Type: uint32(MockRandomInt(10, 10000)), Node: MockRandomNodeID(), Value: "test", Properties: payload}
	r.cIndx = append(r.cIndx, i)

	_ = CDSTransactionRunner(&([]pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_IndexUpdate{IndexUpdate: i}},
	}), r.it)

	return i
}

func (r *ReadCheckValidator) newIndexIRI(payload []byte) string {
	i := r.newIndex(payload)
	return fmt.Sprintf("i/%d/%s/%s", i.Type, i.Value, i.Node)
}

func (r *ReadCheckValidator) newEdge(payload []byte) *pb.Edge {
	e := &pb.Edge{Subject: MockRandomNodeID(), Predicate: uint32(MockRandomInt(10, 10000)), Target: MockRandomNodeID(), Properties: payload}
	r.cEdge = append(r.cEdge, e)

	_ = CDSTransactionRunner(&([]pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_EdgeUpdate{EdgeUpdate: e}},
	}), r.it)

	return e
}

func (r *ReadCheckValidator) newEdgeIRI(payload []byte) string {
	e := r.newEdge(payload)
	return fmt.Sprintf("e/%s/%d/%s", e.Subject, e.Predicate, e.Target)
}

func (r *ReadCheckValidator) newMetaEdge(payload []byte) (m *pb.Meta, e *pb.Edge) {
	e = &pb.Edge{Subject: MockRandomNodeID(), Predicate: uint32(MockRandomInt(10, 10000)), Target: MockRandomNodeID()}
	m = &pb.Meta{Object: &pb.Meta_Edge{Edge: e}, Key: uint32(MockRandomInt(10, 10000)), Val: payload}
	r.cMeta = append(r.cMeta, m)

	_ = CDSTransactionRunner(&([]pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_MetaUpdate{MetaUpdate: m}},
	}), r.it)

	return
}

func (r *ReadCheckValidator) newMetaEdgeIRI(payload []byte) string {
	m, e := r.newMetaEdge(payload)
	return fmt.Sprintf("m/e/%s/%d/%s/%d", e.Subject, e.Predicate, e.Target, m.Key)
}

func (r *ReadCheckValidator) newMetaNode(payload []byte) (m *pb.Meta, n *pb.Node) {
	n = &pb.Node{Type: uint32(MockRandomInt(10, 10000)), Version: 1, Id: MockRandomNodeID()}
	m = &pb.Meta{Object: &pb.Meta_Node{Node: n.Id}, Key: uint32(MockRandomInt(10, 10000)), Val: payload}
	r.cMeta = append(r.cMeta, m)

	_ = CDSTransactionRunner(&([]pb.TransactionAction{
		{ActionId: 1, Action: &pb.TransactionAction_MetaUpdate{MetaUpdate: m}},
	}), r.it)

	return
}

func (r *ReadCheckValidator) newMetaNodeIRI(payload []byte) string {
	m, n := r.newMetaNode(payload)
	return fmt.Sprintf("m/n/%s/%d", n.Id, m.Key)
}

func (r *ReadCheckValidator) touchExists() {
	// positive test
	r.check(&pb.ReadCheckRequest{
		Source: r.sourceIRI, Operator: pb.CheckOperators_EXISTS, Target: &pb.CheckTarget{Target: &pb.CheckTarget_Val{Val: "*"}},
	}, true)

	r.check(&pb.ReadCheckRequest{
		Source: r.sourceIRI, Operator: pb.CheckOperators_TOUCH, Target: &pb.CheckTarget{Target: &pb.CheckTarget_Val{Val: "*"}},
	}, true)
}

func (r *ReadCheckValidator) eqString() {
	equalT := &pb.CheckTarget{Target: &pb.CheckTarget_Val{Val: string(r.p1)}}
	neT := &pb.CheckTarget{Target: &pb.CheckTarget_Val{Val: string(r.p2)}}
	r.eqSpecific(equalT, neT)
}

func (r *ReadCheckValidator) eqNode() {
	equalT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newNodeIRI(r.p1)}}
	neT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newNodeIRI(r.p2)}}
	r.eqSpecific(equalT, neT)
}

func (r *ReadCheckValidator) eqIndex() {
	equalT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newIndexIRI(r.p1)}}
	neT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newIndexIRI(r.p2)}}
	r.eqSpecific(equalT, neT)
}

func (r *ReadCheckValidator) eqEdge() {
	equalT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newEdgeIRI(r.p1)}}
	neT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newEdgeIRI(r.p2)}}
	r.eqSpecific(equalT, neT)
}

func (r *ReadCheckValidator) eqMetaEdge() {
	equalT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newMetaEdgeIRI(r.p1)}}
	neT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newMetaEdgeIRI(r.p2)}}
	r.eqSpecific(equalT, neT)
}

func (r *ReadCheckValidator) eqMetaNode() {
	equalT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newMetaNodeIRI(r.p1)}}
	neT := &pb.CheckTarget{Target: &pb.CheckTarget_Iri{Iri: r.newMetaNodeIRI(r.p2)}}
	r.eqSpecific(equalT, neT)
}

func (r *ReadCheckValidator) eqSpecific(eT *pb.CheckTarget, neT *pb.CheckTarget) {
	r.check(&pb.ReadCheckRequest{Source: r.sourceIRI, Operator: pb.CheckOperators_EQUAL, Target: eT}, true)
	r.check(&pb.ReadCheckRequest{Source: r.sourceIRI, Operator: pb.CheckOperators_NOT_EQUAL, Target: neT}, true)

	r.check(&pb.ReadCheckRequest{Source: r.sourceIRI, Operator: pb.CheckOperators_EQUAL, Target: neT}, false)
	r.check(&pb.ReadCheckRequest{Source: r.sourceIRI, Operator: pb.CheckOperators_NOT_EQUAL, Target: eT}, false)
}

func (r *ReadCheckValidator) check(rq *pb.ReadCheckRequest, expected bool) {
	rc, err := r.it.client.ReadCheck(r.it.ctx, rq)
	r.it.logThing(rc, err, "ReadCheck")

	if rc != nil && rc.Result == expected {
		r.it.test.Logf("ReadCheck(%v) is %v", rq, expected)
	} else {
		r.it.test.Errorf("[E] ReadCheck(%v) failed, expected %v", rq, expected)
	}
}

func (r *ReadCheckValidator) cleanUp() {
	trx := make([]pb.TransactionAction, 0)
	a := uint32(0)

	for n := range r.cNode {
		trx = append(trx, pb.TransactionAction{
			ActionId: a, Action: &pb.TransactionAction_NodeDelete{NodeDelete: r.cNode[n]},
		})
		a += 1
	}

	for e := range r.cEdge {
		trx = append(trx, pb.TransactionAction{
			ActionId: a, Action: &pb.TransactionAction_EdgeDelete{EdgeDelete: r.cEdge[e]},
		})
		a += 1
	}

	for i := range r.cIndx {
		trx = append(trx, pb.TransactionAction{
			ActionId: a, Action: &pb.TransactionAction_IndexDelete{IndexDelete: r.cIndx[i]},
		})
		a += 1
	}

	for m := range r.cMeta {
		trx = append(trx, pb.TransactionAction{
			ActionId: a, Action: &pb.TransactionAction_MetaDelete{MetaDelete: r.cMeta[m]},
		})
		a += 1
	}

	_ = CDSTransactionRunner(&trx, r.it)
}
