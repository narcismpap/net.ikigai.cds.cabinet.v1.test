// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	"testing"
)

/* Node - X */
func TestReadCheckNodeString(t *testing.T) {
	doReadCheck(t, RC_NODE, RC_STRING)
}

func TestReadCheckNodeNode(t *testing.T) {
	doReadCheck(t, RC_NODE, RC_NODE)
}

func TestReadCheckNodeEdge(t *testing.T) {
	doReadCheck(t, RC_NODE, RC_EDGE)
}

func TestReadCheckNodeIndex(t *testing.T) {
	doReadCheck(t, RC_NODE, RC_INDEX)
}

func TestReadCheckNodeMetaEdge(t *testing.T) {
	doReadCheck(t, RC_NODE, RC_META_EDGE)
}

func TestReadCheckNodeMetaNode(t *testing.T) {
	doReadCheck(t, RC_NODE, RC_META_NODE)
}

/* Edge - X */
func TestReadCheckEdgeString(t *testing.T) {
	doReadCheck(t, RC_EDGE, RC_STRING)
}

func TestReadCheckEdgeNode(t *testing.T) {
	doReadCheck(t, RC_EDGE, RC_NODE)
}

func TestReadCheckEdgeEdge(t *testing.T) {
	doReadCheck(t, RC_EDGE, RC_EDGE)
}

func TestReadCheckEdgeIndex(t *testing.T) {
	doReadCheck(t, RC_EDGE, RC_INDEX)
}

func TestReadCheckEdgeMetaEdge(t *testing.T) {
	doReadCheck(t, RC_EDGE, RC_META_EDGE)
}

func TestReadCheckEdgeMetaNode(t *testing.T) {
	doReadCheck(t, RC_EDGE, RC_META_NODE)
}

/* Index - X */
func TestReadCheckIndexString(t *testing.T) {
	doReadCheck(t, RC_INDEX, RC_STRING)
}

func TestReadCheckIndexNode(t *testing.T) {
	doReadCheck(t, RC_INDEX, RC_NODE)
}

func TestReadCheckIndexEdge(t *testing.T) {
	doReadCheck(t, RC_INDEX, RC_EDGE)
}

func TestReadCheckIndexIndex(t *testing.T) {
	doReadCheck(t, RC_INDEX, RC_INDEX)
}

func TestReadCheckIndexMetaEdge(t *testing.T) {
	doReadCheck(t, RC_INDEX, RC_META_EDGE)
}

func TestReadCheckIndexMetaNode(t *testing.T) {
	doReadCheck(t, RC_INDEX, RC_META_NODE)
}

/* MetaNode - X */
func TestReadCheckMetaNodeString(t *testing.T) {
	doReadCheck(t, RC_META_NODE, RC_STRING)
}

func TestReadCheckMetaNodeNode(t *testing.T) {
	doReadCheck(t, RC_META_NODE, RC_NODE)
}

func TestReadCheckMetaNodeEdge(t *testing.T) {
	doReadCheck(t, RC_META_NODE, RC_EDGE)
}

func TestReadCheckMetaNodeIndex(t *testing.T) {
	doReadCheck(t, RC_META_NODE, RC_INDEX)
}

func TestReadCheckMetaNodeMetaEdge(t *testing.T) {
	doReadCheck(t, RC_META_NODE, RC_META_EDGE)
}

func TestReadCheckMetaNodeMetaNode(t *testing.T) {
	doReadCheck(t, RC_META_NODE, RC_META_NODE)
}

/* MetaEdge - X */
func TestReadCheckMetaEdgeString(t *testing.T) {
	doReadCheck(t, RC_META_EDGE, RC_STRING)
}

func TestReadCheckMetaEdgeNode(t *testing.T) {
	doReadCheck(t, RC_META_EDGE, RC_NODE)
}

func TestReadCheckMetaEdgeEdge(t *testing.T) {
	doReadCheck(t, RC_META_EDGE, RC_EDGE)
}

func TestReadCheckMetaEdgeIndex(t *testing.T) {
	doReadCheck(t, RC_META_EDGE, RC_INDEX)
}

func TestReadCheckMetaEdgeMetaEdge(t *testing.T) {
	doReadCheck(t, RC_META_EDGE, RC_META_EDGE)
}

func TestReadCheckMetaEdgeMetaNode(t *testing.T) {
	doReadCheck(t, RC_META_EDGE, RC_META_NODE)
}
