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

func BenchmarkSequentialCreate(b *testing.B) {
	it := CabinetTest{bench: b}
	it.setup(20)

	for n := 0; n < b.N; n++ {
		_, _ = it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: "XXXXX"})
	}

	it.tearDown()
}

func BenchmarkSequentialGet(b *testing.B) {
	it := CabinetTest{bench: b}
	it.setup(20)

	sq, err := it.client.SequentialCreate(it.ctx, &pb.Sequential{Type: "n", Uuid: "XXXXX"})

	if err != nil {
		b.Fatalf("Unable to setup: %v", err)
	}

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		_, _ = it.client.SequentialGet(it.ctx, &pb.Sequential{Type: "n", Seqid: sq.GetSeqid()})
	}

	it.tearDown()
}
