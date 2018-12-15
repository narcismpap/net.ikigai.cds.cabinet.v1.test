// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"context"
	"google.golang.org/grpc"
	"sync"
	"testing"
	"time"
)

const (
	TestParallelSize = 50
	TestSequentialSize = 100
	TestGRPCService = "127.0.0.1:8888"
)

const (
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" // 52 possibilities
	letterIdxBits = 6                    // 6 bits to represent 64 possibilities / indexes
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
)

/*
func TestExample(t *testing.T) {
	it := CabinetTest{test: t}
	it.setup(4)



	it.tearDown()
}
*/

type CabinetTest struct{
	client pb.CDSCabinetClient
	test *testing.T
	bench *testing.B
	conn *grpc.ClientConn

	ctx context.Context
	cancel context.CancelFunc

	parallelIDs []uint32
	parallelMux sync.Mutex
}

func (s *CabinetTest) setup(timout uint32){
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	var err error
	s.conn, err = grpc.Dial(TestGRPCService, opts...)

	if err != nil {
		s.test.Errorf("fail to dial: %v", err)
	}

	s.client = pb.NewCDSCabinetClient(s.conn)
	s.ctx, s.cancel = context.WithTimeout(context.Background(), time.Duration(timout) * time.Second)
}

func (s *CabinetTest) tearDown(){
	err := s.conn.Close()

	if err != nil{
		s.test.Errorf("[E] Unable to close connection %v because %v", s.conn, err)
	}

	s.cancel()
}

func (s *CabinetTest) logThing(object interface{}, err error, method string) (bool, interface{}){
	if err != nil{
		s.test.Errorf("[E] %v.%s(): %v (R: %v)", s.client, method, err, object)
		return true, object
	}else{
		s.test.Logf("[I] %v.%s(): %v", s.client, method, object)
		return false, object
	}
}

func (s *CabinetTest) logRejection(object interface{}, err error, method string){
	if err == nil{
		s.test.Errorf("[E] %s was allowed; should be rejected", method)
	}else{
		s.test.Logf("[I] Rejected %s: %v", method, err)
	}
}

func (s *CabinetTest) randomBytes(length int) []byte{
	return MockRandomBytes(length)
}

func (s *CabinetTest) randomAlpha(length int) string{
	return MockRandomAlpha(length)
}


