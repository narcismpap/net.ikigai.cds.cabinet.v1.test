package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"context"
	"google.golang.org/grpc"
	"log"
	"testing"
	"time"
)

func TestCRUD(t *testing.T) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial("127.0.0.1:8888", opts...)

	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	defer conn.Close()

	client := pb.NewCDSCabinetClient(conn)
	tester := SequenceTest{client: client, test: t}

	lastID := tester.QCreate("n", "XXXXX")

	updStatus := tester.QUpdate("n", "YYYY", lastID)

	if updStatus != pb.MutationStatus_SUCCESS{
		t.Errorf("QUpdate(%d): expected %s, actual %s", lastID, pb.MutationStatus_SUCCESS, updStatus)
	}

	updValue := tester.QGet("n", lastID)

	if updValue != "YYYY"{
		t.Errorf("QGet(%d): expected %s, actual %s", lastID, "YYYY", updValue)
	}

	delStatus := tester.QDelete("n", lastID)

	if delStatus != pb.MutationStatus_SUCCESS{
		t.Errorf("QDelete(%d): expected %s, actual %s", lastID, pb.MutationStatus_SUCCESS, delStatus)
	}

	newVal := tester.QGet("n", lastID)

	t.Logf("[val] is [%s]", newVal)

}

type SequenceTest struct{
	client pb.CDSCabinetClient
	test *testing.T
}

func (s *SequenceTest) QCreate(sType string, sNode string) uint32{
	sqID, err := SequenceCreate(s.client, sType, sNode)

	if err != nil{
		s.test.Errorf("[E] %v.SequenceCreate(_) = _, %v", s.client, err)
	}else{
		s.test.Logf("[I] %v.SequenceCreate(_) = sqID: %v", s.client, sqID)
	}

	return sqID
}

func (s *SequenceTest) QUpdate(sType string, sNode string, sID uint32) pb.MutationStatus{
	updateRp, uerr := SequenceUpdate(s.client, sType, sNode, sID)

	if uerr != nil{
		s.test.Errorf("[E] %v.SequenceUpdate(_) = _, %v", s.client, uerr)
	}else{
		s.test.Logf("[I] %v.SequenceUpdate(_), MutationStatus: %v", s.client, updateRp)
	}

	return updateRp
}

func (s *SequenceTest) QDelete(sType string, sID uint32) pb.MutationStatus{
	updateRp, uerr := SequenceDelete(s.client, sType, sID)

	if uerr != nil{
		s.test.Errorf("[E] %v.SequenceDelete(_) = _, %v", s.client, uerr)
	}else{
		s.test.Logf("[I] %v.SequenceDelete(_), MutationStatus: %v", s.client, updateRp)
	}

	return updateRp
}

func (s *SequenceTest) QGet(sType string, sID uint32) string{
	nodeID, err := SequenceGet(s.client, sType, sID)

	if err != nil{
		s.test.Errorf("[E] %v.SequenceGet(_) = _, %v", s.client, err)
	}else{
		s.test.Logf("[I] %v.SequenceGet(_) = NodeID: %v", s.client, nodeID)
	}

	return nodeID
}


func SequenceCreate(client pb.CDSCabinetClient, sType string, sNode string) (id uint32, err error){
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rqCreate, err := client.SequentialCreate(ctx, &pb.Sequential{Type: sType, Node: sNode})

	if err != nil {
		return 0, err
	}

	return rqCreate.GetSeqid(), nil
}

func SequenceUpdate(client pb.CDSCabinetClient, sType string, sNode string, sID uint32) (status pb.MutationStatus, err error){
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rqUpdate, err := client.SequentialUpdate(ctx, &pb.Sequential{Type: sType, Node: sNode, Seqid: sID})

	if err != nil {
		return 0, err
	}

	return rqUpdate.GetStatus(), nil
}

func SequenceDelete(client pb.CDSCabinetClient, sType string, sID uint32) (status pb.MutationStatus, err error){
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rqUpdate, err := client.SequentialDelete(ctx, &pb.Sequential{Type: sType, Seqid: sID})

	if err != nil {
		return 0, err
	}

	return rqUpdate.GetStatus(), nil
}

func SequenceGet(client pb.CDSCabinetClient, sType string, sID uint32) (node string, err error){
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rqGet, err := client.SequentialGet(ctx, &pb.Sequential{Type: sType, Seqid: sID})

	if err != nil {
		return "", err
	}

	return rqGet.GetNode(), nil
}
