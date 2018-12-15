package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"crypto/rand"
	"log"
	mathrand "math/rand"
	"time"
)

func MockRandomEdge() *pb.Edge{
	return &pb.Edge{Subject: MockRandomNodeID(), Predicate: 2018, Target: MockRandomNodeID()}
}

func MockRandomNode() *pb.Node{
	return &pb.Node{Type: uint32(MockRandomInt(10, 5000)), Id: MockRandomNodeID()}
}

func MockRandomPayload() []byte{
	// return MockRandomBytes(30)
	return []byte( MockRandomAlpha(30) ) // easier to read
}

func MockRandomNodeID() string{
	return "0EKk" + MockRandomAlpha(23)
}

func MockRandomBytes(length int) []byte{
	var randomBytes = make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		log.Fatal("Unable to generate random bytes")
	}
	return randomBytes
}

func MockRandomAlpha(length int) string{
	result := make([]byte, length)
	bufferSize := int(float64(length)*1.3)
	for i, j, randomBytes := 0, 0, []byte{}; i < length; j++ {
		if j%bufferSize == 0 {
			randomBytes = MockRandomBytes(bufferSize)
		}
		if idx := int(randomBytes[j%length] & letterIdxMask); idx < len(letterBytes) {
			result[i] = letterBytes[idx]
			i++
		}
	}

	return string(result)
}

func MockRandomInt(min int, max int) int{
	mathrand.Seed(time.Now().UnixNano())
	return mathrand.Intn(max - min) + min
}