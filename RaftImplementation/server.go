package main

import (
	"encoding/gob"
	"lab3/shared"
	"log"
	"net/http"
	"net/rpc"
)

func init() {
	gob.Register(shared.Heartbeat{})
	gob.Register(shared.VoteRequest{})
	gob.Register(shared.VoteResponse{})
}

func main() {
	rpc.Register(shared.NewMembership()) // optional, still available

	rpc.Register(shared.NewReciever())
	rpc.HandleHTTP()
	log.Println("RPC broker on :9005 …")
	log.Fatal(http.ListenAndServe("localhost:9005", nil))
}
