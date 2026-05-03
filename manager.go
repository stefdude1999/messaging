package main

import (
	"time"
)

func main() {
	p1 := Publisher{name: "stefan"}
	s1 := Subscriber{name: "also_stefan"}

	go s1.initializeSubscriber()

	// Give the server a moment to start
	// (not ideal, but fine for now)
	// Better later: use sync.WaitGroup or channels
	time.Sleep(1 * time.Second)

	p1.sendMessage()

	// Keep program alive long enough to receive message
	time.Sleep(1 * time.Second)
}