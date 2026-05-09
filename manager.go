package main

import (
	"time"
)

func main() {
	p1 := Publisher{name: "stefan"}
	s1 := Subscriber{name: "also_stefan"}
	b1 := Broker{name: "still_stefan"}

	//go s1.initializeSubscriber()

	go b1.initializeBroker()

	// Give the server a moment to start
	// (not ideal, but fine for now)
	// Better later: use sync.WaitGroup or channels
	time.Sleep(1 * time.Second)

	go s1.subscribeToTopic()

	time.Sleep(1 * time.Second)

	p1.publishToTopic()
	//p1.sendMessage()

	// Keep program alive long enough to receive message
	time.Sleep(1 * time.Second)
}
