package main

import (
	//"time"
	"sync"
)

func main() {
	wg := sync.WaitGroup{}
	p1 := Publisher{name: "publisher_1"}
	s1 := Subscriber{name: "subscriber_1"}
	b1 := Broker{name: "broker"}


	wg.Go(func() { b1.initializeBroker() })
 

	wg.Go(func() {s1.subscribeToTopic() })

	wg.Go(func() {p1.publishToTopic() })

	wg.Wait()
}
