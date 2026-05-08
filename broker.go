package main

import (
	"encoding/json"
	"fmt"
	"net"
)

type Broker struct {
	name string
}

// var subscribers map[string][]*net.Conn
var subscribers = make(map[string][]*net.Conn)

func (b Broker) initializeBroker() {

	// Listen for incoming connections on port 8080
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Accept incoming connections and handle them
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		// Handle the connection in a new goroutine
		go acceptSubscriber(conn)
	}
}

func acceptSubscriber(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}

	var msg Message

	err = json.Unmarshal(buf[:n], &msg)
	if err != nil {
		fmt.Println("invalid message format")
		return
	}

	if msg.Command == "SUBSCRIBE" {
		fmt.Println("subscribe to topic:", msg.Topic)

		subscribers[msg.Topic] = append(subscribers[msg.Topic], &conn)
	}

	fmt.Printf("%+v\n", subscribers)
}
