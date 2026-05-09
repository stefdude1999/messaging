package main

import (
	"encoding/json"
	"fmt"
	"net"
)

func (p Publisher) publishToTopic() {
	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		return
	}

	msg := Message{
		Command: "PUBLISH",
		Topic:   "orders",
		Text:    "Test message",
	}

	data, _ := json.Marshal(msg)

	conn.Write(data)

	// Close the connection
	conn.Close()
}

type Publisher struct {
	name string
}
