package main

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/google/uuid"
)

func (p Publisher) publishToTopic(topic string, message string) {
	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		return
	}

	guid := uuid.New()

	msg := Message{
		Command: "PUBLISH",
		Topic:   topic,
		Text:    message,
		GUID:    guid.String(),
	}

	data, _ := json.Marshal(msg)

	conn.Write(data)

	// Close the connection
	conn.Close()
}

type Publisher struct {
	Name string `json:"name"`
}
