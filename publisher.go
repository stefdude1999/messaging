package main

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/google/uuid"
)

func (p *Publisher) publishToTopic(topic string, message string) string {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		return err.Error()
	}
	defer conn.Close()

	guid := uuid.New()

	msg := Message{
		Command: "PUBLISH",
		Topic:   topic,
		Text:    message,
		GUID:    guid.String(),
	}

	data, _ := json.Marshal(msg)
	conn.Write(data)

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("disconnected:", err)
			return "ended"
		}

		var received Message
		json.Unmarshal(buf[:n], &received)

		if received.GUID == guid.String() {
			return "ACK" + ": " + received.GUID
		}
	}
}
