package main

import (
	"encoding/json"
	"fmt"
	"net"
)

type Message struct {
	Command string
	Topic   string
	Text    string
}

type Subscriber struct {
	name string
}

func (s Subscriber) unsubscribeFromTopic(topic string) {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		return
	}

	msg := Message{
		Command: "UNSUBSCRIBE",
		Topic:   topic,
	}

	data, _ := json.Marshal(msg)

	conn.Write(data)
}

func (s Subscriber) subscribeToTopic(topic string) {
	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		return
	}

	msg := Message{
		Command: "SUBSCRIBE",
		Topic:   topic,
	}

	data, _ := json.Marshal(msg)

	conn.Write(data)

	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("disconnected:", err)
			conn.Close()
			break
		}

		msg := string(buf[:n])
		fmt.Println("received:", msg, " by subscriber: ", s.name)
	}
}
