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
var subscribers = make(map[string][]net.Conn)

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
		go handleMessage(conn)
	}
}

func handleMessage(conn net.Conn) {
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("client disconnected:", err)
			return
		}

		var msg Message
		err = json.Unmarshal(buf[:n], &msg)
		if err != nil {
			fmt.Println("invalid message format")
			continue
		}

		switch msg.Command {

		case "SUBSCRIBE":
			fmt.Println("subscribe:", msg.Topic)
			subscribers[msg.Topic] = append(subscribers[msg.Topic], conn)

		case "PUBLISH":
			fmt.Println("publish:", msg.Text)

			for _, c := range subscribers[msg.Topic] {
				_, err := c.Write([]byte(msg.Text))
				if err != nil {
					fmt.Println("write error:", err)
				}
			}
		}
	}
}
