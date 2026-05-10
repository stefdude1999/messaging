package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

type Broker struct {
	mu          sync.RWMutex
	name        string
	subscribers map[string][]net.Conn
}

func newBroker(name string) *Broker {
	return &Broker{
		name:        name,
		subscribers: make(map[string][]net.Conn),
	}
}

func (b *Broker) initializeBroker() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	b.serve(ln)
}

func removeSubscriber(subscribers map[string][]net.Conn, topic string, conn net.Conn) {
	conns := subscribers[topic]

	for i, c := range conns {
		if c == conn {
			// remove element i
			conns = append(conns[:i], conns[i+1:]...)
			break
		}
	}

	// update map or delete key if empty
	if len(conns) == 0 {
		delete(subscribers, topic)
	} else {
		subscribers[topic] = conns
	}
}

func (b *Broker) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go b.handleMessage(conn)
	}
}

func (b *Broker) handleMessage(conn net.Conn) {
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
			fmt.Println("subscribe: ", msg.Topic)

			b.mu.Lock()
			b.subscribers[msg.Topic] = append(b.subscribers[msg.Topic], conn)
			b.mu.Unlock()

		case "UNSUBSCRIBE":
			fmt.Println("unsubscribe: ", msg.Topic)

			b.mu.Lock()
			removeSubscriber(b.subscribers, msg.Topic, conn)
			b.mu.Unlock()

		case "PUBLISH":
			fmt.Println("publish:", msg.Text)

			b.mu.RLock()
			for _, c := range b.subscribers[msg.Topic] {
				_, err := c.Write([]byte(msg.Text))
				if err != nil {
					fmt.Println("write error:", err)
				}
			}
			b.mu.RUnlock()
		}
	}
}
