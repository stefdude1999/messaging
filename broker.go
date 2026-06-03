package main

import (
	"encoding/json"
	"fmt"
	"net"
)

func newBroker(name string) *Broker {
	return &Broker{
		name:        name,
		subscribers: make(map[string][]net.Conn),
		publishers:  make(map[string]net.Conn),
	}
}

func (b *Broker) initializeBroker() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	wal, err := CreateWAL("wal", true)
	if err != nil {
		fmt.Println("failed to create WAL:", err)
		return
	}
	b.wal = wal

	defer b.wal.Close()

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
		b.activeConns.Add(1)
		go func() {
			defer b.activeConns.Add(-1)
			b.handleMessage(conn)
		}()
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

		case "ACK":
			if b.wal != nil {
				b.wal.Remove(msg.GUID)
			}
			b.mu.RLock()
			pubConn, ok := b.publishers[msg.GUID]
			b.mu.RUnlock()
			if !ok {
				fmt.Println("ACK received for unknown GUID:", msg.GUID)
				continue
			}
			data, _ := json.Marshal(msg)
			pubConn.Write(data)

		case "SUBSCRIBE":
			fmt.Println("subscribe: ", msg.Topic)

			b.mu.Lock()
			b.subscribers[msg.Topic] = append(b.subscribers[msg.Topic], conn)
			b.mu.Unlock()
			if b.wal != nil {
				for _, pending := range b.wal.PendingForTopic(msg.Topic) {
					data, _ := json.Marshal(pending)
					conn.Write(data)
				}
			}

		case "UNSUBSCRIBE":
			fmt.Println("unsubscribe: ", msg.Topic)

			b.mu.Lock()
			removeSubscriber(b.subscribers, msg.Topic, conn)
			b.mu.Unlock()

		case "PUBLISH":
			if b.wal != nil {
				b.wal.Write(msg)
			}
			fmt.Println("publish:", msg.Text)

			b.mu.Lock()
			b.publishers[msg.GUID] = conn
			b.mu.Unlock()

			b.mu.RLock()
			data, _ := json.Marshal(msg)
			for _, c := range b.subscribers[msg.Topic] {
				_, err := c.Write(data)
				if err != nil {
					fmt.Println("write error:", err)
				}
			}
			b.mu.RUnlock()
		}
	}
}
