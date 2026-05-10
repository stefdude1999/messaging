package main

import (
	"encoding/json"
	"fmt"
	"net"
)

type Message struct {
	Command    string
	Topic      string
	Text       string
	connection net.Conn
	GUID       string
}

type Subscriber struct {
	name   string
	topics []string
	conns  map[string]net.Conn
}

func (s *Subscriber) unsubscribeFromTopic(topic string) {
	conn, ok := s.conns[topic]
	if !ok {
		fmt.Println("not subscribed to topic:", topic)
		return
	}

	for i := range s.topics {
		if s.topics[i] == topic {
			s.topics = append(s.topics[:i], s.topics[i+1:]...)
			break
		}
	}
	delete(s.conns, topic)

	msg := Message{
		Command: "UNSUBSCRIBE",
		Topic:   topic,
	}

	data, _ := json.Marshal(msg)
	conn.Write(data)
	conn.Close()
}

func (s *Subscriber) subscribeToTopic(topic string) {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		return
	}

	s.topics = append(s.topics, topic)
	if s.conns == nil {
		s.conns = make(map[string]net.Conn)
	}
	s.conns[topic] = conn

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
