package main

import (
	"encoding/json"
	"fmt"
	"net"
)

func (s *Subscriber) sendAck(GUID string) {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		return
	}

	msg := Message{
		Command: "ACK",
		GUID:    GUID,
	}

	data, _ := json.Marshal(msg)

	conn.Write(data)
	conn.Close()
}

func (s *Subscriber) unsubscribeFromTopic(topic string) {
	s.mu.Lock()
	conn, ok := s.conns[topic]
	if !ok {
		s.mu.Unlock()
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
	s.mu.Unlock()

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

	s.mu.Lock()
	s.topics = append(s.topics, topic)
	if s.conns == nil {
		s.conns = make(map[string]net.Conn)
	}
	s.conns[topic] = conn
	s.mu.Unlock()

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

		var received Message
		json.Unmarshal(buf[:n], &received)

		msgText := string(buf[:n])
		fmt.Println("received:", msgText, " by subscriber: ", s.Name)
		s.sendAck(received.GUID)
	}
}
