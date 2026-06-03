package main

import (
	"net"
	"sync"
	"sync/atomic"
)

type Message struct {
	Command    string
	Topic      string
	Text       string
	connection net.Conn
	GUID       string
}

type Subscriber struct {
	Name   string `json:"name"`
	topics []string
	conns  map[string]net.Conn
	mu     sync.RWMutex
}

type Broker struct {
	mu          sync.RWMutex
	activeConns atomic.Int32
	name        string
	subscribers map[string][]net.Conn
	publishers  map[string]net.Conn
	wal         *WAL
}

type Publisher struct {
	Name string `json:"name"`
}

type topic struct {
	Name       string `json:"name"`
	Subscriber string `json:"subscriber"`
}

type publish struct {
	Name    string `json:"name"`
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

type subscriberView struct {
	Name   string   `json:"name"`
	Topics []string `json:"topics"`
}

type stateView struct {
	Publishers  []string         `json:"publishers"`
	Subscribers []subscriberView `json:"subscribers"`
}
