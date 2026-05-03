package main

import (
    "fmt"
    "net"
)

type Subscriber struct {
	name string
}

func (s Subscriber) initializeSubscriber() {
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
        go handleConnection(conn)
    }
}

func handleConnection(conn net.Conn) {
    defer conn.Close()

    buf := make([]byte, 1024)
    n, err := conn.Read(buf)
    if err != nil {
        fmt.Println(err)
        return
    }

    fmt.Printf("Received: %s\n", buf[:n])
}