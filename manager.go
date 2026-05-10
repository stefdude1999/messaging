package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

var subs []Subscriber
var pubs []Publisher

func main() {
	wg := sync.WaitGroup{}
	b1 := newBroker("broker")
	wg.Go(func() { b1.initializeBroker() })

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Enter 1 to create a new subscriber, enter 2 to create a new publisher, 3 to assign a topic to a subscriber, 4 to publish to a topic, 5 to unsubscribe from a topic, 6 to print out everything, enter 7 to exit: ")
		input, err := reader.ReadString('\n')

		if err != nil {
			// Handle any read errors (e.g., EOF)
			fmt.Printf("Error reading input: %v\n", err)
			break
		}

		// Trim whitespace and split into command parts
		input = strings.TrimSpace(input)

		switch input {
		case "":
			continue
		case "1":
			fmt.Println("enter the name of the new subscriber: ")
			subscriber_name, err := reader.ReadString('\n')
			if err != nil {
				// Handle any read errors (e.g., EOF)
				fmt.Printf("Error reading input: %v\n", err)
				break
			}
			subs = append(subs, Subscriber{
				name: subscriber_name,
			})
		case "2":
			fmt.Println("enter the name of the new publisher: ")
			publisher_name, err := reader.ReadString('\n')
			if err != nil {
				// Handle any read errors (e.g., EOF)
				fmt.Printf("Error reading input: %v\n", err)
				break
			}
			pubs = append(pubs, Publisher{
				name: publisher_name,
			})
		case "3":
			fmt.Println("enter the name of the subscriber you're looking for: ")
			subscriber_name, err := reader.ReadString('\n')
			if err != nil {
				// Handle any read errors (e.g., EOF)
				fmt.Printf("Error reading input: %v\n", err)
				break
			}

			to_find := findSub(subs, subscriber_name)
			if to_find != nil {
				println("Found subscriber. Now please enter topic name you'd like to assign this subscriber to: ")
				topic_name, err := reader.ReadString('\n')
				if err != nil {
					// Handle any read errors (e.g., EOF)
					fmt.Printf("Error reading input: %v\n", err)
					break
				}
				wg.Go(func() { to_find.subscribeToTopic(topic_name) })
			} else {
				println("was not able to find that subscriber: ")
			}
		case "4":
			fmt.Println("enter the name of the publisher you're looking for: ")
			publisher_name, err := reader.ReadString('\n')
			if err != nil {
				// Handle any read errors (e.g., EOF)
				fmt.Printf("Error reading input: %v\n", err)
				break
			}

			to_find := findPub(pubs, publisher_name)
			if to_find != nil {
				println("Found subscriber. Now please enter topic name you'd like to publish to: ")
				topic_name, err := reader.ReadString('\n')
				if err != nil {
					// Handle any read errors (e.g., EOF)
					fmt.Printf("Error reading input: %v\n", err)
					break
				}
				println("Now enter message: ")
				message_value, err := reader.ReadString('\n')
				if err != nil {
					// Handle any read errors (e.g., EOF)
					fmt.Printf("Error reading input: %v\n", err)
					break
				}
				wg.Go(func() { to_find.publishToTopic(topic_name, message_value) })
			} else {
				println("was not able to find that publish: ")
			}
		case "5":
			fmt.Println("enter the name of the subscriber you're looking for: ")
			subscriber_name, err := reader.ReadString('\n')
			if err != nil {
				// Handle any read errors (e.g., EOF)
				fmt.Printf("Error reading input: %v\n", err)
				break
			}

			to_find := findSub(subs, subscriber_name)
			if to_find != nil {
				println("Found subscriber. Now please enter topic name you'd like to unsubscribe from: ")
				topic_name, err := reader.ReadString('\n')
				if err != nil {
					// Handle any read errors (e.g., EOF)
					fmt.Printf("Error reading input: %v\n", err)
					break
				}
				wg.Go(func() { to_find.unsubscribeFromTopic(topic_name) })
			} else {
				println("was not able to find that subscriber: ")
			}
		case "6":
			println("List of publishers: ")
			for i := range pubs {
				print(pubs[i].name)
			}

			println("List of subscribers and topics: ")
			for i := range subs {
				print(subs[i].name)
				for j := range subs[i].topics {
					print(" - ", subs[i].topics[j])
				}
			}
		case "7":
			return
		default:
			println("please enter a proper value")
			continue
		}

		// maybe not the best idea, but wait for all messages to be sent/received before prompting the user again. Doesn't have any functional reason but looks nicer in the console
		time.Sleep(1 * time.Second)

	}
	wg.Wait()
}

func findSub(subs []Subscriber, sub_name string) *Subscriber {
	for i := range subs {
		if subs[i].name == sub_name {
			return &subs[i]
		}
	}
	return nil
}

func findPub(pubs []Publisher, pub_name string) *Publisher {
	for i := range pubs {
		if pubs[i].name == pub_name {
			return &pubs[i]
		}
	}
	return nil
}
