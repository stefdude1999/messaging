package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

var subs []Subscriber
var pubs []Publisher

// POST a subscriber, topic, and subscriber
// GET overall structure
// UPDATE unsubscribe

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

func main() {
	//	wg := sync.WaitGroup{}
	b1 := newBroker("broker")
	go b1.initializeBroker()
	router := gin.Default()

	router.POST("/publisher", postPublisher)
	router.POST("/subscriber", postSubscriber)
	router.POST("/topic", postTopic)
	router.POST("/publish", postPublish)
	router.PUT("/unsubscribe", updateUnsubscribe)
	router.GET("/state", getState)

	router.Run("localhost:9001")
	//wg.Wait()
}

func postPublisher(c *gin.Context) {
	var publisher Publisher

	if err := c.BindJSON(&publisher); err != nil {
		return
	}

	pubs = append(pubs, publisher)
}

func postSubscriber(c *gin.Context) {
	var subscriber Subscriber

	if err := c.BindJSON(&subscriber); err != nil {
		return
	}

	subs = append(subs, subscriber)
}

func postTopic(c *gin.Context) {
	var newTopic topic

	// Call BindJSON to bind the received JSON to
	// newAlbum.
	if err := c.BindJSON(&newTopic); err != nil {
		return
	}
	to_find := findSub(subs, newTopic.Subscriber)
	if to_find != nil {
		go to_find.subscribeToTopic(newTopic.Name)
	} else {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "could not find subscriber"})
	}
}

func updateUnsubscribe(c *gin.Context) {
	var newUnsubscribe topic

	if err := c.BindJSON(&newUnsubscribe); err != nil {
		return
	}

	to_find := findSub(subs, newUnsubscribe.Subscriber)
	if to_find != nil {
		go to_find.unsubscribeFromTopic(newUnsubscribe.Name)
	} else {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "could not find subscriber"})
	}
}

func postPublish(c *gin.Context) {
	var newPublish publish

	// Call BindJSON to bind the received JSON to
	// newAlbum.
	if err := c.BindJSON(&newPublish); err != nil {
		return
	}
	to_find := findPub(pubs, newPublish.Name)
	if to_find != nil {

		go to_find.publishToTopic(newPublish.Topic, newPublish.Message)
	} else {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "could not find publisher"})
	}
}

func getState(c *gin.Context) {
	state := stateView{
		Publishers:  make([]string, 0, len(pubs)),
		Subscribers: make([]subscriberView, 0, len(subs)),
	}

	for _, p := range pubs {
		state.Publishers = append(state.Publishers, p.Name)
	}

	for _, s := range subs {
		topics := s.topics
		if topics == nil {
			topics = []string{}
		}
		state.Subscribers = append(state.Subscribers, subscriberView{
			Name:   s.Name,
			Topics: topics,
		})
	}

	c.IndentedJSON(http.StatusOK, state)
}

func findSub(subs []Subscriber, sub_name string) *Subscriber {
	for i := range subs {
		if subs[i].Name == sub_name {
			return &subs[i]
		}
	}
	return nil
}

func findPub(pubs []Publisher, pub_name string) *Publisher {
	for i := range pubs {
		if pubs[i].Name == pub_name {
			return &pubs[i]
		}
	}
	return nil
}
