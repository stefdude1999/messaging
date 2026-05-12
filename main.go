package main

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	docs "example.com/messaging/docs"
	swaggerfiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
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

var globalmu sync.RWMutex

func main() {
	//	wg := sync.WaitGroup{}
	b1 := newBroker("broker")
	go b1.initializeBroker()
	router := gin.Default()
	docs.SwaggerInfo.BasePath = "/api/v1"
	router.POST("/publisher", postPublisher)
	router.POST("/subscriber", postSubscriber)
	router.POST("/topic", postTopic)
	router.POST("/publish", postPublish)
	router.PUT("/unsubscribe", updateUnsubscribe)
	router.GET("/state", getState)
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerfiles.Handler))
	router.Run("localhost:9001")
	//wg.Wait()
}

// @BasePath /api/v1
// stefmessaging godoc
// @Summary Create a new publisher
// @Schemes
// @Description Create a new publisher and give it a name
// @Tags example
// @Accept json
// @Produce json
// @Param publisher body Publisher true "Publisher object"
// @Success 200 {string} Helloworld
// @Router /publisher [post]
func postPublisher(c *gin.Context) {
	var publisher Publisher

	if err := c.BindJSON(&publisher); err != nil {
		return
	}

	globalmu.Lock()
	pubs = append(pubs, publisher)
	globalmu.Unlock()
}

// @BasePath /api/v1
// stefmessaging godoc
// @Summary Create a new subscriber
// @Schemes
// @Description Create a new subscriber with a name
// @Tags example
// @Accept json
// @Produce json
// @Param subscriber body Subscriber true "Subscriber object"
// @Success 200 {string} Helloworld
// @Router /subscriber [post]
func postSubscriber(c *gin.Context) {
	var subscriber Subscriber

	if err := c.BindJSON(&subscriber); err != nil {
		return
	}

	globalmu.Lock()
	subs = append(subs, subscriber)
	globalmu.Unlock()
}

// @BasePath /api/v1
// stefmessaging godoc
// @Summary Create a new topic
// @Schemes
// @Description Create a new topic and associate it with a subscriber
// @Tags example
// @Accept json
// @Produce json
// @Param topic body topic true "Topic object"
// @Success 200 {string} Helloworld
// @Router /topic [post]
func postTopic(c *gin.Context) {
	var newTopic topic

	// Call BindJSON to bind the received JSON to
	// newAlbum.
	if err := c.BindJSON(&newTopic); err != nil {
		return
	}
	globalmu.RLock()
	to_find := findSub(subs, newTopic.Subscriber)
	if to_find != nil {
		go to_find.subscribeToTopic(newTopic.Name)
	} else {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "could not find subscriber"})
	}
	globalmu.RUnlock()
}

// @BasePath /api/v1
// stefmessaging godoc
// @Summary Unsubscribe subscriber to a topic
// @Schemes
// @Description Remove listener from subscriber to a specific topic
// @Tags example
// @Accept json
// @Produce json
// @Param topic body topic true "Topic object"
// @Success 200 {string} Helloworld
// @Router /unsubscribe [put]
func updateUnsubscribe(c *gin.Context) {
	var newUnsubscribe topic

	if err := c.BindJSON(&newUnsubscribe); err != nil {
		return
	}

	globalmu.RLock()
	to_find := findSub(subs, newUnsubscribe.Subscriber)
	if to_find != nil {
		go to_find.unsubscribeFromTopic(newUnsubscribe.Name)
	} else {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "could not find subscriber"})
	}
	globalmu.RUnlock()
}

// @BasePath /api/v1
// stefmessaging godoc
// @Summary Publish a message to a topic
// @Schemes
// @Description Pass in a topic, a publisher and a message
// @Tags example
// @Accept json
// @Produce json
// @Param publish body publish true "Publish object"
// @Success 200 {string} Helloworld
// @Router /publish [post]
func postPublish(c *gin.Context) {
	var newPublish publish

	// Call BindJSON to bind the received JSON to
	// newAlbum.
	if err := c.BindJSON(&newPublish); err != nil {
		return
	}
	globalmu.RLock()
	to_find := findPub(pubs, newPublish.Name)
	if to_find != nil {

		go to_find.publishToTopic(newPublish.Topic, newPublish.Message)
	} else {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "could not find publisher"})
	}
	globalmu.RUnlock()
}

// @BasePath /api/v1

// stefmessaging godoc
// @Summary Print system
// @Schemes
// @Description Print the nested structure of a system
// @Tags example
// @Accept json
// @Produce json
// @Success 200 {string} Helloworld
// @Router /state [get]
func getState(c *gin.Context) {
	state := stateView{
		Publishers:  make([]string, 0, len(pubs)),
		Subscribers: make([]subscriberView, 0, len(subs)),
	}

	globalmu.RLock()
	for _, p := range pubs {
		state.Publishers = append(state.Publishers, p.Name)
	}

	for i := range subs {
		subs[i].mu.RLock()
		topics := subs[i].topics
		if topics == nil {
			topics = []string{}
		}
		view := subscriberView{Name: subs[i].Name, Topics: topics}
		subs[i].mu.RUnlock()
		state.Subscribers = append(state.Subscribers, view)
	}
	globalmu.RUnlock()

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
