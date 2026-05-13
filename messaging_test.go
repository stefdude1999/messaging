package main

// AI Generated

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// TestMain starts a broker on :8080 for API integration tests.
// All existing broker/unit tests use random ports and are unaffected.
func TestMain(m *testing.M) {
	ln, err := net.Listen("tcp", ":8080")
	if err == nil {
		apiTestBroker = newBroker("api-test")
		go apiTestBroker.serve(ln)
		defer ln.Close()
	}
	os.Exit(m.Run())
}

// ---- test helpers ----

// startTestBroker starts a broker on a random port and returns the address.
// The listener is closed automatically via t.Cleanup.
func startTestBroker(t *testing.T) (b *Broker, addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b = newBroker("test")
	go b.serve(ln)
	t.Cleanup(func() { ln.Close() })
	return b, ln.Addr().String()
}

// sendSubscribe connects to addr, sends a SUBSCRIBE message, and returns the
// open connection. The caller owns the connection and must close it.
func sendSubscribe(t *testing.T, addr, topic string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("subscribe dial: %v", err)
	}
	msg := Message{Command: "SUBSCRIBE", Topic: topic}
	data, _ := json.Marshal(msg)
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}
	return conn
}

// sendPublish connects to addr, sends a PUBLISH message, and closes immediately.
func sendPublish(t *testing.T, addr, topic, text string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("publish dial: %v", err)
	}
	defer conn.Close()
	msg := Message{Command: "PUBLISH", Topic: topic, Text: text}
	data, _ := json.Marshal(msg)
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("publish write: %v", err)
	}
}

// readWithTimeout reads from conn and fails the test if nothing arrives within timeout.
// If the received bytes are a JSON Message, the Text field is returned.
func readWithTimeout(t *testing.T, conn net.Conn, timeout time.Duration) string {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("expected message but got read error: %v", err)
	}
	conn.SetReadDeadline(time.Time{})
	var msg Message
	if json.Unmarshal(buf[:n], &msg) == nil && msg.Text != "" {
		return msg.Text
	}
	return string(buf[:n])
}

// readTimesOut returns true if conn produces no data within timeout.
func readTimesOut(conn net.Conn, timeout time.Duration) bool {
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return false
}

// waitForActiveConns polls until b has exactly n active handleMessage goroutines,
// or fails after 1s. Use this to wait for short-lived (publish) connections to
// finish processing before taking the next action.
func waitForActiveConns(t *testing.T, b *Broker, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if int(b.activeConns.Load()) == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d active connection(s)", n)
}

// waitForSubs polls until b has exactly n subscribers for topic, or fails after 1s.
func waitForSubs(t *testing.T, b *Broker, topic string, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		count := len(b.subscribers[topic])
		b.mu.RUnlock()
		if count == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d subscriber(s) on topic %q", n, topic)
}

// pipeSubscribe wires up a net.Pipe, starts handleMessage on the server side,
// and sends a SUBSCRIBE message. Returns both ends.
func pipeSubscribe(t *testing.T, b *Broker, topic string) (server, client net.Conn) {
	t.Helper()
	server, client = net.Pipe()
	go b.handleMessage(server)
	msg := Message{Command: "SUBSCRIBE", Topic: topic}
	data, _ := json.Marshal(msg)
	if _, err := client.Write(data); err != nil {
		t.Fatalf("pipeSubscribe write: %v", err)
	}
	waitForSubs(t, b, topic, 1)
	return server, client
}

// ---- findSub ----

func TestFindSub_Found(t *testing.T) {
	subs := []Subscriber{{Name: "alpha"}, {Name: "beta"}}
	got := findSub(subs, "beta")
	if got == nil || got.Name != "beta" {
		t.Fatalf("expected beta, got %v", got)
	}
}

func TestFindSub_ReturnsPointerIntoSlice(t *testing.T) {
	subs := []Subscriber{{Name: "alpha"}}
	got := findSub(subs, "alpha")
	if got != &subs[0] {
		t.Fatal("expected pointer into original slice")
	}
}

func TestFindSub_NotFound(t *testing.T) {
	subs := []Subscriber{{Name: "alpha"}}
	if got := findSub(subs, "missing"); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestFindSub_EmptySlice(t *testing.T) {
	if got := findSub(nil, "x"); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

// ---- findPub ----

func TestFindPub_Found(t *testing.T) {
	pubs := []Publisher{{Name: "p1"}, {Name: "p2"}}
	got := findPub(pubs, "p2")
	if got == nil || got.Name != "p2" {
		t.Fatalf("expected p2, got %v", got)
	}
}

func TestFindPub_ReturnsPointerIntoSlice(t *testing.T) {
	pubs := []Publisher{{Name: "p1"}}
	got := findPub(pubs, "p1")
	if got != &pubs[0] {
		t.Fatal("expected pointer into original slice")
	}
}

func TestFindPub_NotFound(t *testing.T) {
	pubs := []Publisher{{Name: "p1"}}
	if got := findPub(pubs, "missing"); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestFindPub_EmptySlice(t *testing.T) {
	if got := findPub(nil, "x"); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

// ---- handleMessage unit tests (net.Pipe — no real network) ----

func TestHandleMessage_SubscribeRegistersConnection(t *testing.T) {
	b := newBroker("test")
	server, client := net.Pipe()
	defer client.Close()
	go b.handleMessage(server)

	msg := Message{Command: "SUBSCRIBE", Topic: "orders"}
	data, _ := json.Marshal(msg)
	client.Write(data)
	waitForSubs(t, b, "orders", 1)

	b.mu.RLock()
	n := len(b.subscribers["orders"])
	b.mu.RUnlock()

	if n != 1 {
		t.Fatalf("expected 1 subscriber for 'orders', got %d", n)
	}
}

func TestHandleMessage_SubscribeMultipleTopics(t *testing.T) {
	b := newBroker("test")
	_, c1 := pipeSubscribe(t, b, "topicA")
	_, c2 := pipeSubscribe(t, b, "topicB")
	defer c1.Close()
	defer c2.Close()

	b.mu.RLock()
	a, bk := len(b.subscribers["topicA"]), len(b.subscribers["topicB"])
	b.mu.RUnlock()

	if a != 1 || bk != 1 {
		t.Fatalf("expected 1 subscriber each, got topicA=%d topicB=%d", a, bk)
	}
}

func TestHandleMessage_PublishDelivered(t *testing.T) {
	b := newBroker("test")
	_, subClient := pipeSubscribe(t, b, "orders")
	defer subClient.Close()

	pubServer, pubClient := net.Pipe()
	defer pubClient.Close()
	go b.handleMessage(pubServer)

	pub := Message{Command: "PUBLISH", Topic: "orders", Text: "hello"}
	data, _ := json.Marshal(pub)
	pubClient.Write(data)

	got := readWithTimeout(t, subClient, time.Second)
	if got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
}

func TestHandleMessage_PublishToWrongTopicNotDelivered(t *testing.T) {
	b := newBroker("test")
	_, subClient := pipeSubscribe(t, b, "orders")
	defer subClient.Close()

	pubServer, pubClient := net.Pipe()
	defer pubClient.Close()
	go b.handleMessage(pubServer)

	pub := Message{Command: "PUBLISH", Topic: "payments", Text: "wrong topic"}
	data, _ := json.Marshal(pub)
	pubClient.Write(data)

	if !readTimesOut(subClient, 150*time.Millisecond) {
		t.Fatal("subscriber received a message it should not have")
	}
}

func TestHandleMessage_InvalidJSONIgnored(t *testing.T) {
	b := newBroker("test")
	server, client := net.Pipe()
	defer client.Close()
	go b.handleMessage(server)

	client.Write([]byte("not json {{{}"))
	time.Sleep(20 * time.Millisecond)

	b.mu.RLock()
	total := 0
	for _, conns := range b.subscribers {
		total += len(conns)
	}
	b.mu.RUnlock()

	if total != 0 {
		t.Fatalf("invalid message should not register a subscriber, got %d", total)
	}
}

func TestHandleMessage_UnknownCommandIgnored(t *testing.T) {
	b := newBroker("test")
	server, client := net.Pipe()
	defer client.Close()
	go b.handleMessage(server)

	msg := Message{Command: "DELETE", Topic: "orders"}
	data, _ := json.Marshal(msg)
	client.Write(data)
	time.Sleep(20 * time.Millisecond)

	b.mu.RLock()
	n := len(b.subscribers["orders"])
	b.mu.RUnlock()

	if n != 0 {
		t.Fatalf("unknown command should not register subscriber, got %d", n)
	}
}

func TestHandleMessage_ReturnsOnClientDisconnect(t *testing.T) {
	b := newBroker("test")
	server, client := net.Pipe()

	done := make(chan struct{})
	go func() {
		b.handleMessage(server)
		close(done)
	}()

	client.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleMessage did not return after client disconnect")
	}
}

// ---- integration tests (real TCP, broker on :0) ----

func TestIntegration_SubscribeAndReceive(t *testing.T) {
	b, addr := startTestBroker(t)

	sub := sendSubscribe(t, addr, "events")
	defer sub.Close()
	waitForSubs(t, b, "events", 1)

	sendPublish(t, addr, "events", "hello world")

	got := readWithTimeout(t, sub, time.Second)
	if got != "hello world" {
		t.Fatalf("expected 'hello world', got %q", got)
	}
}

func TestIntegration_MultipleSubscribersSameTopic(t *testing.T) {
	b, addr := startTestBroker(t)

	sub1 := sendSubscribe(t, addr, "news")
	sub2 := sendSubscribe(t, addr, "news")
	sub3 := sendSubscribe(t, addr, "news")
	defer sub1.Close()
	defer sub2.Close()
	defer sub3.Close()
	waitForSubs(t, b, "news", 3)

	sendPublish(t, addr, "news", "broadcast")

	for i, conn := range []net.Conn{sub1, sub2, sub3} {
		got := readWithTimeout(t, conn, time.Second)
		if got != "broadcast" {
			t.Fatalf("subscriber %d: expected 'broadcast', got %q", i+1, got)
		}
	}
}

func TestIntegration_TopicIsolation(t *testing.T) {
	b, addr := startTestBroker(t)

	subA := sendSubscribe(t, addr, "topicA")
	subB := sendSubscribe(t, addr, "topicB")
	defer subA.Close()
	defer subB.Close()
	waitForSubs(t, b, "topicA", 1)
	waitForSubs(t, b, "topicB", 1)

	sendPublish(t, addr, "topicA", "only for A")

	got := readWithTimeout(t, subA, time.Second)
	if got != "only for A" {
		t.Fatalf("subA: expected 'only for A', got %q", got)
	}
	if !readTimesOut(subB, 150*time.Millisecond) {
		t.Fatal("subB should not receive a message published to topicA")
	}
}

func TestIntegration_MultipleTopicsIndependent(t *testing.T) {
	b, addr := startTestBroker(t)

	subA := sendSubscribe(t, addr, "alpha")
	subB := sendSubscribe(t, addr, "beta")
	defer subA.Close()
	defer subB.Close()
	waitForSubs(t, b, "alpha", 1)
	waitForSubs(t, b, "beta", 1)

	sendPublish(t, addr, "alpha", "msg-alpha")
	sendPublish(t, addr, "beta", "msg-beta")

	if got := readWithTimeout(t, subA, time.Second); got != "msg-alpha" {
		t.Fatalf("subA: expected 'msg-alpha', got %q", got)
	}
	if got := readWithTimeout(t, subB, time.Second); got != "msg-beta" {
		t.Fatalf("subB: expected 'msg-beta', got %q", got)
	}
}

func TestIntegration_PublishWithNoSubscribers(t *testing.T) {
	_, addr := startTestBroker(t)

	// Must not panic or block.
	sendPublish(t, addr, "empty-topic", "nobody home")
}

func TestIntegration_PublishBeforeSubscribe(t *testing.T) {
	b, addr := startTestBroker(t)

	// Publish first, then subscribe — subscriber must NOT receive the old message.
	sendPublish(t, addr, "late", "early bird")
	waitForActiveConns(t, b, 0) // ensure publish goroutine has finished before subscribing

	sub := sendSubscribe(t, addr, "late")
	defer sub.Close()
	waitForSubs(t, b, "late", 1)

	if !readTimesOut(sub, 150*time.Millisecond) {
		t.Fatal("subscriber should not receive messages published before it subscribed")
	}
}

func TestIntegration_SequentialMessages(t *testing.T) {
	b, addr := startTestBroker(t)

	sub := sendSubscribe(t, addr, "seq")
	defer sub.Close()
	waitForSubs(t, b, "seq", 1)

	messages := []string{"one", "two", "three"}
	for _, m := range messages {
		sendPublish(t, addr, "seq", m)
	}

	// TCP is a stream — reads may coalesce, so use a JSON decoder to handle
	// concatenated message objects.
	received := []string{}
	dec := json.NewDecoder(sub)
	sub.SetReadDeadline(time.Now().Add(time.Second))
	for len(received) < len(messages) {
		var msg Message
		if err := dec.Decode(&msg); err != nil {
			break
		}
		received = append(received, msg.Text)
	}
	sub.SetReadDeadline(time.Time{})

	// Each publish uses a separate TCP connection handled by a separate broker
	// goroutine, so arrival order is not guaranteed. Verify all three messages arrived.
	if len(received) != len(messages) {
		t.Fatalf("expected %d messages, got %d: %v", len(messages), len(received), received)
	}
	for _, msg := range messages {
		found := false
		for _, r := range received {
			if r == msg {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected %q in received messages, got %v", msg, received)
		}
	}
}

// TestIntegration_ConcurrentPublish verifies no data races under concurrent
// publishes. Run with: go test -race
func TestIntegration_ConcurrentPublish(t *testing.T) {
	b, addr := startTestBroker(t)

	sub := sendSubscribe(t, addr, "stream")
	defer sub.Close()
	waitForSubs(t, b, "stream", 1)

	const n = 30
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendPublish(t, addr, "stream", "x")
		}()
	}
	wg.Wait()

	// Drain until we've received at least n bytes (1 per message) or timeout.
	received := 0
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	for received < n {
		k, err := sub.Read(buf)
		if err != nil {
			break
		}
		received += k
	}
	sub.SetReadDeadline(time.Time{})

	if received < n {
		t.Fatalf("expected at least %d bytes, got %d", n, received)
	}
}

// TestIntegration_ConcurrentSubscribeAndPublish exercises simultaneous
// subscribe and publish — intended for -race detection.
func TestIntegration_ConcurrentSubscribeAndPublish(t *testing.T) {
	b, addr := startTestBroker(t)

	var mu sync.Mutex
	var conns []net.Conn

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := sendSubscribe(t, addr, "race")
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}()
	}
	wg.Wait()
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	waitForSubs(t, b, "race", 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendPublish(t, addr, "race", "ping")
		}()
	}
	wg.Wait()
}

// ---- API test infrastructure ----

// apiTestBroker is started on :8080 by TestMain for API integration tests.
var apiTestBroker *Broker

// setupAPITest resets global pub/sub state and returns a fresh Gin router
// wired up with all API handlers.
func setupAPITest(t *testing.T) *gin.Engine {
	t.Helper()
	subs = nil
	pubs = nil
	t.Cleanup(func() {
		subs = nil
		pubs = nil
	})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/publisher", postPublisher)
	r.POST("/subscriber", postSubscriber)
	r.POST("/topic", postTopic)
	r.POST("/publish", postPublish)
	r.PUT("/unsubscribe", updateUnsubscribe)
	r.GET("/state", getState)
	return r
}

// apiReq fires an HTTP request through the router and returns the recorder.
func apiReq(t *testing.T, r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

// requireAPIBroker skips the test if the shared broker on :8080 is unavailable.
func requireAPIBroker(t *testing.T) {
	t.Helper()
	if apiTestBroker == nil {
		t.Skip("port 8080 unavailable; skipping API integration test")
	}
}

// ---- API unit tests (no broker required) ----

func TestAPI_CreatePublisher(t *testing.T) {
	r := setupAPITest(t)
	w := apiReq(t, r, "POST", "/publisher", `{"name":"pub1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if len(pubs) != 1 || pubs[0].Name != "pub1" {
		t.Fatalf("expected pubs=[pub1], got %v", pubs)
	}
}

func TestAPI_CreateSubscriber(t *testing.T) {
	r := setupAPITest(t)
	w := apiReq(t, r, "POST", "/subscriber", `{"name":"sub1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if len(subs) != 1 || subs[0].Name != "sub1" {
		t.Fatalf("expected subs=[sub1], got %v", subs)
	}
}

func TestAPI_Subscribe_UnknownSubscriber_Returns404(t *testing.T) {
	r := setupAPITest(t)
	w := apiReq(t, r, "POST", "/topic", `{"name":"orders","subscriber":"nobody"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestAPI_Publish_UnknownPublisher_Returns404(t *testing.T) {
	r := setupAPITest(t)
	w := apiReq(t, r, "POST", "/publish", `{"name":"ghost","topic":"orders","message":"hi"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestAPI_Unsubscribe_UnknownSubscriber_Returns404(t *testing.T) {
	r := setupAPITest(t)
	w := apiReq(t, r, "PUT", "/unsubscribe", `{"name":"orders","subscriber":"nobody"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestAPI_BadJSON_Publisher_Returns400(t *testing.T) {
	r := setupAPITest(t)
	w := apiReq(t, r, "POST", "/publisher", `not json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAPI_BadJSON_Subscriber_Returns400(t *testing.T) {
	r := setupAPITest(t)
	w := apiReq(t, r, "POST", "/subscriber", `not json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// ---- GET /state unit tests (no broker required) ----

func TestAPI_GetState_Empty(t *testing.T) {
	r := setupAPITest(t)
	w := apiReq(t, r, "GET", "/state", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var state stateView
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(state.Publishers) != 0 {
		t.Fatalf("expected no publishers, got %v", state.Publishers)
	}
	if len(state.Subscribers) != 0 {
		t.Fatalf("expected no subscribers, got %v", state.Subscribers)
	}
}

func TestAPI_GetState_PublishersOnly(t *testing.T) {
	r := setupAPITest(t)
	pubs = []Publisher{{Name: "pub1"}, {Name: "pub2"}}

	w := apiReq(t, r, "GET", "/state", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var state stateView
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(state.Publishers) != 2 || state.Publishers[0] != "pub1" || state.Publishers[1] != "pub2" {
		t.Fatalf("unexpected publishers: %v", state.Publishers)
	}
	if len(state.Subscribers) != 0 {
		t.Fatalf("expected no subscribers, got %v", state.Subscribers)
	}
}

func TestAPI_GetState_SubscriberWithNoTopics(t *testing.T) {
	r := setupAPITest(t)
	subs = []Subscriber{{Name: "idle-sub"}}

	w := apiReq(t, r, "GET", "/state", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var state stateView
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(state.Subscribers) != 1 {
		t.Fatalf("expected 1 subscriber, got %d", len(state.Subscribers))
	}
	if state.Subscribers[0].Name != "idle-sub" {
		t.Fatalf("unexpected subscriber name: %q", state.Subscribers[0].Name)
	}
	if len(state.Subscribers[0].Topics) != 0 {
		t.Fatalf("expected empty topics, got %v", state.Subscribers[0].Topics)
	}
}

func TestAPI_GetState_SubscribersWithTopics(t *testing.T) {
	r := setupAPITest(t)
	subs = []Subscriber{
		{Name: "analytics-sub", topics: []string{"orders", "payments"}},
		{Name: "logging-sub", topics: []string{"orders"}},
	}

	w := apiReq(t, r, "GET", "/state", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var state stateView
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(state.Subscribers) != 2 {
		t.Fatalf("expected 2 subscribers, got %d", len(state.Subscribers))
	}

	a := state.Subscribers[0]
	if a.Name != "analytics-sub" || len(a.Topics) != 2 || a.Topics[0] != "orders" || a.Topics[1] != "payments" {
		t.Fatalf("unexpected analytics-sub state: %+v", a)
	}
	l := state.Subscribers[1]
	if l.Name != "logging-sub" || len(l.Topics) != 1 || l.Topics[0] != "orders" {
		t.Fatalf("unexpected logging-sub state: %+v", l)
	}
}

func TestAPI_GetState_Full(t *testing.T) {
	r := setupAPITest(t)
	pubs = []Publisher{{Name: "orders-publisher"}, {Name: "payments-publisher"}}
	subs = []Subscriber{
		{Name: "analytics-sub", topics: []string{"orders", "payments"}},
		{Name: "logging-sub", topics: []string{"orders"}},
		{Name: "idle-sub"},
	}

	w := apiReq(t, r, "GET", "/state", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var state stateView
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(state.Publishers) != 2 {
		t.Fatalf("expected 2 publishers, got %d", len(state.Publishers))
	}
	if len(state.Subscribers) != 3 {
		t.Fatalf("expected 3 subscribers, got %d", len(state.Subscribers))
	}
	if len(state.Subscribers[2].Topics) != 0 {
		t.Fatalf("idle-sub should have no topics, got %v", state.Subscribers[2].Topics)
	}
}

// ---- API integration tests (require broker on :8080) ----

func TestAPI_Subscribe_RegistersWithBroker(t *testing.T) {
	requireAPIBroker(t)
	r := setupAPITest(t)

	apiReq(t, r, "POST", "/subscriber", `{"name":"sub1"}`)
	w := apiReq(t, r, "POST", "/topic", `{"name":"reg-topic","subscriber":"sub1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	waitForSubs(t, apiTestBroker, "reg-topic", 1)

	apiTestBroker.mu.RLock()
	n := len(apiTestBroker.subscribers["reg-topic"])
	apiTestBroker.mu.RUnlock()
	if n != 1 {
		t.Fatalf("expected 1 subscriber for 'reg-topic', got %d", n)
	}
}

func TestAPI_Publish_DeliversMessage(t *testing.T) {
	requireAPIBroker(t)
	r := setupAPITest(t)

	apiReq(t, r, "POST", "/publisher", `{"name":"pub1"}`)
	apiReq(t, r, "POST", "/subscriber", `{"name":"sub1"}`)
	apiReq(t, r, "POST", "/topic", `{"name":"delivery-topic","subscriber":"sub1"}`)
	waitForSubs(t, apiTestBroker, "delivery-topic", 1)

	// Open a monitor directly on the broker to capture the published message.
	monitor := sendSubscribe(t, "localhost:8080", "delivery-topic")
	defer monitor.Close()
	waitForSubs(t, apiTestBroker, "delivery-topic", 2)

	w := apiReq(t, r, "POST", "/publish", `{"name":"pub1","topic":"delivery-topic","message":"hello-api"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	got := readWithTimeout(t, monitor, time.Second)
	if got != "hello-api" {
		t.Fatalf("expected 'hello-api', got %q", got)
	}
}

func TestAPI_Unsubscribe_RemovesFromBroker(t *testing.T) {
	requireAPIBroker(t)
	r := setupAPITest(t)

	apiReq(t, r, "POST", "/subscriber", `{"name":"sub1"}`)
	apiReq(t, r, "POST", "/topic", `{"name":"unsub-topic","subscriber":"sub1"}`)
	waitForSubs(t, apiTestBroker, "unsub-topic", 1)

	apiTestBroker.mu.RLock()
	before := len(apiTestBroker.subscribers["unsub-topic"])
	apiTestBroker.mu.RUnlock()

	w := apiReq(t, r, "PUT", "/unsubscribe", `{"name":"unsub-topic","subscriber":"sub1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	waitForSubs(t, apiTestBroker, "unsub-topic", 0)

	apiTestBroker.mu.RLock()
	after := len(apiTestBroker.subscribers["unsub-topic"])
	apiTestBroker.mu.RUnlock()

	if before != 1 {
		t.Fatalf("expected 1 subscriber before unsubscribe, got %d", before)
	}
	if after != 0 {
		t.Fatalf("expected 0 subscribers after unsubscribe, got %d", after)
	}
}

func TestAPI_GetState_ReflectsSubscriptions(t *testing.T) {
	requireAPIBroker(t)
	r := setupAPITest(t)

	apiReq(t, r, "POST", "/publisher", `{"name":"pub1"}`)
	apiReq(t, r, "POST", "/subscriber", `{"name":"sub1"}`)
	apiReq(t, r, "POST", "/subscriber", `{"name":"sub2"}`)
	apiReq(t, r, "POST", "/topic", `{"name":"orders","subscriber":"sub1"}`)
	apiReq(t, r, "POST", "/topic", `{"name":"payments","subscriber":"sub1"}`)
	apiReq(t, r, "POST", "/topic", `{"name":"orders","subscriber":"sub2"}`)
	waitForSubs(t, apiTestBroker, "orders", 2)
	waitForSubs(t, apiTestBroker, "payments", 1)

	w := apiReq(t, r, "GET", "/state", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var state stateView
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if len(state.Publishers) != 1 || state.Publishers[0] != "pub1" {
		t.Fatalf("unexpected publishers: %v", state.Publishers)
	}
	if len(state.Subscribers) != 2 {
		t.Fatalf("expected 2 subscribers, got %d", len(state.Subscribers))
	}

	sub1 := state.Subscribers[0]
	if sub1.Name != "sub1" || len(sub1.Topics) != 2 {
		t.Fatalf("unexpected sub1 state: %+v", sub1)
	}
	sub2 := state.Subscribers[1]
	if sub2.Name != "sub2" || len(sub2.Topics) != 1 || sub2.Topics[0] != "orders" {
		t.Fatalf("unexpected sub2 state: %+v", sub2)
	}
}
