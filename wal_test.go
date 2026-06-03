package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := CreateWAL(dir, false)
	if err != nil {
		t.Fatalf("CreateWAL: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, dir
}

func segmentPath(dir string) string {
	return filepath.Join(dir, "segment-0.log")
}

// TestWAL_WriteAppearsInFile verifies that a written message ends up on disk.
func TestWAL_WriteAppearsInFile(t *testing.T) {
	w, dir := newTestWAL(t)

	msg := Message{Command: "PUBLISH", Topic: "orders", Text: "hello", GUID: "guid-1"}
	if err := w.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data, err := os.ReadFile(segmentPath(dir))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var got Message
	if err := json.Unmarshal(data[:len(data)-1], &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.GUID != "guid-1" || got.Text != "hello" {
		t.Fatalf("unexpected message on disk: %+v", got)
	}
}

// TestWAL_WritePopulatesPending verifies the in-memory pending map is updated.
func TestWAL_WritePopulatesPending(t *testing.T) {
	w, _ := newTestWAL(t)

	msg := Message{Command: "PUBLISH", Topic: "orders", Text: "hello", GUID: "guid-1"}
	w.Write(msg)

	w.mu.Lock()
	_, ok := w.pending["guid-1"]
	w.mu.Unlock()

	if !ok {
		t.Fatal("expected guid-1 in pending after Write")
	}
}

// TestWAL_RemoveDeletesFromPending verifies the pending map entry is removed.
func TestWAL_RemoveDeletesFromPending(t *testing.T) {
	w, _ := newTestWAL(t)

	msg := Message{Command: "PUBLISH", Topic: "orders", Text: "hello", GUID: "guid-1"}
	w.Write(msg)

	if err := w.Remove("guid-1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	w.mu.Lock()
	_, ok := w.pending["guid-1"]
	w.mu.Unlock()

	if ok {
		t.Fatal("expected guid-1 to be removed from pending")
	}
}

// TestWAL_RemoveDestroysFileWhenEmpty verifies the file is deleted when the last message is removed.
func TestWAL_RemoveDestroysFileWhenEmpty(t *testing.T) {
	w, dir := newTestWAL(t)

	w.Write(Message{GUID: "guid-1", Topic: "orders"})
	if err := w.Remove("guid-1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if _, err := os.Stat(segmentPath(dir)); !os.IsNotExist(err) {
		t.Fatal("expected WAL file to be deleted after last message removed")
	}
}

// TestWAL_RemoveRewritesFileWithRemainingMessages verifies that after removing one
// message, the other is still present on disk.
func TestWAL_RemoveRewritesFileWithRemainingMessages(t *testing.T) {
	w, dir := newTestWAL(t)

	w.Write(Message{GUID: "guid-1", Topic: "orders", Text: "first"})
	w.Write(Message{GUID: "guid-2", Topic: "orders", Text: "second"})

	if err := w.Remove("guid-1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	data, err := os.ReadFile(segmentPath(dir))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var remaining Message
	if err := json.Unmarshal(data[:len(data)-1], &remaining); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if remaining.GUID != "guid-2" {
		t.Fatalf("expected guid-2 to remain on disk, got %q", remaining.GUID)
	}
}

// TestWAL_RemoveNoopForUnknownGUID verifies no error when removing a GUID not in the WAL.
func TestWAL_RemoveNoopForUnknownGUID(t *testing.T) {
	w, _ := newTestWAL(t)

	if err := w.Remove("does-not-exist"); err != nil {
		t.Fatalf("expected no error removing unknown GUID, got: %v", err)
	}
}

// TestWAL_PendingForTopicFiltersCorrectly verifies only messages for the given topic are returned.
func TestWAL_PendingForTopicFiltersCorrectly(t *testing.T) {
	w, _ := newTestWAL(t)

	w.Write(Message{GUID: "guid-1", Topic: "orders", Text: "order msg"})
	w.Write(Message{GUID: "guid-2", Topic: "payments", Text: "payment msg"})
	w.Write(Message{GUID: "guid-3", Topic: "orders", Text: "another order"})

	msgs := w.PendingForTopic("orders")
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages for 'orders', got %d", len(msgs))
	}
	for _, m := range msgs {
		if m.Topic != "orders" {
			t.Fatalf("expected topic 'orders', got %q", m.Topic)
		}
	}
}

// TestWAL_PendingForTopicEmptyWhenNoneMatch verifies an empty slice is returned for an unmatched topic.
func TestWAL_PendingForTopicEmptyWhenNoneMatch(t *testing.T) {
	w, _ := newTestWAL(t)

	w.Write(Message{GUID: "guid-1", Topic: "orders"})

	msgs := w.PendingForTopic("payments")
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages for 'payments', got %d", len(msgs))
	}
}

// TestWAL_CreateWAL_LoadsExistingMessages verifies that on restart, messages
// written in a previous session are loaded into pending.
func TestWAL_CreateWAL_LoadsExistingMessages(t *testing.T) {
	dir := t.TempDir()

	// First session: write a message and close.
	w1, err := CreateWAL(dir, false)
	if err != nil {
		t.Fatalf("CreateWAL session 1: %v", err)
	}
	w1.Write(Message{GUID: "guid-1", Topic: "orders", Text: "persisted"})
	w1.Close()

	// Second session: open the same directory.
	w2, err := CreateWAL(dir, false)
	if err != nil {
		t.Fatalf("CreateWAL session 2: %v", err)
	}
	defer w2.Close()

	msgs := w2.PendingForTopic("orders")
	if len(msgs) != 1 || msgs[0].GUID != "guid-1" {
		t.Fatalf("expected guid-1 to be loaded on restart, got %v", msgs)
	}
}

// TestWAL_WriteAfterEmpty verifies that writing after the WAL file was destroyed
// (all messages acked) correctly recreates the file.
func TestWAL_WriteAfterEmpty(t *testing.T) {
	w, dir := newTestWAL(t)

	w.Write(Message{GUID: "guid-1", Topic: "orders"})
	w.Remove("guid-1") // destroys file

	// Write again after file was destroyed.
	if err := w.Write(Message{GUID: "guid-2", Topic: "orders", Text: "new"}); err != nil {
		t.Fatalf("Write after empty: %v", err)
	}

	if _, err := os.Stat(segmentPath(dir)); err != nil {
		t.Fatalf("expected WAL file to be recreated, got: %v", err)
	}

	msgs := w.PendingForTopic("orders")
	if len(msgs) != 1 || msgs[0].GUID != "guid-2" {
		t.Fatalf("expected guid-2 in pending after recreate, got %v", msgs)
	}
}

// startTestBrokerWithWAL starts a broker with a WAL on a random port.
func startTestBrokerWithWAL(t *testing.T) (b *Broker, addr string) {
	t.Helper()
	dir := t.TempDir()
	wal, err := CreateWAL(dir, false)
	if err != nil {
		t.Fatalf("CreateWAL: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b = newBroker("test")
	b.wal = wal
	go b.serve(ln)
	t.Cleanup(func() {
		ln.Close()
		wal.Close()
	})
	return b, ln.Addr().String()
}

// TestWAL_ReplayOnSubscriberReconnect verifies that a message written to the WAL
// (but never ACKed because the subscriber went down) is replayed to a new subscriber
// when it reconnects to the same topic.
func TestWAL_ReplayOnSubscriberReconnect(t *testing.T) {
	b, addr := startTestBrokerWithWAL(t)

	// Subscribe and wait for registration.
	sub1 := sendSubscribe(t, addr, "orders")
	waitForSubs(t, b, "orders", 1)

	// Publish a message — it is written to WAL and sent to sub1.
	sendPublish(t, addr, "orders", "important message")
	readWithTimeout(t, sub1, time.Second) // drain so publish is processed

	// Subscriber goes down without sending an ACK.
	sub1.Close()

	// Verify the message is still in the WAL pending map.
	pending := b.wal.PendingForTopic("orders")
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending WAL message after subscriber disconnect, got %d", len(pending))
	}

	// New subscriber connects to the same topic — should receive the replayed message.
	sub2 := sendSubscribe(t, addr, "orders")
	defer sub2.Close()
	waitForSubs(t, b, "orders", 1)

	got := readWithTimeout(t, sub2, time.Second)
	if got != "important message" {
		t.Fatalf("expected replayed message 'important message', got %q", got)
	}
}
