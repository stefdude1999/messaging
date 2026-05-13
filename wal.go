package main

//minimal things we need in our write ahead log
// the way kafka does things:
// - data is written to the end of the last segment
// old segments are rolled over based on size or time
// kafka maintains index files for efficient
// looks something like:
// { "type": "deposit", "amount": 100, "user_id": 42 }
// { "type": "withdraw", "amount": 50, "user_id": 42 }

// how kafka works: producers have three acks
// - fire and forget mode
// - ack once written to WAL
// - most durable - broker waits until all consumers have written to THEIR WALs
