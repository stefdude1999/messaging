# Stefan Messaging

## What is Stefan Messaging

Simple pub sub service. Allows the creation of arbitrary publishers and subscribers, and assigning topics to any subscribers, and sending messages to these topics. Subscribers can have several topics, and publishers can subscribe to any topics. Runs entirely in the API. Can print out the overall structure of the system, and unsubscribe.

## How to run

The steps below are if you want to set up the repo using SSL and swagger. If you don't want to use swagger, skip to the section below for directly using curl. 

### 1. Install cfssl

```bash
go install github.com/cloudflare/cfssl/cmd/cfssl@latest
go install github.com/cloudflare/cfssl/cmd/cfssljson@latest
```

Make sure `$(go env GOPATH)/bin` is in your PATH:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

### 2. Generate certificates

```bash
# Generate the CA
cat > ca-csr.json <<EOF
{
  "CN": "My CA",
  "key": { "algo": "rsa", "size": 2048 },
  "names": [{ "C": "US", "ST": "CA", "L": "San Francisco", "O": "My Org" }]
}
EOF
cfssl gencert -initca ca-csr.json | cfssljson -bare ca

# Generate the server cert (trusted for localhost)
mkdir -p cert
echo '{"CN":"localhost","hosts":["localhost","127.0.0.1"],"key":{"algo":"rsa","size":2048}}' > server-csr.json
cfssl gencert -ca=ca.pem -ca-key=ca-key.pem server-csr.json | cfssljson -bare cert/server
```

### 3. Trust the CA (macOS)

```bash
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain ca.pem
```

Restart your browser after running this.

### 4. Run

```bash
go run .
```

To test, run `go test -race`

The API is available at https://localhost:9001/swagger/index.html

If you want to use curl, here's some sample curl commands

Create a publisher:

```base
curl -X 'POST' \
  'https://localhost:9001/api/v1/messaging/publisher' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "pub1"
}'
```

Create a subscriber:

```base
curl -X 'POST' \
  'https://localhost:9001/api/v1/messaging/subscriber' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "sub1"
}'
```

Create a topic:

```base
curl -X 'POST' \
  'https://localhost:9001/api/v1/messaging/topic' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "topic1",
  "subscriber": "sub1"
}'
```

Get the state:

```base
curl -X 'GET' \
  'https://localhost:9001/api/v1/messaging/state' \
  -H 'accept: application/json'
```

Publish a message:

```base
curl -X 'POST' \
  'https://localhost:9001/api/v1/messaging/publish' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "message": "test",
  "name": "pub1",
  "topic": "topic1"
}'
```

Unsubscribe from topic:

```base
curl -X 'PUT' \
  'https://localhost:9001/api/v1/messaging/unsubscribe' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "topic1",
  "subscriber": "sub1"
}'
```

## What's Next, In Order I'd Like To Make The Changes

- ~~Take a subscriber with a list of topics, and unsubscribe from any topic~~
- ~~"Print out" structure with a numbered input, so list publishers, and list subscribers as well as the topics each subscriber is assigned to~~
- ~~Currently runs in a big nasty for loop with various substeps that check previous input before asking for further input, which can get quite complicated as the project grows and more features are added. I would like to make it more like an API server, where the user can make POST/GET/UPDATE requests to add pubs, subs, topics, etc. Inspired by Google's [pub/sub APIs](https://docs.cloud.google.com/pubsub/docs/reference/rest?rep_location=global)~~
- Use wildcards when publishing messages. Right now, you have to manually type out the topic you wish to publish to. I'd like to have something like `*` which publishes to every available topic, and then like `orders.*` which would publish to everything that has the suffix of "orders", and then even something like `a.*.b`, etc
- Creating a visual interface using React, where you can visually create new pubs/subs, and then make API calls to update the structure of the Pub/Sub accordingly
- ~Add a "swagger" equivalent to the API~
- Dockerize everything
- Remove ability to create duplicate resources (enforce name uniqueness)

### Advanced Features

- At least once delivery
- Offset tracking
- Consumer acknowledgement
- Write ahead queue

### Super Advanced Features (Way Down The Line)

- Saga coordination on top
  - Saga registry
  - Correlation based ID routing
  - Timeout handling
  - Compensation transaction support

AI generated improvement ideas below:

### Code Quality

- ~Protect global `subs` and `pubs` slices with a mutex — currently unguarded against concurrent API requests~
- Return proper error responses from API handlers on JSON bind failures (currently return silently with no response)
- Replace `fmt.Println` logging with a structured logger (e.g. `log/slog`) with severity levels
- Fix silently ignored `json.Marshal` errors in publisher
- Increase or dynamically size TCP read buffers — currently fixed at 1024 bytes, silently truncates larger messages
- Make ports configurable via environment variables instead of hardcoded values

### Security

- Add authentication (JWT or API keys) — currently any client can publish/subscribe freely
- Add TLS for TCP broker connections — all traffic is currently plaintext
- Add input validation and request size limits on API endpoints
- Add rate limiting per publisher

### Production Readiness

- Add health check endpoint (`GET /health`)
- Add graceful shutdown with OS signal handling
- Add metrics and observability (e.g. Prometheus)
- Add `context.Context` propagation and timeouts on network operations
- Add API versioning (e.g. `/v1/...`)