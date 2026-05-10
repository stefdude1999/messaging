# Stefan Messaging

## What is Stefan Messaging

Simple pub sub service. Allows the creation of arbitrary publishers and subscribers, and assigning topics to any subscribers, and sending messages to these topics. Subscribers can have several topics, and publishers can subscribe to any topics. Runs entirely in the CLI, and the user goes down a "tree" essentially of selecting next steps before an action is undertaken. Unsubscribe given a topic.

## How to run

Clone the repo, and then run "go run .", and follow the instructions in the prompts. To test, run "go test -race"

## What's Next, In Order I'd Like To Make The Changes
~- Take a subscriber with a list of topics, and unsubscribe from any topic~
- "Print out" structure with a numbered input, so list publishers, and list subscribers as well as the topics each subscriber is assigned to
- Use wildcards when publishing messages. Right now, you have to manually type out the topic you wish to publish to. I'd like to have something like * which publishes to every available topic, and then like orders.* which would publish to everything that has the suffix of "orders", and then even something like a.*.b, etc 
- Currently runs in a big nasty for loop with various substeps that check previous input before asking for further input, which can get quite complicated as the project grows and more features are added. I would like to make it more like an API server, where the user can make POST/GET/UPDATE requests to add pubs, subs, topics, etc. Inspired by Google's [pub/sub APIs](https://docs.cloud.google.com/pubsub/docs/reference/rest?rep_location=global)
- Creating a visual interface using react, where you can visually create new pubs/subs in React or something, and then make API calls to update the structure of the Pub/Sub accordingly 
- Some more advanced features
  - At least once delivery
  - offset tracking
  - consumer acknowledgement
  - Write ahead queue
- Super advanced features way down the line
  - Saga coordination on top
    - Saga registry
    - correlation based ID routing
    - timeout handling 
    - compensation transaction support