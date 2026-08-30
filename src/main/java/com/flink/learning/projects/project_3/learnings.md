## Tumbling Windows

* ValueState asks: "What is the current state for this user?"
* Tumbling Window asks: "What happened during a fixed time interval?"

Think of it like a CCTV recording. Instead of remembering a value forever, Flink collects events into fixed-size boxes of time, processes each box once, and then closes it.

### What is a Tumbling Window?
A Tumbling Window divides time into equal, non-overlapping intervals.


* Every tumbling window starts the count from the begining not from the last end of prvious window. 

### How does it work?

* keyBy() is needed to split the stream by a key (for example, userId). Without it, Flink treats all events as belonging to the same window and aggregates them together.
* A Tumbling Window collects events for a fixed time interval (for example, 5 seconds).
* When the window ends, Flink calculates the result (count, sum, etc.) and emits it downstream, such as to a Kafka topic.
* After emitting the result, Flink clears the state for that completed window and starts a new window for the next time interval.


### Linkedin Post: https://lnkd.in/p/dpMgEjYB