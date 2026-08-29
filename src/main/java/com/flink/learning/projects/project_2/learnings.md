## Stateful processing
Since you've already built a Kafka → Flink → Kafka pipeline, Stateful Processing is the next concept that makes Flink truly different from simple stream transformations. So far, your map(), filter(), and print() operations are stateless—each event is processed independently. Stateful processing allows Flink to remember information from previous events.

keyBy()