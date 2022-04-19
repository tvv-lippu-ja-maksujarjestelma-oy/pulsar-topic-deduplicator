# pulsar-topic-deduplicator

Receive messages from an Apache Pulsar topic and send the first of each unique message into another topic, in order.

This operation is similar to compacting a topic but instead of keeping the last occurrence, keep the first occurrence.
It is used to remove duplicated messages that were created by e.g. multiple replicas of [mqtt-pulsar-forwarder](https://github.com/tvv-lippu-ja-maksujarjestelma-oy/mqtt-pulsar-forwarder).
