const kafka = require("kafka-node");

// Kafka broker address
const kafkaServer = "192.168.12.88:9092";

// Kafka topic
const topic = "demo";
let count = 1;

// Consumer
const consumerClient = new kafka.KafkaClient({ kafkaHost: kafkaServer });
const consumer = new kafka.Consumer(consumerClient, [
  { topic: topic, fromBeginning: true },
]);

consumer.on("message", (message) => {
  console.log("[" + count + "]" + " Received message:", message.value);
  count++;
});

consumer.on("error", (err) => {
  console.error("Consumer error:", err);
});

// Handle process termination
process.on("SIGINT", () => {
  consumer.close(true, () => process.exit());
});
