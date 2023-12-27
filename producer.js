const kafka = require("kafka-node");

// Kafka broker address
const kafkaServer = "192.168.12.88:9092";

// Kafka topic
const topic = "demo";
const messageContent = "Usman Bhaiiiiiii! time py jany diya krien";

// Number of times to send the message
const numberOfMessages = 100000;

// Producer
const producerClient = new kafka.KafkaClient({
  kafkaHost: kafkaServer,
  requestTimeout: 90000,
});
const producer = new kafka.Producer(producerClient);

producer.on("ready", () => {
  // Loop to send the message multiple times
  for (let i = 1; i <= numberOfMessages; i++) {
    const message = {
      topic: topic,
      messages: [messageContent],
    };

    producer.send([message], (err, data) => {
      if (err) {
        console.error("Error producing message:", err.message);
        console.error("Error stack trace:", err.stack);
        console.error("Failed message content:", messageContent);
        // Close the producer after handling the error
        producer.close(() => process.exit(1));
      } else {
        console.log(
          "[" + i + "]" + " Message produced successfully:",
          messageContent
        );
        // Close the producer after sending the required number of messages
        if (i > numberOfMessages) {
          producer.close(() => process.exit());
        }
      }
    });
  }
});

producer.on("error", (err) => {
  console.error("Producer error:", err);
  // Close the producer after handling the error
  producer.close(() => process.exit(1));
});
