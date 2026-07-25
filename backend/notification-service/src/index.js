require('dotenv').config();
const express = require('express');
const { createClient } = require('redis');
const { Kafka } = require('kafkajs');

const app = express();
const port = process.env.PORT || 3002;

// Redis setup
const redisClient = createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.log('Redis Client Error', err));

// Kafka setup
const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'notification-service-group' });

async function setup() {
  await redisClient.connect();
  await consumer.connect();
  await consumer.subscribe({ topics: ['order-created', 'order-processed', 'order-failed'], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const payload = JSON.stringify({
        event: topic,
        data: JSON.parse(message.value.toString())
      });
      await redisClient.publish('order-updates', payload);
      console.log(`Published to Redis: ${topic}`);
    },
  });
}

setup().catch(console.error);

app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

app.listen(port, () => {
  console.log(`Notification Service listening at http://localhost:${port}`);
});