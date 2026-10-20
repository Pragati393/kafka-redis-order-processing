require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const { Kafka, Partitioners } = require('kafkajs');
const logger = require('./utils/logger');
const Order = require('./models/Order');

const app = express();
const port = process.env.PORT || 3005;

// MongoDB connection
mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/order-management', {
  useNewUrlParser: true,
  useUnifiedTopology: true
}).then(() => {
  logger.info('Order Service connected to MongoDB');
}).catch((error) => {
  logger.error('MongoDB connection error:', error);
  process.exit(1);
});

// Kafka setup
const kafka = new Kafka({
  clientId: 'order-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});
const consumer = kafka.consumer({ groupId: 'order-service-group' });

const PROCESSING_DELAY_MS = parseInt(process.env.PROCESSING_DELAY_MS || '2000', 10);
// Probability (0-1) that a simulated order fails, so the FAILED path is visible in demos.
const FAILURE_RATE = parseFloat(process.env.FAILURE_RATE || '0.1');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Kafka delivers at-least-once, so the SAME message can arrive twice.
// Idempotency rule: an order may only move PENDING -> PROCESSED/FAILED ONCE.
// We enforce it with a conditional update ({ status: 'PENDING' }), which MongoDB
// applies atomically. Only the update that wins publishes the result event.
async function processOrder(orderData) {
  const { orderId } = orderData;
  try {
    logger.info('Processing order:', { orderId });

    const order = await Order.findOne({ orderId });
    if (!order) {
      // The gateway saves the order BEFORE publishing the event, so this should not happen.
      logger.warn('Order not found, ignoring event:', { orderId });
      return;
    }

    // Quick check to avoid wasted work on an obvious duplicate.
    if (order.status !== 'PENDING') {
      logger.info('Order already handled, skipping duplicate:', { orderId, status: order.status });
      return;
    }

    // Simulate order processing
    await sleep(PROCESSING_DELAY_MS);

    if (Math.random() < FAILURE_RATE) {
      throw new Error('Simulated processing failure (payment declined)');
    }

    // Atomic state change: matches ONLY if the order is still PENDING.
    const updatedOrder = await Order.findOneAndUpdate(
      { orderId, status: 'PENDING' },
      { status: 'PROCESSED', processedAt: new Date() },
      { new: true }
    );

    if (!updatedOrder) {
      // Someone else already finished this order, so do not publish a second event.
      logger.info('Lost the race, order already handled:', { orderId });
      return;
    }

    await producer.send({
      topic: 'order-processed',
      messages: [{
        key: orderId,
        value: JSON.stringify({ ...orderData, status: 'PROCESSED', processedAt: updatedOrder.processedAt.toISOString() })
      }]
    });
    logger.info('Order processed and event sent:', { orderId, topic: 'order-processed' });
  } catch (error) {
    logger.error('Error processing order:', { orderId, error: error.message });

    try {
      // Same guard: only a PENDING order can become FAILED.
      const failedOrder = await Order.findOneAndUpdate(
        { orderId, status: 'PENDING' },
        { status: 'FAILED', failedAt: new Date(), error: error.message },
        { new: true }
      );

      if (!failedOrder) {
        logger.info('Order not PENDING, no failure event sent:', { orderId });
        return;
      }

      await producer.send({
        topic: 'order-failed',
        messages: [{
          key: orderId,
          value: JSON.stringify({ ...orderData, status: 'FAILED', error: error.message, failedAt: failedOrder.failedAt.toISOString() })
        }]
      });
      logger.info('Order failed event sent:', { orderId, topic: 'order-failed' });
    } catch (updateError) {
      logger.error('Error recording failed order:', { orderId, error: updateError.message });
    }
  }
}

async function setupKafka() {
  try {
    await producer.connect();
    await consumer.connect();
    
    logger.info('Connected to Kafka');

    // Subscribe to order-created topic
    await consumer.subscribe({ 
      topic: 'order-created',
      fromBeginning: false
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const orderData = JSON.parse(message.value.toString());
          logger.info('Received order for processing:', {
            orderId: orderData.orderId,
            topic,
            partition
          });
          
          await processOrder(orderData);
        } catch (error) {
          logger.error('Error processing message:', {
            error: error.message,
            topic,
            partition
          });
        }
      },
    });

    logger.info('Kafka consumer is running');
  } catch (error) {
    logger.error('Error setting up Kafka:', error);
    process.exit(1);
  }
}

// Express middleware
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK' });
});

// Start server
app.listen(port, () => {
  logger.info(`Order Service listening at http://localhost:${port}`);
  setupKafka().catch(error => {
    logger.error('Failed to setup Kafka:', error);
    process.exit(1);
  });
}); 