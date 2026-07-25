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

async function processOrder(orderData) {
  try {
    logger.info('Processing order:', {
      orderId: orderData.orderId,
      userId: orderData.userId,
      product: orderData.product,
      quantity: orderData.quantity
    });

    // First, check if the order exists
    let order = await Order.findOne({ orderId: orderData.orderId });

    
    if (!order) {
      // If order doesn't exist, create it
      order = new Order({
        orderId: orderData.orderId,
        userId: orderData.userId,
        product: orderData.product,
        quantity: orderData.quantity,
        status: 'PENDING'
      });
      
      logger.info('Creating new order in Order Service:', {
        orderId: order.orderId,
        status: order.status
      });
      
      await order.save();
    }
    else{
      logger.info('Order Found:', order.id);
    }
    
    // Simulate order processing
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Update order status in MongoDB
    const updatedOrder = await Order.findOneAndUpdate(
      { orderId: orderData.orderId },
      { 
        status: 'PROCESSED',
        processedAt: new Date()
      },
      { new: true }
    );
    
    if (!updatedOrder) {
      throw new Error(`Order not found: ${orderData.orderId}`);
    }

    logger.info('Order processed successfully:', {
      orderId: updatedOrder.orderId,
      status: updatedOrder.status,
      processedAt: updatedOrder.processedAt
    });

    // Send to processed topic
    const processedMessage = {
      ...orderData,
      status: 'PROCESSED',
      processedAt: new Date().toISOString()
    };

    logger.debug('Sending processed order to Kafka:', {
      topic: 'order-processed',
      message: processedMessage
    });

    await producer.send({
      topic: 'order-processed',
      messages: [
        {
          key: orderData.orderId,
          value: JSON.stringify(processedMessage)
        }
      ]
    });

    logger.info('Order processed event sent to Kafka:', { 
      orderId: orderData.orderId,
      topic: 'order-processed'
    });
  } catch (error) {
    logger.error('Error processing order:', {
      orderId: orderData.orderId,
      error: error.message,
      stack: error.stack
    });

    // Update order status to FAILED in case of error
    try {
      const failedOrder = await Order.findOneAndUpdate(
        { orderId: orderData.orderId },
        { 
          status: 'FAILED',
          failedAt: new Date(),
          error: error.message
        },
        { new: true }
      );

      if (failedOrder) {
        logger.info('Order status updated to FAILED:', {
          orderId: failedOrder.orderId,
          status: failedOrder.status,
          error: failedOrder.error
        });
      }

      const failedMessage = {
        ...orderData,
        status: 'FAILED',
        error: error.message,
        failedAt: new Date().toISOString()
      };

      logger.debug('Sending failed order to Kafka:', {
        topic: 'order-failed',
        message: failedMessage
      });

      await producer.send({
        topic: 'order-failed',
        messages: [
          {
            key: orderData.orderId,
            value: JSON.stringify(failedMessage)
          }
        ]
      });

      logger.info('Order failed event sent to Kafka:', {
        orderId: orderData.orderId,
        topic: 'order-failed'
      });
    } catch (updateError) {
      logger.error('Error updating failed order status:', {
        orderId: orderData.orderId,
        error: updateError.message,
        stack: updateError.stack
      });
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