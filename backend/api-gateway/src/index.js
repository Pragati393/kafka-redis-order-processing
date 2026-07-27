require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { createServer } = require('http');
const { Server } = require('socket.io');
const { Kafka, Partitioners } = require('kafkajs');
const { createClient } = require('redis');
const logger = require('./utils/logger');
const connectDB = require('./config/db');
const Order = require('./models/Order');
const mongoose = require('mongoose');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: true,
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

// Request logging middleware
app.use((req, res, next) => {
  logger.info(`Incoming ${req.method} request to ${req.url}`, {
    method: req.method,
    url: req.url,
    query: req.query,
    ip: req.ip
  });
  next();
});

// Kafka configuration
const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

// Redis subscriber setup (for real-time WebSocket broadcast)
const redisSub = createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379'
});
redisSub.on('error', (err) => logger.error('Redis Sub Error:', { error: err.message }));

// Redis client for caching order reads (cache-aside pattern)
const redisCache = createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379'
});
redisCache.on('error', (err) => logger.error('Redis Cache Error:', { error: err.message }));
redisCache.connect().catch((err) => logger.error('Failed to connect Redis cache client:', { error: err.message }));

const CACHE_TTL_SECONDS = 10;

const topicToEventMap = {
  'order-created': 'orderCreated',
  'order-processed': 'orderProcessed',
  'order-failed': 'orderFailed'
};

async function setupRedisSub() {
  await redisSub.connect();
  await redisSub.subscribe('order-updates', (message) => {
    const parsed = JSON.parse(message);
    const eventName = topicToEventMap[parsed.event] || parsed.event;
    logger.info('Received Redis update, broadcasting via WebSocket:', { event: eventName });
    io.emit(eventName, parsed.data);
  });
  logger.info('✅ Subscribed to Redis channel: order-updates');
}

setupRedisSub().catch((err) => logger.error('Failed to set up Redis subscriber:', { error: err.message }));

// Connect to MongoDB
connectDB();

// Connect to Kafka
const connectProducer = async () => {
  try {
    await producer.connect();
    logger.info('✅ Connected to Kafka');
  } catch (error) {
    logger.error('❌ Failed to connect to Kafka:', { error: error.message, stack: error.stack });
  }
};

connectProducer();

// Socket.IO connection handling
io.on('connection', (socket) => {
  logger.info(`Client connected: ${socket.id}`);

  socket.on('disconnect', () => {
    logger.info(`Client disconnected: ${socket.id}`);
  });

  socket.on('error', (error) => {
    logger.error('Socket error:', { socketId: socket.id, error: error.message });
  });
});

// Routes
app.get("/health", (req, res) => {
  logger.info('Health check endpoint called');
  res.status(200).json({ status: "OK" });
});

// Get orders with optional status filter (cache-aside pattern)
app.get('/api/orders', async (req, res) => {
  try {
    const { status } = req.query;
    const statusKey = status ? status.toUpperCase() : 'ALL';
    const cacheKey = `orders:${statusKey}`;

    // Try cache first
    const cached = await redisCache.get(cacheKey);
    if (cached) {
      logger.info('Orders served from cache:', { status: statusKey });
      return res.json({
        success: true,
        orders: JSON.parse(cached),
        fromCache: true
      });
    }

    // Cache miss - query MongoDB
    const query = status ? { status: statusKey } : {};

    logger.info('Cache miss, fetching orders from MongoDB:', {
      status: statusKey,
      query
    });

    const orders = await Order.find(query)
      .sort({ createdAt: -1 })
      .limit(100);

    logger.info('Orders fetched successfully:', {
      count: orders.length,
      status: statusKey
    });

    // Store in cache for next time
    await redisCache.set(cacheKey, JSON.stringify(orders), { EX: CACHE_TTL_SECONDS });

    res.json({
      success: true,
      orders,
      fromCache: false
    });
  } catch (error) {
    logger.error('Error fetching orders:', {
      error: error.message,
      stack: error.stack
    });
    res.status(500).json({
      success: false,
      message: 'Error fetching orders',
      error: error.message
    });
  }
});

app.post('/api/orders', async (req, res) => {
  try {
    const { userId, product, quantity } = req.body;
    const orderId = `ORD-${Date.now()}`;

    logger.info('Creating new order:', {
      orderId,
      userId,
      product,
      quantity,
      timestamp: new Date().toISOString()
    });

    // Create order in MongoDB
    const order = new Order({
      orderId,
      userId,
      product,
      quantity,
      status: 'PENDING'
    });

    logger.debug('Order model created:', {
      order: order.toJSON(),
      collection: order.collection.name,
      modelName: order.constructor.modelName
    });

    const savedOrder = await order.save();
    logger.info('Order saved to MongoDB:', {
      orderId: savedOrder.orderId,
      id: savedOrder._id,
      status: savedOrder.status,
      collection: savedOrder.collection.name,
      modelName: savedOrder.constructor.modelName
    });

    // Send to Kafka
    const kafkaMessage = {
      orderId,
      userId,
      product,
      quantity,
      timestamp: Date.now()
    };

    logger.debug('Sending order to Kafka:', {
      topic: 'order-created',
      message: kafkaMessage
    });

    await producer.send({
      topic: 'order-created',
      messages: [
        {
          key: orderId,
          value: JSON.stringify(kafkaMessage)
        }
      ]
    });

    logger.info('Order sent to Kafka:', {
      orderId,
      topic: 'order-created'
    });

    io.emit('orderCreated', savedOrder);
    logger.debug('Socket.IO event emitted:', {
      event: 'orderCreated',
      order: savedOrder.toJSON()
    });

    // Invalidate caches since a new order was added
    await redisCache.del('orders:ALL');
    await redisCache.del('orders:PENDING');
    logger.info('Invalidated order caches after new order creation');

    res.status(201).json({
      success: true,
      message: 'Order created successfully',
      order: savedOrder
    });
  } catch (error) {
    logger.error('Error creating order:', {
      error: error.message,
      stack: error.stack,
      code: error.code,
      body: req.body
    });
    res.status(500).json({
      success: false,
      message: 'Error creating order',
      error: error.message
    });
  }
});

const startServer = async () => {
  try {
    await producer.connect();
    logger.info('Connected to Kafka');

    httpServer.listen(PORT, () => {
      logger.info(`API Gateway running on port ${PORT}`);
    });
  } catch (error) {
    logger.error('Error starting server:', error);
    process.exit(1);
  }
};

// Graceful shutdown
process.on('SIGTERM', async () => {
  try {
    await producer.disconnect();
    await redisSub.disconnect();
    await redisCache.disconnect();
    await mongoose.connection.close();
    logger.info('Gracefully shutting down API Gateway');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown:', error);
    process.exit(1);
  }
});

startServer();