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
const CORS_ORIGIN = process.env.CORS_ORIGIN || true; // set to your frontend URL in production

const io = new Server(httpServer, {
  cors: {
    origin: CORS_ORIGIN,
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.PORT || 4000;

app.use(cors({ origin: CORS_ORIGIN }));
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
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
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
  url: process.env.REDIS_URL || 'redis://localhost:6379',
  // If Redis is down, fail immediately instead of queueing commands until it
  // comes back. That lets the API fall back to MongoDB right away.
  disableOfflineQueue: true
});
redisCache.on('error', (err) => logger.error('Redis Cache Error:', { error: err.message }));
redisCache.connect().catch((err) => logger.error('Failed to connect Redis cache client:', { error: err.message }));

const CACHE_TTL_SECONDS = 10;
const CACHE_KEYS = ['orders:ALL', 'orders:PENDING', 'orders:PROCESSED', 'orders:FAILED'];

// Clear cached order lists. Never throws: a Redis problem must not break the request.
async function invalidateOrderCaches() {
  try {
    await redisCache.del(CACHE_KEYS);
  } catch (err) {
    logger.warn('Cache invalidation failed:', { error: err.message });
  }
}

// ---------------- Outbox pattern ----------------
// Problem: saving to MongoDB and publishing to Kafka are two separate systems,
// so one can succeed while the other fails (an order saved but never announced).
// Fix: the order is saved with eventPublished=false. We then try to publish.
// Only after Kafka accepts the event do we flip the flag to true. A background
// relay keeps retrying every order whose flag is still false.
const OUTBOX_INTERVAL_MS = parseInt(process.env.OUTBOX_INTERVAL_MS || '5000', 10);
// The relay ignores very recent orders so it doesn't race with the normal request path.
const OUTBOX_GRACE_MS = parseInt(process.env.OUTBOX_GRACE_MS || '5000', 10);

async function publishOrderCreated(order) {
  await producer.send({
    topic: 'order-created',
    messages: [{
      key: order.orderId,
      value: JSON.stringify({
        orderId: order.orderId,
        userId: order.userId,
        product: order.product,
        quantity: order.quantity,
        timestamp: Date.now()
      })
    }]
  });
  // Only mark as published AFTER Kafka accepted the message.
  await Order.updateOne({ _id: order._id }, { $set: { eventPublished: true } });
}

let relayRunning = false;
async function runOutboxRelay() {
  if (relayRunning) return; // never run two relays at once
  relayRunning = true;
  try {
    const cutoff = new Date(Date.now() - OUTBOX_GRACE_MS);
    const unpublished = await Order.find({ eventPublished: false, createdAt: { $lt: cutoff } })
      .sort({ createdAt: 1 })
      .limit(50);

    for (const order of unpublished) {
      try {
        await publishOrderCreated(order);
        logger.info('Outbox relay published event:', { orderId: order.orderId });
      } catch (err) {
        logger.warn('Outbox relay could not publish, will retry later:', { orderId: order.orderId, error: err.message });
        break; // Kafka is probably still down, try again on the next tick
      }
    }
  } catch (err) {
    logger.error('Outbox relay error:', { error: err.message });
  } finally {
    relayRunning = false;
  }
}

const topicToEventMap = {
  'order-created': 'orderCreated',
  'order-processed': 'orderProcessed',
  'order-failed': 'orderFailed'
};

async function setupRedisSub() {
  await redisSub.connect();
  await redisSub.subscribe('order-updates', (message) => {
    const parsed = JSON.parse(message);

    // The gateway already emits 'orderCreated' itself in POST /api/orders,
    // so skip the bridged copy to avoid duplicate events on the client.
    if (parsed.event === 'order-created') return;

    // An order changed status, so any cached list is now stale.
    invalidateOrderCaches();

    const eventName = topicToEventMap[parsed.event] || parsed.event;
    logger.info('Received Redis update, broadcasting via WebSocket:', { event: eventName });
    io.emit(eventName, parsed.data);
  });
  logger.info('✅ Subscribed to Redis channel: order-updates');
}

setupRedisSub().catch((err) => logger.error('Failed to set up Redis subscriber:', { error: err.message }));

// Connect to MongoDB
connectDB();


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

    // Try cache first. If Redis is down this is treated as a cache miss.
    let cached = null;
    try {
      cached = await redisCache.get(cacheKey);
    } catch (cacheError) {
      logger.warn('Redis unavailable, falling back to MongoDB:', { error: cacheError.message });
    }
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

    // Store in cache for next time (best effort: a failure here is not an error)
    try {
      await redisCache.set(cacheKey, JSON.stringify(orders), { EX: CACHE_TTL_SECONDS });
    } catch (cacheError) {
      logger.warn('Could not write to Redis cache:', { error: cacheError.message });
    }

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

    // Outbox: the order is saved with eventPublished=false. Try to publish now;
    // if Kafka is down we still return 201 and the relay retries in the background.
    try {
      await publishOrderCreated(savedOrder);
      savedOrder.eventPublished = true;
      logger.info('Order sent to Kafka:', { orderId, topic: 'order-created' });
    } catch (kafkaError) {
      logger.warn('Kafka publish failed, outbox relay will retry:', {
        orderId,
        error: kafkaError.message
      });
    }

    io.emit('orderCreated', savedOrder);
    logger.debug('Socket.IO event emitted:', {
      event: 'orderCreated',
      order: savedOrder.toJSON()
    });

    // Invalidate caches since a new order was added
    await invalidateOrderCaches();
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

    setInterval(runOutboxRelay, OUTBOX_INTERVAL_MS);
    logger.info(`Outbox relay running every ${OUTBOX_INTERVAL_MS} ms`);
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