const mongoose = require('mongoose');

const orderSchema = new mongoose.Schema({
  orderId: {
    type: String,
    required: true,
    unique: true
  },
  userId: {
    type: Number,
    required: true
  },
  product: {
    type: String,
    required: true
  },
  quantity: {
    type: Number,
    required: true,
    min: 1
  },
  status: {
    type: String,
    required: true,
    enum: ['PENDING', 'PROCESSED', 'FAILED'],
    default: 'PENDING'
  },
  // Outbox flag: false until the 'order-created' event has reached Kafka.
  // It lives in the same document as the order, so "order saved" and
  // "event still to be published" are written together in ONE atomic write.
  eventPublished: {
    type: Boolean,
    default: false
  },
  createdAt: {
    type: Date,
    default: Date.now
  }
}, {
  timestamps: true
});

// Add indexes for common queries
orderSchema.index({ userId: 1 });
orderSchema.index({ status: 1 });
orderSchema.index({ createdAt: -1 });
orderSchema.index({ eventPublished: 1, createdAt: 1 }); // used by the outbox relay

const Order = mongoose.model('Order', orderSchema);

module.exports = Order; 