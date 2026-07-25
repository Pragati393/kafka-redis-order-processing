import React, { useState, useEffect } from 'react';
import { Container, Grid, Paper, Typography, FormControl, InputLabel, Select, MenuItem } from '@mui/material';
import { io } from 'socket.io-client';
import OrderTable from './OrderTable';
import OrderChart from './OrderChart';
import MockOrderForm from '../MockOrderForm/MockOrderForm';
import logger from '../../utils/logger';

const API_URL = 'http://localhost:4000';

const Dashboard = () => {
  const [orders, setOrders] = useState([]);
  const [socket, setSocket] = useState(null);
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [statusFilter, setStatusFilter] = useState('ALL');

  // Fetch orders based on status filter
  const fetchOrders = async () => {
    try {
      logger.info('Fetching orders', { statusFilter });
      const url = statusFilter === 'ALL' 
        ? `${API_URL}/api/orders`
        : `${API_URL}/api/orders?status=${statusFilter}`;
      
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch orders');
      }
      
      const data = await response.json();
      logger.info('Orders fetched successfully', { count: data.orders.length });
      setOrders(data.orders);
    } catch (error) {
      logger.error('Error fetching orders', error);
    }
  };

  // Fetch orders when status filter changes
  useEffect(() => {
    fetchOrders();
  }, [statusFilter]);

  useEffect(() => {
    logger.info('Initializing WebSocket connection');
    
    // Connect to WebSocket server
    const newSocket = io(API_URL, {
      transports: ['websocket', 'polling']
    });

    newSocket.on('connect', () => {
      logger.info('WebSocket connected', { socketId: newSocket.id });
      setConnectionStatus('connected');
    });

    newSocket.on('disconnect', () => {
      logger.warn('WebSocket disconnected', { socketId: newSocket.id });
      setConnectionStatus('disconnected');
    });

    newSocket.on('connect_error', (error) => {
      logger.error('WebSocket connection error', error);
      setConnectionStatus('disconnected');
    });

    newSocket.on('orderCreated', (newOrder) => {
      logger.info('New order received', { orderId: newOrder.orderId });
      if (statusFilter === 'ALL' || statusFilter === newOrder.status) {
        setOrders((prevOrders) => [newOrder, ...prevOrders]);
      }
    });

    newSocket.on('orderProcessed', (processedOrder) => {
      logger.info('Order processed', { orderId: processedOrder.orderId });
      setOrders((prevOrders) => {
        const updatedOrders = prevOrders.map(order => 
          order.orderId === processedOrder.orderId 
            ? { ...order, status: 'PROCESSED', processedAt: processedOrder.processedAt }
            : order
        );
        return statusFilter === 'ALL' || statusFilter === 'PROCESSED'
          ? updatedOrders
          : updatedOrders.filter(order => order.status === statusFilter);
      });
    });

    newSocket.on('orderFailed', (failedOrder) => {
      logger.info('Order failed', { orderId: failedOrder.orderId, error: failedOrder.error });
      setOrders((prevOrders) => {
        const updatedOrders = prevOrders.map(order => 
          order.orderId === failedOrder.orderId 
            ? { ...order, status: 'FAILED', failedAt: failedOrder.failedAt, error: failedOrder.error }
            : order
        );
        return statusFilter === 'ALL' || statusFilter === 'FAILED'
          ? updatedOrders
          : updatedOrders.filter(order => order.status === statusFilter);
      });
    });

    setSocket(newSocket);

    // Cleanup on unmount
    return () => {
      if (newSocket) {
        logger.info('Closing WebSocket connection', { socketId: newSocket.id });
        newSocket.close();
      }
    };
  }, [statusFilter]);

  // Log orders state changes
  useEffect(() => {
    logger.debug('Orders updated', { count: orders.length });
  }, [orders]);

  const handleStatusFilterChange = (event) => {
    setStatusFilter(event.target.value);
  };

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Typography variant="h4" component="h1" gutterBottom>
        Order Dashboard
        <Typography 
          variant="caption" 
          sx={{ 
            ml: 2, 
            color: connectionStatus === 'connected' ? 'success.main' : 'error.main' 
          }}
        >
          ({connectionStatus})
        </Typography>
      </Typography>
      
      <Grid container spacing={3}>
        {/* Mock Order Form */}
        <Grid item xs={12}>
          <MockOrderForm onOrderCreated={fetchOrders} />
        </Grid>

        {/* Status Filter */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Status Filter</InputLabel>
              <Select
                value={statusFilter}
                label="Status Filter"
                onChange={handleStatusFilterChange}
              >
                <MenuItem value="ALL">All Orders</MenuItem>
                <MenuItem value="PENDING">Pending</MenuItem>
                <MenuItem value="PROCESSED">Processed</MenuItem>
                <MenuItem value="FAILED">Failed</MenuItem>
              </Select>
            </FormControl>
          </Paper>
        </Grid>

        {/* Chart */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2, display: 'flex', flexDirection: 'column' }}>
            <Typography variant="h6" gutterBottom>
              Order Statistics
            </Typography>
            <OrderChart orders={orders} />
          </Paper>
        </Grid>
        
        {/* Recent Orders */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2, display: 'flex', flexDirection: 'column' }}>
            <Typography variant="h6" gutterBottom>
              Recent Orders ({orders.length})
            </Typography>
            <OrderTable orders={orders} />
          </Paper>
        </Grid>
      </Grid>
    </Container>
  );
};

export default Dashboard; 