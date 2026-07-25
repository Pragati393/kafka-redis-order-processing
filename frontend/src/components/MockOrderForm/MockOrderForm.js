import React, { useState } from 'react';
import { faker } from '@faker-js/faker';
import {
  Button,
  Card,
  CardContent,
  CardHeader,
  Grid,
  TextField,
  MenuItem,
  Snackbar,
  Alert,
} from '@mui/material';
import logger from '../../utils/logger';

const PRODUCT_LIST = [
  'Laptop',
  'Smartphone',
  'Headphones',
  'Monitor',
  'Keyboard',
  'Mouse',
  'Tablet',
  'Printer'
];

const API_URL = 'http://localhost:4000'; // Updated to use port 4000

const MockOrderForm = ({ onOrderCreated }) => {
  const [order, setOrder] = useState({
    userId: faker.number.int({ min: 1000, max: 9999 }),
    product: PRODUCT_LIST[0],
    quantity: 1,
  });
  const [notification, setNotification] = useState({ open: false, message: '', severity: 'success' });

  const generateRandomOrder = () => {
    const newOrder = {
      userId: faker.number.int({ min: 1000, max: 9999 }),
      product: PRODUCT_LIST[Math.floor(Math.random() * PRODUCT_LIST.length)],
      quantity: faker.number.int({ min: 1, max: 10 }),
    };
    logger.debug('Generated random order', newOrder);
    setOrder(newOrder);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    logger.info('Submitting order', order);

    try {
      const response = await fetch(`${API_URL}/api/orders`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(order),
      });

      if (response.ok) {
        const result = await response.json();
        logger.info('Order created successfully', { orderId: result.orderId });
        
        setNotification({
          open: true,
          message: `Order created successfully! Order ID: ${result.order.orderId}`,
          severity: 'success',
        });
        generateRandomOrder();
        if (onOrderCreated) {
          onOrderCreated();
        }
      } else {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to create order');
      }
    } catch (error) {
      logger.error('Failed to create order', error);
      
      setNotification({
        open: true,
        message: 'Failed to create order: ' + error.message,
        severity: 'error',
      });
    }
  };

  const handleChange = (e) => {
    const updatedOrder = {
      ...order,
      [e.target.name]: e.target.value,
    };
    logger.debug('Order form updated', { 
      field: e.target.name, 
      value: e.target.value,
      order: updatedOrder 
    });
    setOrder(updatedOrder);
  };

  return (
    <Card sx={{ mb: 3 }}>
      <CardHeader title="Generate Mock Order" />
      <CardContent>
        <form onSubmit={handleSubmit}>
          <Grid container spacing={2}>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="User ID"
                name="userId"
                value={order.userId}
                onChange={handleChange}
                type="number"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                select
                label="Product"
                name="product"
                value={order.product}
                onChange={handleChange}
              >
                {PRODUCT_LIST.map((product) => (
                  <MenuItem key={product} value={product}>
                    {product}
                  </MenuItem>
                ))}
              </TextField>
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Quantity"
                name="quantity"
                value={order.quantity}
                onChange={handleChange}
                type="number"
                inputProps={{ min: 1 }}
              />
            </Grid>
            <Grid item xs={12}>
              <Grid container spacing={2}>
                <Grid item>
                  <Button
                    variant="contained"
                    color="primary"
                    type="submit"
                  >
                    Create Order
                  </Button>
                </Grid>
                <Grid item>
                  <Button
                    variant="outlined"
                    onClick={generateRandomOrder}
                  >
                    Generate Random Values
                  </Button>
                </Grid>
              </Grid>
            </Grid>
          </Grid>
        </form>

        <Snackbar
          open={notification.open}
          autoHideDuration={6000}
          onClose={() => setNotification({ ...notification, open: false })}
        >
          <Alert
            onClose={() => setNotification({ ...notification, open: false })}
            severity={notification.severity}
            sx={{ width: '100%' }}
          >
            {notification.message}
          </Alert>
        </Snackbar>
      </CardContent>
    </Card>
  );
};

export default MockOrderForm; 