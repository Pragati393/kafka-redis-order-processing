import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

const OrderChart = ({ orders }) => {
  // Process orders data for the chart
  const processChartData = () => {
    const productQuantities = orders.reduce((acc, order) => {
      if (!acc[order.product]) {
        acc[order.product] = 0;
      }
      acc[order.product] += order.quantity;
      return acc;
    }, {});

    return Object.entries(productQuantities).map(([product, quantity]) => ({
      product,
      quantity,
    }));
  };

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={processChartData()}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="product" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Bar dataKey="quantity" fill="#8884d8" name="Quantity Ordered" />
      </BarChart>
    </ResponsiveContainer>
  );
};

export default OrderChart; 