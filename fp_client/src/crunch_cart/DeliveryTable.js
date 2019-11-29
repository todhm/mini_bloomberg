import React from 'react';
import {
  Table,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from '@material-ui/core';

const DeliveryTable = (props) =>{
  const {deliveryData} = props;
  return (
    <Table>
      <TableHead>
        <TableRow>
        <TableCell></TableCell>
        <TableCell>배송방식</TableCell>
        <TableCell>배송가격</TableCell>
        <TableCell></TableCell>
        </TableRow>
      </TableHead>
      <TableBody>
      {deliveryData?Object.keys(deliveryData).map((deliveryKey)=>(
        <TableRow key={deliveryKey}>
          <TableCell></TableCell>
          <TableCell>{deliveryData[deliveryKey]['deliveryData'].goodsDeliveryMethod}</TableCell>
          <TableCell>{deliveryData[deliveryKey]['deliveryData'].goodsDeliveryPrice}</TableCell>
          <TableCell></TableCell>
        </TableRow>
      )):null}
      </TableBody>
    </Table>
  );
}


export default DeliveryTable;
