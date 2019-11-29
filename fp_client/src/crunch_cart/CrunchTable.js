import React, {Component, Fragment} from 'react';
import {connect} from 'react-redux'
import * as cartAction from './action'
import {withStyles} from '@material-ui/core/styles';
import ArrowUpwardIcon from '@material-ui/icons/ArrowUpward';
import ArrowDownwardIcon from '@material-ui/icons/ArrowDownward';
import {styles,tempStype} from '../utils/styles';
import {getMoneyString} from '../utils/stringutils';
import ConfirmModal from '../utils/ConfirmModal';
import {
    Table,
    TableBody,
    TableHead,
    TableCell,
    TableRow,
    Checkbox,
    Card,
    Typography,
    Grid,
    Button,
    CardMedia
  } from '@material-ui/core';




const  CrunchTable=(props)=>{

    const{
        CartReducer,classes,checkList,
        showModal,handleCheck,deleteCart,
        handleAllCheck,handleModalOpen
    } = props;
    return (
        <Table>
            <TableHead>
                <TableRow>
                    <TableCell><Checkbox checked={checkList.length===CartReducer.length} onClick={(e)=>handleAllCheck()} /></TableCell>
                    <TableCell>전체선택({checkList.length}/{CartReducer.length})</TableCell>
                    <TableCell>상품정보</TableCell>
                    <TableCell>수량</TableCell>
                    <TableCell>상품금액</TableCell>
                </TableRow>
            </TableHead>
            <TableBody>
                {CartReducer.map((cartData)=>
                <Fragment key={cartData.sno+cartData.goodsNo}>
                    <TableRow>
                        <TableCell> <Checkbox onClick={(e)=>handleCheck(cartData.sno)} checked={checkList.includes(cartData.sno)} /></TableCell>
                        <TableCell>
                        <Card raised={true} >
                                <CardMedia
                                style={tempStype}
                                    image={cartData.goodsImage}
                                    title={cartData.goodsNm}
                                />
                        </Card>
                        </TableCell>
                        <TableCell>
                            <Typography className={classes.productField} variant="h5" component="h5">
                                {cartData.goodsNm}
                            </Typography>
                            <Typography className={classes.pos} color="textSecondary">
                                {getMoneyString(cartData.price.goodsPriceSum)}
                            </Typography>
                        </TableCell>
                        <TableCell>
                            <Grid item={true}  sm={12} style={{ display: 'inline-flex' }}>
                                <Button onClick={(e)=>   props.changeCount(cartData.sno,1)}  >
                                    <ArrowUpwardIcon/>
                                </Button>
                                <Typography className={classes.pos} color="textSecondary">
                                    {cartData.goodsCnt}
                                </Typography>
                                <Button onClick={(e)=>   props.changeCount(cartData.sno,-1)}>
                                    <ArrowDownwardIcon/>
                                </Button>
                            </Grid>
                        </TableCell>
                        <TableCell>{getMoneyString(cartData.price.goodsPriceSum)}</TableCell>
                    </TableRow>
                </Fragment>
                )}
                <TableRow>
                    <TableCell><Checkbox checked={checkList.length===CartReducer.length} onClick={(e)=>handleAllCheck()} /></TableCell>
                    <TableCell>전체선택({checkList.length}/{CartReducer.length})</TableCell>
                    <TableCell>
                            <Card>
                                <Button onClick={handleModalOpen}>
                                    <Typography>
                                        선택삭제
                                    </Typography>
                                </Button>
                            </Card>
                    </TableCell>
                </TableRow>
                <ConfirmModal
                    open={showModal}
                    handleConfirm={(e)=>deleteCart(checkList)}
                    handleClose={handleModalOpen}
                    confirmMessage={"선택한 상품을 장바구니에서 삭제하시겠습니까?"}
                />

            </TableBody>
        </Table>
        )
}
    


const mapStateToProps = ({CartReducer}) => {
    return {
        CartReducer
    }
}

const mapDispatchToProps = (dispatch) => {
    return {
        changeCount: (sno,cnt) => dispatch(cartAction.changeCount(sno,cnt)),
        deleteCart: (checkList) =>{
            checkList.map((sno)=>{
                dispatch(cartAction.deleteCart(sno))
            })
            
        }

    }
}

export default withStyles(styles)(connect(mapStateToProps, mapDispatchToProps)(CrunchTable))