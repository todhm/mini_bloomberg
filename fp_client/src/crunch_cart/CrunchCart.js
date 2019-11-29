import React, {Component, Fragment} from 'react';
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {withStyles} from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import RemoveCircleOutlineIcon from '@material-ui/icons/RemoveCircleOutline';
import AddCircleOutlineIcon from '@material-ui/icons/AddCircleOutline';
import CrunchTable from './CrunchTable';
import {styles} from '../utils/styles';
import {getMoneyString} from '../utils/stringutils';
import CartTotalPart from './CartTotalPart';

import {
    Grid,
  } from '@material-ui/core';

class CrunchCart extends Component{
    static contextTypes = {
        router: PropTypes.object
    }
    state={
        checkList:[],
        showModal:false,
    }
    handleModalOpen=(e)=>{
        this.setState({showModal: !this.state.showModal})
    }
    componentDidMount=()=>{
        const{CartReducer} = this.props;
        const checkList = CartReducer.map((cd)=>{
            return cd.sno
        })
        this.setState({checkList:[...checkList]});

    }

    handleAllCheck=()=>{
        const {CartReducer} = this.props;
        this.state.checkList.length==CartReducer.length?
            this.setState({checkList:[]})
            :this.setState({checkList:[
                ...CartReducer.map((cd)=>cd.sno)]})

    }

    
    handleCheck=(sno)=>{
        const checkList = this.state.checkList;
        checkList.includes(sno)?
            this.setState({checkList:checkList.filter((c)=>c!=sno)})
            :this.setState({checkList:[...checkList,sno]})

    }

    emptyCheckList=()=>{
        const checkList = this.state.checkList;
        this.setState({checkList:[]});
    }


    render=()=>{
        const {checkList,showModal} = this.state;
        const{CartReducer,classes} = this.props;
        let totalTempPrice = 0 ;
        let totalDeliveryPrice = 0 ; 
        let totalDiscountPrice =0 ; 
        let totalFinalPrice =0 ; 
        let totalGoodsPrice = 0;
        CartReducer.forEach((cd)=>{
            if(checkList.includes(cd.sno)){
                totalTempPrice += cd.price.goodsPriceSum;
                totalDeliveryPrice += cd.price.goodsDeliveryPrice;
                totalGoodsPrice += parseInt(cd.price.baseGoodsPrice) * cd.goodsCnt;
            }
        })
        totalDiscountPrice = totalGoodsPrice - totalTempPrice;
        totalFinalPrice = totalTempPrice + totalDeliveryPrice;
        let totalObjectList = [
            {title:"상품금액",sumText:getMoneyString(totalGoodsPrice),additional:"",rightSign:"m"},
            {title:"상품할인금액",sumText:getMoneyString(totalDiscountPrice),additional:"",rightSign:"p"},
            {title:"배송비",sumText:getMoneyString(totalDeliveryPrice),additional:"",rightSign:"e"},
            {title:"결제예정금액",sumText:getMoneyString(totalFinalPrice),additional:"",rightSign:""},
        ]
        return (
        <Fragment>
        <Grid container={true} spacing={16}>
            <Grid item={true} sm={1} xs={1}/>
            <Grid item={true} sm={10} xs={10}>
                <CrunchTable 
                    checkList={checkList}
                    showModal={showModal}
                    handleCheck={this.handleCheck}
                    emptyCheckList={this.emptyCheckList}
                    handleAllCheck={this.handleAllCheck}
                    handleModalOpen={this.handleModalOpen}
                    />

            </Grid>
            <Grid item={true} sm={1} xs={1}/>
        </Grid>
        <Grid container={true} spacing={16}>
            <Grid item={true} sm={1} xs={1}/>
            <Grid item={true} sm={9} xs={9}>
                <Grid container={true}>
                    {totalObjectList.map((obj,idx)=>{
                        return(
                            <Grid key={obj.title+idx.toString()} item={true} sm={3} xs={3}>
                            <Grid container={true}>
                                <Grid item={true} sm={10} xs={10}>
                                    <CartTotalPart
                                        key={obj.title}
                                        title={obj.title}
                                        sumText={obj.sumText}
                                        additional={obj.additional}
                                    />
                                </Grid>
                                <Grid item={true} sm={2} xs={2}>
                                    { (obj.rightSign=='m')&& <RemoveCircleOutlineIcon  className={classes.placeMiddle}/>
                                        ||  (obj.rightSign=='p') && <AddCircleOutlineIcon className={classes.placeMiddle} />
                                        ||  (obj.rightSign=='e') && <Typography className={classes.placeMiddle} component="h2">=</Typography>
                                        || null
                                        }
                                </Grid>
                            </Grid>
                        </Grid>                
                        )
                    })}
                </Grid>
            </Grid>
            <Grid item={true} sm={1} xs={1}/>              
        </Grid>
    </Fragment>);
    }
}


const mapStateToProps = ({CartReducer}) => {
    return {
        CartReducer
    }
}

export default withStyles(styles)(connect(mapStateToProps, null)(CrunchCart))