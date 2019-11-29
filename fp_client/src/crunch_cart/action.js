import * as cartAction from './actionTypes'


export const fetchCart=(data)=>({
    type:cartAction.GET_CART,
    data:data
})


export const changeCount=(sno,changeCount)=>({
    type:cartAction.CHANGE_COUNT,
    sno:sno,
    changeCount,changeCount
})


export const deleteCart=(sno)=>({
    type: cartAction.DELETE_CART,
    sno,
})