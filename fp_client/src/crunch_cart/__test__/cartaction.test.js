import  * as cartAction from '../action';
import  * as cartActionType from '../actionTypes';

import CartReducer from '../../reducers/cartreducer'


describe('Cart Scenarios', () => {
  describe('actions', () => {
    it('should create actions', () => {

      const expectedActions =[
        { type: cartActionType.CHANGE_COUNT,sno:"1", changeCount:1},
        { type: cartActionType.DELETE_CART,sno:"1" },
      ];
      const actions = [
        cartAction.changeCount("1",1),
        cartAction.deleteCart("1"),
      ];
      expect(actions).toEqual(expectedActions);
    });
  });

  describe('reducer', () => {
    let state;
    beforeEach(()=>{
        state = CartReducer(undefined,{})
    })
    it("change count to 2 make change well",()=>{
        state = CartReducer(state,cartAction.changeCount(state[1].sno,1))
        expect(state[1].goodsCnt).toEqual(2)
        expect(parseInt(state[1].price.goodsPrice)).toEqual(parseInt(state[1].goodsUnitPrice2));
        expect(state[1].cartUpdated).toEqual(true);
    })
    it("delete product make change well",()=>{
        state = CartReducer(state,cartAction.deleteCart(state[1].sno))
        expect(state[1].goodsCnt).toEqual(undefined)
        expect(state[1].cartDeleted).toEqual(true);
        
    })
  });
});