import React, { Component } from 'react';
import {Route, Router} from 'react-router-dom';
import logo from './logo.svg';
import './App.css';
import {createStore, applyMiddleware, compose} from 'redux';
import {Provider} from 'react-redux';
import reducer from './reducers';
import createBrowserHistory from 'history/createBrowserHistory'
import CrunchCart from './crunch_cart/CrunchCart';

const store = createStore(reducer);
const history = createBrowserHistory()


class App extends Component {
  render() {
    return (
      <div className="App">
        <Provider store={store}>
          <Router history={history}>
            <Route exact={false} path='/order/cart.php' render={() =>< CrunchCart />}/>
          </Router>
        </Provider>
      </div>
    );
  }
}

export default App;
