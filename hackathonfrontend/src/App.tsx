import React from 'react';
import logo from './logo.svg';
import './App.css';
import BLEControl from "./BLEControl";
import Login from "./login"
function App() {
  return (
    <div className="App">
      <div>
    <Login />
    <BLEControl />
      </div>
    </div>
  );
}

export default App;
