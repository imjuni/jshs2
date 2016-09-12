'use strict';

var util = require('util');

function ConfigureError (message) {
  Error.call(this, message);
  this.message = message;
}

function CursorError (message) {
  Error.call(this, message);
  this.message = message;
}

function ConnectionError (message) {
  Error.call(this, message);
  this.message = message;
}

function ExecutionError (message) {
  Error.call(this, message);
  this.message = message;
}

function OperationError (message) {
  Error.call(this, message);
  this.message = message;
}

function FetchError (message) {
  Error.call(this, message);
  this.message = message;
}

function FactoryError (message) {
  Error.call(this, message);
  this.message = message;
}

util.inherits(ConfigureError, Error);
util.inherits(CursorError, Error);

util.inherits(ConnectionError, Error);
util.inherits(ExecutionError, Error);
util.inherits(OperationError, Error);
util.inherits(FetchError, Error);
util.inherits(FactoryError, Error);

module.exports = {
  ConfigureError: ConfigureError,
  CursorError: CursorError,
  ConnectionError: ConnectionError,
  ExecutionError: ExecutionError,
  OperationError: OperationError,
  FetchError: FetchError,
  FactoryError: FactoryError
};
