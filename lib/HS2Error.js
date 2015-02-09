(function () {
  'use strict';

  var util = require('util');

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

  util.inherits(ConnectionError, Error);
  util.inherits(ExecutionError, Error);
  util.inherits(OperationError, Error);
  util.inherits(FetchError, Error);

  module.exports = {
    ConnectionError: ConnectionError,
    ExecutionError: ExecutionError,
    OperationError: OperationError,
    FetchError: FetchError
  };
})();


