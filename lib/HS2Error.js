var util = require('util');

module.exports = function () {
  function ConnectionError (message) {
    Error.call(this, message);

    this.message = message;
  }

  function ExecutionError (message) {
    Error.call(this, message);

    this.message = message;
  }

  function FetchError (message) {
    Error.call(this, message);

    this.message = message;
  }

  util.inherits(ConnectionError, Error);
  util.inherits(ExecutionError, Error);
  util.inherits(FetchError, Error);

  var HS2Error = {
    ConnectionError: ConnectionError,
    ExecutionError: ExecutionError,
    FetchError: FetchError
  };

  return HS2Error;
};