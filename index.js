
module.exports.Connection = require('./lib/Connection');
module.exports.Cursor = require('./lib/Cursor');
module.exports.Configure = require('./lib/Configure');
module.exports.IdlContainer = require('./lib/IdlContainer');

module.exports.ConnectionError = require('./lib/HS2Error').ConnectionError;
module.exports.ExecutionError = require('./lib/HS2Error').ExecutionError;
module.exports.OperationError = require('./lib/HS2Error').OperationError;
module.exports.FetchError = require('./lib/HS2Error').FetchError;
