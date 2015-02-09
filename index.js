
module.exports.Connection = require('./lib/Connection');
module.exports.Cursor = require('./lib/Cursor');
module.exports.HS2Util = require('./lib/HS2Util');
module.exports.ConnectionError = require('./lib/HS2Error').ConnectionError;
module.exports.ExecutionError = require('./lib/HS2Error').ExecutionError;
module.exports.OperationError = require('./lib/HS2Error').OperationError;
module.exports.FetchError = require('./lib/HS2Error').FetchError;