
module.exports.PConnection = require('./lib/PConnection');
module.exports.CConnection = require('./lib/CConnection');
module.exports.PCursor = require('./lib/PCursor');
module.exports.CCursor = require('./lib/CCursor');
module.exports.Configure = require('./lib/Configure');
module.exports.IdlContainer = require('./lib/Common/IdlContainer');

module.exports.ConnectionError = require('./lib/Common/HS2Error').ConnectionError;
module.exports.ExecutionError = require('./lib/Common/HS2Error').ExecutionError;
module.exports.OperationError = require('./lib/Common/HS2Error').OperationError;
module.exports.FetchError = require('./lib/Common/HS2Error').FetchError;
