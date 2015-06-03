(function () {
  'use strict';

  var _ = require('lodash');
  var util = require('util');
  var debug = require('debug')('jshs2:CCursor');

  var ExecutionError = require('./Common/HS2Error').ExecutionError;
  var OperationError = require('./Common/HS2Error').OperationError;
  var FetchError = require('./Common/HS2Error').FetchError;
  var IdlContainer = require('./Common/IdlContainer');
  var hs2util = require('./Common/HS2Util');
  var Cursor = require('./Cursor');

  function CCursor (connection) {
    Cursor.call(this, connection);
  }

  util.inherits(CCursor, Cursor);

  CCursor.prototype.cancel = function cancel (callback) {
    var that = this;
    var serviceType = IdlContainer.getServiceType();
    var client = that.getClient();

    var req = new serviceType.TCancelOperationReq({
      operationHandle: that.getOperationHandle()
    });

    client.CancelOperation(req, function (err, res) {
      if (err) {
        callback(new OperationError(err.message || 'cancel fail, unknown error'));
      } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
        callback(new OperationError(res.status.errorMessage || 'cancel fail, unknown error'));
      } else {
        callback(null, true);
      }
    });
  };

  module.exports = CCursor;
})();