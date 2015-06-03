(function () {
  'use strict';

  var _ = require('lodash');
  var util = require('util');
  var debug = require('debug')('jshs2:PCursor');

  var ExecutionError = require('./Common/HS2Error').ExecutionError;
  var OperationError = require('./Common/HS2Error').OperationError;
  var FetchError = require('./Common/HS2Error').FetchError;
  var IdlContainer = require('./Common/IdlContainer');
  var hs2util = require('./Common/HS2Util');
  var Cursor = require('./Cursor');

  function PCursor (connection) {
    Cursor.call(this, connection);
  }

  util.inherits(PCursor, Cursor);

  PCursor.prototype.cancel = function cancel () {
    var that = this;

    return new Promise(function (resolve, reject) {
      var serviceType = IdlContainer.getServiceType();
      var client = that.getClient();

      var req = new serviceType.TCancelOperationReq({
        operationHandle: that.getOperationHandle()
      });

      client.CancelOperation(req, function (err, res) {
        if (err) {
          reject(new OperationError(err.message || 'cancel fail, unknown error'));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          debug('CancelOperation -> Error from HiveServer2\n', res.status.infoMessages);
          debug('CancelOperation -> Error from HiveServer2\n', res.status.errorMessage);

          reject(new OperationError([
            res.status.errorMessage,
            res.status.infoMessages,
            '-- Error caused from HiveServer2'
          ].join('\n\n')));
        } else {
          resolve(true);
        }
      });
    });
  };

  PCursor.prototype.execute = function execute (hql) {
    var that = this;

    return new Promise(function (resolve, reject) {
      var serviceType = IdlContainer.getServiceType();
      var client = that.getClient();

      var req = new serviceType.TExecuteStatementReq({
        sessionHandle: that.getSessionHandle(),
        statement: hql,
        runAsync: true
      });

      debug('ExecuteStatement start -> async, ', hql);

      client.ExecuteStatement(req, function (err, res) {
        if (err) {
          debug('execute statement:Error, statusCode error');
          reject(new ExecutionError(err.message + '\n Error caused from HiveServer2'));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          debug('ExecuteStatement -> Error from HiveServer2\n', res.status.infoMessages);
          debug('ExecuteStatement -> Error from HiveServer2\n', res.status.errorMessage);

          reject(new OperationError([
            res.status.errorMessage,
            res.status.infoMessages,
            '-- Error caused from HiveServer2'
          ].join('\n\n')));
        } else {
          that.setOperationHandle(res.operationHandle);
          that.setHasResultSet(res.operationHandle.hasResultSet);

          resolve({ hasResultSet: that.getHasResultSet() });
        }
      });
    });
  };

  PCursor.prototype.getLog = function getLog () {
    return this[this.getLogFactory()].call(this);
  };


  PCursor.prototype.getLogOnCDH = function getLogOnCDH () {
    var that = this;

    return new Promise(function (resolve, reject) {
      if (!that.getOperationHandle()) {
        return reject(new OperationError('Invalid operationHandle in GetLog, on CDH'));
      }

      var serviceType = IdlContainer.getServiceType();
      var client = that.getClient();

      var req = new serviceType.TGetLogReq({
        operationHandle: that.getOperationHandle()
      });

      client.GetLog(req, function (err, res) {
        if (err) {
          reject(err);
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          debug('GetLog -> Error from HiveServer2\n', res.status.infoMessages);
          debug('GetLog -> Error from HiveServer2\n', res.status.errorMessage);

          reject(new OperationError([
            res.status.errorMessage,
            res.status.infoMessages,
            '-- Error caused from HiveServer2'
          ].join('\n\n')));
        } else {
          resolve(res.log);
        }
      });
    });
  };

  PCursor.prototype.getLogOnHive = function getLogOnCDH () {
    var that = this;

    return new Promise(function (resolve, reject) {
      if (!that.getOperationHandle()) {
        return reject(new OperationError('Invalid operationHandle in GetLog, on Hive'));
      }

      var serviceType = IdlContainer.getServiceType();
      var client = that.getClient();

      var req = new serviceType.TFetchResultsReq({
        operationHandle: that.getOperationHandle(),
        orientation: serviceType.TFetchOrientation.FETCH_NEXT,
        maxRows: that.getConfigure().getMaxRows(),
        fetchType: hs2util.FETCH_TYPE.LOG
      });

      client.FetchResults(req, function (err, res) {
        if (err) {
          reject(err);
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          debug('FetchResults(LOG, type-1) -> Error from HiveServer2\n', res.status.infoMessages);
          debug('FetchResults(LOG, type-1) -> Error from HiveServer2\n', res.status.errorMessage);

          reject(new OperationError([
            res.status.errorMessage,
            res.status.infoMessages,
            '-- Error caused from HiveServer2'
          ].join('\n\n')));
        } else {
          var firstColumn = _(res.results.columns).first();

          if (!firstColumn) {
            resolve('HiveServer2 not response log');
          } else {
            resolve(firstColumn.stringVal.values.join('\n'));
          }
        }
      });
    });
  };

  PCursor.prototype.getOperationStatus = function getOperationStatus () {
    var that = this;

    debug('GetOperationStatus -> function start');

    return new Promise(function (resolve, reject) {
      if (!that.getOperationHandle()) {
        return reject(new OperationError('Invalid operationHandle in getOperationStatus'));
      }

      var serviceType = IdlContainer.getServiceType();
      var client = that.getClient();

      var req = new serviceType.TGetOperationStatusReq({
        operationHandle: that.getOperationHandle()
      });

      debug('GetOperationStatus -> request start');

      client.GetOperationStatus(req, function (err, res) {
        debug('GetOperationStatus -> server response');

        if (err) {
          debug('GetOperationStatus -> Error(', err.message, ')');
          reject(err);
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          debug('GetOperationStatus -> Error from HiveServer2\n', res.status.infoMessages);
          debug('GetOperationStatus -> Error from HiveServer2\n', res.status.errorMessage);

          reject(new OperationError([
            res.status.errorMessage,
            res.status.infoMessages,
            '-- Error caused from HiveServer2'
          ].join('\n\n')));
        } else {
          resolve(res.operationState);
        }
      });
    });
  };

  PCursor.prototype.getSchema = function getSchema () {
    var that = this;

    debug('GetSchema -> function start');

    return new Promise(function (resolve, reject) {
      if (!that.getOperationHandle()) {
        return reject(new OperationError('Invalid operationHandle in getOperationStatus'));
      }

      var serviceType = IdlContainer.getServiceType();
      var client = that.getClient();

      var req = new serviceType.TGetResultSetMetadataReq({
        operationHandle: that.getOperationHandle()
      });

      client.GetResultSetMetadata(req, function (err, res) {
        if (err) {
          reject(err);
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          debug('GetResultSetMetadata -> Error from HiveServer2\n', res.status.infoMessages);
          debug('GetResultSetMetadata -> Error from HiveServer2\n', res.status.errorMessage);

          reject(new OperationError([
            res.status.errorMessage,
            res.status.infoMessages,
            '-- Error caused from HiveServer2'
          ].join('\n\n')));
        } else {
          if (res.schema) {
            resolve(_.map(res.schema.columns, function (column) {
              return {
                type: hs2util.getType(column.typeDesc),
                columnName: column.columnName,
                comment: column.comment
              };
            }));
          } else {
            resolve(null);
          }
        }
      });
    });
  };

  PCursor.prototype.fetchBlock = function fetchBlock () {
    var that = this;

    debug('FetchBlock -> function start');

    return new Promise(function (resolve, reject) {
      if (!that.getOperationHandle()) {
        return reject(new OperationError('Invalid operationHandle in fetchBlock'));
      }

      var serviceType = IdlContainer.getServiceType();
      var client = that.getClient();

      var req = new serviceType.TFetchResultsReq({
        operationHandle: that.getOperationHandle(),
        orientation: serviceType.TFetchOrientation.FETCH_NEXT,
        maxRows: that.getConfigure().getMaxRows(),
        fetchType: hs2util.FETCH_TYPE.ROW
      });

      debug('FetchBlock -> request start');

      client.FetchResults(req, function (err, res) {
        debug('FetchBlock -> request responsed');

        if (err) {
          debug('FetchBlock -> response error, ', err.message);
          reject(err);
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          debug('FetchBlock -> Error from HiveServer2\n', res.status.infoMessages);
          debug('FetchBlock -> Error from HiveServer2\n', res.status.errorMessage);

          reject(new OperationError([
            res.status.errorMessage,
            res.status.infoMessages,
            '-- Error caused from HiveServer2'
          ].join('\n\n')));
        } else {
          try {
            if (!that.getTypeCache()) {
              that.setTypeCache(hs2util.makeStoredType(res.results));
            }

            var typeCache = that.getTypeCache();
            var firstColumn = _(res.results.columns).first();
            var firstType = _(typeCache).first();
            var fetchLength = firstColumn[firstType].values.length;

            debug('HiveServer2 1.2.x under, not support hasMoreRows');
            debug('FetchBlock -> hasMoreRows: ', res.hasMoreRows);

            if (!!fetchLength) {
              return resolve({
                hasMoreRows: !!fetchLength,
                rows: hs2util.makeArray(that.getConfigure(), typeCache, res.results.columns)
              });
            } else {
              that.setTypeCache(null);
              return resolve({
                hasMoreRows: !!fetchLength,
                rows: []
              });
            }
          } catch (err) {
            reject(new FetchError(err.message));
          }
        }
      });
    });
  };

  module.exports = PCursor;
})();