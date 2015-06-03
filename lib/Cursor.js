(function () {
  'use strict';

  var _ = require('lodash');
  var util = require('util');
  var debug = require('debug')('jshs2:cursor');
  var Int64 = require('node-int64');

  var CursorError = require('./HS2Error').CursorError;
  var ExecutionError = require('./HS2Error').ExecutionError;
  var OperationError = require('./HS2Error').OperationError;
  var FetchError = require('./HS2Error').FetchError;
  var IdlContainer = require('./IdlContainer');
  var hs2util = require('./HS2Util');

  function HS2Cursor (connection) {
    this.connection = connection;
    this.client = connection.getClient();
    this.sessionHandle = connection.getSessionHandle();
    this.configure = connection.getConfigure();
    this.operationHandle = null;
    this.hasResultSet = false;
    this.typeCache = null;
  }

  HS2Cursor.prototype.setTypeCache = function setTypeCache (typeCache) {
    this.typeCache = typeCache;
  };

  HS2Cursor.prototype.getTypeCache = function getTypeCache () {
    return this.typeCache;
  };

  HS2Cursor.prototype.getConnection = function getConnection () {
    return this.connection;
  };

  HS2Cursor.prototype.getConfigure = function getConfigure () {
    return this.configure;
  };

  HS2Cursor.prototype.getOperationHandle = function getOperationHandle () {
    return this.operationHandle;
  };

  HS2Cursor.prototype.setOperationHandle = function setOperationHandle (operationHandle) {
    this.operationHandle = operationHandle;
  };

  HS2Cursor.prototype.getHasResultSet = function getHasResultSet () {
    return this.hasResultSet;
  };

  HS2Cursor.prototype.setHasResultSet = function setHasResultSet (hasResultSet) {
    this.hasResultSet = hasResultSet;
  };

  HS2Cursor.prototype.getSessionHandle = function getSessionHandle () {
    return this.sessionHandle;
  };

  HS2Cursor.prototype.getClient = function getClient () {
    return this.client;
  };

  HS2Cursor.prototype.getLog = function getLog () {
    var that = this;
    var serviceType = IdlContainer.getServiceType();
    var protocolVersion = serviceType.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8 || 7;

    if (that.getConnection().getServerProtocolVersion() < protocolVersion &&
      that.getConfigure().getHiveType() === hs2util.HIVE_TYPE.CDH) {
      debug('getLog, selected getLogOnCDH -> ', that.getConfigure().getHiveType());
      return that.getLogOnCDH();
    } else if (that.getConnection().getServerProtocolVersion() >= protocolVersion) {
      debug('getLog, selected getLogOnHive -> ', that.getConfigure().getHiveType());
      return that.getLogOnHive();
    } else {
      debug('getLog -> Not implementation Error');
      return Promise.reject(new OperationError('Not support GetLog function'));
    }
  };

  function HS2PromiseCursor (connection) {
    HS2Cursor.call(this, connection);

    if (!IdlContainer .getIsInit()) {
      throw new Error('Can\'t use Connection without IdlContainer initialization');
    }
  }

  util.inherits(HS2PromiseCursor, HS2Cursor);

  HS2PromiseCursor.prototype.cancel = function cancel () {
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
          reject(new OperationError(res.status.errorMessage || 'cancel fail, unknown error'));
        } else {
          resolve(true);
        }
      });
    });
  };

  HS2PromiseCursor.prototype.execute = function execute (hql) {
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
          debug('execute statement:Error, statusCode error');
          reject(new ExecutionError(res.status.errorMessage + '\n\n Error caused from HiveServer2'));
        } else {
          that.setOperationHandle(res.operationHandle);
          that.setHasResultSet(res.operationHandle.hasResultSet);

          resolve({ hasResultSet: that.getHasResultSet() });
        }
      });
    });
  };

  HS2PromiseCursor.prototype.getLogOnCDH = function getLogOnCDH () {
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
          reject(new OperationError(res.status.errorMessage + '\n\n Error caused from HiveServer2'));
        } else {
          resolve(res.log);
        }
      });
    });
  };

  HS2PromiseCursor.prototype.getLogOnHive = function getLogOnCDH () {
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
          reject(new OperationError(res.status.errorMessage + '\n\n Error caused from HiveServer2'));
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

  HS2PromiseCursor.prototype.getOperationStatus = function getOperationStatus () {
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
          debug('GetOperationStatus -> Error from HiveServer2(', res.status.errorMessage , ')');
          reject(new OperationError(res.status.errorMessage + '\n\n -- Error caused from HiveServer2'));
        } else {
          resolve(res.operationState);
        }
      });
    });
  };

  HS2PromiseCursor.prototype.getSchema = function getSchema () {
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
          reject(new OperationError(res.status.errorMessage + '\n\n -- Error caused from HiveServer2'));
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

  HS2PromiseCursor.prototype.fetchBlock = function fetchBlock () {
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

  function HS2CallbackCursor (connection) {
    HS2Cursor.call(this, connection);

    if (!IdlContainer .getIsInit()) {
      throw new Error('Can\'t use Connection without IdlContainer initialization');
    }
  }

  util.inherits(HS2CallbackCursor, HS2Cursor);

  function Cursor (client, sessionHandle, opt) {
    var $opt = {};
    var $client = client;
    var $sessionHandle = sessionHandle;

    var $store = {};
    var $forFetchBlock = {};
    var $CursorObject = {};
    var bitMask = [1, 2, 4, 8, 16, 32, 64, 128];

    _.assign($opt, opt);

    if (_.isEmpty($opt) || !_.isNumber($opt.maxRows)) {
      $opt.maxRows = 1024;
    }

    if ($opt.nullStr === undefined || $opt.nullStr === null) {
      $opt.nullStr = 'NULL';
    }

    var $service = createService($opt);

    debug('max fetch rows count: ' + JSON.stringify($opt || {}));

    function cancel (cb) {
      var req = new $service.TTypes.TCancelOperationReq({
        operationHandle: $store.operationHandle
      });

      $client.CancelOperation(req, function (err, res) {
        if (err) {
          return cb(new OperationError(err.message || 'cancel fail, unknown error'));
        } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
          return cb(new OperationError(res.status.errorMessage || 'cancel fail, unknown error'));
        } else {
          return cb(null);
        }
      });
    }

    function execute (hql, cb) {
      var req = new $service.TTypes.TExecuteStatementReq({
        sessionHandle: $sessionHandle,
        statement: hql,
        runAsync: true
      });

      $client.ExecuteStatement(req, function (err, res) {
        if (err) {
          debug('execute statement:Error, statusCode error');
          return cb(new ExecutionError(err.message || 'execute fail, unknown error'));
        } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
          debug('execute statement:Error, statusCode error');
          return cb(new ExecutionError(res.status.errorMessage || 'execute fail, unknown error'));
        } else {
          $store.operationHandle = res.operationHandle;
          $store.hasResultSet = res.operationHandle.hasResultSet;

          return cb(err, { hasResultSet: $store.hasResultSet });
        }
      });
    }

    function getLog (cb) {
      debug('get log: start');

      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid operationHandle'));
      } else if ($client.GetLog === undefined || $client.GetLog === null) {
        return cb(new OperationError('Not Implementation GetLog'));
      } else {
        var req = new $service.TTypes.TGetLogReq({
          operationHandle: $store.operationHandle
        });

        $client.GetLog(req, function (err, res) {
          if (err) {
            return cb(new OperationError(err.message));
          } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new OperationError(res.status.errorMessage));
          } else {
            cb(null, res.log);
          }
        });
      }
    }

    function getOperationStatus (cb) {
      debug('get operation status: start');

      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new $service.TTypes.TGetOperationStatusReq({
          operationHandle: $store.operationHandle
        });

        $client.GetOperationStatus(req, function (err, res) {
          if (err) {
            return cb(new OperationError(err.message || 'getOperationStatus fail, unknown error'));
          } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new OperationError(res.status.errorMessage || 'getOperationStatus fail, unknown error'));
          } else {
            return cb(null, $service.getState(res.operationState));
          }
        });
      }
    }

    function getSchema (cb) {
      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new $service.TTypes.TGetResultSetMetadataReq({
          operationHandle: $store.operationHandle
        });

        $client.GetResultSetMetadata(req, function (err, res) {
          if (err) {
            return cb(new OperationError(err.message || 'error caused from GetResultSetMetadata'));
          } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new OperationError(res.status.errorMessage || 'error caused from GetResultSetMetadata'));
          } else {
            if (res.schema) {
              var columns = _.map(res.schema.columns, function (column) {
                return {
                  type: $service.getType(column.typeDesc),
                  columnName: column.columnName,
                  comment: column.comment
                };
              });

              cb(err, columns);
            } else {
              cb(err, null);
            }
          }
        });
      }
    }

    function $fetch (storedTypes, filter, event, schemas, rows, req, cb) {
      $client.FetchResults(req, function (err, res) {
        if (err) {
          return cb(new FetchError(err.message));
        } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
          return cb(new FetchError(res.status.errorMessage || 'Fetch Error'));
        } else {
          if (_.isEmpty(storedTypes)) {
            storedTypes = makeStoredType(res.results);
          }

          if (_(res.results.columns).first()[_(storedTypes).first()].values.length) {
            var currentRows;

            if (typeof filter === 'function') {
              currentRows = filter.call($CursorObject, storedTypes, res.results.columns, schemas);
            } else {
              currentRows = makeArray.call($CursorObject, storedTypes, res.results.columns, schemas);
            }

            if (typeof event === 'function') {
              process.nextTick(function () {
                setImmediate(function () {
                  return event.func(null, event.args.concat([currentRows]));
                });
              });
            }

            rows = rows.concat(currentRows);

            if (res.hasMoreRows) {
              return process.nextTick(function () {
                setImmediate(function () {
                  return $fetch(storedTypes, filter, event, schemas, rows, req, cb);
                });
              });
            } else {
              return process.nextTick(function () {
                setImmediate(function () {
                  return cb(null, rows);
                });
              });
            }
          } else {
            return cb(null, rows);
          }
        }
      });
    }

    function jsonFetch (columns, cb) {
      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var rows = [];
        var req = new $service.TTypes.TFetchResultsReq({
          operationHandle: $store.operationHandle,
          orientation: $service.TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        return $fetch(null, makeJson, null, columns, rows, req, cb);
      }
    }

    function fetch (cb) {
      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var rows = [];
        var req = new $service.TTypes.TFetchResultsReq({
          operationHandle: $store.operationHandle,
          orientation: $service.TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        return $fetch(null, makeArray, null, null, rows, req, cb);
      }
    }

    function eventWithFetch (event, cb) {
      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var rows = [];
        var req = new $service.TTypes.TFetchResultsReq({
          operationHandle: $store.operationHandle,
          orientation: $service.TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        return $fetch(null, makeArray, event, null, rows, req, cb);
      }
    }

    function eventWithJsonFetch (headers, event, cb) {
      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var rows = [];
        var req = new $service.TTypes.TFetchResultsReq({
          operationHandle: $store.operationHandle,
          orientation: $service.TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        return $fetch(null, makeJson, event, headers, rows, req, cb);
      }
    }

    function fetchBlock (cb) {
      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new $service.TTypes.TFetchResultsReq({
          operationHandle: $store.operationHandle,
          orientation: $service.TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        $client.FetchResults(req, function (err, res) {
          if (err) {
            return cb(new FetchError(err.message || 'Fetch Error'));
          } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new FetchError(res.status.errorMessage || 'Fetch Error'));
          } else {
            try {
              $forFetchBlock.fbStoreType = (_.isEmpty($forFetchBlock.fbStoreType)) ? makeStoredType(res.results) : $forFetchBlock.fbStoreType;
              var retValLen = _(res.results.columns).first()[_($forFetchBlock.fbStoreType).first()].values.length;

              debug('hasMoreRows: ' + res.hasMoreRows);

              if (!!retValLen) {
                cb.call($CursorObject, null, !!retValLen, makeArray.call($CursorObject, $forFetchBlock.fbStoreType, res.results.columns));
              } else {
                cb.call($CursorObject, null, !!retValLen, []);
              }
            } catch (err) {
              cb.call($CursorObject, new FetchError(err.message));
            }
          }
        });
      }
    }

    function jsonFetchBlock (schemas, cb) {
      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new $service.TTypes.TFetchResultsReq({
          operationHandle: $store.operationHandle,
          orientation: $service.TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        $client.FetchResults(req, function (err, res) {
          if (err) {
            return cb(new FetchError(err.message || 'Fetch Error'));
          } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new FetchError(res.status.errorMessage || 'Fetch Error'));
          } else {
            try {
              $forFetchBlock.jfbStoreType = (_.isEmpty($forFetchBlock.jfbStoreType)) ? makeStoredType(res.results) : $forFetchBlock.jfbStoreType;
              var retValLen = _(res.results.columns).first()[_($forFetchBlock.jfbStoreType).first()].values.length;

              debug('hasMoreRows: ' + res.hasMoreRows);

              if (!!retValLen) {
                cb.call($CursorObject, null, !!retValLen, makeJson.call($CursorObject, $forFetchBlock.jfbStoreType, res.results.columns, schemas));
              } else {
                cb.call($CursorObject, null, !!retValLen, []);
              }
            } catch (err) {
              cb.call($CursorObject, new FetchError(err.message));
            }
          }
        });
      }
    }

    function close (cb) {
      if (_.isEmpty($store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new $service.TTypes.TCloseOperationReq({
          operationHandle: $store.operationHandle
        });

        $client.CloseOperation(req, function (err, res) {
          if (err) {
            return cb(new OperationError(err.message || 'cursor close fail, unknown error'));
          } else if (res.status.statusCode === $service.TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new OperationError(res.status.errorMessage || 'cursor close fail, unknown error'));
          } else {
            return cb(null);
          }
        });
      }
    }

    $CursorObject.cancel = cancel;
    $CursorObject.execute = execute;
    $CursorObject.getOperationStatus = getOperationStatus;
    $CursorObject.getSchema = getSchema;
    $CursorObject.jsonFetch = jsonFetch;
    $CursorObject.fetch = fetch;
    $CursorObject.fetchBlock = fetchBlock;
    $CursorObject.jsonFetchBlock = jsonFetchBlock;
    $CursorObject.eventWithFetch = eventWithFetch;
    $CursorObject.eventWithJsonFetch = eventWithJsonFetch;
    $CursorObject.close = close;

    if ($opt.connOpt.hiveType === 'CDH') {
      $CursorObject.getLog = getLog;
    } else {
      $CursorObject.getLog = function getLog (cb) {
        cb(new OperationError('Not implementation Exception'));
      };
    }

    return $CursorObject;
  }

  module.exports = {
    HS2PromiseCursor: HS2PromiseCursor,
    HS2CallbackCursor: HS2CallbackCursor
  };
})();

