"use strict";

var _ = require('lodash');
var debug = require('debug')('jshs2:cursor');
var EventEmitter = require('events').EventEmitter;
var TTypes = require('../idl/gen-nodejs/TCLIService_types.js');
var HS2Error = require('./HS2Error')();
var Int64 = require('node-int64');

module.exports = function () {
  function Cursor (connection, options) {
    var self = this;

    debug('max fetch rows count: ' + JSON.stringify(options || {}));

    self.options = options || { maxRows: 1024 };
    self.connection = connection;

    self.TTypesValueToName = _.map(_.keys(TTypes.TTypeId), function (typeName) {
      return typeName.replace('_TYPE', '').toLowerCase();
    });

    self.TTypesState = _.map(_.keys(TTypes.TOperationState), function (state) {
      return state.replace('_STATE', '').toLowerCase();
    });

    debug('instance created');
  }

  Cursor.prototype.getState = function (id) {
    id = id || TTypes.TOperationState.UKNOWN_STATE;

    return this.TTypesState[parseInt(id, 10)];
  };

  Cursor.prototype.getType = function (typeDesc) {
    for (var i = 0, len = typeDesc.types.length; i < len; i++) {
      if (typeDesc.types[i].primitiveEntry) {
        return this.TTypesValueToName[typeDesc.types[i].primitiveEntry.type|0];
      } else if (typeDesc.types[i].mapEntry) {
        return typeDesc.types[i].mapEntry;
      } else if (typeDesc.types[i].unionEntry) {
        return typeDesc.types[i].unionEntry;
      } else if (typeDesc.types[i].arrayEntry) {
        return typeDesc.types[i].arrayEntry;
      } else if (typeDesc.types[i].structEntry) {
        return typeDesc.types[i].structEntry;
      } else if (typeDesc.types[i].userDefinedTypeEntry) {
        return typeDesc.types[i].userDefinedTypeEntry;
      }
    }
  };

  Cursor.prototype.getValue = function (colValue) {
    if (colValue.boolVal) {
      return colValue.boolVal.value;
    } else if (colValue.byteVal) {
      return colValue.byteVal.value
    } else if (colValue.i16Val) {
      return colValue.i16Val.value
    } else if (colValue.i32Val) {
      return colValue.i32Val.value
    } else if (colValue.i64Val) {
      if (colValue.i64Val.value && colValue.i64Val.value.buffer) {
        return (new Int64(colValue.i64Val.value.buffer)).toString();
      } else {
        return 'int64_error';
      }
    } else if (colValue.doubleVal) {
      return colValue.doubleVal.value
    } else if (colValue.stringVal) {
      return colValue.stringVal.value
    }
  };

  Cursor.prototype.cancel = function (callback) {
    debug('get operation status: start');
    var self = this;

    if (!self.operationHandle) { return callback(new Error('Cancel failed, null operationHandle')); }

    var req = new TTypes.TCancelOperationReq({ operationHandle: self.operationHandle });

    self.connection.client().CancelOperation(req, function (err, res) {
      if (err) {
        return callback(new Error(err.message || 'cancel fail, unknown error'));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        return callback(new Error(res.status.errorMessage || 'cancel fail, unknown error'));
      }

      callback(null);
    });
  };

  Cursor.prototype.getOperationHandle = function () {
    return this.operationHandle;
  };

  Cursor.prototype.getLog = function (callback) {
    debug('get log: start');

    var self = this;

    if (!self.operationHandle) { return callback(new Error('getLog failed, null operationHandle')); }

    var req = new TTypes.TGetLogReq({ operationHandle: self.operationHandle });

    self.connection.client().GetLog(req, function (err, res) {
      if (err) {
        return callback(new Error(err.message));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        return callback(new Error(res.status.errorMessage));
      }

      debug('get log: end');

      callback(null, res.log);
    });
  };

  Cursor.prototype.getOperationStatus = function (callback) {
    debug('get operation status: start');

    var self = this;

    if (!self.operationHandle) { return callback(new Error('getOperationStatus fail, null operationHandle')); }

    var req = new TTypes.TGetOperationStatusReq({ operationHandle: self.operationHandle });

    self.connection.client().GetOperationStatus(req, function (err, res) {
      if (err) {
        return callback(new Error(err.message || 'getOperationStatus fail, unknown error'));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        return callback(new Error(res.status.errorMessage || 'getOperationStatus fail, unknown error'));
      }

      debug('get operation status: end');
      callback(null, self.getState(res.operationState));
    });
  };

  Cursor.prototype.execute = function (hql, callback) {
    debug('execute: start');

    var self = this;
    var req = new TTypes.TExecuteStatementReq({
      sessionHandle: self.connection.session(),
      statement: hql,
      runAsync: true
    });

    debug('execute statement:start');

    self.connection.client().ExecuteStatement(req, function (err, res) {
      if (err) {
        debug('execute statement:Error, statusCode error');
        return callback(new Error(err.message || 'execute fail, unknown error'));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        debug('execute statement:Error, statusCode error');
        return callback(new Error(res.status.errorMessage || 'execute fail, unknown error'));
      }

      self.operationHandle = res.operationHandle;
      self.hasMorwRows = res.operationHandle.hasResultSet;

      debug('execute statement:end');
      callback(err, self);
    });
  };

  Cursor.prototype.getSchema = function (callback) {
    debug('getSchema: start');

    var self = this;

    if (!self.operationHandle) { return callback(new Error('invalid operationHandle')); }

    var req = new TTypes.TGetResultSetMetadataReq({ operationHandle: self.operationHandle });

    self.connection.client().GetResultSetMetadata(req, function (err, res) {
      if (err) {
        debug('getSchema: got exception error');
        return callback(new Error(err.message || 'error caused from GetResultSetMetadata'));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        debug('getSchema: got exception error(' + res.status.errorMessage + ')');
        return callback(new Error(res.status.errorMessage || 'error caused from GetResultSetMetadata'));
      }

      if (res.schema) {
        var columns = _.map(res.schema.columns, function (column) {
          return {
            type: self.getType(column.typeDesc),
            columnName: column.columnName,
            comment: column.comment
          };
        });

        debug('getSchema: end');

        callback(err, columns);
      } else {
        debug('getSchema: end');

        callback(err, null);
      }
    });
  };

  Cursor.prototype.jsonFetch = function (columns, callback) {
    var self = this;
    var ee = new EventEmitter();
    var fetched = [];

    ee.on('catch', function (err) {
      callback(err);
    });

    ee.on('fetch', function (rows) {
      fetched = fetched.concat(rows || []);
    });

    ee.on('finish', function () {
      callback(null, fetched);
    });

    var req = new TTypes.TFetchResultsReq({
      operationHandle: self.operationHandle,
      orientation: TTypes.TFetchOrientation.FETCH_NEXT,
      maxRows: self.options.maxRows
    });

    debug('cursor jsonFetch:start');
    self._jsonFetch(columns, req, ee);
  };

  Cursor.prototype._jsonFetch = function (columns, req, ee) {
    debug('cursor _jsonFetch:start');

    var self = this;

    self.connection.client().FetchResults(req, function (err, res) {
      debug('cursor _jsonFetch: comm success');

      if (err) {
        debug('cursor _jsonFetch: comm error, err');
        return ee.emit('catch', new HS2Error.FetchError(err.message));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        debug('cursor _jsonFetch: comm error, err resped');
        return ee.emit('catch', new HS2Error.FetchError(res.status.errorMessage));
      }

      var rows;

      if (res.results.rows.length > 0) {
        process.nextTick((function (columns, self, ee, rows) {
          return function () {
            rows = _.map(res.results.rows, function (row) {
              var jsonRow = {};

              _.each(row.colVals, function (colVal, index) {
                jsonRow[columns[index].columnName] = self.getValue(colVal)
              });

              return jsonRow;
            });

            ee.emit('fetch', rows);

            debug('cursor _jsonFetch: success fetch block, next read');

            return setImmediate(function () { self._jsonFetch(columns, req, ee) });
          };
        }(columns, self, ee, rows)));
      } else {
        self.hasMorwRows = false;

        ee.emit('finish');

        debug('cursor _jsonFetch:end');

        return [];
      }
    });
  };

  Cursor.prototype.fetch = function (callback) {
    var self = this;
    var ee = new EventEmitter();
    var fetched = [];

    ee.on('catch', function (err) {
      callback(err);
    });

    ee.on('fetch', function (rows) {
      fetched = fetched.concat(rows || []);
    });

    ee.on('finish', function () {
      callback(null, fetched);
    });

    var req = new TTypes.TFetchResultsReq({
      operationHandle: self.operationHandle,
      orientation: TTypes.TFetchOrientation.FETCH_NEXT,
      maxRows: self.options.maxRows
    });

    debug('cursor fetch:start');
    self._fetch(req, ee);
  };

  Cursor.prototype.fetchBlock = function (callback) {
    var self = this;

    var req = new TTypes.TFetchResultsReq({
      operationHandle: self.operationHandle,
      orientation: TTypes.TFetchOrientation.FETCH_NEXT,
      maxRows: self.options.maxRows
    });

    debug('cursor fetchBlock:start, (' + self.options.maxRows +')');

    self.connection.client().FetchResults(req, function (err, res) {
      if (err) {
        debug('cursor fetchBlock: comm error, err');
        return callback(new Error(err.message || 'fetchBlock fail, unknown error'));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        debug('cursor fetchBlock: comm error, err resped');
        return callback(new Error(res.status.errorMessage || 'fetchBlock fail, unknown error'));
      }

      var rows;

      if (res.results.rows.length > 0) {
        process.nextTick((function (self, rows) {
          return function () {
            setImmediate(function () {
              rows = _.map(res.results.rows, function (row) {
                return _.map(row.colVals, function (colVal) {
                  return self.getValue(colVal);
                });
              });

              debug('cursor fetchBlock: success fetch block, next read');

              return callback(null, self.hasMorwRows, rows);
            });
          };
        }(self, rows)));
      } else {
        self.hasMorwRows = false;

        debug('cursor fetchBlock:end');

        return callback(null, self.hasMorwRows, null);
      }
    });
  };

  Cursor.prototype._fetch = function (req, ee) {
    debug('cursor _fetch:start');
    var self = this;

    self.connection.client().FetchResults(req, function (err, res) {
      debug('cursor _fetch: comm success');

      if (err) {
        debug('cursor _fetch: comm error, err');
        ee.emit('catch', new HS2Error.FetchError(err.message));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        debug('cursor _fetch: comm error, err resped');
        ee.emit('catch', new HS2Error.FetchError(res.status.errorMessage));
      } else {
        var rows;

        if (res.results.rows.length > 0) {
          process.nextTick((function (self, ee, rows) {
            return function () {
              rows = _.map(res.results.rows, function (row) {
                return _.map(row.colVals, function (colVal) {
                  return self.getValue(colVal);
                });
              });

              ee.emit('fetch', rows);

              debug('cursor _fetch: success fetch block, next read');

              return setImmediate(function () { self._fetch(req, ee) });
            };
          }(self, ee, rows)));
        } else {
          self.hasMorwRows = false;

          ee.emit('finish');

          debug('cursor _fetch:end');

          return;
        }
      }
    });
  };

  Cursor.prototype.close = function (callback) {
    debug('cursor close:start');
    var self = this;

    if (!self.operationHandle) {
      return callback(new Error('close fail, invalid operationHandle'));
    }

    var closeOperationReq = new TTypes.TCloseOperationReq({ operationHandle: self.operationHandle});

    debug('operation close:start');

    self.connection.client().CloseOperation(closeOperationReq, function (err, res) {
      if (err) {
        debug('cursor close: got exception error');
        return callback(new Error(err.message || 'cursor close fail, unknown error'));
      } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
        debug('cursor close: got exception error(' + res.status.errorMessage + ')');
        return callback(new Error(res.status.errorMessage || 'cursor close fail, unknown error'));
      }

      callback(null);
    });

    debug('cursor close:end');
  };

  return Cursor;
};