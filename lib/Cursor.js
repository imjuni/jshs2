"use strict";

var _ = require('underscore');
var debug = require('debug')('jshs2:cursor');
var EventEmitter = require('events').EventEmitter;
var TCLIServiceTypes = require('../idl/gen-nodejs/TCLIService_types.js');
var HS2Error = require('./HS2Error')();

module.exports = function () {
  function Cursor (connection, options) {
    var self;

    self = this;
    self.options = options || { maxRows: 1024 };
    self.connection = connection;
    self.TTypeValueToName = _.map(_.keys(TCLIServiceTypes.TTypeId), function (typeName) {
      return typeName.replace('_TYPE', '').toLowerCase();
    });

//      ttypes.TStatusCode = {
//        'SUCCESS_STATUS' : 0,
//        'SUCCESS_WITH_INFO_STATUS' : 1,
//        'STILL_EXECUTING_STATUS' : 2,
//        'ERROR_STATUS' : 3,
//        'INVALID_HANDLE_STATUS' : 4
//      };

    debug('instance created');
  }

  Cursor.prototype.getType = function (typeDesc) {
    for (var i = 0, len = typeDesc.types.length; i < len; i++) {
      if (typeDesc.types[i].primitiveEntry) {
        return this.TTypeValueToName[typeDesc.types[i].primitiveEntry.type|0];
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
      return colValue.i64Val.value
    } else if (colValue.doubleVal) {
      return colValue.doubleVal.value
    } else if (colValue.stringVal) {
      return colValue.stringVal.value
    }
  };

  Cursor.prototype.execute = function (hql, callback) {
    debug('execute: start');

    var self = this;
    var req = new TCLIServiceTypes.TExecuteStatementReq({
      sessionHandle: self.connection.session(),
      statement: hql,
      runAsync: true
    });

    debug('execute statement:start');
    self.connection.client().ExecuteStatement(req, function (err, res) {
      if (err) {
        callback(new HS2Error.ExecutionError(err.message));
      } else if (res.status.statusCode === TCLIServiceTypes.TStatusCode.ERROR_STATUS) {
        debug('execute statement:Error, statusCode error');

        callback(new HS2Error.ExecutionError(res.status.errorMessage));
      } else {
        self.operation = res.operationHandle;
        self.hasMorwRows = true;

        console.log(res);

        debug('execute statement:end');

        if (callback) {
          debug('execute:end');

          callback(err, self);
        }
      }
    });
  };

  Cursor.prototype.getShcema = function (callback) {
    debug('getSchema: start');

    var self = this;

    if (self.operation) {
      var req = new TCLIServiceTypes.TGetResultSetMetadataReq({ operationHandle: self.operation });

      self.connection.client().GetResultSetMetadata(req, function (err, res) {
        if (err) {
          debug('getSchema: got exception error');

          callback(new HS2Error.ExecutionError(err.message || 'error caused from GetResultSetMetadata'));
        } else if (res.status.statusCode === TCLIServiceTypes.TStatusCode.ERROR_STATUS) {
          debug('getSchema: got exception error(' + res.status.errorMessage + ')');

          callback(new HS2Error.ExecutionError(res.status.errorMessage || 'error caused from GetResultSetMetadata'));
        } else {
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
        }
      });
    }
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

    var req = new TCLIServiceTypes.TFetchResultsReq({
      operationHandle: self.operation,
      orientation: TCLIServiceTypes.TFetchOrientation.FETCH_NEXT,
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
        ee.emit('catch', new HS2Error.ConnectionError(err.message));
      } else if (res.status.statusCode === TCLIServiceTypes.TStatusCode.ERROR_STATUS) {
        debug('cursor _jsonFetch: comm error, err resped');
        ee.emit('catch', new HS2Error.ConnectionError(res.status.errorMessage));
      } else {
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

          return;
        }

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

    var req = new TCLIServiceTypes.TFetchResultsReq({
      operationHandle: self.operation,
      orientation: TCLIServiceTypes.TFetchOrientation.FETCH_NEXT,
      maxRows: self.options.maxRows
    });

    debug('cursor fetch:start');
    self._fetch(req, ee);
  };

  Cursor.prototype._fetch = function (req, ee) {
    debug('cursor _fetch:start');
    var self = this;

    self.connection.client().FetchResults(req, function (err, res) {
      debug('cursor _fetch: comm success');

      if (err) {
        debug('cursor _fetch: comm error, err');
        ee.emit('catch', new HS2Error.ConnectionError(err.message));
      } else if (res.status.statusCode === TCLIServiceTypes.TStatusCode.ERROR_STATUS) {
        debug('cursor _fetch: comm error, err resped');
        ee.emit('catch', new HS2Error.ConnectionError(res.status.errorMessage));
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

    if (self.operation) {
      var closeOperationReq = new TCLIServiceTypes.TCloseOperationReq({
        operationHandle: self.operation
      });

      debug('operation close:start');
      self.connection.client().CloseOperation(closeOperationReq, function (err) {

        debug('operation close:end');
        callback(err);
      });
    }

    debug('cursor close:end');
  };

  return Cursor;
};