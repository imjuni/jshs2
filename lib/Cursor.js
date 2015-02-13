(function () {
  'use strict';

  var _ = require('lodash');
  var debug = require('debug')('jshs2:cursor');
  var TTypes = require('../idl/gen-nodejs/TCLIService_types.js');
  var ExecutionError = require('./HS2Error').ExecutionError;
  var OperationError = require('./HS2Error').OperationError;
  var FetchError = require('./HS2Error').FetchError;
  var Int64 = require('node-int64');

  var TTypesValueToName = _.map(_.keys(TTypes.TTypeId), function (typeName) {
    return typeName.replace('_TYPE', '').toLowerCase();
  });

  var TTypesState = _.map(_.keys(TTypes.TOperationState), function (state) {
    return state.replace('_STATE', '').toLowerCase();
  });

  function getState (id) {
    id = id || TTypes.TOperationState.UKNOWN_STATE;

    return TTypesState[parseInt(id, 10)];
  }

  function getType (typeDesc) {
    for (var i = 0, len = typeDesc.types.length; i < len; i++) {
      if (typeDesc.types[i].primitiveEntry) {
        return TTypesValueToName[typeDesc.types[i].primitiveEntry.type|0];
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
  }

  function getValue (colValue) {
    if (colValue.boolVal) {
      return colValue.boolVal.value;
    } else if (colValue.byteVal) {
      return colValue.byteVal.value;
    } else if (colValue.i16Val) {
      return colValue.i16Val.value;
    } else if (colValue.i32Val) {
      return colValue.i32Val.value;
    } else if (colValue.i64Val) {
      if (colValue.i64Val.value && colValue.i64Val.value.buffer) {
        return (new Int64(colValue.i64Val.value.buffer)).toString();
      } else if (colValue.i64Val.value === null || colValue.i64Val.value === undefined) {
        return null;
      } else {
        return 'int64_error';
      }
    } else if (colValue.doubleVal) {
      return colValue.doubleVal.value;
    } else if (colValue.stringVal) {
      return colValue.stringVal.value;
    }
  }

  function Cursor (client, sessionHandle, opt) {
    var $opt = {};
    var $client = client;
    var $sessionHandle = sessionHandle;

    var store = {};
    var forFetchBlock = {};

    var $CursorObject = {};

    _.assign($opt, opt);

    if (_.isEmpty($opt) || !_.isNumber($opt.maxRows)) {
      $opt.maxRows = 1024;
    }

    debug('max fetch rows count: ' + JSON.stringify($opt || {}));

    function cancel (cb) {
      var req = new TTypes.TCancelOperationReq({
        operationHandle: store.operationHandle
      });

      $client.CancelOperation(req, function (err, res) {
        if (err) {
          return cb(new OperationError(err.message || 'cancel fail, unknown error'));
        } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
          return cb(new OperationError(res.status.errorMessage || 'cancel fail, unknown error'));
        } else {
          return cb(null);
        }
      });
    }

    function execute (hql, cb) {
      var req = new TTypes.TExecuteStatementReq({
        sessionHandle: $sessionHandle,
        statement: hql,
        runAsync: true
      });

      $client.ExecuteStatement(req, function (err, res) {
        if (err) {
          debug('execute statement:Error, statusCode error');
          return cb(new ExecutionError(err.message || 'execute fail, unknown error'));
        } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
          debug('execute statement:Error, statusCode error');
          return cb(new ExecutionError(res.status.errorMessage || 'execute fail, unknown error'));
        } else {
          store.operationHandle = res.operationHandle;
          store.hasResultSet = res.operationHandle.hasResultSet;

          return cb(err, { hasResultSet: store.hasResultSet });
        }
      });
    }

    function getLog (cb) {
      debug('get log: start');

      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid operationHandle'));
      } else {
        var req = new TTypes.TGetLogReq({
          operationHandle: store.operationHandle
        });

        $client.GetLog(req, function (err, res) {
          if (err) {
            return cb(new OperationError(err.message));
          } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new OperationError(res.status.errorMessage));
          } else {
            cb(null, res.log);
          }
        });
      }
    }

    function getOperationStatus (cb) {
      debug('get operation status: start');

      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new TTypes.TGetOperationStatusReq({
          operationHandle: store.operationHandle
        });

        $client.GetOperationStatus(req, function (err, res) {
          if (err) {
            return cb(new OperationError(err.message || 'getOperationStatus fail, unknown error'));
          } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new OperationError(res.status.errorMessage || 'getOperationStatus fail, unknown error'));
          } else {
            return cb(null, getState(res.operationState));
          }
        });
      }
    }

    function getSchema (cb) {
      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new TTypes.TGetResultSetMetadataReq({
          operationHandle: store.operationHandle
        });

        $client.GetResultSetMetadata(req, function (err, res) {
          if (err) {
            return cb(new OperationError(err.message || 'error caused from GetResultSetMetadata'));
          } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new OperationError(res.status.errorMessage || 'error caused from GetResultSetMetadata'));
          } else {
            if (res.schema) {
              var columns = _.map(res.schema.columns, function (column) {
                return {
                  type: getType(column.typeDesc),
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

    function modifyVal (storedType, value) {
      if (storedType === 'i64Val') {
        if (value && value.buffer) {
          return (new Int64(value.buffer)).toString();
        } else if (value === null || value === undefined) {
          return null;
        } else {
          return 'int64_error';
        }
      } else {
        return value;
      }
    }

    function makeJson (storedTypes, columns, schema) {
      var rows = [];
      var ilen = _(columns).first()[_(storedTypes).first()].values.length;
      var jlen = columns.length;

      for (var i = 0; i < ilen; i++) {
        var row = {};

        for (var j = 0; j < jlen; j++) {
          row[schema[j].columnName] = modifyVal(storedTypes[j], columns[j][storedTypes[j]].values[i]);
        }

        rows.push(row);
      }

      return rows;
    }

    function makeArray (storedTypes, columns) {
      var rows = [];
      var ilen = _(columns).first()[_(storedTypes).first()].values.length;
      var jlen = columns.length;

      for (var i = 0; i < ilen; i++) {
        var row = [];

        for (var j = 0; j < jlen; j++) {
          row.push(modifyVal(storedTypes[j], columns[j][storedTypes[j]].values[i]));
        }

        rows.push(row);
      }

      return rows;
    }

    function makeStoredType (results) {
      return _(results.columns).map(function (column, index) {
        if (!_.isEmpty(column.binaryVal)) {
          return 'binaryVal';
        } else if (!_.isEmpty(column.boolVal)) {
          return 'boolVal';
        } else if (!_.isEmpty(column.byteVal)) {
          return 'byteVal';
        } else if (!_.isEmpty(column.doubleVal)) {
          return 'doubleVal';
        } else if (!_.isEmpty(column.i16Val)) {
          return 'i16Val';
        } else if (!_.isEmpty(column.i32Val)) {
          return 'i32Val';
        } else if (!_.isEmpty(column.i64Val)) {
          return 'i64Val';
        } else { // stringVal
          return 'stringVal';
        }
      }).value();
    }

    function $fetch (storedTypes, filter, event, schemas, rows, req, cb) {
      $client.FetchResults(req, function (err, res) {
        if (err) {
          return cb(new FetchError(err.message));
        } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
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
      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var rows = [];
        var req = new TTypes.TFetchResultsReq({
          operationHandle: store.operationHandle,
          orientation: TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        return $fetch(null, makeJson, null, columns, rows, req, cb);
      }
    }

    function fetch (cb) {
      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var rows = [];
        var req = new TTypes.TFetchResultsReq({
          operationHandle: store.operationHandle,
          orientation: TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        return $fetch(null, makeArray, null, null, rows, req, cb);
      }
    }

    function eventWithFetch (event, cb) {
      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var rows = [];
        var req = new TTypes.TFetchResultsReq({
          operationHandle: store.operationHandle,
          orientation: TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        return $fetch(null, makeArray, event, null, rows, req, cb);
      }
    }

    function eventWithJsonFetch (headers, event, cb) {
      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var rows = [];
        var req = new TTypes.TFetchResultsReq({
          operationHandle: store.operationHandle,
          orientation: TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        return $fetch(null, makeJson, event, headers, rows, req, cb);
      }
    }

    function fetchBlock (cb) {
      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new TTypes.TFetchResultsReq({
          operationHandle: store.operationHandle,
          orientation: TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        $client.FetchResults(req, function (err, res) {
          if (err) {
            return cb(new FetchError(err.message || 'Fetch Error'));
          } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new FetchError(res.status.errorMessage || 'Fetch Error'));
          } else {
            try {
              forFetchBlock.fbStoreType = (_.isEmpty(forFetchBlock.fbStoreType)) ? makeStoredType(res.results) : forFetchBlock.fbStoreType;
              var retValLen = _(res.results.columns).first()[_(forFetchBlock.fbStoreType).first()].values.length;

              debug('hasMoreRows: ' + res.hasMoreRows);

              if (!!retValLen) {
                cb.call($CursorObject, null, !!retValLen, makeArray.call($CursorObject, forFetchBlock.fbStoreType, res.results.columns));
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
      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new TTypes.TFetchResultsReq({
          operationHandle: store.operationHandle,
          orientation: TTypes.TFetchOrientation.FETCH_NEXT,
          maxRows: $opt.maxRows
        });

        $client.FetchResults(req, function (err, res) {
          if (err) {
            return cb(new FetchError(err.message || 'Fetch Error'));
          } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new FetchError(res.status.errorMessage || 'Fetch Error'));
          } else {
            try {
              forFetchBlock.jfbStoreType = (_.isEmpty(forFetchBlock.jfbStoreType)) ? makeStoredType(res.results) : forFetchBlock.jfbStoreType;
              var retValLen = _(res.results.columns).first()[_(forFetchBlock.jfbStoreType).first()].values.length;

              debug('hasMoreRows: ' + res.hasMoreRows);

              if (!!retValLen) {
                cb.call($CursorObject, null, !!retValLen, makeJson.call($CursorObject, forFetchBlock.jfbStoreType, res.results.columns, schemas));
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
      if (_.isEmpty(store.operationHandle)) {
        return cb(new OperationError('Invalid OperationHandle'));
      } else {
        var req = new TTypes.TCloseOperationReq({
          operationHandle: store.operationHandle
        });

        $client.CloseOperation(req, function (err, res) {
          if (err) {
            return cb(new OperationError(err.message || 'cursor close fail, unknown error'));
          } else if (res.status.statusCode === TTypes.TStatusCode.ERROR_STATUS) {
            return cb(new OperationError(res.status.errorMessage || 'cursor close fail, unknown error'));
          } else {
            return cb(null);
          }
        });
      }
    }

    $CursorObject.cancel = cancel;
    $CursorObject.execute = execute;
    $CursorObject.getLog = getLog;
    $CursorObject.getOperationStatus = getOperationStatus;
    $CursorObject.getSchema = getSchema;
    $CursorObject.jsonFetch = jsonFetch;
    $CursorObject.fetch = fetch;
    $CursorObject.fetchBlock = fetchBlock;
    $CursorObject.jsonFetchBlock = jsonFetchBlock;
    $CursorObject.eventWithFetch = eventWithFetch;
    $CursorObject.eventWithJsonFetch = eventWithJsonFetch;
    $CursorObject.close = close;

    return $CursorObject;
  }

  module.exports = Cursor;
})();

