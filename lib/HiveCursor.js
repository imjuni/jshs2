const _ = require('lodash');
const debug = require('debug')('jshs2:CCursor');
const ExecutionError = require('./error/ExecutionError');
const OperationError = require('./error/OperationError');
const FetchError = require('./error/FetchError');
const HS2Util = require('./common/HS2Util');
const Cursor = require('./Cursor');
const Timer = require('./common/Timer');

class HiveCursor extends Cursor {
  cancel() {
    return new Promise((resolve, reject) => {
      const timer = new Timer();
      const serviceType = this.Conn.IDL.ServiceType;
      const request = new serviceType.TCancelOperationReq({
        operationHandle: this.OperationHandle,
      });

      timer.start();
      this.Client.CancelOperation(request, (err, res) => {
        timer.end();

        if (err) {
          reject(new OperationError(err));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
            res.status, 'ExecuteStatement operation fail,... !!');

          debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
          debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

          reject(new OperationError(message));
        } else {
          resolve(true);
        }
      });
    });
  }

  execute(hql, runAsync = true) {
    return new Promise((resolve, reject) => {
      const timer = new Timer();
      const serviceType = this.Conn.IDL.ServiceType;
      const request = new serviceType.TExecuteStatementReq({
        sessionHandle: this.SessionHandle,
        statement: hql,
        runAsync,
      });

      timer.start();
      debug('ExecuteStatement start -> ', (runAsync) ? 'async' : 'sync', ', ', hql);
      this.Client.ExecuteStatement(request, (err, res) => {
        timer.end();

        if (err) {
          debug('execute statement:Error, statusCode error');
          resolve(new ExecutionError(`${err.message}\n Error caused from HiveServer2`));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
            res.status, 'ExecuteStatement operation fail,... !!');

          debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
          debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

          reject(new OperationError(message));
        } else {
          this.OperationHandle = res.operationHandle;
          this.HasResultSet = res.operationHandle.hasResultSet;

          reject({ hasResultSet: this.HasResultSet });
        }
      });
    });
  }

  getLog() {
    return new Promise((resolve, reject) => {
      const func = this.getLogFactory();

      debug('getLog, selected function name ->', func);

      if (func) {
        func.call(this, resolve, reject);
      } else {
        reject(new OperationError('Not implementation getLog or Not support'));
      }
    });
  }

  getLogOnCDH(resolve, reject) {
    if (!this.OperationHandle) {
      reject(new OperationError('Invalid operationHandle in GetLog, on CDH'));
    } else {
      const timer = new Timer();
      const serviceType = this.Conn.IDL.ServiceType;
      const request = new serviceType.TGetLogReq({
        operationHandle: this.OperationHandle,
      });

      timer.start();
      this.Client.GetLog(request, (err, res) => {
        timer.end();

        if (err) {
          resolve(err);
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
            res.status, 'ExecuteStatement operation fail,... !!');

          debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
          debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

          reject(new OperationError(message));
        } else {
          resolve(res.log);
        }
      });
    }
  }

  getLogOnHive(resolve, reject) {
    if (!this.OperationHandle) {
      reject(new OperationError('Invalid operationHandle in GetLog, on Hive'));
    } else {
      const timer = new Timer();
      const serviceType = this.Conn.IDL.ServiceType;
      const request = new serviceType.TFetchResultsReq({
        operationHandle: this.OperationHandle,
        orientation: serviceType.TFetchOrientation.FETCH_NEXT,
        maxRows: this.Configure.MaxRows,
        fetchType: HS2Util.FETCH_TYPE.LOG,
      });

      timer.start();
      this.Client.FetchResults(request, (err, res) => {
        timer.end();

        if (err) {
          reject(new OperationError(err));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
            res.status, 'ExecuteStatement operation fail,... !!');

          debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
          debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

          reject(new OperationError(message));
        } else {
          const [first] = res.results.columns;

          if (!first) {
            reject(null, 'HiveServer2 not response log');
          } else {
            resolve(null, first.stringVal.values.join('\n'));
          }
        }
      });
    }
  }

  getOperationStatus() {
    return new Promise((resolve, reject) => {
      if (!this.OperationHandle) {
        reject(new OperationError('Invalid operationHandle in getOperationStatus'));
      } else {
        debug('GetOperationStatus -> function start');

        const timer = new Timer();
        const serviceType = this.Conn.IDL.ServiceType;
        const request = new serviceType.TGetOperationStatusReq({
          operationHandle: this.OperationHandle,
        });

        debug('GetOperationStatus -> request start');

        timer.start();
        this.Client.GetOperationStatus(request, (err, res) => {
          timer.end();

          debug('GetOperationStatus -> server response');

          if (err) {
            debug('GetOperationStatus -> Error(', err.message, ')');
            reject(new OperationError(err));
          } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
            const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
              res.status, 'ExecuteStatement operation fail,... !!');

            debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
            debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

            reject(new OperationError(message));
          } else {
            reject(null, res.operationState);
          }
        });
      }
    });
  }

  getSchema() {
    return new Promise((resolve, reject) => {
      if (!this.OperationHandle) {
        reject(new OperationError('Invalid operationHandle in getOperationStatus'));
      } else {
        debug('GetSchema -> function start');

        const timer = new Timer();
        const serviceType = this.Conn.IDL.ServiceType;
        const req = new serviceType.TGetResultSetMetadataReq({
          operationHandle: this.OperationHandle,
        });

        timer.start();
        this.Client.GetResultSetMetadata(req, (err, res) => {
          timer.end();

          if (err) {
            reject(err);
          } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
            const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
              res.status, 'ExecuteStatement operation fail,... !!');

            debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
            debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

            reject(new OperationError(message));
          } else if (res.schema) {
            resolve(res.schema.columns.map(column => ({
              type: HS2Util.getType(serviceType, column.typeDesc),
              columnName: column.columnName,
              comment: column.comment,
            })));
          } else {
            reject(null);
          }
        });
      }
    });
  }

  fetchBlock() {
    return new Promise((resolve, reject) => {
      if (!this.OperationHandle) {
        reject(new OperationError('Invalid operationHandle in fetchBlock'));
      } else {
        debug('FetchBlock -> function start');

        const timer = new Timer();
        const serviceType = this.Conn.IDL.ServiceType;
        const request = new serviceType.TFetchResultsReq({
          operationHandle: this.operationHandle,
          orientation: serviceType.TFetchOrientation.FETCH_NEXT,
          maxRows: this.Configure.MaxRows,
          fetchType: HS2Util.FETCH_TYPE.ROW,
        });

        debug('FetchBlock -> request start');

        timer.start();
        this.Client.FetchResults(request, (err, res) => {
          timer.end();

          debug('FetchBlock -> request responsed');

          if (err) {
            debug('FetchBlock -> response error, ', err.message);
            resolve(new OperationError(err));
          } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
            const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
              res.status, 'ExecuteStatement operation fail,... !!');

            debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
            debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

            reject(new OperationError(message));
          } else {
            try {
              if (!this.TypeCache) {
                this.TypeCache = HS2Util.makeStoredType(res.results);
              }

              const typeCache = this.TypeCache;
              const firstColumn = _(res.results.columns).first();
              const firstType = _(typeCache).first();
              const fetchLength = firstColumn[firstType].values.length;

              debug('HiveServer2 1.2.x under, not support hasMoreRows');
              debug('FetchBlock -> hasMoreRows: ', res.hasMoreRows);

              if (fetchLength) {
                resolve({
                  hasMoreRows: Boolean(fetchLength),
                  rows: HS2Util.makeArray(this.Configure, typeCache, res.results.columns),
                });
              } else {
                this.TypeCache = null;

                resolve(null, {
                  hasMoreRows: Boolean(fetchLength),
                  rows: [],
                });
              }
            } catch (catchErr) {
              reject(new FetchError(catchErr));
            }
          }
        });
      }
    });
  }

  close() {
    return new Promise((resolve, reject) => {
      if (!this.OperationHandle) {
        reject(new OperationError('Invalid operationHandle in close'));
      } else {
        debug('CloseOperation -> function start');

        const timer = new Timer();
        const serviceType = this.Conn.IDL.ServiceType;
        const req = new serviceType.TCloseOperationReq({
          operationHandle: this.OperationHandle,
        });

        timer.start();
        this.Client.CloseOperation(req, (err, res) => {
          timer.end();

          if (err) {
            debug('CloseOperation -> response error, ', err.message);
            reject(new OperationError(err));
          } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
            const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
              res.status, 'ExecuteStatement operation fail,... !!');

            debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
            debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

            reject(new OperationError(message));
          } else {
            resolve(true);
          }
        });
      }
    });
  }
}

module.exports = HiveCursor;
