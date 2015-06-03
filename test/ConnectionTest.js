(function () {
  'use strict';

  process.env.DEBUG = 'jshs2:connection,jshs2:cursor,jsh2:ConnectionTestSuite,tcliservice:Service';

  var _ = require('lodash');
  var fs = require('fs');
  var util = require('util');
  var co = require('co');
  var async = require('async');
  var EventEmitter = require('events').EventEmitter;
  var should = require('chai').Should();
  var Promise = require('Promise');
  var debug = require('debug')('jsh2:ConnectionTestSuite');

  var jshs2 = require('../index.js');
  var hs2util = require('../lib/HS2Util');
  var IdlContainer = jshs2.IdlContainer;

  function wait (_cursor) {
    var cursor = _cursor;

    return new Promise(function (resolve, reject) {
      var ee = new EventEmitter();

      ee.waitCount = 0;

      ee.on('wait', function () {
        co(function* () {
          var status = yield cursor.getOperationStatus();
          var serviceType = IdlContainer.getServiceType();

          debug('wait, status -> ', status);

          ee.waitCount = ee.waitCount + 1;

          if (status !== serviceType.TOperationState.FINISHED_STATE) {
            setTimeout(function () {
              ee.emit('wait');
            }, 2000);
          } else if (ee.waitCount > 10) {
            ee.emit('error', new Error('Over exceed timeout'));
          } else {
            ee.emit('finish');
          }
        }).catch(function (err) {
          setImmediate(function () {
            ee.emit('error', err);
          });
        });
      });

      ee.on('finish', function () {
        resolve(true);
      });

      ee.on('error', function (err) {
        reject(err);
      });

      ee.emit('wait');
    });
  }

  function waitAndLog (_cursor) {
    var cursor = _cursor;

    return new Promise(function (resolve, reject) {
      var ee = new EventEmitter();

      ee.waitCount = 0;

      ee.on('wait', function () {
        co(function* () {
          var status = yield cursor.getOperationStatus();
          var log = yield cursor.getLog();
          var serviceType = IdlContainer.getServiceType();

          debug('wait, status -> ', hs2util.getState(serviceType, status));
          debug('wait, log -> ', log);

          ee.waitCount = ee.waitCount + 1;

          if (status !== serviceType.TOperationState.FINISHED_STATE) {
            setTimeout(function () {
              ee.emit('wait');
            }, 2000);
          } else if (ee.waitCount > 10) {
            ee.emit('error', new Error('Over exceed timeout'));
          } else {
            ee.emit('finish');
          }
        }).catch(function (err) {
          setImmediate(function () {
            ee.emit('error', err);
          });
        });
      });

      ee.on('finish', function () {
        resolve(true);
      });

      ee.on('error', function (err) {
        reject(err);
      });

      ee.emit('wait');
    });
  }

  describe('ThriftDriverTest', function () {
    it('HiveDriver Promise Test', function (done) {
      var Connection = jshs2.Connection.HS2PromiseConnection;
      var Configure = jshs2.Configure;

      co(function* () {
        var config = JSON.parse(yield new Promise(function (resolve, reject) {
          fs.readFile('./cluster.json', function (err, buf) {
            if (err) {
              reject(err);
            } else {
              resolve(buf);
            }
          });
        }));

        var options = {};

        options.auth = 'NOSASL';
        options.host = _.first(config.Hive.Servers);
        options.port = config.Hive.Port;
        options.timeout = config.Hive.LoginTimeout;
        options.username = config.Hive.username;
        options.hiveType = hs2util.HIVE_TYPE.HIVE;
        options.hiveVer = '1.2.0';
        options.thriftVer = '0.9.2';

        options.maxRows = 5120;
        options.nullStr = 'NULL';

        var configure = new Configure(options);

        yield IdlContainer.initialize(configure);

        var connection = new Connection(configure);
        var cursor = yield connection.connect();

        yield cursor.execute(config.Hive.query02);
        yield waitAndLog(cursor);
        var result = yield cursor.fetchBlock();

        debug('rows ->', result.rows.length);
        debug('rows ->', result.hasMoreRows);

        result = yield cursor.fetchBlock();

        debug('rows ->', result.rows.length);
        debug('rows ->', result.hasMoreRows);

        result = yield cursor.fetchBlock();

        debug('rows ->', result.rows.length);
        debug('rows ->', result.hasMoreRows);

        yield connection.close();

      }).then(function (res) {
        done();
      }).catch(function (err) {
        debug('Error caused, ');
        debug('message:  ' + err.message);
        debug('message:  ' + err.stack);

        setImmediate(function () {
          should.not.exist(err);
        });
      });
    });

    it('HiveDriver Callback Test', function (done) {
      var jshs2 = require('../index.js');
      var IdlContainer = jshs2.IdlContainer;
      var Connection = jshs2.Connection.HS2CallbackConnection;
      var Configure = jshs2.Configure;

      var options, config, configure, connection;

      async.waterfall([
        function (callback) {
          fs.readFile('./cluster.json', function (err, buf) {
            if (err) {
              callback(err);
            } else {
              callback(null, buf);
            }
          });
        },
        function (buf, callback) {
          config = JSON.parse(buf);

          options = {};

          options.auth = 'NOSASL';
          options.host = _.first(config.Hive.Servers);
          options.port = config.Hive.Port;
          options.timeout = config.Hive.LoginTimeout;
          options.username = config.Hive.username;
          options.hiveType = 'hive';
          options.hiveVer = '1.0.0';
          options.thriftVer = '0.9.2';

          options.maxRows = 5120;
          options.nullStr = 'NULL';

          configure = new Configure(options);

          IdlContainer.cb$initialize(configure, function (err) {
            if (err) {
              callback(err);
            } else {
              callback(null);
            }
          });
        },
        function (callback) {
          connection = new Connection(configure);

          connection.connect(function (err) {
            callback(err);
          });
        },
        function (callback) {
          connection.close(function (err) {
            callback(err);
          });
        }
      ], function (err) {
        setImmediate(function () {
          if (err) {
            debug('Error caused, ');
            debug('message:  ' + err.message);
            debug('message:  ' + err.stack);

            setImmediate(function () {
              should.not.exist(err);
            });
          } else {
            done();
          }
        });
      });
    });
  });

  //describe('ThriftDriverTest', function() {
  //  it('HiveDriverTest', function (done) {
  //    var async = require('async');
  //    var jshs2 = require('../index.js');
  //    var EventEmitter = require('events').EventEmitter;
  //
  //    debug('HiveServerConnectionTest start,...');
  //    var config = JSON.parse(fs.readFileSync('./cluster.json')).Hive;
  //
  //    debug(util.format('Host(all)   : %s', config.Servers));
  //    debug(util.format('Host(first) : %s', _.first(config.Servers)));
  //    debug(util.format('Port        : %s', config.Port));
  //    debug(util.format('Timeout     : %s', config.LoginTimeout));
  //
  //    var connOpt = {
  //      auth: 'NOSASL',
  //      host: _.first(config.Servers),
  //      port: config.Port,
  //      timeout: config.LoginTimeout,
  //      username: config.username,
  //      hiveType: 'cdh',
  //      hiveVer: '0.13.1',
  //      thriftVer: '0.9.2',
  //      cdhVer: '5.3.0'
  //    };
  //
  //    var cursorOpt = {
  //      maxRows: 5120,
  //      nullStr: 'null'
  //    };
  //
  //    var conn = new jshs2.Connection(connOpt);
  //    var limit = 2000;
  //    var sql = config.query;
  //
  //    function waiter (cursor, cb) {
  //      var ee = new EventEmitter();
  //      var i = 0;
  //
  //      ee.on('start', function (cursor) {
  //        cursor.getOperationStatus(function (err, status) {
  //          debug('OperationStatus: ' + status);
  //
  //          if (err) {
  //            ee.emit('error', err);
  //          } else if (status.toLowerCase() === 'running') {
  //            setTimeout(function () {
  //              ee.emit('start', cursor);
  //            }, 6000);
  //          } else if (i > 100) {
  //            ee.emit('error', new Error('Timeout, over then 60s'));
  //          } else {
  //            ee.emit('end', cursor);
  //          }
  //        });
  //      });
  //
  //      ee.on('error', function (err) {
  //        setImmediate(function () {
  //          cb(err);
  //        });
  //      });
  //
  //      ee.on('end', function (cursor) {
  //        setImmediate(function () {
  //          cb(null, cursor);
  //        });
  //      });
  //
  //      ee.emit('start', cursor);
  //    }
  //
  //    async.waterfall([
  //      function (callback) {
  //        debug('connect test start,...');
  //
  //        conn.connect(cursorOpt, function (err, cursor) {
  //          callback(err, cursor);
  //        });
  //      },
  //      function (cursor, callback) {
  //        debug('execute test start,...');
  //
  //        sql = sql.replace(/\$\{\{limit\}\}/g, limit.toString());
  //
  //        cursor.execute(sql, function (err) {
  //          callback(err, cursor);
  //        });
  //      },
  //      function (cursor, callback) {
  //        debug('get operation status test start,...');
  //
  //        cursor.getOperationStatus(function (err) {
  //          callback(err, cursor);
  //        });
  //      },
  //      function (cursor, callback) {
  //        debug('get log test start,...');
  //
  //        cursor.getLog(function (err) {
  //          callback(err, cursor);
  //        });
  //      },
  //      function (cursor, callback) {
  //        waiter(cursor, callback);
  //      },
  //      function (cursor, callback) {
  //        cursor.getSchema(function (err, columns) {
  //          callback(err, cursor, columns);
  //        });
  //      },
  //      function (cursor, columns, callback) {
  //        cursor.fetchBlock(function (err, hasMoreRow, result) {
  //          callback(err, cursor, columns, result);
  //        });
  //      },
  //      function (cursor, columns, result, callback) {
  //        debug('cursor cslose test start,...');
  //
  //        cursor.close(function (err) {
  //          callback(err, cursor, columns, result);
  //        });
  //      },
  //      function (cursor, columns, result, callback) {
  //        debug('connection close test start,...');
  //
  //        conn.close(function (err) {
  //          callback(err, cursor, columns, result);
  //        });
  //      }
  //    ], function (err, cursor, columns, result) {
  //
  //      setImmediate(function () {
  //        if (err) {
  //          debug('Error caused, ');
  //          debug('message:  ' + err.message);
  //          debug('message:  ' + err.stack);
  //        }
  //
  //        should.not.exist(err);
  //
  //        (result.length === limit).should.true();
  //
  //        done();
  //      });
  //    });
  //  });
  //});
})();

