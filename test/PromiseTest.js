'use strict';

process.env.DEBUG = 'jshs2:*';

var _ = require('lodash');
var fs = require('fs');
var util = require('util');
var co = require('co');
var EventEmitter = require('events').EventEmitter;
var should = require('chai').Should();
var Promise = require('promise');
var debug = require('debug')('jshs2:OperationTestSuite');

var jshs2 = require('../index.js');
var hs2util = require('../lib/Common/HS2Util');

function wait (_cursor) {
  var cursor = _cursor;

  return new Promise(function (resolve, reject) {
    var ee = new EventEmitter();

    ee.waitCount = 0;

    ee.on('wait', function () {
      co(function* () {
        var status = yield cursor.getOperationStatus();
        var serviceType = cursor.getConfigure().getServiceType();

        debug('wait, status -> ', status);

        ee.waitCount = ee.waitCount + 1;

        if (status !== serviceType.TOperationState.FINISHED_STATE) {
          setTimeout(function () {
            ee.emit('wait');
          }, 2000);
        } else if (ee.waitCount > 100) {
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
        var serviceType = cursor.getConfigure().getServiceType();

        debug('wait, status -> ', hs2util.getState(serviceType, status));
        debug('wait, log -> ', log);

        ee.waitCount = ee.waitCount + 1;

        if (status !== serviceType.TOperationState.FINISHED_STATE) {
          setTimeout(function () {
            ee.emit('wait');
          }, 2000);
        } else if (ee.waitCount > 100) {
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
    var Connection = jshs2.PConnection;
    var Configuration = jshs2.Configuration;

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

      options.auth = config[config.use].auth;
      options.host = config[config.use].host;
      options.port = config[config.use].port;
      options.timeout = config[config.use].timeout;
      options.username = config[config.use].username;
      options.hiveType = config[config.use].hiveType;
      options.hiveVer = config[config.use].hiveVer;
      options.cdhVer = config[config.use].cdhVer;
      options.thriftVer = config[config.use].thriftVer;

      options.maxRows = config[config.use].maxRows;
      options.nullStr = config[config.use].nullStr;

      var configuration = new Configuration(options);

      yield configuration.initialize();

      var connection = new Connection(configuration);
      var cursor = yield connection.connect();

      yield cursor.execute(config.Query.query);
      yield waitAndLog(cursor);

      var schema = yield cursor.getSchema();

      debug('schema -> ', schema);

      var result = yield cursor.fetchBlock();

      debug('rows ->', result.rows.length);
      debug('rows ->', result.hasMoreRows);

      yield cursor.close();

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

  /* End of the Callback test */
});
