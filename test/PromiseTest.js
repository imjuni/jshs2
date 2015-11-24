'use strict';

process.env.DEBUG = 'jshs2:*';

var _ = require('lodash');
var fs = require('fs');
var util = require('util');
var co = require('co');
var EventEmitter = require('events').EventEmitter;
var should = require('chai').should();
var Promise = require('promise');
var debug = require('debug')('jshs2:OperationTestSuite');

var jshs2 = require('../index.js');
var hs2util = require('../lib/Common/HS2Util');
var Connection = jshs2.PConnection;
var Configuration = jshs2.Configuration;

describe('ThriftDriverTest', function () {
  // Test environment variable
  var testConf = {};
  var configuration, connection, cursor, serviceType;

  before(function (done) {
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

      testConf.config = config;
      testConf.jshs2 = options;

      configuration = new Configuration(testConf.jshs2);

      yield configuration.initialize();

      connection = new Connection(configuration);
      cursor = yield connection.connect();

      serviceType = cursor.getConfigure().getServiceType();

    }).then(function () {
      debug('before task complete, ...');

      setImmediate(function () {
        done();
      });
    }).catch(function (err) {
      debug('Error caused from before task, ...');
      debug(err.message);
      debug(err.stack);
    });
  });

  after(function (done) {
    co(function* () {
      yield cursor.close();

      yield connection.close();
    }).then(function () {
      setImmediate(function () {
        done();
      });
    }).catch(function (err) {
      debug('Error caused from after task, ...');
      debug(err.message);
      debug(err.stack);
    });
  });

  it('HiveDriver Promise Test Async', function (done) {
    co(function* () {
      var i, len, status, log;

      yield cursor.execute(testConf.config.Query.query);

      for (i = 0, len = 1000; i < len; i++) {
        status = yield cursor.getOperationStatus();
        log = yield cursor.getLog();

        debug('wait, status -> ', hs2util.getState(serviceType, status));
        debug('wait, log -> ', log);

        if (hs2util.isFinish(cursor, status)) {
          debug('Status -> ', status, ' -> stop waiting');

          break;
        }

        yield hs2util.pSleep(10000);
      }

      var schema = yield cursor.getSchema();

      debug('schema -> ', schema);

      var result = yield cursor.fetchBlock();

      debug('rows ->', result.rows.length);
      debug('rows ->', result.hasMoreRows);

      return result.rows;

    }).then(function (rows) {
      should.not.equal(rows.lenght > 1);

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

  it('HiveDriver Promise Test Sync', function (done) {
    co(function* () {
      yield cursor.execute(testConf.config.Query.query, false);

      var schema = yield cursor.getSchema();

      debug('schema -> ', schema);

      var result = yield cursor.fetchBlock();

      debug('rows ->', result.rows.length);
      debug('rows ->', result.hasMoreRows);

      return result.rows;

    }).then(function (rows) {
      should.not.equal(rows.length > 1);

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
