'use strict';

/*
 * I don't recommend this style, because First, callback version is deprecate 0.3.0.
 * Second, use latest node.js LTS version (2015/11/24, LTS node.js version is 4.2.2)
 * that is more secure, faster etc. Third, latest thrift(2015/11/25 latest thrift version
 * 0.9.4) compile promise spec. . So, jshs2 0.3.0 has deprecate callback version.
 *
 */

process.env.DEBUG = 'jshs2:*';

var _ = require('lodash');
var fs = require('fs');
var util = require('util');
var co = require('co');
var EventEmitter = require('events').EventEmitter;
var should = require('chai').should();
var debug = require('debug')('jshs2:OperationTestSuite');
var async = require('async');

var jshs2 = require('../index.js');
var hs2util = require('../lib/Common/HS2Util');
var Connection = jshs2.CConnection;
var Configuration = jshs2.Configuration;

function wait (_cursor, callback) {
  var cursor = _cursor;
  var ee = new EventEmitter();

  ee.waitCount = 0;

  ee.on('wait', function () {
    cursor.getOperationStatus(function (err, status) {
      if (err) {
        setImmediate(function () {
          ee.emit('error', err);
        });
      } else {
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
      }
    });
  });

  ee.on('finish', function () {
    callback(null, true);
  });

  ee.on('error', function (err) {
    callback(err);
  });

  ee.emit('wait');
}

function waitAndLog (_cursor, callback) {
  var cursor = _cursor;
  var ee = new EventEmitter();

  ee.waitCount = 0;

  ee.on('wait', function () {
    cursor.getOperationStatus(function (err, status) {
      if (err) {
        setImmediate(function () {
          ee.emit('error', err);
        });
      } else {
        cursor.getLog(function (err, log) {
          if (err) {
            setImmediate(function () {
              ee.emit('error', err);
            });
          } else {
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
          }

        });
      }
    });
  });

  ee.on('finish', function () {
    callback(null, true);
  });

  ee.on('error', function (err) {
    callback(err);
  });

  ee.emit('wait');
}

describe('ThriftDriverTest', function () {
  var testConf = {};
  var configuration, connection, cursor, serviceType;

  before(function (done) {
    async.waterfall([
      function clusterJsonReadFile (callback) {
        fs.readFile('./cluster.json', function (err, buf) {
          if (err) {
            callback(err);
          } else {
            testConf.config = JSON.parse(buf.toString());

            callback(null);
          }
        });
      },
      function createConfiguration (callback) {
        testConf.jshs2 = {};
        testConf.jshs2.auth = testConf.config[testConf.config.use].auth;
        testConf.jshs2.host = testConf.config[testConf.config.use].host;
        testConf.jshs2.port = testConf.config[testConf.config.use].port;
        testConf.jshs2.timeout = testConf.config[testConf.config.use].timeout;
        testConf.jshs2.username = testConf.config[testConf.config.use].username;
        testConf.jshs2.hiveType = testConf.config[testConf.config.use].hiveType;
        testConf.jshs2.hiveVer = testConf.config[testConf.config.use].hiveVer;
        testConf.jshs2.cdhVer = testConf.config[testConf.config.use].cdhVer;
        testConf.jshs2.thriftVer = testConf.config[testConf.config.use].thriftVer;

        testConf.jshs2.maxRows = testConf.config[testConf.config.use].maxRows;
        testConf.jshs2.nullStr = testConf.config[testConf.config.use].nullStr;

        callback(null);
      },
      function initializeConfiguration (callback) {
        configuration = new Configuration(testConf.jshs2);

        configuration.cb$initialize(function (err) {
          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        });
      },
      function createConnection (callback) {
        connection = new Connection(configuration);

        connection.connect(function (err, _cursor) {
          if (err) {
            callback(err);
          } else {
            cursor = _cursor;
            callback(null);
          }
        });
      }
    ], function (err) {
      if (err) {
        debug('Error caused from Callback Test before, ...');
        debug(err.message);
        debug(err.stack);
      } else {
        setImmediate(function () {
          done();
        });
      }
    });
  });

  after(function (done) {
    async.waterfall([
      function closeCursor (callback) {
        cursor.close(function (err) {
          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        });
      },
      function connectionClose (callback) {
        connection.close(function (err) {
          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        });
      }
    ], function (err) {
      if (err) {
        debug('Error caused from Callback Test after, ...');
        debug(err.message);
        debug(err.stack);
      } else {
        setImmediate(function () {
          done();
        });
      }
    });
  });

  it('HiveDriver Promise Test Async', function (done) {
    async.waterfall([
      function executeStep (callback) {
        cursor.execute(testConf.config.Query.query, true, function (err, result) {
          debug('Execute, hasMoreRow -> ', result);

          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        });
      },
      function waitAndLogStep (callback) {
        waitAndLog(cursor, function (err) {
          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        });
      },
      function schemaStep (callback) {
        cursor.getSchema(function (err, schema) {
          if (err) {
            callback(err);
          } else {
            debug('schema -> ', schema);

            callback(null);
          }
        });
      },
      function fetchBlockStep (callback) {
        cursor.fetchBlock(function (err, result) {
          if (err) {
            callback(err);
          } else {
            debug('rows ->', result.rows.length);
            debug('rows ->', result.hasMoreRows);

            callback(null, result.rows);
          }
        });
      }
    ], function (err, rows) {
      if (err) {
        debug('Error caused, ');
        debug('message:  ' + err.message);
        debug('message:  ' + err.stack);

        setImmediate(function () {
          should.not.exist(err);
        });
      } else {
        setImmediate(function () {
          should.not.equal(rows.length > 1);

          done();
        });
      }
    });
  });

  it('HiveDriver Promise Test Sync', function (done) {
    async.waterfall([
      function executeStep (callback) {
        cursor.execute(testConf.config.Query.query, false, function (err, result) {
          debug('Execute, hasMoreRow -> ', result);

          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        });
      },
      function schemaStep (callback) {
        cursor.getSchema(function (err, schema) {
          if (err) {
            callback(err);
          } else {
            debug('schema -> ', schema);

            callback(null);
          }
        });
      },
      function fetchBlockStep (callback) {
        cursor.fetchBlock(function (err, result) {
          if (err) {
            callback(err);
          } else {
            debug('rows ->', result.rows.length);
            debug('rows ->', result.hasMoreRows);

            callback(null, result.rows);
          }
        });
      }
    ], function (err, rows) {
      if (err) {
        debug('Error caused, ');
        debug('message:  ' + err.message);
        debug('message:  ' + err.stack);

        setImmediate(function () {
          should.not.exist(err);
        });
      } else {
        setImmediate(function () {
          should.not.equal(rows.length > 1);

          done();
        });
      }
    });
  });

  /* End of the Callback test */
});
