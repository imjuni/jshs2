'use strict';

process.env.DEBUG = 'jshs2:*';

var _ = require('lodash');
var fs = require('fs');
var util = require('util');
var co = require('co');
var EventEmitter = require('events').EventEmitter;
var should = require('chai').Should();
var debug = require('debug')('jshs2:OperationTestSuite');
var async = require('async');

var jshs2 = require('../index.js');
var hs2util = require('../lib/Common/HS2Util');

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
  it('HiveDriver Promise Test', function (done) {
    var Connection = jshs2.CConnection;
    var Configuration = jshs2.Configuration;
    var options = {};

    async.waterfall([
      function loadTestEnvStep (callback) {
        fs.readFile('./cluster.json', function (err, buf) {
          if (err) {
            callback(err);
          } else {
            var testEnv = JSON.parse(buf.toString());

            callback(null, testEnv);
          }
        });
      },
      function configurationStep (testEnv, callback) {
        options.auth = testEnv[testEnv.use].auth;
        options.host = testEnv[testEnv.use].host;
        options.port = testEnv[testEnv.use].port;
        options.timeout = testEnv[testEnv.use].timeout;
        options.username = testEnv[testEnv.use].username;
        options.hiveType = testEnv[testEnv.use].hiveType;
        options.hiveVer = testEnv[testEnv.use].hiveVer;
        options.cdhVer = testEnv[testEnv.use].cdhVer;
        options.thriftVer = testEnv[testEnv.use].thriftVer;

        options.maxRows = testEnv[testEnv.use].maxRows;
        options.nullStr = testEnv[testEnv.use].nullStr;

        var configuration = new Configuration(options);

        configuration.cb$initialize(function (err) {
          if (err) {
            callback(err);
          } else {
            callback(null, testEnv, configuration);
          }
        });
      },
      function connectStep (testEnv, configuration, callback) {
        var connection = new Connection(configuration);

        connection.connect(function (err, cursor) {
          if (err) {
            callback(err);
          } else {
            callback(null, testEnv, configuration, connection, cursor);
          }
        });
      },
      function executeStep (testEnv, configuration, connection, cursor, callback) {
        cursor.execute(testEnv.Query.query, false, function (err, result) {
          debug('Execute, hasMoreRow -> ', result);

          if (err) {
            callback(err);
          } else {
            callback(null, testEnv, configuration, connection, cursor);
          }
        });
      },
      function waitAndLogStep (testEnv, configuration, connection, cursor, callback) {
        waitAndLog(cursor, function (err) {
          if (err) {
            callback(err);
          } else {
            callback(null, testEnv, configuration, connection, cursor);
          }
        });
      },
      function schemaStep (testEnv, configuration, connection, cursor, callback) {
        cursor.getSchema(function (err, schema) {
          if (err) {
            callback(err);
          } else {
            debug('schema -> ', schema);

            callback(null, testEnv, configuration, connection, cursor);
          }
        });
      },
      function fetchBlockStep (testEnv, configuration, connection, cursor, callback) {
        cursor.fetchBlock(function (err, result) {
          if (err) {
            callback(err);
          } else {
            debug('rows ->', result.rows.length);
            debug('rows ->', result.hasMoreRows);

            callback(null, testEnv, configuration, connection, cursor);
          }
        });
      },
      function cursorCloseStep (testEnv, configuration, connection, cursor, callback) {
        cursor.close(function (err) {
          if (err) {
            callback(err);
          } else {
            callback(null, testEnv, configuration, connection, cursor);
          }
        });
      },
      function connectionCloseStep (testEnv, configuration, connection, cursor, callback) {
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
        debug('Error caused, ');
        debug('message:  ' + err.message);
        debug('message:  ' + err.stack);

        setImmediate(function () {
          should.not.exist(err);
        });
      } else {
        setImmediate(function () {
          done();
        });
      }
    });
  });

  /* End of the Callback test */
});
