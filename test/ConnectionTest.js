(function () {
  'use strict';

  process.env.DEBUG = 'jshs2:connection,jshs2:cursor,jsh2:ConnectionTestSuite,tcliservice:Service';

  var _ = require('lodash');
  var fs = require('fs');
  var util = require('util');
  var should = require('chai').Should();
  var debug = require('debug')('jsh2:ConnectionTestSuite');

  describe('ThriftDriverTest', function() {
    it('HiveDriverTest', function (done) {
      var async = require('async');
      var jshs2 = require('../index.js');
      var EventEmitter = require('events').EventEmitter;

      debug('HiveServerConnectionTest start,...');
      var config = JSON.parse(fs.readFileSync('./cluster.json')).Hive;

      debug(util.format('Host(all)   : %s', config.Servers));
      debug(util.format('Host(first) : %s', _.first(config.Servers)));
      debug(util.format('Port        : %s', config.Port));
      debug(util.format('Timeout     : %s', config.LoginTimeout));

      var connOpt = {
        auth: 'NOSASL',
        host: _.first(config.Servers),
        port: config.Port,
        timeout: config.LoginTimeout,
        username: config.username,
        hiveType: 'cdh',
        hiveVer: '0.13.1',
        thriftVer: '0.9.2',
        cdhVer: '5.3.0'
      };

      var cursorOpt = {
        maxRows: 5120,
        nullStr: 'null'
      };

      var conn = new jshs2.Connection(connOpt);
      var limit = 2000;
      var sql = config.query;

      function waiter (cursor, cb) {
        var ee = new EventEmitter();
        var i = 0;

        ee.on('start', function (cursor) {
          cursor.getOperationStatus(function (err, status) {
            debug('OperationStatus: ' + status);

            if (err) {
              ee.emit('error', err);
            } else if (status.toLowerCase() === 'running') {
              setTimeout(function () {
                ee.emit('start', cursor);
              }, 6000);
            } else if (i > 100) {
              ee.emit('error', new Error('Timeout, over then 60s'));
            } else {
              ee.emit('end', cursor);
            }
          });
        });

        ee.on('error', function (err) {
          setImmediate(function () {
            cb(err);
          });
        });

        ee.on('end', function (cursor) {
          setImmediate(function () {
            cb(null, cursor);
          });
        });

        ee.emit('start', cursor);
      }

      async.waterfall([
        function (callback) {
          debug('connect test start,...');

          conn.connect(cursorOpt, function (err, cursor) {
            callback(err, cursor);
          });
        },
        function (cursor, callback) {
          debug('execute test start,...');

          sql = sql.replace(/\$\{\{limit\}\}/g, limit.toString());

          cursor.execute(sql, function (err) {
            callback(err, cursor);
          });
        },
        function (cursor, callback) {
          debug('get operation status test start,...');

          cursor.getOperationStatus(function (err) {
            callback(err, cursor);
          });
        },
        function (cursor, callback) {
          debug('get log test start,...');

          cursor.getLog(function (err) {
            callback(err, cursor);
          });
        },
        function (cursor, callback) {
          waiter(cursor, callback);
        },
        function (cursor, callback) {
          cursor.getSchema(function (err, columns) {
            callback(err, cursor, columns);
          });
        },
        function (cursor, columns, callback) {
          cursor.fetchBlock(function (err, hasMoreRow, result) {
            callback(err, cursor, columns, result);
          });
        },
        function (cursor, columns, result, callback) {
          debug('cursor cslose test start,...');

          cursor.close(function (err) {
            callback(err, cursor, columns, result);
          });
        },
        function (cursor, columns, result, callback) {
          debug('connection close test start,...');

          conn.close(function (err) {
            callback(err, cursor, columns, result);
          });
        }
      ], function (err, cursor, columns, result) {

        setImmediate(function () {
          if (err) {
            debug('Error caused, ');
            debug('message:  ' + err.message);
            debug('message:  ' + err.stack);
          }

          should.not.exist(err);

          (result.length === limit).should.true();

          done();
        });
      });
    });
  });
})();

