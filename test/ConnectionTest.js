(function () {
  'use strict';

  process.env.DEBUG = 'jshs2:connection,jshs2:cursor,jsh2:ConnectionTest,tcliservice:Service';

  var _ = require('lodash');
  var fs = require('fs');
  var util = require('util');
  var should = require('chai').Should();
  var debug = require('debug')('jsh2:ConnectionTestSuite');

  describe('ThriftDriverTest', function() {
  //  it('ImpalaDriverTest', function (done) {
  //    var async = require('async');
  //    var jshs2 = require('../index.js');
  //    var conf;
  //
  //    conf = JSON.parse(fs.readFileSync('cluster.json'))['impala'];
  //
  //    debug('HiveServerConnectionTest start,...');
  //
  //    var conn = new jshs2.Connections(conf);
  //    var cursor = conn.cursor();
  //    var limit = 2000;
  //    var sql = conf.query;
  //
  //    async.waterfall([
  //      function (callback) {
  //        debug('connect test start,...');
  //
  //        conn.connect(function (err) {
  //          callback(err);
  //        });
  //      },
  //      function (callback) {
  //        debug('execute test start,...');
  //
  //        cursor.execute(util.format(sql, limit), function (err) {
  //          callback(err);
  //        });
  //      },
  //      function (callback) {
  //        debug('get operation status test start,...');
  //
  //        cursor.getOperationStatus(function (err) {
  //          callback(err);
  //        });
  //      },
  ////      function (callback) {
  ////        debug('get operation status test start,...');
  ////
  ////        cursor.getLog(function (err) {
  ////          callback(err);
  ////        });
  ////      },
  //      function (callback) {
  //        try {
  //          cursor.getShcema(function (err, columns) {
  //            callback(err, columns);
  //          });
  //        } catch (err) {
  //          debug('error caused: ' + err.message);
  //        }
  //      },
  //      function (columns, callback) {
  //        cursor.jsonFetch(columns, function (err, result) {
  //          callback(err, columns, result);
  //        });
  //      },
  //      function (columns, result, callback) {
  //        debug('cursor cslose test start,...');
  //
  //        cursor.close(function (err) {
  //          callback(err, columns, result);
  //        });
  //      },
  //      function (columns, result, callback) {
  //        debug('connection close test start,...');
  //
  //        conn.close(function (err) {
  //          callback(err, columns, result);
  //        });
  //      }
  //    ], function (err, columns, result) {
  //      if (err) {
  //        debug('Error caused, ');
  //        debug('message:  ' + err.message);
  //      }
  //
  //      should.not.exist(err);
  //      (result.length === limit).should.be.true;
  //
  //      done();
  //    });
  //  });

    it('HiveDriverTest', function (done) {
      var async = require('async');
      var jshs2 = require('../index.js');

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
        thriftVer: '0.9.2',
        cdhVer: '5.3.0'
      };

      var cursorOpt = {
        maxRows: 5120
      };

      var conn = new jshs2.Connection(connOpt);
      var limit = 2000;
      var sql = config.query;

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
          cursor.getSchema(function (err, columns) {
            callback(err, cursor, columns);
          });
        },
        function (cursor, columns, callback) {
          cursor.jsonFetch(columns, function (err, result) {
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
})();

