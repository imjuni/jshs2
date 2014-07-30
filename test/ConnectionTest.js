process.env.DEBUG = 'jshs2:connection,jshs2:cursor,jsh2:ConnectionTest,tcliservice:Service';

var should = require('should');
var debug = require('debug')('jsh2:ConnectionTest');
var util = require('util');

describe('ThriftDriverTest', function() {
  it('ImpalaDriverTest', function (done) {
    var async = require('async');
    var jshs2 = require('../index.js');

    debug('HiveServerConnectionTest start,...');

    var impala = {
      host: 'localhost',
      port: '21050',
      auth: 'NOSASL'
    };

    var conn = new jshs2.Connections(impala);
    var cursor = conn.cursor();
    var limit = 3000;
    var sql = 'select 1';  // enter your query

    async.waterfall([
      function (callback) {
        debug('connect test start,...');

        conn.connect(function (err) {
          callback(err);
        });
      },
      function (callback) {
        debug('execute test start,...');

        cursor.execute(util.format(sql, limit), function (err) {
          callback(err);
        });
      },
      function (callback) {
        debug('get operation status test start,...');

        cursor.getOperationStatus(function (err) {
          callback(err);
        });
      },
      function (callback) {
        debug('get operation status test start,...');

        cursor.getLog(function (err) {
          callback(err);
        });
      },
      function (callback) {
        cursor.getShcema(function (err, columns) {
          callback(err, columns);
        });
      },
      function (columns, callback) {
        cursor.jsonFetch(columns, function (err, result) {
          callback(err, columns, result);
        });
      },
      function (columns, result, callback) {
        debug('cursor cslose test start,...');

        cursor.close(function (err) {
          callback(err, columns, result);
        });
      },
      function (columns, result, callback) {
        debug('connection close test start,...');

        conn.close(function (err) {
          callback(err, columns, result);
        });
      }
    ], function (err, columns, result) {
      if (err) {
        debug('Error caused, ');
        debug('message:  ' + err.message);
      }

      should.not.exist(err);
      (result.length === limit).should.be.true;

      done();
    });
  });

  it('HiveDriverTest', function (done) {
    var async = require('async');
    var jshs2 = require('../index.js');

    debug('HiveServerConnectionTest start,...');

    var hive = {
      host: 'localhost',
      port: '10000',
      auth: 'NOSASL'
    };

    var conn = new jshs2.Connections(hive);
    var cursor = conn.cursor();
    var limit = 3000;
    var sql = 'select 1'; // enter your query

    async.waterfall([
      function (callback) {
        debug('connect test start,...');

        conn.connect(function (err) {
          callback(err);
        });
      },
      function (callback) {
        debug('execute test start,...');

        cursor.execute(util.format(sql, limit), function (err) {
          callback(err);
        });
      },
      function (callback) {
        debug('get operation status test start,...');

        cursor.getOperationStatus(function (err) {
          callback(err);
        });
      },
      function (callback) {
        debug('get operation status test start,...');

        cursor.getLog(function (err) {
          callback(err);
        });
      },
      function (callback) {
        cursor.getShcema(function (err, columns) {
          callback(err, columns);
        });
      },
      function (columns, callback) {
        cursor.jsonFetch(columns, function (err, result) {
          callback(err, columns, result);
        });
      },
      function (columns, result, callback) {
        debug('cursor cslose test start,...');

        cursor.close(function (err) {
          callback(err, columns, result);
        });
      },
      function (columns, result, callback) {
        debug('connection close test start,...');

        conn.close(function (err) {
          callback(err, columns, result);
        });
      }
    ], function (err, columns, result) {
      if (err) {
        debug('Error caused, ');
        debug('message:  ' + err.message);
      }

      should.not.exist(err);
      (result.length === limit).should.be.true;

      done();
    });
  });
});