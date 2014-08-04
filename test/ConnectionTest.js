process.env.DEBUG = 'jshs2:connection,jshs2:cursor,jsh2:ConnectionTest,tcliservice:Service';

var should = require('should')
  debug = require('debug')('jsh2:ConnectionTest'),
  util = require('util'),
  fs = require('fs');

describe('ThriftDriverTest', function() {
  it('ImpalaDriverTest', function (done) {
    var async = require('async'),
      jshs2 = require('../index.js'),
      conf;

    conf = JSON.parse(fs.readFileSync('cluster.json'))['impala'];

    debug('HiveServerConnectionTest start,...');

    var conn = new jshs2.Connections(conf),
      cursor = conn.cursor(),
      limit = 2000,
      sql = conf.query;

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
//      function (callback) {
//        debug('get operation status test start,...');
//
//        cursor.getLog(function (err) {
//          callback(err);
//        });
//      },
      function (callback) {
        try {
          cursor.getShcema(function (err, columns) {
            callback(err, columns);
          });
        } catch (err) {
          debug('error caused: ' + err.message);
        }
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
    var async = require('async'),
      jshs2 = require('../index.js'),
      conf;

    debug('HiveServerConnectionTest start,...');
    conf = JSON.parse(fs.readFileSync('cluster.json'))['impala'];

    var conn = new jshs2.Connections(conf),
      cursor = conn.cursor(),
      limit = 2000,
      sql = conf.query;

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