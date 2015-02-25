(function () {
  'use strict';

  var _ = require('lodash');
  var debug = require('debug')('jshs2:connection');
  var thrift = require('thrift');
  var Cursor = require('./Cursor');
  var util = require('util');
  var ConnectionError = require('./HS2Error').ConnectionError;

  var authMechanisms = {
    NOSASL: {
      connection: thrift.createConnection,
      transport: thrift.TBufferedTransport
    },
    PLAIN: {
      connection: thrift.createConnection,
      transport: thrift.TBufferedTransport
    }
  };

  function Connection (opt) {
    if (!opt.host) {
      throw new ConnectionError('Invalid Host');
    }

    var $Connection = {};
    var $opt = {};
    var $store = {};
    var $service = {};

    _.assign($opt, opt);

    $opt.port = $opt.port || 10000;
    $opt.timeout = $opt.timeout || 5;
    $opt.hiveType = $opt.hiveType || 'HIVE';
    $opt.hiveVer = $opt.hiveVer || '0.13.1';
    $opt.thriftVer = $opt.thriftVer || '0.9.2';
    $opt.cdhVer = $opt.cdhVer || '5.3.0';

    $opt.hiveType = $opt.hiveType.toUpperCase();

    $opt.dir = util.format('Thrift_%s_Hive_%s', $opt.thriftVer, $opt.hiveVer);
    $opt.dir = ($opt.hiveType === 'CDH') ? util.format('%s_CDH_%s', $opt.dir, $opt.cdhVer) : $opt.dir;

    $service.TCLIService = require(util.format('../idl/%s/TCLIService.js', $opt.dir));
    $service.TCLIServiceTypes = require(util.format('../idl/%s/TCLIService_types.js', $opt.dir));

    debug('Create Hs2Connection Instance');
    debug(util.format('Host    : %s', $opt.host));
    debug(util.format('Port    : %s', $opt.port));
    debug(util.format('auth    : %s', $opt.auth));
    debug(util.format('Timeout : %s', $opt.timeout));

    function getOptions () {
      return $opt;
    }

    function getClient () {
      return $store.client || null;
    }

    function getSessionHandle () {
      return $store.sessionHandle || null;
    }

    function connect (cursorOpt, cb) {
      debug(util.format('Start, Connect -> %s', $opt.host));

      var Connection = authMechanisms[$opt.auth.toUpperCase()].connection;
      var connOpts = {
        timeout: $opt.timeout,
        transport: authMechanisms[$opt.auth.toUpperCase()].transport
      };

      var conn = new Connection($opt.host, $opt.port, connOpts);
      var req = new $service.TCLIServiceTypes.TOpenSessionReq();
      var client = thrift.createClient($service.TCLIService, conn);
      var $cursorOpt = {};

      $store.conn = conn;
      $store.client = client;

      req.username = $opt.username || 'anonymous';
      req.password = $opt.password || '';

      _.assign($cursorOpt, cursorOpt);

      $cursorOpt.connOpt = {};
      _.assign($cursorOpt.connOpt, $opt);

      conn.once('error', function (err) {
        debug(util.format('Error caused, from Connection.connect'));
        debug(err.message);
        debug(err.stack);

        return cb(err);
      });

      client.OpenSession(req, function (err, res) {
        if (err) {
          return cb(new ConnectionError(err.message));
        } else if (res.status.statusCode === $service.TCLIServiceTypes.TStatusCode.ERROR_STATUS) {
          return cb(new ConnectionError(res.status.errorMessage));
        } else {
          $store.sessionHandle = res.sessionHandle;
          $store.configuration = res.configuration;
          $store.serverProtocolVersion = res.serverProtocolVersion;

          var cursor = new Cursor($store.client, $store.sessionHandle, $cursorOpt);

          return cb(err, cursor);
        }
      });
    }

    function close (cb) {
      if (_.isEmpty($store.sessionHandle)) {
        $store.conn.end();
      } else {
        var req = new $service.TCLIServiceTypes.TCloseSessionReq({
          sessionHandle: $store.sessionHandle
        });

        $store.client.CloseSession(req, function (err) {
          $store.conn.end();

          cb(err);
        });
      }
    }

    $Connection.getOptions = getOptions;
    $Connection.getClient = getClient;
    $Connection.getSessionHandle = getSessionHandle;

    $Connection.connect = connect;
    $Connection.close = close;

    return $Connection;
  }

  module.exports = Connection;
}());