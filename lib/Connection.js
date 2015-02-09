(function () {
  'use strict';

  var _ = require('lodash');
  var debug = require('debug')('jshs2:connection');
  var thrift = require('thrift');
  var Cursor = require('./Cursor');
  var util = require('util');
  var hsutil = require('./HS2Util');
  var TCLIService = require('../idl/gen-nodejs/TCLIService.js');
  var TCLIServiceTypes = require('../idl/gen-nodejs/TCLIService_types.js');
  var ConnectionError = require('./HS2Error').ConnectionError;
  //var _ = (function (lodash) { return (lodash.str = require('underscore.string')); })(require('lodash'));

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

    _.assign($opt, opt);

    $opt.port = $opt.port || 10000;
    $opt.timeout = $opt.timeout || 5;

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
      var req = new TCLIServiceTypes.TOpenSessionReq();
      var client = thrift.createClient(TCLIService, conn);

      $store.conn = conn;
      $store.client = client;

      req.username = $opt.username || 'anonymous';
      req.password = $opt.password || '';

      conn.once('error', function (err) {
        debug(util.format('Error caused, from Connection.connect'));
        debug(err.message);
        debug(err.stack);

        return cb(err);
      });

      client.OpenSession(req, function (err, res) {
        if (err) {
          return cb(new ConnectionError(err.message));
        } else if (res.status.statusCode === TCLIServiceTypes.TStatusCode.ERROR_STATUS) {
          return cb(new ConnectionError(res.status.errorMessage));
        } else {
          $store.sessionHandle = res.sessionHandle;
          $store.configuration = res.configuration;
          $store.serverProtocolVersion = res.serverProtocolVersion;

          var cursor = new Cursor($store.client, $store.sessionHandle, cursorOpt);

          return cb(err, cursor);
        }
      });
    }

    function close (cb) {
      if (_.isEmpty($store.sessionHandle)) {
        $store.conn.end();
      } else {
        var req = new TCLIServiceTypes.TCloseSessionReq({
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