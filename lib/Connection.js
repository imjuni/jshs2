(function () {
  'use strict';

  var _ = require('lodash');
  var debug = require('debug')('jshs2:connection');
  var thrift = require('thrift');
  var Cursor = require('./Cursor');
  var util = require('util');
  var Promise = require('Promise');
  var ConnectionError = require('./HS2Error').ConnectionError;
  var IdlFactory = require('./IdlFactory');
  var IdlContainer = require('./IdlContainer');

  var authMechanisms = {
    nosasl: {
      connection: thrift.createConnection,
      transport: thrift.TBufferedTransport
    },
    plain: {
      connection: thrift.createConnection,
      transport: thrift.TBufferedTransport
    }
  };

  function HS2Connection (_configure) {
    if (!IdlContainer .getIsInit()) {
      throw new Error('Can\'t use Connection without IdlContainer initialization');
    }

    this.sessionHandle = null;
    this.connectionConfiguration = null;
    this.serverProtocolVersion = null;
    this.configure = _configure;
    this.client = null;
  }

  HS2Connection.prototype.getConfigure = function getConfigure () {
    return this.configure;
  };

  HS2Connection.prototype.setSessionHandle = function setSessionHandle (sessionHandle) {
    this.sessionHandle = sessionHandle;
  };

  HS2Connection.prototype.getSessionHandle = function getSessionHandle () {
    return this.sessionHandle;
  };

  HS2Connection.prototype.setConnectionConfiguration = function setConnectionConfiguration (connectionConfiguration) {
    this.connectionConfiguration = connectionConfiguration;
  };

  HS2Connection.prototype.getConnectionConfiguration = function getConnectionConfiguration () {
    return this.connectionConfiguration;
  };

  HS2Connection.prototype.setServerProtocolVersion = function setServerProtocolVersion (serverProtocolVersion) {
    this.serverProtocolVersion = serverProtocolVersion;
  };

  HS2Connection.prototype.getServerProtocolVersion = function getServerProtocolVersion () {
    return this.serverProtocolVersion;
  };

  HS2Connection.prototype.getClient = function getClient () {
    return this.client;
  };

  function HS2PromiseConnection (_configure) {
    HS2Connection.call(this, _configure);
  }

  util.inherits(HS2PromiseConnection, HS2Connection);

  HS2PromiseConnection.prototype.connect = function connect () {
    var that = this;

    return new Promise(function (resolve, reject) {
      debug('Initialize, Connect -> ', that.configure.getHost());
      debug('Initialize, Auth    -> ', that.configure.getAuth());

      var Connection = authMechanisms[that.configure.getAuth()].connection;
      var service = IdlContainer.getService();
      var serviceType = IdlContainer.getServiceType();
      var options = {};

      options.timeout = that.configure.getTimeout();
      options.transport = authMechanisms[that.configure.getAuth()].transport;

      that.connection = new Connection(that.configure.getHost(), that.configure.getPort(), options);
      that.client = thrift.createClient(service, that.connection);

      var req = new serviceType.TOpenSessionReq();

      req.username = that.configure.getUsername() || 'anonymous';
      req.password = that.configure.getPassword() || '';

      debug('Initialize, username -> ', req.username);
      debug('Initialize, password -> ', req.password);

      /*
       * Not use this code, because this is use for not match version between hive and client.
       * but, jshs2 is fully suitable version used. So this code no needed.
       *
       * req.client_protocol = $service.TCLIServiceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;
       */

      debug('Start, OpenSession -> ');

      that.client.OpenSession(req, function (err, res) {
        debug('Operation end, OpenSession -> ', err);

        if (err) {
          return reject(new ConnectionError(err.message));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          return reject(new ConnectionError('Error caused from HiveServer2!\n' + res.status.errorMessage));
        } else {
          that.sessionHandle = res.sessionHandle;
          that.connectionConfiguration = res.configuration;
          that.serverProtocolVersion = res.serverProtocolVersion;

          debug('Operation end, OpenSession -> finished');
          debug('Operation end, serverProtocolVersion -> ', res.serverProtocolVersion);

          return resolve(new Cursor.HS2PromiseCursor(that));
        }
      });
    });
  };

  HS2PromiseConnection.prototype.close = function close () {
    var that = this;

    return new Promise(function (resolve, reject) {
      if (!that.getSessionHandle()) {
        that.connection.end();
        resolve(true);
      } else {
        var serviceType = IdlContainer.getServiceType();
        var req = new serviceType.TCloseSessionReq({
          sessionHandle: that.getSessionHandle()
        });

        that.client.CloseSession(req, function (err) {
          that.connection.end();

          if (err) {
            reject(err);
          } else {
            resolve(true);
          }
        });
      }
    });
  };

  function HS2CallbackConnection (_configure) {
    HS2Connection.call(this, _configure);
  }

  util.inherits(HS2CallbackConnection, HS2Connection);

  HS2CallbackConnection.prototype.connect = function connect (callback) {
    var that = this;

    debug('Start, Connect -> ', that.configure.getHost());
    debug('Start, Connect -> ', that.configure.getPort());

    var Connection = authMechanisms[that.configure.getAuth()].connection;
    var options = {
      timeout: that.configure.getTimeout(),
      transport: authMechanisms[that.configure.getAuth()].transport
    };

    var service = IdlContainer.getService();
    var serviceType = IdlContainer.getServiceType();

    that.connection = new Connection(that.configure.getHost(), that.configure.getPort(), options);
    that.client = thrift.createClient(service, that.connection);

    var req = new serviceType.TOpenSessionReq();

    req.username = that.configure.getUsername() || 'anonymous';
    req.password = that.configure.getPassword() || '';

    /*
     * Not use this code, because this is use for not match version between hive and client.
     * but, jshs2 is fully suitable version used. So this code no needed.
     *
     * req.client_protocol = $service.TCLIServiceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;
     */

    that.connection.once('error', function (err) {
      debug(util.format('Error caused, from Connection.connect'));
      debug(err.message);
      debug(err.stack);

      return callback(err);
    });

    that.client.OpenSession(req, function (err, res) {
      if (err) {
        return reject(new ConnectionError(err.message));
      } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
        return reject(new ConnectionError('Error caused from HiveServer2!\n' + res.status.errorMessage));
      } else {
        that.sessionHandle = res.sessionHandle;
        that.connectionConfiguration = res.configuration;
        that.serverProtocolVersion = res.serverProtocolVersion;

        return callback(null, new Cursor.HS2CallbackCursor(that));
      }
    });
  };

  HS2CallbackConnection.prototype.close = function close (callback) {
    var that = this;

    if (!that.getSessionHandle()) {
      that.connection.end();

      callback(null);
    } else {
      var serviceType = IdlContainer.getServiceType();
      var req = new serviceType.TCloseSessionReq({
        sessionHandle: that.getSessionHandle()
      });

      that.client.CloseSession(req, function (err) {
        if (err) {
          callback(err);
        } else {
          callback(null);
        }
      });
    }
  };

  module.exports = {
    HS2Connection: HS2Connection,
    HS2PromiseConnection: HS2PromiseConnection,
    HS2CallbackConnection: HS2CallbackConnection
  };
}());