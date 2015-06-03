(function () {
  'use strict';

  var util = require('util');
  var thrift = require('thrift');
  var Promise = require('Promise');
  var debug = require('debug')('jshs2:PConnection');

  var Connection = require('./Connection');
  var PCursor = require('./PCursor');
  var IdlContainer = require('./Common/IdlContainer');
  var ConnectionError = require('./Common/HS2Error').ConnectionError;

  function PConnection (configure) {
    Connection.call(this, configure);
  }

  util.inherits(PConnection, Connection);

  PConnection.prototype.connect = function connect () {
    var that = this;

    return new Promise(function (resolve, reject) {

      debug('Initialize, Connect -> ', that.getConfigure().getHost());
      debug('Initialize, Auth    -> ', that.getConfigure().getAuth());

      var ThriftConnection = that.AUTH_MECHANISMS[that.configure.getAuth()].connection;
      var service = IdlContainer.getService();
      var serviceType = IdlContainer.getServiceType();
      var options = {};

      options.timeout = that.configure.getTimeout();
      options.transport = that.AUTH_MECHANISMS[that.configure.getAuth()].transport;

      that.thriftConnection = new ThriftConnection(that.configure.getHost(), that.configure.getPort(), options);
      that.client = thrift.createClient(service, that.thriftConnection);

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

      that.thriftConnection.once('error', function (err) {
        debug(util.format('Error caused, from Connection.connect'));
        debug(err.message);
        debug(err.stack);

        return reject(err);
      });

      debug('Start, OpenSession -> ', req);

      that.client.OpenSession(req, function (err, res) {
        debug('Operation end, OpenSession -> ', err);

        if (err) {
          reject(new ConnectionError(err.message));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          debug('OpenSession -> Error from HiveServer2\n', res.status.infoMessages);
          debug('OpenSession -> Error from HiveServer2\n', res.status.errorMessage);

          reject(new ConnectionError([
            res.status.errorMessage,
            res.status.infoMessages,
            '-- Error caused from HiveServer2'
          ].join('\n\n')));
        } else {
          that.setSessionHandle(res.sessionHandle);
          that.setThriftConnectionConfiguration(res.configuration);
          that.setServerProtocolVersion(res.serverProtocolVersion);

          debug('Operation end, OpenSession -> finished');
          debug('Operation end, serverProtocolVersion -> ', res.serverProtocolVersion);

          resolve(new PCursor(that));
        }
      });
    });
  };

  PConnection.prototype.close = function close () {
    var that = this;

    debug('CloseSession -> function start');

    return new Promise(function (resolve, reject) {
      if (!that.getSessionHandle()) {
        that.thriftConnection.end();
        resolve(true);
      } else {
        var serviceType = IdlContainer.getServiceType();
        var req = new serviceType.TCloseSessionReq({
          sessionHandle: that.getSessionHandle()
        });
        debug('CloseSession -> request start');

        that.client.CloseSession(req, function (err) {
          debug('CloseSession -> request responsed');
          that.thriftConnection.end();

          if (err) {
            reject(err);
          } else {
            resolve(true);
          }
        });
      }
    });
  };

  module.exports = PConnection;
})();