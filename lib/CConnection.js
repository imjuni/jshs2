(function () {
  'use strict';

  var util = require('util');
  var thrift = require('thrift');
  var debug = require('debug')('jshs2:CConnection');

  var Connection = require('./Connection');
  var CCursor = require('./CCursor');
  var ConnectionError = require('./Common/HS2Error').ConnectionError;

  function CConnection (_configure) {
    Connection.call(this, _configure);
  }

  util.inherits(CConnection, Connection);

  CConnection.prototype.connect = function connect (isCursor, callback) {
    var that = this;

    if (typeof isCursor === 'function') {
      callback = isCursor;
      isCursor = true;
    }

    debug('OpenSession function start, -> ', that.getConfigure().getHost());
    debug('OpenSession function start, -> ', that.getConfigure().getPort());

    var ThriftConnection = that.AUTH_MECHANISMS[that.getConfigure().getAuth()].connection;
    var options = {
      timeout: that.getConfigure().getTimeout(),
      transport: that.AUTH_MECHANISMS[that.getConfigure().getAuth()].transport
    };

    var service = that.getConfigure().getService();
    var serviceType = that.getConfigure().getServiceType();

    that.thriftConnection = new ThriftConnection(that.getConfigure().getHost(), that.getConfigure().getPort(), options);
    that.client = thrift.createClient(service, that.thriftConnection);

    var req = new serviceType.TOpenSessionReq();

    req.username = that.getConfigure().getUsername() || 'anonymous';
    req.password = that.getConfigure().getPassword() || '';

    debug('Initialize, username -> ', req.username);
    debug('Initialize, password -> ', req.password);

    /*
     * Not use this code, because this is use for not match version between hive and client.
     * but, jshs2 is fully suitable version used. So this code no needed.
     *
     * req.client_protocol = $service.TCLIServiceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;
     */

    that.thriftConnection.once('error', function (err) {
      debug('Error caused, from Connection.connect');
      debug(err.message);
      debug(err.stack);

      return callback(err);
    });

    debug('OpenSession request start, ', req);
    that.client.OpenSession(req, function (err, res) {
      debug('OpenSession request end -> ', err);

      if (err) {
        setImmediate(function () {
          callback(new ConnectionError(err.message));
        });
      } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
        setImmediate(function () {
          debug('OpenSession -> Error from HiveServer2\n', res.status.infoMessages);
          debug('OpenSession -> Error from HiveServer2\n', res.status.errorMessage);

          var errorMessage = res.status.errorMessage || 'OpenSession operation fail,... !!';
          var infoMessages = res.status.infoMessages;

          if (infoMessages && infoMessages.length) {
            infoMessages = infoMessages.join('\n');
          } else {
            infoMessages = '';
          }

          callback(new ConnectionError([
            errorMessage,
            '-- Error caused from HiveServer2\n\n',
            infoMessages
          ].join('\n\n')));
        });
      } else {
        setImmediate(function () {
          that.setSessionHandle(res.sessionHandle);
          that.setThriftConnectionConfiguration(res.configuration);
          that.setServerProtocolVersion(res.serverProtocolVersion);

          debug('Operation end, OpenSession -> finished');
          debug('Operation end, serverProtocolVersion -> ', res.serverProtocolVersion);

          if (isCursor) {
            callback(null, new CCursor(that.getConfigure(), that));
          } else {
            callback(null);
          }
        });
      }
    });
  };

  CConnection.prototype.close = function close (callback) {
    var that = this;

    if (!that.getSessionHandle()) {
      that.thriftConnection.end();

      callback(null);
    } else {
      var serviceType = that.getConfigure().getServiceType();
      var req = new serviceType.TCloseSessionReq({
        sessionHandle: that.getSessionHandle()
      });

      that.client.CloseSession(req, function (err, res) {
        if (err) {
          callback(err);
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          setImmediate(function () {
            debug('CloseSession -> Error from HiveServer2\n', res.status.infoMessages);
            debug('CloseSession -> Error from HiveServer2\n', res.status.errorMessage);

            var errorMessage = res.status.errorMessage || 'CloseSession operation fail,... !!';
            var infoMessages = res.status.infoMessages;

            if (infoMessages && infoMessages.length) {
              infoMessages = infoMessages.join('\n');
            } else {
              infoMessages = '';
            }

            callback(new ConnectionError([
              errorMessage,
              '-- Error caused from HiveServer2\n\n',
              infoMessages
            ].join('\n\n')));
          });
        } else {
          callback(null);
        }
      });
    }
  };

  module.exports = CConnection;
})();