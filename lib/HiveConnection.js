const thrift = require('thrift');
const debug = require('debug')('jshs2:CConnection');
const HS2Util = require('./common/HS2Util');
const Connection = require('./Connection');
const Timer = require('./common/Timer');
const HiveCursor = require('./HiveCursor');
const ConnectionError = require('./error/ConnectionError');

class HiveConnection extends Connection {
  constructor(configure, idlContainer) {
    super(configure, idlContainer);

    this.connect = this.connect.bind(this);
    this.close = this.close.bind(this);
  }

  connect() {
    return new Promise((resolve, reject) => {
      debug(`OpenSession function start, -> ${this.configure.Host}:${this.configure.Port}`);

      const ThriftConnection = Connection.AUTH_MECHANISMS[this.configure.Auth].connection;
      const option = {
        timeout: this.configure.Timeout,
        transport: Connection.AUTH_MECHANISMS[this.configure.Auth].transport,
      };

      const service = this.IDL.Service;
      const serviceType = this.IDL.ServiceType;
      const timer = new Timer();

      this.Conn = new ThriftConnection(this.Configure.Host, this.Configure.Port, option);
      this.Client = thrift.createClient(service, this.Conn);

      const request = new serviceType.TOpenSessionReq();

      request.username = this.Configure.Username;
      request.password = this.Configure.Password;

      debug(`Initialize, username -> ${request.username}/ ${request.password}`);

      timer.start();
      debug(`OpenSession request start, ${timer.Start}`, request);

      this.Conn.once('error', (err) => {
        debug('Error caused, from Connection.connect');
        debug(err.message);
        debug(err.stack);

        reject(new ConnectionError(err));
      });

      this.Client.OpenSession(request, (err, res) => {
        timer.end();
        debug(`OpenSession request end -> ${timer.End}/ ${timer.Diff}`, err);

        if (err) {
          reject(new ConnectionError(err));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
            res.status, 'ExecuteStatement operation fail,... !!');

          debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
          debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

          reject(new ConnectionError(message));
        } else {
          this.SessionHandle = res.sessionHandle;
          this.ThriftConnConfiguration = res.configuration;
          this.ServerProtocolVersion = res.serverProtocolVersion;

          debug('Operation end, OpenSession -> finished');
          debug('Operation end, serverProtocolVersion -> ', this.ServerProtocolVersion);

          resolve(new HiveCursor(this.Configure, this));
        }
      });
    });
  }

  close() {
    return new Promise((resolve, reject) => {
      const serviceType = this.IDL.ServiceType;
      const timer = new Timer();

      const request = new serviceType.TCloseSessionReq({
        sessionHandle: this.SessionHandle,
      });

      timer.start();
      debug(`Connection close -> CloseSession start, ...${timer.Start}`);

      this.Client.CloseSession(request, (err, res) => {
        timer.end();

        if (err) {
          this.Conn.end();
          reject(new ConnectionError(err));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
            res.status, 'ExecuteStatement operation fail,... !!');

          debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
          debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

          this.Conn.end();

          reject(new ConnectionError(message));
        } else {
          this.Conn.end();
          resolve(true);
        }
      });
    });
  }
}

module.exports = HiveConnection;
