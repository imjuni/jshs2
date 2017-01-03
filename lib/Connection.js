const thrift = require('thrift');

const AUTH_MECHANISMS = {
  NOSASL: {
    connection: thrift.createConnection,
    transport: thrift.TBufferedTransport,
  },
  PLAIN: {
    connection: thrift.createConnection,
    transport: thrift.TBufferedTransport,
  },
};

class Connection {
  constructor(configure, idlContainer) {
    if (!idlContainer.IsInit) {
      throw new Error('Can\'t use Connection without IdlContainer initialization');
    }

    this.conn = null;
    this.sessionHandle = null;
    this.thriftConnConfiguration = null;
    this.serverProtocolVersion = null;
    this.configure = configure;
    this.idl = idlContainer;
    this.client = null;
  }

  get Configure() { return this.configure; }
  get IDL() { return this.idl; }

  get SessionHandle() { return this.sessionHandle; }
  set SessionHandle(value) { this.sessionHandle = value; }
  get ThriftConnConfiguration() { return this.thriftConnConfiguration; }
  set ThriftConnConfiguration(value) { this.thriftConnConfiguration = value; }
  get ServerProtocolVersion() { return this.serverProtocolVersion; }
  set ServerProtocolVersion(value) { this.serverProtocolVersion = value; }
  get Conn() { return this.conn; }
  set Conn(value) { this.conn = value; }
  get Client() { return this.client; }
  set Client(value) { this.client = value; }
}

Connection.AUTH_MECHANISMS = AUTH_MECHANISMS;

module.exports = Connection;
