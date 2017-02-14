const debug = require('debug')('jshs2:Cursor');
const HS2Util = require('./common/HS2Util');

class Cursor {
  constructor(configure, conn) {
    if (!conn.IDL.IsInit) {
      throw new Error('Can\'t use Connection without IdlContainer initialization');
    }

    this.conn = conn;
    this.client = conn.Client;
    this.sessionHandle = conn.SessionHandle;
    this.configure = configure;
    this.operationHandle = null;
    this.hasResultSet = false;
    this.typeCache = null;

    this.getLogFactory = this.getLogFactory.bind(this);
  }

  getLogFactory() {
    const serviceType = this.Conn.IDL.ServiceType;
    const protocolVersion = serviceType.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7 || 6;

    let logType;

    if (this.conn.ServerProtocolVersion < protocolVersion &&
      this.configure.HiveType === HS2Util.HIVE_TYPE.CDH) {
      debug(`getLog, selected getLogOnCDH -> ${this.configure.HiveType}`);
      logType = this.getLogOnCDH;
    } else if (this.conn.ServerProtocolVersion >= protocolVersion) {
      debug(`getLog, selected getLogOnHive -> ${this.configure.HiveType}`);
      logType = this.getLogOnHive;
    } else {
      debug('getLog -> Not implementation Error');
      logType = null;
    }

    return logType;
  }

  get TypeCache() { return this.typeCache; }
  set TypeCache(value) { this.typeCache = value; }
  get HasResultSet() { return this.hasResultSet; }
  set HasResultSet(value) { this.hasResultSet = value; }
  get OperationHandle() { return this.operationHandle; }
  set OperationHandle(value) { this.operationHandle = value; }
  get SessionHandle() { return this.sessionHandle; }
  set SessionHandle(value) { this.sessionHandle = value; }
  get Client() { return this.client; }
  set Client(value) { this.client = value; }
  get Configure() { return this.configure; }
  get Conn() { return this.conn; }

}

module.exports = Cursor;
