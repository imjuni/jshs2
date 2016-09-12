'use strict';

var debug = require('debug')('jshs2:Cursor');
var IdlContainer = require('./Common/IdlContainer');
var hs2util = require('./Common/HS2Util');

function Cursor (configure, connection) {
  if (!configure.getIsInit()) {
    throw new Error('Can\'t use Connection without IdlContainer initialization');
  }

  this.configure = configure;
  this.connection = connection;
  this.client = connection.getClient();
  this.sessionHandle = connection.getSessionHandle();
  this.configure = connection.getConfigure();
  this.operationHandle = null;
  this.hasResultSet = false;
  this.typeCache = null;
}

Cursor.prototype.setConfigure = function setConfigure (configure) {
  this.configure = configure;
};

Cursor.prototype.getConfigure = function getConfigure () {
  return this.configure;
};

Cursor.prototype.setTypeCache = function setTypeCache (typeCache) {
  this.typeCache = typeCache;
};

Cursor.prototype.getTypeCache = function getTypeCache () {
  return this.typeCache;
};

Cursor.prototype.getConnection = function getConnection () {
  return this.connection;
};

Cursor.prototype.getConfigure = function getConfigure () {
  return this.configure;
};

Cursor.prototype.getOperationHandle = function getOperationHandle () {
  return this.operationHandle;
};

Cursor.prototype.setOperationHandle = function setOperationHandle (operationHandle) {
  this.operationHandle = operationHandle;
};

Cursor.prototype.getHasResultSet = function getHasResultSet () {
  return this.hasResultSet;
};

Cursor.prototype.setHasResultSet = function setHasResultSet (hasResultSet) {
  this.hasResultSet = hasResultSet;
};

Cursor.prototype.getSessionHandle = function getSessionHandle () {
  return this.sessionHandle;
};

Cursor.prototype.getClient = function getClient () {
  return this.client;
};

Cursor.prototype.getLogFactory = function getLogFactory () {
  var that = this;
  var serviceType = that.getConfigure().getServiceType();
  var protocolVersion = serviceType.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7 || 6;

  if (that.getConnection().getServerProtocolVersion() < protocolVersion &&
    that.getConfigure().getHiveType() === hs2util.HIVE_TYPE.CDH) {
    debug('getLog, selected getLogOnCDH -> ', that.getConfigure().getHiveType());
    return 'getLogOnCDH';
  } else if (that.getConnection().getServerProtocolVersion() >= protocolVersion) {
    debug('getLog, selected getLogOnHive -> ', that.getConfigure().getHiveType());
    return 'getLogOnHive';
  } else {
    debug('getLog -> Not implementation Error');
    return null;
  }
};

module.exports = Cursor;
