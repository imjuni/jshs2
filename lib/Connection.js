'use strict';

var thrift = require('thrift');
var IdlContainer = require('./Common/IdlContainer');

function Connection (configure) {
  if (!configure.getIsInit()) {
    throw new Error('Can\'t use Connection without IdlContainer initialization');
  }

  this.sessionHandle = null;
  this.thriftConnectionConfiguration = null;
  this.serverProtocolVersion = null;
  this.configure = configure;
  this.client = null;
}

Connection.prototype.AUTH_MECHANISMS = {
  nosasl: {
    connection: thrift.createConnection,
    transport: thrift.TBufferedTransport
  },
  plain: {
    connection: thrift.createConnection,
    transport: thrift.TBufferedTransport
  }
};

Connection.prototype.getConfigure = function getConfigure () {
  return this.configure;
};

Connection.prototype.setSessionHandle = function setSessionHandle (sessionHandle) {
  this.sessionHandle = sessionHandle;
};

Connection.prototype.getSessionHandle = function getSessionHandle () {
  return this.sessionHandle;
};

Connection.prototype.setThriftConnectionConfiguration = function setThriftConnectionConfiguration (connectionConfiguration) {
  this.thriftConnectionConfiguration = connectionConfiguration;
};

Connection.prototype.getThriftConnectionConfiguration = function getThriftConnectionConfiguration () {
  return this.thriftConnectionConfiguration;
};

Connection.prototype.setServerProtocolVersion = function setServerProtocolVersion (serverProtocolVersion) {
  this.serverProtocolVersion = serverProtocolVersion;
};

Connection.prototype.getServerProtocolVersion = function getServerProtocolVersion () {
  return this.serverProtocolVersion;
};

Connection.prototype.getClient = function getClient () {
  return this.client;
};

module.exports = Connection;
