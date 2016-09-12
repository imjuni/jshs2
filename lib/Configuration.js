'use strict';

var debug = require('debug')('jshs2:configure');
var Promise = require('promise');
var IdlContainer = require('./Common/IdlContainer');
var ConfigureError = require('./Common/HS2Error').ConfigureError;
var hs2util = require('./Common/HS2Util');

function Configuration (_options) {
  var that = this;
  var options = {};

  if (!_options.host) {
    debug('Error caused from hostname, host name not null value -> ', _options.host);
    throw new ConfigureError('Invalid Hostname');
  }

  // --------------------------------------------------------------------------------
  // Configuration of Connection
  // --------------------------------------------------------------------------------

  // Connection
  options.host = _options.host;
  options.auth = (!!_options.auth) ? _options.auth.toLowerCase() : 'nosasl';
  options.port = _options.port || 10000;
  options.timeout = _options.timeout || 5;
  options.username = _options.username;
  options.password = _options.password || '';

  // Factory
  options.hiveType = _options.hiveType || hs2util.HIVE_TYPE.HIVE;
  options.hiveVer = _options.hiveVer || '0.13.1';
  options.thriftVer = _options.thriftVer || '0.9.2';
  options.cdhVer = _options.cdhVer || null;
  options.idlContainer = null;

  debug('port, :', options.port);
  debug('timeout, :', options.timeout);
  debug('hiveType, :', options.hiveType);
  debug('hiveVer, :', options.hiveVer);
  debug('thirftVer, :', options.thriftVer);
  debug('cdhVer, :', options.cdhVer);

  that.getAuth      = function getAuth () { return options.auth; };
  that.setAuth      = function setAuth (auth) { options.auth = auth; };
  that.getHost      = function getHost () { return options.host; };
  that.setHost      = function setHost (host) { options.host = host; };
  that.getPort      = function getPort () { return options.port; };
  that.setPort      = function setPort (port) { options.port = port; };
  that.getTimeout   = function getTimeout () { return options.timeout; };
  that.setTimeout   = function setTimeout (timeout) { options.timeout = timeout; };
  that.getUsername  = function getUsername () { return options.username; };
  that.setUsername  = function setUsername (username) { options.username = username; };
  that.getPassword  = function getPassword () { return options.password; };
  that.setPassword  = function setPassword (password) { options.password = password; };
  that.getHiveType  = function getHiveType () { return options.hiveType; };
  that.setHiveType  = function setHiveType (hiveType) { options.hiveType = hiveType; };
  that.getHiveVer   = function getHiveVer () { return options.hiveVer; };
  that.setHiveVer   = function setHiveVer (hiveType) { options.hiveVer = hiveVer; };
  that.getThriftVer = function getThriftVer () { return options.thriftVer; };
  that.setThriftVer = function setThriftVer (thriftVer) { options.thriftVer = thriftVer; };
  that.getCDHVer    = function getCDHVer () { return options.cdhVer; };
  that.setCDHVer    = function setCDHVer (cdhVer) { options.cdhVer = cdhVer; };

  // --------------------------------------------------------------------------------
  // Configuration of Cursor
  // --------------------------------------------------------------------------------
  // Cursor
  options.maxRows = _options.maxRows || 5120;
  options.nullStr = _options.nullStr || 'NULL';
  options.i64ToString = _options.i64ToString || true;

  that.getMaxRows     = function getMaxRows () { return options.maxRows; };
  that.setMaxRows     = function setMaxRows (maxRows) { options.maxRows = maxRows; };
  that.getNullStr     = function getNullStr () { return options.nullStr; };
  that.setNullStr     = function setNullStr (nullStr) { options.nullStr = nullStr; };
  that.getI64ToString = function getI64ToString () { return options.i64ToString; };
  that.setI64ToString = function setI64ToString (i64ToString) { options.i64ToString = i64ToString; };

  that.initialize = function initialize () {
    options.idlContainer = new IdlContainer();

    return new Promise(function (resolve, reject) {
      options.idlContainer.cb$initialize(that, function (err) {
        if (err) {
          reject(err);
        } else {
          resolve(null);
        }
      });
    });
  };

  that.cb$initialize = function cb$initialize (cb) {
    options.idlContainer = new IdlContainer();
    options.idlContainer.cb$initialize(that, function (err) {
      if (err) {
        cb(err);
      } else {
        cb(null);
      }
    });
  };

  that.getService     = function getService () { return options.idlContainer.getService(); };
  that.getServiceType = function getServiceType () { return options.idlContainer.getServiceType(); };
  that.getIsInit      = function getIsInit () { return !!options.idlContainer; };

  that.getOperationStatuses = function getOperationStatuses () {
    return options.idlContainer.getServiceType().TOperationState;
  };
}

module.exports = Configuration;
