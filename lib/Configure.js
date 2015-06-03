(function () {
  'use strict';

  var debug = require('debug')('jshs2:configure');
  var IdlContainer = require('./Common/IdlContainer');
  var ConfigureError = require('./Common/HS2Error').ConfigureError;
  var hs2util = require('./Common/HS2Util');

  function Configure (_options) {
    var that = this;
    var options = {};

    if (!_options.host) {
      debug('Error caused from hostname, host name not null value -> ', _options.host);
      throw new ConfigureError('Invalid Hostname');
    }

    // HS2Connection
    options.host = _options.host;
    options.auth = _options.auth || 'nosasl';
    options.port = _options.port || 10000;
    options.timeout = _options.timeout || 5;
    options.username = _options.username;
    options.password = _options.password || '';

    // Factory
    options.hiveType = _options.hiveType || hs2util.HIVE_TYPE.HIVE;
    options.hiveVer = _options.hiveVer || '0.13.1';
    options.thriftVer = _options.thriftVer || '0.9.2';
    options.cdhVer = _options.cdhVer || null;

    // HS2Cursor
    options.maxRows = _options.maxRows || 5120;
    options.nullStr = _options.nullStr || 'NULL';

    options.auth = options.auth.toLowerCase();

    debug('port, :', options.port);
    debug('timeout, :', options.timeout);
    debug('hiveType, :', options.hiveType);
    debug('hiveVer, :', options.hiveVer);
    debug('thirftVer, :', options.thriftVer);
    debug('cdhVer, :', options.cdhVer);

    // Auth
    function getAuth () {
      return options.auth;
    }

    function setAuth (auth) {
      options.auth = auth;
    }

    // Host
    function getHost () {
      return options.host;
    }

    function setHost (host) {
      options.host = host;
    }

    // Port
    function getPort () {
      return options.port;
    }

    function setPort (port) {
      options.port = port;
    }

    // Timeout
    function getTimeout () {
      return options.timeout;
    }

    function setTimeout (timeout) {
      options.timeout = timeout;
    }

    // Username
    function getUsername () {
      return options.username;
    }

    function setUsername (username) {
      options.username = username;
    }

    // Password
    function getPassword () {
      return options.password;
    }

    function setPassword (password) {
      options.password = password;
    }

    // HiveType
    function getHiveType () {
      return options.hiveType;
    }

    function setHiveType (hiveType) {
      options.hiveType = hiveType;
    }

    function getHiveVer () {
      return options.hiveVer;
    }

    function setHiveVer (hiveType) {
      options.hiveVer = hiveVer;
    }

    function getThriftVer () {
      return options.thriftVer;
    }

    function setThriftVer (thriftVer) {
      options.thriftVer = thriftVer;
    }

    function getCDHVer () {
      return options.cdhVer;
    }

    function setCDHVer (cdhVer) {
      options.cdhVer = cdhVer;
    }

    options.maxRows = _options.maxRows || 5120;
    options.nullStr = _options.nullStr || 'NULL';

    function getMaxRows () {
      return options.maxRows;
    }

    function setMaxRows (maxRows) {
      options.maxRows = maxRows;
    }

    function getNullStr () {
      return options.nullStr;
    }

    function setNullStr (nullStr) {
      options.nullStr = nullStr;
    }

    that.getAuth = getAuth;
    that.setAuth = setAuth;
    that.getHost = getHost;
    that.setHost = setHost;
    that.getPort = getPort;
    that.setPort = setPort;
    that.getTimeout = getTimeout;
    that.setTimeout = setTimeout;
    that.getUsername = getUsername;
    that.setUsername = setUsername;
    that.getPassword = getPassword;
    that.setPassword = setPassword;
    that.getHiveType = getHiveType;
    that.setHiveType = setHiveType;
    that.getHiveVer = getHiveVer;
    that.setHiveVer = setHiveVer;
    that.getThriftVer = getThriftVer;
    that.setThriftVer = setThriftVer;
    that.getCDHVer = getCDHVer;
    that.setCDHVer = setCDHVer;
    that.getMaxRows = getMaxRows;
    that.setMaxRows = setMaxRows;
    that.getNullStr = getNullStr;
    that.setNullStr = setNullStr;
  }

  module.exports = Configure;
})();