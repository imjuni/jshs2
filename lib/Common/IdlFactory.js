'use strict';

var util = require('util');
var fs = require('fs');
var path = require('path');
var debug = require('debug')('jshs2:Factory');

function IdlFactory () {}

IdlFactory.prototype.extractConfig = function extractConfig (configure) {
  var thrift = util.format('Thrift_%s', configure.getThriftVer() || '');
  var hive = util.format('Hive_%s', configure.getHiveVer() || '');
  var cdh, path;

  if (configure.getCDHVer()) {
    cdh = util.format('CDH_%s', configure.getCDHVer());
    path = [thrift, hive, cdh].join('_');
  } else {
    path = [thrift, hive].join('_');
  }

  return {
    thrift: thrift,
    hive: hive,
    cdh: cdh,
    path: path
  };
};

IdlFactory.prototype.serviceFactory = function serviceFactory (configure) {
  var that = this;
  return new Promise(function (resolve, reject) {
    that.cb$serviceFactory(function (err, service) {
      if (err) {
        reject(err);
      } else {
        resolve(service);
      }

    });
  });
};

IdlFactory.prototype.cb$serviceFactory = function cb$serviceFactory (configure, callback) {
  try {
    var factoryConfig = this.extractConfig(configure);
    var service = require(path.join(__dirname, '..', '..', 'idl', factoryConfig.path, 'TCLIService.js'));

    callback(null, service);
  } catch (err) {
    callback(err);
  }

};

IdlFactory.prototype.serviceTypeFactory = function serviceTypeFactory (configure) {
  var that = this;

  return new Promise(function (resolve, reject) {
    that.cb$serviceTypeFactory(function (err, serviceType) {
      if (err) {
        reject(err);
      } else {
        resolve(serviceType);
      }
    });
  });
};

IdlFactory.prototype.cb$serviceTypeFactory = function cb$serviceTypeFactory (configure, callback) {
  try {
    var factoryConfig = this.extractConfig(configure);
    var serviceType = require(path.join(__dirname, '..', '..', 'idl', factoryConfig.path, 'TCLIService_types.js'));

    callback(null, serviceType);
  } catch (err) {
    callback(err);
  }

};

module.exports = new IdlFactory();
