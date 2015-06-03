(function () {
  'use strict';

  var util = require('util');
  var fs = require('fs');
  var path = require('path');
  var debug = require('debug')('jshs2:Factory');
  var customErrors = require('./HS2Error');

  function extractConfig (configure) {
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
  }

  function serviceFactory (configure) {
    var factoryConfig = extractConfig(configure);
    var servicePath = path.join(process.cwd(), 'idl', factoryConfig.path, 'TCLIService.js');

    return new Promise(function (resolve, reject) {
      fs.exists(servicePath, function (exists) {
        if (exists) {
          resolve(require(servicePath));
        } else {
          reject(new customErrors.FactoryError('Invalid version from Configure'));
        }
      });
    });

  }

  function serviceTypeFactory (configure) {
    var factoryConfig = extractConfig(configure);
    var serviceTypePath = path.join(process.cwd(), 'idl', factoryConfig.path, 'TCLIService_types.js');

    return new Promise(function (resolve, reject) {
      fs.exists(serviceTypePath, function (exists) {
        if (exists) {
          resolve(require(serviceTypePath));
        } else {
          reject(new customErrors.FactoryError('Invalid version from Configure'));
        }
      });
    });
  }

  var IdlFactory = {};
  IdlFactory.serviceFactory = serviceFactory;
  IdlFactory.serviceTypeFactory = serviceTypeFactory;

  module.exports = IdlFactory;
})();