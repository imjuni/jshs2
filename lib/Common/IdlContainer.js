(function () {
  'use strict';

  var Promise = require('promise');
  var IdlFactory = require('./IdlFactory');

  function IdlContainer () {
    var that = this;
    var service, serviceType;

    that.isInit = false;

    function initialize (configuration) {
      return new Promise(function (resolve, reject) {
        cb$initialize(configuration, function (err, result) {
          if (err) {
            reject(err);
          } else {
            service = result[0];
            serviceType = result[1];

            that.isInit = true;

            resolve(result);
          }
        });
      });
    }

    function cb$initialize (configuration, callback) {
      IdlFactory.cb$serviceFactory(configuration, function (err, _service) {
        if (err) {
          callback(err);
        } else {
          IdlFactory.cb$serviceTypeFactory(configuration, function (err, _serviceType) {
            if (err) {
              callback(err);
            } else {
              service = _service;
              serviceType = _serviceType;

              that.isInit = true;

              callback(null, [service, serviceType]);
            }
          });
        }
      });
    }

    function getService () {
      return service;
    }

    function getServiceType () {
      return serviceType;
    }

    that.initialize = initialize;
    that.cb$initialize = cb$initialize;
    that.getService = getService;
    that.getServiceType = getServiceType;
  }

  IdlContainer.prototype.getIsInit = function getIsInit () {
    return this.isInit;
  };


  module.exports = IdlContainer;
})();