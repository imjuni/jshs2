(function () {
  'use strict';

  var Promise = require('Promise');
  var IdlFactory = require('./IdlFactory');

  function IdlContainer () {
    var that = this;
    var service, serviceType;

    that.isInit = false;

    function initialize (configure) {
      return new Promise(function (resolve, reject) {
        Promise.all([
          IdlFactory.serviceFactory(configure),
          IdlFactory.serviceTypeFactory(configure)
        ]).then(function (res) {
          service = res[0];
          serviceType = res[1];

          that.isInit = true;

          resolve(true);
        }).catch(function (err) {
          reject(err);
        });
      });
    }

    function cb$initialize (configure, callback) {
      Promise.all([
        IdlFactory.serviceFactory(configure),
        IdlFactory.serviceTypeFactory(configure)
      ]).then(function (res) {
        service = res[0];
        serviceType = res[1];

        that.isInit = true;

        callback(null, true);
      }).catch(function (err) {
        callback(err);
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


  module.exports = new IdlContainer();
})();