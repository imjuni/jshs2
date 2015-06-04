(function () {
  'use strict';

  var _ = require('lodash');
  var util = require('util');
  var debug = require('debug')('jshs2:PCursor');
  var CCursor = require('./CCursor');

  function PCursor (connection) {
    CCursor.call(this, connection);
  }

  util.inherits(PCursor, CCursor);

  PCursor.prototype.cancel = function cancel () {
    var that = this;

    return new Promise(function (resolve, reject) {
      PCursor.super_.prototype.cancel.call(that, function (err) {
        if (err) {
          reject(err);
        } else {
          resolve(true);
        }
      });
    });
  };

  PCursor.prototype.execute = function execute (hql) {
    var that = this;

    return new Promise(function (resolve, reject) {
      PCursor.super_.prototype.execute.call(that, hql, function (err, result) {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  };

  PCursor.prototype.getLog = function getLog () {
    var that = this;

    return new Promise(function (resolve, reject) {
      PCursor.super_.prototype.getLog.call(that, function (err, log) {
        if (err) {
          reject(err);
        } else {
          resolve(log);
        }
      });
    });
  };

  PCursor.prototype.getOperationStatus = function getOperationStatus () {
    var that = this;

    return new Promise(function (resolve, reject) {
      PCursor.super_.prototype.getOperationStatus.call(that, function (err, status) {
        if (err) {
          reject(err);
        } else {
          resolve(status);
        }
      });
    });
  };

  PCursor.prototype.getSchema = function getSchema () {
    var that = this;

    return new Promise(function (resolve, reject) {
      PCursor.super_.prototype.getSchema.call(that, function (err, schema) {
        if (err) {
          reject(err);
        } else {
          resolve(schema);
        }
      });
    });
  };

  PCursor.prototype.fetchBlock = function fetchBlock () {
    var that = this;

    return new Promise(function (resolve, reject) {
      PCursor.super_.prototype.fetchBlock.call(that, function (err, result) {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  };

  PCursor.prototype.close = function close () {
    var that = this;

    return new Promise(function (resolve, reject) {
      PCursor.super_.prototype.close.call(that, function (err) {
        if (err) {
          reject(err);
        } else {
          resolve(true);
        }
      });
    });
  };

  module.exports = PCursor;
})();