(function () {
  'use strict';

  var util = require('util');
  var thrift = require('thrift');
  var Promise = require('promise');
  var debug = require('debug')('jshs2:PConnection');

  var CConnection = require('./CConnection');
  var PCursor = require('./PCursor');

  function PConnection (configure) {
    CConnection.call(this, configure);
  }

  util.inherits(PConnection, CConnection);

  PConnection.prototype.connect = function connect () {
    var that = this;

    return new Promise(function (resolve, reject) {
      PConnection.super_.prototype.connect.call(that, false, function (err) {
        if (err) {
          reject(err);
        } else {
          resolve(new PCursor(that.getConfigure(), that));
        }
      });
    });
  };

  PConnection.prototype.close = function close () {
    var that = this;

    return new Promise(function (resolve, reject) {
      PConnection.super_.prototype.close.call(that, function (err) {
        if (err) {
          reject(err);
        } else {
          resolve(true);
        }
      });
    });
  };

  module.exports = PConnection;
})();