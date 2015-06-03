"use strict";

(function () {
  var util = require('util'),
    binary = require('thrift/lib/thrift/binary'),
    thrift = require('thrift');

  var InputBufferUnderrunError = exports.InputBufferUnderrunError = function() {
  };

  var TSaslClientTransport = function (buffer, callback) {
    thrift.TBufferedTransport.call(this, buffer, callback);
  };

  util.inherits(TSaslClientTransport, thrift.TBufferedTransport);

  TSaslClientTransport.receiver = function(callback) {
    var reader = new TSaslClientTransport();

    return function(data) {
      if (reader.writeCursor + data.length > reader.inBuf.length) {
        var buf = new Buffer(reader.writeCursor + data.length);
        reader.inBuf.copy(buf, 0, 0, reader.writeCursor);
        reader.inBuf = buf;
      }
      data.copy(reader.inBuf, reader.writeCursor, 0);
      reader.writeCursor += data.length;

      callback(reader);
    };
  };

  /*
  TSaslClientTransport.prototype = {
    commitPosition: function(){
      var unreadedSize = this.writeCursor - this.readCursor;
      var bufSize = (unreadedSize * 2 > this.defaultReadBufferSize) ? unreadedSize * 2 : this.defaultReadBufferSize;
      var buf = new Buffer(bufSize);
      if (unreadedSize > 0) {
        this.inBuf.copy(buf, 0, this.readCursor, this.writeCursor);
      }
      this.readCursor = 0;
      this.writeCursor = unreadedSize;
      this.inBuf = buf;
    },
    rollbackPosition: function(){
      this.readCursor = 0;
    },

    // TODO: Implement open/close support
    isOpen: function() {return true;},
    open: function() {},
    close: function() {},

    ensureAvailable: function(len) {
      if (this.readCursor + len > this.writeCursor) {
        throw new InputBufferUnderrunError();
      }
    },

    read: function(len) {
      this.ensureAvailable(len)
      var buf = new Buffer(len);
      this.inBuf.copy(buf, 0, this.readCursor, this.readCursor + len);
      this.readCursor += len;
      return buf;
    },

    readByte: function() {
      this.ensureAvailable(1)
      return binary.readByte(this.inBuf[this.readCursor++]);
    },

    readI16: function() {
      this.ensureAvailable(2)
      var i16 = binary.readI16(this.inBuf, this.readCursor);
      this.readCursor += 2;
      return i16;
    },

    readI32: function() {
      this.ensureAvailable(4)
      var i32 = binary.readI32(this.inBuf, this.readCursor);
      this.readCursor += 4;
      return i32;
    },

    readDouble: function() {
      this.ensureAvailable(8)
      var d = binary.readDouble(this.inBuf, this.readCursor);
      this.readCursor += 8;
      return d;
    },

    readString: function(len) {
      this.ensureAvailable(len)
      var str = this.inBuf.toString('utf8', this.readCursor, this.readCursor + len);
      this.readCursor += len;
      return str;
    },


    readAll: function() {
      if (this.readCursor >= this.writeCursor) {
        throw new InputBufferUnderrunError();
      }
      var buf = new Buffer(this.writeCursor - this.readCursor);
      this.inBuf.copy(buf, 0, this.readCursor, this.writeCursor);
      this.readCursor = this.writeCursor;
      return buf;
    },

    write: function(buf, encoding) {
      if (typeof(buf) === "string") {
        // Defaulting to ascii encoding here since that's more like the original
        // code, but I feel like 'utf8' would be a better choice.
        buf = new Buffer(buf, encoding || 'ascii');
      }
      if (this.outCount + buf.length > this.writeBufferSize) {
        this.flush();
      }

      this.outBuffers.push(buf);
      this.outCount += buf.length;

      if (this.outCount >= this.writeBufferSize) {
        this.flush();
      }
    },

    flush: function() {
      if (this.outCount < 1) {
        return;
      }

      var msg = new Buffer(this.outCount),
        pos = 0;
      this.outBuffers.forEach(function(buf) {
        buf.copy(msg, pos, 0);
        pos += buf.length;
      });

      if (this.onFlush) {
        this.onFlush(msg);
      }

      this.outBuffers = [];
      this.outCount = 0;
    }
  };
  */

  module.exports = TSaslClientTransport;
}());