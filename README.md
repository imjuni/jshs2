JSHS2
=====

# Introduction
jsHS2 is a node.js cient driver for connecting to hive server 2. see test/ConnectionTest.js
for an example of how to use it. jsHS2 reference from pyhs2 after rewrite javascript. but some
feature modify suitable for javascript. For example, Asynchronous call and jsonFetch, etc.

## I need help!
And I need your help. jsHS2 is not implementation SASL. I hope that someone add SASL on this project.
Contact imjuni@gmail.com with questions

# Install
```
npm install jshs2
```

# Option
## for Connection
```
var connOpt = {
  auth: 'NOSASL',
  host: '101.102.103.104',
  port: '1234',
  timeout: 10,
  username: 'batman'
  hiveType: 'cdh',
  thriftVer: '0.9.2',
  cdhVer: '5.3.0'
};
```
* auth - using auth mechanisms. At now only support 'NOSASL'
* host - hive server ip address or domain
* port - hive server port
* timeout - timeout for connect function
* username - username for hive server, maybe that logging username on hive server
* hiveType - Hive Type, CDH or not (that is hive).
* thriftVer - using thrift version
* cdhVer - if you using CDH, describe version parameter

## for Cursor
```
var cursorOpt = {
  maxRows: 5120
};
```
* maxRows - fetch size

# Hive in CDH
getLog function is difference between vanilla Hive and Hive in CDH(CDH Hive).
CDH Hive must support Hue. And that is display query operation status. So,
Cloudera is add GetLog api on hive.

If you using CDH Hive, describe hiveType 'cdh' after you using getLog function.
Reference Simple Usage

# Simple Usage
```
var jshs2 = require('jshs2');
var async = require('async');

var connOpt = {
  auth: 'NOSASL',
  host: '101.102.103.104',
  port: '1234',
  timeout: 10,
  username: 'batman',
  hiveType: 'cdh',
  thriftVer: '0.9.2',
  cdhVer: '5.3.0'
};

var cursorOpt = {
  maxRows: 5120
};

async.waterfall([
  function (callback) {
    conn.connect(cursorOpt, function (err, cursor) {
      callback(err, cursor);
    });
  },
  function (cursor, callback) {
    cursor.execute('select * from db.table limit 1000', function (err) {
      callback(err, cursor);
    });
  },
  function (cursor, callback) {
    cursor.getOperationStatus(function (err) {
      callback(err, cursor);
    });
  },
  function (cursor, callback) {
    cursor.getLog(function (err) {
      callback(err, cursor);
    });
  },
  function (cursor, callback) {
    cursor.getSchema(function (err, columns) {
      callback(err, cursor, columns);
    });
  },
  function (cursor, columns, callback) {
    cursor.jsonFetch(columns, function (err, result) {
      callback(err, cursor, columns, result);
    });
  },
  function (cursor, columns, result, callback) {
    cursor.close(function (err) {
      callback(err, cursor, columns, result);
    });
  },
  function (cursor, columns, result, callback) {
    conn.close(function (err) {
      callback(err, cursor, columns, result);
    });
  }
], function (err, cursor, columns, result) {
  if (err) {
    debug('Error caused, ');
    debug('message:  ' + err.message);
    debug('message:  ' + err.stack);
  }

  should.not.exist(err);

  (result.length === limit).should.true();

  done();
});
```