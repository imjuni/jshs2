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

# Simple Usage
```
var jshs2 = require('jshs2');
var async = require('async');

var connOpt = {
  auth: 'NOSASL',
  host: '101.102.103.104',
  port: '1234',
  timeout: 10,
  username: 'batman'
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

# Apache Hive Server2 & CDH
JSHS2 running on CDH(Cloudera's open source software distribution and consists of Apache Hadoop
and additional key open source projects to ensure you get the most out of Hadoop). Because
getLog function exists on CDH Hive only, Not exists Apache Hive. So, if you need using jshs2
on Apache Hive server, remove getLog function.