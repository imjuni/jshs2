JSHS2
=====

# Introduction
jsHS2 is a node.js client driver for hive server 2. See test/ConnectionTest.js, for an example of how to use it. 
jsHS2 reference from pyhs2 after rewrite javascript. But some feature modify suitable for javascript(ex> Promise support).

jsHS2 include IDL(Interface Description Language). For example, Thrift_0.9.2_Hive_1.1.0 in idl directory.

## I need help!
And I need your help. jsHS2 is not implementation SASL. I hope that someone add SASL on this project.
Contact imjuni@gmail.com with questions

# Install
```
npm install jshs2 --save
```

# Option
## Code
```
var options = {};

options.auth = 'NOSASL';
options.host = '101.102.103.104';           // HiveServer2 hostname
options.port = '12340';                     // HiveServer2 port
options.timeout = 10000;                    // Connection timeout
options.username = 'jshs2tester';           // HiveServer2 user
options.password = '';                      // HiveServer2 password
options.hiveType = hs2util.HIVE_TYPE.HIVE;  // HiveServer2 type, (Hive or CDH Hive)
options.hiveVer = '1.1.0';                  // HiveServer2 Version
options.thriftVer = '0.9.2';                // Thrift version at IDL Compile time

// maybe if you need chdVer below after line 
options.cdhVer = '5.3.0';

options.maxRows = 5120;
options.nullStr = 'NULL';

var configure = new Configure(options);

// using callback
IdlContainer.cb$initialize(configure, function () {
  // insert your code!
});

// using promise
IdlContainer.initialize(configure).then(function () {
  // insert your code!
});
```

### Description
Configure new class. It contain connection configuration and cursor configuration.

* auth - using auth mechanisms. At now only support 'NOSASL'
* host - hive server2 ip address or domain
* port - hive server2 port
* timeout - timeout for connect function
* username - username for hive server, maybe that logging username on hive server
* hiveType - Hive Type, CDH or not (that is hive).
* thriftVer - using thrift version
* cdhVer - if you using CDH, describe version parameter
* maxRows - fetch size
* nullStr - Maybe column value is NULL, replace nullStr. Default value is 'NULL'.

# Hive in CDH
getLog function is difference between vanilla Hive and Hive in CDH(CDH Hive).
CDH Hive must support Hue. And that is display query operation status. So,
Cloudera is add GetLog api on hive.

If you using CDH Hive, describe hiveType 'cdh' after you using getLog function.
Reference Simple Usage

# Interface Description Language(IDL)
Hive support thrift protocol, that is using by IDL. jshs2 can use your idl(idl for your
environment). See under idl directory that was created using by simple rule.

* Use Hive in CDH
    * /idl/Thrift\__[Thrift version]_\_Hive\__[Hive version]_\_CDH\__[CDH version]_
* Use Vanilla Hive
    * /idl/Thrift\__[Thrift version]_\_Hive\__[Hive version]_

## jsHS2 include & Test 
* Thrift\_0.9.2\_Hive\_0.13.1_CDH_5.3.0
* Thrift\_0.9.2\_Hive\_1.0.0
* Thrift\_0.9.2\_Hive\_1.1.0
* Thrift\_0.9.2\_Hive\_1.2.0

## Custom IDL
Interface file compile from Hive(hive-0.13.1-cdh5.3.0/service/if). After copy and rename
jshs2 idl directory, And You specify version. That is it!

# Test
Using mocha. See test directory, and that is good example.
```
sh> cp cluster.json.sample cluster.json # modify value by your environment
sh> mocha test/
```

# Simple Usage
## Node.js >= 0.11.x (or io.js) 
```
var co = require('co');
var debug = require('debug')('jshs2:test');
var jshs2 = require('jshs2');
var hs2util = require('../lib/Common/HS2Util');
var EventEmitter = require('events').EventEmitter;

var Connection = jshs2.PConnection;
var Configuration = jshs2.Configuration;

function waitAndLog (_cursor) {
  var cursor = _cursor;

  return new Promise(function (resolve, reject) {
    var ee = new EventEmitter();

    ee.waitCount = 0;

    ee.on('wait', function () {
      co(function* () {
        var status = yield cursor.getOperationStatus();
        var log = yield cursor.getLog();
        var serviceType = cursor.getConfigure().getServiceType();

        debug('wait, status -> ', hs2util.getState(serviceType, status));
        debug('wait, log -> ', log);

        ee.waitCount = ee.waitCount + 1;

        if (status !== serviceType.TOperationState.FINISHED_STATE) {
          setTimeout(function () {
            ee.emit('wait');
          }, 2000);
        } else if (ee.waitCount > 100) {
          ee.emit('error', new Error('Over exceed timeout'));
        } else {
          ee.emit('finish');
        }
      }).catch(function (err) {
        setImmediate(function () {
          ee.emit('error', err);
        });
      });
    });

    ee.on('finish', function () {
      resolve(true);
    });

    ee.on('error', function (err) {
      reject(err);
    });

    ee.emit('wait');
  });
}

co(function* () {
  // cluster.json file copy from cluster.json.sample. and modify value by your environment
  
  var config = JSON.parse(yield new Promise(function (resolve, reject) {
    fs.readFile('./cluster.json', function (err, buf) {
      if (err) {
        reject(err);
      } else {
        resolve(buf);
      }
    });
  }));

  var options = {};

  options.auth = 'NOSASL';
  options.host = _.first(config.Hive.Servers);
  options.port = config.Hive.Port;
  options.timeout = config.Hive.LoginTimeout;
  options.username = config.Hive.username;
  options.hiveType = hs2util.HIVE_TYPE.HIVE;
  options.hiveVer = '1.1.0';
  options.thriftVer = '0.9.2';

  options.maxRows = 5120;
  options.nullStr = 'NULL';

  var configuration = new Configuration(options);

  yield configuration.initialize();

  var connection = new Connection(configuration);
  var cursor = yield connection.connect();

  yield cursor.execute(config.Hive.query);
  yield waitAndLog(cursor);

  var schema = yield cursor.getSchema();

  debug('schema -> ', schema);

  var result = yield cursor.fetchBlock();

  debug('rows ->', result.rows.length);
  debug('rows ->', result.hasMoreRows);

  yield cursor.close();

  yield connection.close();

}).then(function (res) {
  done();
}).catch(function (err) {
  debug('Error caused, ');
  debug('message:  ' + err.message);
  debug('message:  ' + err.stack);

  setImmediate(function () {
    should.not.exist(err);
  });
});
```

if you use node.js, add option harmony. see below, 

```
sh> node --harmony simpleUsage.js 
```

If you use node.js <= 0.10.x, see test/CallbackTest.js