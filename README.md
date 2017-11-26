JSHS2
----

# Introduction
JSHS2 is a node.js client driver for hive server 2. See test/PromiseTest.js, for an example of how to use it. JSHS2 reference from pyhs2 after rewrite javascript. But some feature modify suitable for javascript(ex> Promise support).

JSHS2 include IDL(Interface Description Language). For example, Thrift_0.9.2_Hive_1.1.0 in idl directory.

# Important
If you want to use JSHS2, You must change hive server 2 configuration. See [Hive Security Configuration](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2#SettingUpHiveServer2-Authentication/SecurityConfiguration), after change `hive.server2.authentication` value to NOSASL. If you meet connection timeout or querying timeout, most case you don't set NOSASL mode. Because hive server 2 default option is SASL mode.

```
<!-- in conf/hive-site.xml, change configuration restart hive server -->

<configuration>
  <property>
    <name>hive.exec.authentication</name>
    <value>NOSASL</value>
  </property>
</configuration>
```

# What is differenc node-java with Hive JDBC between jshs2?
JDBC connect hive via node-java (using C++ JNI interface). JDBC don't need to set NOSASL, and fully support Apache communify. But JDBC query connection, execution that is some slow because node-jav JNI interface not fast furthermore JDBC need so much loop. If you need execute heavy query that don't suitable for your need.

# Breaking Change
JSHS2 0.4.0 refactoring to Node.js 7.x. JSHS2 develop using by [ES2015](https://github.com/lukehoban/es6features#readme) feature. For example Class, Destructuring, etc...

If you use under Node.js 7.x, use JSHS2 0.3.1.


# I need help!
And I need your help. JSHS2 is not implementation SASL. I hope that someone add SASL on this project.
Contact imjuni@gmail.com with questions

# Install
```
npm i jshs2 --save
```

# Option
## Code
```js
const options = {
  // Connection configuration
  auth: 'NOSASL',
  host: '101.102.103.104',           // HiveServer2 hostname
  port: '12340',                     // HiveServer2 port
  timeout: 10000,                    // Connection timeout
  username: 'jshs2tester',           // HiveServer2 user
  password: '',                      // HiveServer2 password
  hiveType: HS2Util.HIVE_TYPE.HIVE,  // HiveServer2 type, (Hive or CDH Hive)
  hiveVer: '1.1.0',                  // HiveServer2 Version
  thriftVer: '0.9.2',                // Thrift version at IDL Compile time

  // maybe if you need chdVer below after line
  cdhVer: '5.3.0',

  // Cursor configuration
  maxRows: 5120,
  nullStr: 'NULL',
  i64ToString: true,
};


const configure = new Configuration(options);
const idl = new IDLContainer();

idl.initialize(configure).then(() => {
  // your code, ...
});
```

## Description
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
* i64ToString - javascript number type present floating number. So, i64 interger value cannot display using number. If you enable this flag,

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
* Thrift_0.9.2_Hive_0.13.1_CDH_5.3.0
* Thrift_0.9.2_Hive_1.0.0
* Thrift_0.9.2_Hive_1.1.0
* Thrift_0.9.2_Hive_1.2.0
* Thrift_0.9.2_Hive_1.2.1
* Thrift_0.9.2_Hive_2.0.0
* Thrift_0.9.3_Hive_1.0.0
* Thrift_0.9.3_Hive_1.1.0
* Thrift_0.9.3_Hive_1.2.0
* Thrift_0.9.3_Hive_1.2.1
* Thrift_0.9.3_Hive_2.0.0
* Thrift_0.9.3_Hive_2.1.0

## Custom IDL
Interface file compile from Hive(hive-0.13.1-cdh5.3.0/service/if). After copy and rename
jshs2 idl directory, And You specify version. That is it!

# Test
```bash
# Create cluster.json file, after modify value by your environment
$ cp cluster.json.sample cluster.json

$ npm run test     # without debug message
$ npm run test:msg # with debug message
```

# Example
```js
const fs = require('fs');
const co = require('co');
const expect = require('chai').expect;
const debug = require('debug')('jshs2:PromiseTest');
const jshs2 = require('../index.js');

const HS2Util = jshs2.HS2Util;
const IDLContainer = jshs2.IDLContainer;
const HiveConnection = jshs2.HiveConnection;
const Configuration = jshs2.Configuration;

const config = JSON.parse(fs.readFileSync('./cluster.json.sample'));
const options = {};

options.auth = config[config.use].auth;
options.host = config[config.use].host;
options.port = config[config.use].port;
options.timeout = config[config.use].timeout;
options.username = config[config.use].username;
options.hiveType = config[config.use].hiveType;
options.hiveVer = config[config.use].hiveVer;
options.cdhVer = config[config.use].cdhVer;
options.thriftVer = config[config.use].thriftVer;

options.maxRows = config[config.use].maxRows;
options.nullStr = config[config.use].nullStr;
options.i64ToString = config[config.use].i64ToString;

co(function* coroutine() {
  const execResult = yield cursor.execute(config.Query.query);

  for (let i = 0, len = 1000; i < len; i += 1) {
    const status = yield cursor.getOperationStatus();
    const log = yield cursor.getLog();

    debug('wait, status -> ', HS2Util.getState(serviceType, status));
    debug('wait, log -> ', log);

    if (HS2Util.isFinish(cursor, status)) {
      debug('Status -> ', status, ' -> stop waiting');

      break;
    }

    yield HS2Util.sleep(5000);
  }

  debug('execResult -> ', execResult);

  let fetchResult;
  if (execResult.hasResultSet) {
    const schema = yield cursor.getSchema();

    debug('schema -> ', schema);

    fetchResult = yield cursor.fetchBlock();

    debug('first row ->', JSON.stringify(fetchResult.rows[0]));
    debug('rows ->', fetchResult.rows.length);
    debug('rows ->', fetchResult.hasMoreRows);
  }

  return {
    hasResultSet: execResult.hasResultSet,
    rows: (execResult.hasResultSet) ? fetchResult.rows : [],
  };
}).then((data) => {
  console.log(data);
}).catch((err) => {
  debug('Error caused, ');
  debug(`message:  ${err.message}`);
  debug(`stack:  ${err.stack}`);
});
```
