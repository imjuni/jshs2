/* eslint-env node, mocha */
/* global describe, after, it */

const fs = require('fs');
const co = require('co');
const expect = require('chai').expect;
const debug = require('debug')('jshs2:PromiseTest');
const jshs2 = require('../index.js');

const HS2Util = jshs2.HS2Util;
const IDLContainer = jshs2.IDLContainer;
const HiveConnection = jshs2.HiveConnection;
const Configuration = jshs2.Configuration;

describe('ThriftDriverTest', () => {
  // Test environment variable
  const config = JSON.parse(fs.readFileSync('./cluster.json'));
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

  const configuration = new Configuration(options);
  const idl = new IDLContainer();

  let connection;
  let cursor;
  let serviceType;

  before((done) => {
    co(function* coroutine() {
      yield idl.initialize(configuration);

      connection = new HiveConnection(configuration, idl);
      cursor = yield connection.connect();
      serviceType = idl.ServiceType;
    }).then(() => {
      debug('before task complete, ...');

      setImmediate(() => {
        done();
      });
    }).catch((err) => {
      debug('Error caused from before task, ...');
      debug(err.message);
      debug(err.stack);

      setImmediate(() => {
        expect(err).to.not.exist();
      });
    });
  });

  after((done) => {
    co(function* coroutine() {
      yield cursor.close();
      yield connection.close();
    }).then(() => {
      setImmediate(() => {
        done();
      });
    }).catch((err) => {
      debug('Error caused from after task, ...');
      debug(err.message);
      debug(err.stack);

      setImmediate(() => {
        expect(err).to.not.exist();
      });
    });
  });

  it('HiveDriver Promise Test Async', (done) => {
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
      setImmediate(() => {
        expect(data.hasResultSet).to.exist;
        expect(data.rows.length).to.be.a('number');

        done();
      });
    }).catch((err) => {
      debug('Error caused, ');
      debug(`message:  ${err.message}`);
      debug(`stack:  ${err.stack}`);

      setImmediate(() => {
        expect(err).to.not.exist();
      });
    });
  });

  it('HiveDriver Promise Test Sync', (done) => {
    co(function* coroutine() {
      const execResult = yield cursor.execute(config.Query.query, false);

      let fetchResult;
      if (execResult.hasResultSet) {
        const schema = yield cursor.getSchema();

        debug('schema -> ', schema);

        fetchResult = yield cursor.fetchBlock();

        debug('rows ->', fetchResult.rows.length);
        debug('rows ->', fetchResult.hasMoreRows);
      }

      return {
        hasResultSet: execResult.hasResultSet,
        rows: (execResult.hasResultSet) ? fetchResult.rows : [],
      };
    }).then((data) => {
      setImmediate(() => {
        expect(data.hasResultSet).to.exist;
        expect(data.rows.length).to.be.a('number');

        done();
      });
    }).catch((err) => {
      debug('Error caused, ');
      debug(`message:  ${err.message}`);
      debug(`stack:  ${err.stack}`);

      setImmediate(() => {
        expect(err).to.not.exist;
      });
    });
  });
});
