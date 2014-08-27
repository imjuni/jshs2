"use strict";

var _ = require('underscore'),
  debug = require('debug')('jshs2:connection'),
  thrift = require('thrift'),
  TCLIService = require('../idl/gen-nodejs/TCLIService.js'),
  TCLIServiceTypes = require('../idl/gen-nodejs/TCLIService_types.js'),
  Cursor = require('./Cursor')(),
  HS2Error = require('./HS2Error')(),
  hsutil = require('./HS2Util');

module.exports = function () {
  // 패치 진행
  hsutil.thriftPatch();

  function Connection (options) {

    if (!options.host) {
      throw new Error('need host');
    }

    var authMechanisms = _.keys(Connection.prototype.authMechanisms);
    if (!options.auth || authMechanisms.indexOf(options.auth) < 0) {
      throw new Error('select authenticate mechanism');
    }

    options.port = options.port || 10000;

    this._options = options || {};
    this._options.timeout = options.timeout || 5;

    debug('instance created');
  }

  Connection.prototype.authMechanisms = {
    NOSASL: {
      connection: thrift.createConnection,
      transport: thrift.TBufferedTransport
    },
    PLAIN: {
      connection: thrift.createSASLConnection,
      transport: thrift.TSaslClientTransport
    }
  };

  Connection.prototype.options = function () {
    return this._options;
  };

  Connection.prototype.session = function () {
    return this._session;
  };

  Connection.prototype.client = function () {
    return this._client;
  };

  Connection.prototype.cursor = function (options) {
    return new Cursor(this, options);
  };

  Connection.prototype.connect = function (callback) {
    debug('connect:start');
    var self = this;

    /*
     * create connection이 pyhs2에서 tsocket을 만드는 것과 동일한 효과가 있다
     * 즉 sasl을 지원하려고 하면, 여기서 이미 protocol을 지원해야 한다는 뜻.
     *
     * 하지만...지원 안함...
     *
     * transport와 protocol을 지정하지 않으면 알아서 TBufferedTransport, TBinaryProtocol 으로 만든다
     *
     * options 설정 가능 목록
     * transport
     * protocol
     * timeout
     */

    var createConnection, connOptions;

    connOptions = { timeout: self._options.timeout };

    if (self._options.auth === 'PLAIN') {
      createConnection = self.authMechanisms[self._options.auth].connection;
      connOptions.transport = self.authMechanisms[self._options.auth].transport;

    } else {
      createConnection = self.authMechanisms[self._options.auth].connection;
      connOptions.transport = self.authMechanisms[self._options.auth].transport;
    }

    self.connection = createConnection(self._options.host, self._options.port, connOptions);

    var client = thrift.createClient(TCLIService, self.connection);
    var req = new TCLIServiceTypes.TOpenSessionReq();

    // 2층 hive를 사용할 때는 req.client_protocol 버전을 0으로 설정해야 정상적으로 동작한다.
    // 층수를 바꾸거나 hive 서버 버전이 변경될 때는 반드시 gen-nodejs에 idl을 새로 컴파일해서 올려야 한다
    req.client_protocol = 0;

    self._client = client;

    req.username = self._options.username || 'anonymous';
    req.password = self._options.password || '';

    /*
     * Configuration
     *
       {
         ABORT_ON_DEFAULT_LIMIT_EXCEEDED: '0',
         ABORT_ON_ERROR: '0',
         ALLOW_UNSUPPORTED_FORMATS: '0',
         BATCH_SIZE: '0',
         DEBUG_ACTION: '',
         DEFAULT_ORDER_BY_LIMIT: '-1',
         DISABLE_CACHED_READS: '0',
         DISABLE_CODEGEN: '0',
         EXPLAIN_LEVEL: '0',
         HBASE_CACHE_BLOCKS: '0',
         HBASE_CACHING: '0',
         MAX_ERRORS: '0',
         MAX_IO_BUFFERS: '0',
         MAX_SCAN_RANGE_LENGTH: '0',
         MEM_LIMIT: '0',
         NUM_NODES: '0',
         NUM_SCANNER_THREADS: '0',
         PARQUET_COMPRESSION_CODEC: 'SNAPPY',
         PARQUET_FILE_SIZE: '0',
         REQUEST_POOL: '',
         RESERVATION_REQUEST_TIMEOUT: '0',
         SYNC_DDL: '1',
         V_CPU_CORES: '0'
       }
     */

    self.connection.on('error', function (err) {
      debug('open session:catch uncatched error');

      callback(err);
    });

    debug('open session:start');
    debug('open session:target: ' + self._options.host + ':' + self._options.port);

    client.OpenSession(req, function (err, res) {
      debug('open session:end');

      if (err) {
        callback(new HS2Error.ConnectionError(err.message));
      } else {
        if (res.status.statusCode === TCLIServiceTypes.TStatusCode.ERROR_STATUS) {
          callback(new HS2Error.ConnectionError(res.status.errorMessage));
        } else {
          self._session = res.sessionHandle;
          self.configuration = res.configuration;
          self.serverProtocolVersion = res.serverProtocolVersion;

          callback(err, self);

          debug('connect:end');
        }
      }
    });


  };

  Connection.prototype.close = function (callback) {
    debug('connection close:start');

    var self = this;

    if (self._session) {
      var req = TCLIServiceTypes.TCloseSessionReq({
        sessionHandle: self._session
      });

      debug('session close:start');
      self._client.CloseSession(req, function (err) {
        debug('session close:end');

        self.connection.end();

        callback(err);
      });
    } else {
      self.connection.end();

      debug('connection close:end');
    }
  };

  return Connection;
};