Connections
----

# Options
create connection이 pyhs2에서 tsocket을 만드는 것과 동일한 효과가 있다. 즉 sasl을 지원하려고 하면, 여기서
protocol을 지원해야 한다는 뜻이다. 하지만 아직 Node.js에서는 해당 부분을 지원하지 않는다. transport와 protocol을 지정하지
않으면 알아서 TBufferedTransport, TBinaryProtocol 으로 만든다. Java나 Python에서는 지원되는데 Node.js에서는
아직 지원되지 않으니 이 부분은 추가적인 개발이 필요하다.

options 설정 가능 목록
transport
protocol
timeout

# Auth
아래 코드는 TSaslTransport가 지원될 때 사용하는 부분.
```
if (opt.auth === 'PLAIN') {
  Connection = authMechanisms[opt.auth].connection;
  connOpts.transport = authMechanisms[opt.auth].transport;
} else {
  Connection = authMechanisms[opt.auth].connection;
  connOpts.transport = authMechanisms[opt.auth].transport;
}
```

# Client Protocol Version
2층 hive를 사용할 때는 req.client_protocol 버전을 0으로 설정해야 정상적으로 동작한다.
층수를 바꾸거나 hive 서버 버전이 변경될 때는 반드시 gen-nodejs에 idl을 새로 컴파일해서 올려야 한다

```
req.client_protocol = 0;
```

# Server option
OpenSession 이 후, res 객체에 포함된 Configuration 객체에는 다음과 같은 설정이 있다.

```
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
```

# CDH 버전 일람
2015-01-27일 CHD 5.3, Hive 0.13.0 기반 IDL 적용