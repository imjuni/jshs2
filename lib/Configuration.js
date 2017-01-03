class Configuration {
  static setDefault(value, defaultValue) {
    let result = value;

    if (!result) {
      result = defaultValue;
    }

    return result;
  }

  constructor(option) {
    this.option = Object.assign({}, option);

    // Default value setting for Connection
    this.option.auth = Configuration.setDefault(this.option.auth.toUpperCase(), 'NOSASL');
    this.option.port = Configuration.setDefault(this.option.port, 10000);
    this.option.username = Configuration.setDefault(this.option.username, 'anonymous');
    this.option.password = Configuration.setDefault(this.option.password, '');
    this.option.hiveType = Configuration.setDefault(this.option.hiveType, 0);
    this.option.hiveVer = Configuration.setDefault(this.option.hiveVer, '1.1.0');
    this.option.thriftVer = Configuration.setDefault(this.option.thriftVer, '0.9.3');
    this.option.cdhVer = Configuration.setDefault(this.option.cdhVer, null);

    // Default value setting for Cursor
    this.option.maxRows = Configuration.setDefault(this.option.maxRows, 5120);
    this.option.nullStr = Configuration.setDefault(this.option.nullStr, 'NULL');
    this.option.i64ToString = Configuration.setDefault(this.option.i64ToString, true);
  }

  get Auth() { return this.option.auth; }
  set Auth(auth) { this.option.auth = auth; }
  get Host() { return this.option.host; }
  set Host(host) { this.option.host = host; }
  get Port() { return this.option.port; }
  set Port(port) { this.option.port = port; }
  get Timeout() { return this.option.timeout; }
  set Timeout(timeout) { this.option.timeout = timeout; }
  get Username() { return this.option.username; }
  set Username(username) { this.option.username = username; }
  get Password() { return this.option.password; }
  set Password(password) { this.option.password = password; }
  get HiveType() { return this.option.hiveType; }
  set HiveType(hiveType) { this.option.hiveType = hiveType; }
  get HiveVer() { return this.option.hiveVer; }
  set HiveVer(hiveVer) { this.option.hiveVer = hiveVer; }
  get ThriftVer() { return this.option.thriftVer; }
  set ThriftVer(thriftVer) { this.option.thriftVer = thriftVer; }
  get CDHVer() { return this.option.cdhVer; }
  set CDHVer(cdhVer) { this.option.cdhVer = cdhVer; }
  get MaxRows() { return this.option.maxRows; }
  set MaxRows(maxRows) { this.option.maxRows = maxRows; }
  get NullStr() { return this.option.nullStr; }
  set NullStr(nullStr) { this.option.nullStr = nullStr; }
  get I64ToString() { return this.option.i64ToString; }
  set I64ToString(i64ToString) { this.option.i64ToString = i64ToString; }
}

module.exports = Configuration;
