const path = require('path');
const FactoryError = require('../error/FactoryError');

class IDLFactory {
  static extractConfig(configure) {
    const thrift = `Thrift_${configure.ThriftVer}`;
    const hive = `Hive_${configure.HiveVer}`;
    const [modulePath, cdh] = (() => {
      let result;

      if (configure.CDHVer) {
        const cdhPath = `CDH_${configure.CDHVer}`;
        result = [[thrift, hive, cdhPath].join('_'), cdhPath];
      } else {
        result = [[thrift, hive].join('_'), null];
      }

      return result;
    })();

    return {
      thrift,
      hive,
      cdh,
      path: modulePath,
    };
  }

  /* eslint-disable global-require, import/no-dynamic-require */
  static serviceFactory(configure) {
    return new Promise((resolve, reject) => {
      try {
        const factoryConfig = IDLFactory.extractConfig(configure);
        const modulePath = path.resolve(path.join(__dirname, '..', '..', 'idl',
          factoryConfig.path, 'TCLIService'));
        const service = require(modulePath);

        resolve(service);
      } catch (err) {
        reject(new FactoryError(err));
      }
    });
  }

  static serviceTypeFactory(configure) {
    return new Promise((resolve, reject) => {
      try {
        const factoryConfig = IDLFactory.extractConfig(configure);
        const modulePath = path.resolve(path.join(__dirname, '..', '..', 'idl',
          factoryConfig.path, 'TCLIService_types'));
        const serviceType = require(modulePath);

        resolve(serviceType);
      } catch (err) {
        reject(new FactoryError(err));
      }
    });
  }
  /* eslint-enable */
}

module.exports = IDLFactory;
