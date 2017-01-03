const IDLFactory = require('./IDLFactory');
const debug = require('debug')('jshs2:IDLContainer');

class IDLContainer {
  constructor() {
    this.isInit = false;
  }

  * initialize(configure) {
    this.service = yield IDLFactory.serviceFactory(configure);
    this.serviceType = yield IDLFactory.serviceTypeFactory(configure);

    this.isInit = true;

    debug(`isInit -> ${this.isInit}`);

    return [this.service, this.serviceType];
  }

  get Service() { return this.service; }
  get ServiceType() { return this.serviceType; }
  get IsInit() { return this.isInit; }
}

module.exports = IDLContainer;
