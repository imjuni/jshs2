const IDLFactory = require('./IDLFactory');
const debug = require('debug')('jshs2:IDLContainer');

class IDLContainer {
  constructor() {
    this.isInit = false;
  }

  initialize(configure) {
    return new Promise((resolve, reject) => {
      Promise.all([
        IDLFactory.serviceFactory(configure),
        IDLFactory.serviceTypeFactory(configure),
      ]).then((values) => {
        [this.service, this.serviceType] = values;
        this.isInit = true;

        debug('IDLContainer, isInit -> ', this.isInit);

        resolve(values);
      }).catch(err => reject(err));
    });
  }

  get Service() { return this.service; }
  get ServiceType() { return this.serviceType; }
  get IsInit() { return this.isInit; }
}

module.exports = IDLContainer;
