const moment = require('moment');

class Timer {
  constructor(format = 'YYYY-MM-DD HH:mm:ss.SSS', diffUnit = 'ms') {
    this.startTime = null;
    this.endTime = null;
    this.diffTime = -1;
    this.diffUnit = diffUnit;
    this.format = format;
  }

  start() {
    this.startTime = moment(new Date());
  }

  end() {
    this.endTime = moment(new Date());
  }

  diff() {
    if (this.startTime && this.endTime) {
      this.diffTime = this.endTime.diff(this.startTime, this.diffUnit);
    }

    return this.diffTime;
  }

  get Start() { return this.startTime.format(this.format); }
  get End() { return this.endTime.format(this.format); }
  get Diff() { return `${this.diffTime}${this.diffUnit}`; }
}

module.exports = Timer;
