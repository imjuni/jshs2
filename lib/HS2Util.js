(function () {
  var Thrift = require('thrift').Thrift;
  var TCLIService = require('../idl/gen-nodejs/TCLIService.js');
  var TCLIServiceTypes = require('../idl/gen-nodejs/TCLIService_types.js');
  var HS2Util = {};

  HS2Util.thriftPatch = function () {
    /*
     * via
     * https://github.com/tagomoris/shib/commit/e682ea3cf6e216c80422dd524a450bc29d2dd019#diff-c0c12a1f158ff7bc2b0c3d0b0445f4a7
     * https://issues.apache.org/jira/browse/THRIFT-1351
     *
     * sessionId.guid와 sessionId.secret는 binary로 다루어야 한다. 그래서, 이 부분을 패치를 해줘야 하는데, 내가 직접 TCLIService_types를
     * 수정하기보다는 이와 같은 형식으로 패치를 하는 것으로 수정했다. 그 이유는 TCLIService_types를 수정하면 가장 간편하게 수정할 수는
     * 있겠지만 추 후 새롭게 IDL을 다시 생성할 경우 이러한 부분을 망각하게 될 확률이 높아서 그냥 patch로 빼고, 다른 사람이 보아도
     * 그 이유를 명확하게 알 수 있도록 변경했다.
     * 그리고, 이 부분은 connection 객체에서 강제로 패치를 진행하게 코드를 추가하여, 동작 자체에 신경쓰는 일은 없도록 할 것이다.
     *
     * 참고로, 2번째 링크에 가보면 thrift 1.0에서 이 문제는 해결되었다.
     *
     */
    TCLIServiceTypes.THandleIdentifier.prototype._read = TCLIServiceTypes.THandleIdentifier.prototype.read;
    TCLIServiceTypes.THandleIdentifier.prototype._write = TCLIServiceTypes.THandleIdentifier.prototype.write;

    TCLIServiceTypes.THandleIdentifier.prototype.read = function(input) {
      input.readStructBegin();
      while (true)
      {
        var ret = input.readFieldBegin();
        var fname = ret.fname;
        var ftype = ret.ftype;
        var fid = ret.fid;
        if (ftype == Thrift.Type.STOP) {
          break;
        }
        switch (fid)
        {
          case 1:
            if (ftype == Thrift.Type.STRING) {
              this.guid = input.readBinary();
            } else {
              input.skip(ftype);
            }
            break;
          case 2:
            if (ftype == Thrift.Type.STRING) {
              this.secret = input.readBinary();
            } else {
              input.skip(ftype);
            }
            break;
          default:
            input.skip(ftype);
        }
        input.readFieldEnd();
      }
      input.readStructEnd();
      return;
    };

    TCLIServiceTypes.THandleIdentifier.prototype.write = function(output) {
      output.writeStructBegin('THandleIdentifier');
      if (this.guid !== null && this.guid !== undefined) {
        output.writeFieldBegin('guid', Thrift.Type.STRING, 1);
        output.writeBinary(this.guid);
        output.writeFieldEnd();
      }
      if (this.secret !== null && this.secret !== undefined) {
        output.writeFieldBegin('secret', Thrift.Type.STRING, 2);
        output.writeBinary(this.secret);
        output.writeFieldEnd();
      }
      output.writeFieldStop();
      output.writeStructEnd();
      return;
    };

  };

  module.exports = HS2Util;
}());