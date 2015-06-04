(function () {
  'use strict';

  var _ = require('lodash');
  var IdlContainer = require('./IdlContainer');
  var util = require('util');
  var Int64 = require('node-int64');

  function HS2Util () {}

  HS2Util.prototype.FETCH_TYPE = {};
  HS2Util.prototype.FETCH_TYPE.ROW = 0;
  HS2Util.prototype.FETCH_TYPE.LOG = 1;

  HS2Util.prototype.HIVE_TYPE = {};
  HS2Util.prototype.HIVE_TYPE.HIVE = 0;
  HS2Util.prototype.HIVE_TYPE.CDH = 1;
  HS2Util.prototype.BITMASK = [1, 2, 4, 8, 16, 32, 64, 128];

  HS2Util.prototype.modifyVal = function modifyVal (storedType, value) {
    if (storedType === 'i64Val') {
      if (value && value.buffer) {
        return (new Int64(value.buffer)).toString();
      } else if (value === null || value === undefined) {
        return null;
      } else {
        return 'int64_error';
      }
    } else {
      return value;
    }
  };

  HS2Util.prototype.makeJson = function makeJson (storedTypes, columns, schema) {
    var rows = [];
    var ilen = _(columns).first()[_(storedTypes).first()].values.length;
    var jlen = columns.length;

    for (var i = 0; i < ilen; i++) {
      var row = {};

      for (var j = 0; j < jlen; j++) {
        row[schema[j].columnName] = modifyVal(storedTypes[j], columns[j][storedTypes[j]].values[i]);
      }

      rows.push(row);
    }

    return rows;
  };

  HS2Util.prototype.getIsNull = function getIsNull (buf, index) {
    var bufIdx = (index !== 0) ? Math.floor(index / 8) : 0;
    var pos = index % 8;

    return !!(buf[bufIdx] & this.BITMASK[pos]);
  };

  HS2Util.prototype.makeArray = function makeArray (configure, storedTypes, columns) {
    var that = this;
    var rows = [];
    var rowLen = _(columns).first()[_(storedTypes).first()].values.length;
    var colLen = columns.length;
    var rowIdx, colIdx, row, colType;

    for (rowIdx = 0; rowIdx < rowLen; rowIdx++) {
      row = [];

      for (colIdx = 0; colIdx < colLen; colIdx++) {
        colType = storedTypes[colIdx];

        if (that.getIsNull(columns[colIdx][colType].nulls, rowIdx)) {
          row.push(configure.getNullStr());
        } else {
          row.push(that.modifyVal(colType, columns[colIdx][colType].values[rowIdx]));
        }
      }

      rows.push(row);
    }

    return rows;
  };

  HS2Util.prototype.makeStoredType = function makeStoredType (results) {
    return _(results.columns).map(function (column) {
      if (!_.isEmpty(column.binaryVal)) {
        return 'binaryVal';
      } else if (!_.isEmpty(column.boolVal)) {
        return 'boolVal';
      } else if (!_.isEmpty(column.byteVal)) {
        return 'byteVal';
      } else if (!_.isEmpty(column.doubleVal)) {
        return 'doubleVal';
      } else if (!_.isEmpty(column.i16Val)) {
        return 'i16Val';
      } else if (!_.isEmpty(column.i32Val)) {
        return 'i32Val';
      } else if (!_.isEmpty(column.i64Val)) {
        return 'i64Val';
      } else { // stringVal
        return 'stringVal';
      }
    }).value();
  };

  HS2Util.prototype.getState = function getState (serviceType, id) {
    var that = this;

    if (!that.states) {
      that.states = _.map(_.keys(serviceType.TOperationState), function (state) {
        return state.replace('_STATE', '').toLowerCase();
      });
    }

    id = id || serviceType.TTypes.TOperationState.UKNOWN_STATE;
    return that.states[parseInt(id, 10)];
  };

  HS2Util.prototype.getType = function getType (serviceType, typeDesc) {
    var that = this;
    var i, len;

    if (!that.types) {
      that.types = _.map(_.keys(serviceType.TTypeId), function (typeName) {
        return typeName.replace('_TYPE', '').toLowerCase();
      });
    }

    for (i = 0, len = typeDesc.types.length; i < len; i++) {
      if (typeDesc.types[i].primitiveEntry) {
        return that.types[typeDesc.types[i].primitiveEntry.type];
      } else if (typeDesc.types[i].mapEntry) {
        return that.types[typeDesc.types[i].mapEntry.type];
      } else if (typeDesc.types[i].unionEntry) {
        return that.types[typeDesc.types[i].unionEntry.type];
      } else if (typeDesc.types[i].arrayEntry) {
        return that.types[typeDesc.types[i].arrayEntry.type];
      } else if (typeDesc.types[i].structEntry) {
        return that.types[typeDesc.types[i].structEntry.type];
      } else if (typeDesc.types[i].userDefinedTypeEntry) {
        return that.types[typeDesc.types[i].userDefinedTypeEntry.type];
      }
    }
  };

  HS2Util.prototype.getValue = function getValue (colValue) {
    if (colValue.boolVal) {
      return colValue.boolVal.value;
    } else if (colValue.byteVal) {
      return colValue.byteVal.value;
    } else if (colValue.i16Val) {
      return colValue.i16Val.value;
    } else if (colValue.i32Val) {
      return colValue.i32Val.value;
    } else if (colValue.i64Val) {
      if (colValue.i64Val.value && colValue.i64Val.value.buffer) {
        return (new Int64(colValue.i64Val.value.buffer)).toString();
      } else if (colValue.i64Val.value === null || colValue.i64Val.value === undefined) {
        return null;
      } else {
        return 'int64_error';
      }
    } else if (colValue.doubleVal) {
      return colValue.doubleVal.value;
    } else if (colValue.stringVal) {
      return colValue.stringVal.value;
    }
  };

  module.exports = new HS2Util();
})();