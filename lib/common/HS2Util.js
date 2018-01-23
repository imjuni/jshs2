const _ = require('lodash');
const Int64 = require('node-int64');
const Big = require('big.js');

const FETCH_TYPE = {
  ROW: 0,
  LOG: 1,
};

const HIVE_TYPE = {
  HIVE: 0,
  CDH: 1,
};

const BITMASK = [1, 2, 4, 8, 16, 32, 64, 128];

let OPERATION_STATES = null;
let TTYPE_ID = null;

class HS2Util {
  static sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve();
      }, ms);
    });
  }

  /* eslint-disable no-bitwise */
  static negate(value) {
    let result = (value ^ 0xff) + 1;
    result &= 0xff;

    return result;
  }

  static getInt64(buffer, offset) {
    // This code from Int64 toNumber function. Using Big.js, convert to string.
    const b = buffer;
    const o = offset;

    // Running sum of octets, doing a 2's complement
    const negate = b[o] & 0x80;
    let value = new Big(0);
    let m = new Big(1);
    let carry = 1;

    for (let i = 7; i >= 0; i -= 1) {
      let v = b[o + i];

      // 2's complement for negative numbers
      if (negate) {
        v = (v ^ 0xff) + carry;
        carry = v >> 8;
        v &= 0xff;
      }

      value = value.plus((new Big(v)).times(m));
      m = m.times(256);
    }

    if (negate) {
      value = value.times(-1);
    }

    return value;
  }
  /* eslint-enable */

  static modifyVal(storedType, value, i64ToString) {
    if (storedType === 'i64Val') {
      if (value && value.buffer) {
        if (i64ToString) {
          return HS2Util.getInt64(value.buffer, value.offset).toString();
        }

        return (new Int64(value.buffer, value.offset)).toString();
      } else if (value === null || value === undefined) {
        return null;
      }

      return 'int64_error';
    }

    return value;
  }

  static makeJson(storedTypes, columns, schema) {
    const rows = [];
    const ilen = _(columns).first()[_(storedTypes).first()].values.length;
    const jlen = columns.length;

    for (let i = 0; i < ilen; i += 1) {
      const row = {};

      for (let j = 0; j < jlen; j += 1) {
        row[schema[j].columnName] = HS2Util
          .modifyVal(storedTypes[j], columns[j][storedTypes[j]].values[i]);
      }

      rows.push(row);
    }

    return rows;
  }

  /* eslint-disable no-bitwise */
  static getIsNull(buf, index) {
    const bufIdx = (index !== 0) ? Math.floor(index / 8) : 0;
    const pos = index % 8;

    return !!(buf[bufIdx] & BITMASK[pos]);
  }
  /* eslint-enable */

  static makeArray(configure, storedTypes, columns) {
    const rows = [];
    const rowLen = _(columns).first()[_(storedTypes).first()].values.length;
    const colLen = columns.length;
    let rowIdx;
    let colIdx;
    let row;
    let colType;

    for (rowIdx = 0; rowIdx < rowLen; rowIdx += 1) {
      row = [];

      for (colIdx = 0; colIdx < colLen; colIdx += 1) {
        colType = storedTypes[colIdx];

        if (HS2Util.getIsNull(columns[colIdx][colType].nulls, rowIdx)) {
          row.push(configure.NullStr);
        } else {
          row.push(HS2Util.modifyVal(colType, columns[colIdx][colType]
            .values[rowIdx], configure.I64ToString));
        }
      }

      rows.push(row);
    }

    return rows;
  }

  static makeStoredType(results) {
    return results.columns.map((column) => {
      let resultType = 'stringVal';

      if (column.binaryVal) {
        resultType = 'binaryVal';
      } else if (column.boolVal) {
        resultType = 'boolVal';
      } else if (column.byteVal) {
        resultType = 'byteVal';
      } else if (column.doubleVal) {
        resultType = 'doubleVal';
      } else if (column.i16Val) {
        resultType = 'i16Val';
      } else if (column.i32Val) {
        resultType = 'i32Val';
      } else if (column.i64Val) {
        resultType = 'i64Val';
      } else { // stringVal
        resultType = 'stringVal';
      }

      return resultType;
    });
  }

  static getState(serviceType, id) {
    if (!OPERATION_STATES) {
      OPERATION_STATES = Object.keys(serviceType.TOperationState)
        .map(state => state.replace('_STATE', '').toLowerCase());
    }

    const stateIndex = id || serviceType.TOperationState.UKNOWN_STATE;
    return OPERATION_STATES[parseInt(stateIndex, 10)];
  }

  static getType(serviceType, typeDesc) {
    if (!TTYPE_ID) {
      TTYPE_ID = Object
        .keys(serviceType.TTypeId)
        .map(typeName => typeName.replace('_TYPE', '').toLowerCase());
    }

    return ['string', typeDesc].reduce((type, description) => {
      let result;
      const [firstDescription] = description.types;

      if (firstDescription.primitiveEntry) {
        result = TTYPE_ID[firstDescription.primitiveEntry.type];
      } else if (firstDescription.mapEntry) {
        result = TTYPE_ID[firstDescription.mapEntry.type];
      } else if (firstDescription.unionEntry) {
        result = TTYPE_ID[firstDescription.unionEntry.type];
      } else if (firstDescription.arrayEntry) {
        result = TTYPE_ID[firstDescription.arrayEntry.type];
      } else if (firstDescription.structEntry) {
        result = TTYPE_ID[firstDescription.structEntry.type];
      } else if (firstDescription.userDefinedTypeEntry) {
        result = TTYPE_ID[firstDescription.userDefinedTypeEntry.type];
      }

      return result;
    });
  }

  static isFinish(cursor, status) {
    const serviceType = cursor.Conn.IDL.ServiceType;

    return (status === serviceType.TOperationState.FINISHED_STATE ||
      status === serviceType.TOperationState.CANCELED_STATE ||
      status === serviceType.TOperationState.CLOSED_STATE ||
      status === serviceType.TOperationState.ERROR_STATE);
  }

  static getValue(colValue) {
    let value = null;

    if (colValue.boolVal) {
      value = colValue.boolVal.value;
    } else if (colValue.byteVal) {
      value = colValue.byteVal.value;
    } else if (colValue.i16Val) {
      value = colValue.i16Val.value;
    } else if (colValue.i32Val) {
      value = colValue.i32Val.value;
    } else if (colValue.i64Val) {
      if (colValue.i64Val.value && colValue.i64Val.value.buffer) {
        value = (new Int64(colValue.i64Val.value.buffer)).toString();
      } else if (colValue.i64Val.value === null || colValue.i64Val.value === undefined) {
        value = null;
      } else {
        value = 'int64_error';
      }
    } else if (colValue.doubleVal) {
      value = colValue.doubleVal.value;
    } else if (colValue.stringVal) {
      value = colValue.stringVal.value;
    }

    return value;
  }

  static getThriftErrorMessage(status, defaultMessage) {
    const errorMessage = status.errorMessage || defaultMessage;
    let infoMessages = status.infoMessages;

    if (infoMessages && infoMessages.length) {
      infoMessages = infoMessages.join('\n');
    } else {
      infoMessages = '';
    }

    return [
      errorMessage,
      infoMessages,
      [
        errorMessage,
        '-- Error caused from HiveServer2\n\n',
        infoMessages,
      ].join('\n\n'),
    ];
  }
}

HS2Util.FETCH_TYPE = FETCH_TYPE;
HS2Util.HIVE_TYPE = HIVE_TYPE;
HS2Util.BITMASK = BITMASK;

module.exports = HS2Util;
