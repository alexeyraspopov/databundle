export function createTable(source) {
  let columns = new Map();
  let length = source.data.length;
  for (let field of source.schema.fields) {
    let column;
    switch (field.type) {
      case "number": {
        let values = source.data.map((record) => toNumber(record[field.name]));
        column = { type: "number", values };
        break;
      }
      case "string": {
        let values = source.data.map((record) => toString(record[field.name]));
        column = { type: "string", values };
        break;
      }
      case "datetime": {
        let values = source.data.map((record) => toDate(record[field.name]));
        column = { type: "datetime", values };
        break;
      }
      default:
        throw new Error();
    }

    columns.set(field.name, column);
  }
  let TArray = getTypedArray(length);
  return {
    columns,
    bitset: null,
    order: TArray.from({ length }, (_, i) => i),
    size: length,
  };
}

function toNumber(value) {
  return value != null ? +value : null;
}

function toString(value) {
  return value != null ? "" + value : null;
}

function toDate(value) {
  return value != null ? new Date(value) : null;
}

export function derive(table, params) {
  let columns = new Map(table.columns);
  let lense = {
    get(key) {
      let column = columns.get(key);
      if (column == null) throw new Error();
      return column;
    },
  };
  for (let key in params) {
    let derivation = params[key];
    let column = derivation(lense);
    columns.set(key, column);
  }
  return {
    columns,
    bitset: table.bitset,
    order: table.order,
    size: table.size,
  };
}

export function query(table, params) {
  let size = table.size;
  let bitsetSize = Math.ceil(size / 32);
  let predicate = getFilterFn(Object.keys(params));
  predicate = predicate(params, table.columns);
  let bitset = new Uint32Array(bitsetSize);
  for (let index = 0; index < size; index++) {
    if (predicate(index)) bitSetOne(bitset, index);
  }
  return {
    columns: table.columns,
    bitset: table.bitset != null ? intersection(bitset, table.bitset) : bitset,
    order: table.order,
    size: table.size,
  };
}

function getFilterFn(keys) {
  let variables = keys.map(
    (key, index) => `predicate${index}=params["${key}"],v${index}=columns.get("${key}").values`,
  );
  let expressions = keys.map((key, index) => `predicate${index}(v${index}[index])`);
  return new Function(
    "params",
    "columns",
    `let ${variables.join(",")};\nreturn index => ${expressions.join("&&")}`,
  );
}

const ONE = 0x80000000;

function intersection(a, b) {
  for (let i = 0; i < a.length; i++) {
    a[i] &= b[i];
  }
  return a;
}

function includes(bits, n) {
  return (bits[n >>> 5] & (ONE >>> n)) !== 0;
}

function bitSetOne(bits, n) {
  bits[n >>> 5] |= ONE >>> n;
}

export function sort(table, params) {
  let sort = getSortFn(Object.keys(params));
  let order = sort(table.order.slice(), params, table.columns);
  return {
    columns: table.columns,
    bitset: table.bitset,
    order,
    size: table.size,
  };
}

function getSortFn(keys) {
  let variables = keys.map(
    (key, index) => `cmp${index}=params["${key}"],v${index}=columns.get("${key}").values`,
  );
  let expressions = keys.map((key, index) => `cmp${index}(v${index}[iA],v${index}[iB])`);
  return new Function(
    "order",
    "params",
    "columns",
    `let ${variables.join(",")};\nreturn order.sort((iA,iB)=>${expressions.join("||")})`,
  );
}

export function rollup(table, params) {
  let result = Object.create(params);
  for (let key in params) {
    let column = table.columns.get(key);
    if (column == null) throw new Error();
    result[key] = result[key](
      column.values,
      table.bitset != null
        ? (
            (bits) => (i) =>
              includes(bits, i)
          )(table.bitset)
        : () => true,
    );
  }
  return result;
}

export function take(table, params) {
  return Array.from(generateRecords(table, params));
}

function* generateRecords(table, { offset, limit }) {
  let columns = Array.from(table.columns.entries());
  for (let cursor = 0, count = 0; count < limit && cursor < table.size; cursor++) {
    let rowIndex = table.order[cursor];
    if (table.bitset != null && !includes(table.bitset, rowIndex)) continue;
    if (offset > 0) {
      offset--;
      continue;
    }
    yield Object.fromEntries(columns.map(([name, column]) => [name, column.values[rowIndex]]));
    count++;
  }
}

const MAX_8BIT_INTEGER = Math.pow(2, 8) - 1;
const MAX_16BIT_INTEGER = Math.pow(2, 16) - 1;
const MAX_32BIT_INTEGER = Math.pow(2, 32) - 1;

function getTypedArray(size) {
  let maxIndex = size - 1;

  if (maxIndex <= MAX_8BIT_INTEGER) {
    return Uint8Array;
  }

  if (maxIndex <= MAX_16BIT_INTEGER) {
    return Uint16Array;
  }

  if (maxIndex <= MAX_32BIT_INTEGER) {
    return Uint32Array;
  }

  return Float64Array;
}
