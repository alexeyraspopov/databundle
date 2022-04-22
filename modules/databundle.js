// @flow

type TypedArray = Uint8Array | Uint16Array | Uint32Array | Float64Array;

export type Table = {
  columns: Map<string, Column>,
  bitset: ?Uint32Array,
  order: TypedArray,
  size: number,
};

type Column =
  | { type: "number", values: Array<?number> }
  | { type: "string", values: Array<?string> }
  | { type: "datetime", values: Array<?Date> };

type PandasDataFrame = {
  data: Array<{ [k: string]: number | string | Date | null }>,
  schema: {
    fields: Array<{ name: string, type: "number" | "string" | "datetime" }>,
  },
};

export function createTable(source: PandasDataFrame): Table {
  let columns = new Map<string, Column>();
  let length = source.data.length;
  for (let field of source.schema.fields) {
    let column: Column;
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
    // $FlowFixMe Flow doesn't know you can do this trick
    order: TArray.from({ length }, (_, i: number) => i),
    size: length,
  };
}

function toNumber(value: ?any): ?number {
  return value != null ? +value : null;
}

function toString(value: ?any): ?string {
  return value != null ? "" + value : null;
}

function toDate(value: ?any): ?Date {
  return value != null ? new Date(value) : null;
}

type DeriveParams = { [k: string]: ({ get: (string) => Column }) => Column };

export function derive(table: Table, params: DeriveParams): Table {
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
    let column: Column = derivation(lense);
    columns.set(key, column);
  }
  return {
    columns,
    bitset: table.bitset,
    order: table.order,
    size: table.size,
  };
}

export function query(table: Table, params: { [k: string]: Function }): Table {
  let size = table.size;
  let sets = [];
  for (let key in params) {
    let column = table.columns.get(key);
    if (column == null) throw new Error();
    let bitset = new Uint32Array(Math.ceil(size / 32));
    let predicate = params[key];
    let values = column.values;
    for (let index = 0; index < size; index++) {
      let value = values[index];
      if (predicate(value)) bitSetOne(bitset, index);
    }
    sets.push(bitset);
  }
  let bitset = sets.length > 1 ? sets.reduce(intersection) : sets[0];
  return {
    columns: table.columns,
    bitset: table.bitset != null ? intersection(bitset, table.bitset) : bitset,
    order: table.order,
    size: table.size,
  };
}

const ONE = 0x80000000;

function intersection(a: Uint32Array, b: Uint32Array): Uint32Array {
  for (let i = 0; i < a.length; i++) {
    a[i] &= b[i];
  }
  return a;
}

function includes(bits: Uint32Array, n: number): boolean {
  return (bits[n >>> 5] & (ONE >>> n)) !== 0;
}

function bitSetOne(bits: Uint32Array, n: number): void {
  bits[n >>> 5] |= ONE >>> n;
}

type SortParams = { [k: string]: (a: any, b: any) => -1 | 0 | 1 };

export function sort(table: Table, params: SortParams): Table {
  let sort = getSortFn(Object.keys(params));
  let order = sort(table.order.slice(), params, table.columns);
  return {
    columns: table.columns,
    bitset: table.bitset,
    order,
    size: table.size,
  };
}

function getSortFn(keys): (TypedArray, SortParams, Map<string, Column>) => Uint32Array {
  let variables = keys.map(
    (key, index) => `cmp${index}=params["${key}"],v${index}=columns.get("${key}").values`,
  );
  let expressions = keys.map((key, index) => `cmp${index}(v${index}[iA],v${index}[iB])`);
  // $FlowFixMe
  return new Function(
    "order",
    "params",
    "columns",
    `let ${variables.join(",")};\nreturn order.sort((iA,iB)=>${expressions.join("||")})`,
  );
}

export function rollup(table: Table, params: { [k: string]: Function }): Object {
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

export function take(table: Table, params: { offset: number, limit: number }): Array<Object> {
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

function getTypedArray(size): Class<TypedArray> {
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
