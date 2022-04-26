import { test, expect } from "@jest/globals";
import { createTable, query, derive, sort, take, rollup } from "./databundle.js";
import { mean, ascending, descending } from "d3-array";

test("query", () => {
  let table = createTable({
    data: [
      { name: "Ann", age: 27 },
      { name: "Liza", age: 32 },
      { name: "John", age: 29 },
      { name: "Kate", age: 25 },
    ],
    schema: {
      fields: [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
      ],
    },
  });
  let table2 = query(table, {
    age: (v) => v > 27,
  });
  let table3 = query(table2, {
    name: (v) => v === "Liza",
  });
  let table4 = query(table, {
    age: (v) => v > 27,
    name: (v) => v === "Liza",
  });
  expect(take(table2, { offset: 0, limit: 10 })).toEqual([
    { name: "Liza", age: 32 },
    { name: "John", age: 29 },
  ]);
  expect(take(table3, { offset: 0, limit: 10 })).toEqual([{ name: "Liza", age: 32 }]);
  expect(take(table4, { offset: 0, limit: 10 })).toEqual([{ name: "Liza", age: 32 }]);
});

test("derive", () => {
  let table = createTable({
    data: [
      { name: "Ann", age: 27 },
      { name: "Liza", age: 32 },
      { name: "John", age: 29 },
      { name: "Kate", age: 25 },
    ],
    schema: {
      fields: [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
      ],
    },
  });
  let table2 = derive(table, {
    birth: ({ get }) => {
      let age = get("age");
      let dates = age.values.map((n) => (n != null ? new Date(`${2022 - n}-01-01`) : null));
      return { type: "datetime", values: dates };
    },
  });

  let table3 = query(table2, {
    birth: (v) => v.getFullYear() < 1993,
  });
  expect(take(table3, { offset: 0, limit: 10 })).toEqual([
    { name: "Liza", age: 32, birth: new Date("1990-01-01") },
    { name: "John", age: 29, birth: new Date("1993-01-01") },
  ]);
});

test("sort", () => {
  let table = createTable({
    data: [
      { name: "Ann", age: 25 },
      { name: "Liza", age: 32 },
      { name: "John", age: 29 },
      { name: "Kate", age: 25 },
    ],
    schema: {
      fields: [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
      ],
    },
  });

  let table2 = sort(table, {
    age: ascending,
  });
  let table3 = query(table2, {
    age: (v) => v > 27,
  });
  let table4 = sort(table, {
    age: ascending,
    name: descending,
  });

  expect(take(table2, { offset: 0, limit: 10 })).toEqual([
    { name: "Ann", age: 25 },
    { name: "Kate", age: 25 },
    { name: "John", age: 29 },
    { name: "Liza", age: 32 },
  ]);
  expect(take(table3, { offset: 0, limit: 10 })).toEqual([
    { name: "John", age: 29 },
    { name: "Liza", age: 32 },
  ]);
  expect(take(table4, { offset: 0, limit: 10 })).toEqual([
    { name: "Kate", age: 25 },
    { name: "Ann", age: 25 },
    { name: "John", age: 29 },
    { name: "Liza", age: 32 },
  ]);
});

test("rollup", () => {
  let table = createTable({
    data: [
      { name: "Ann", age: 27 },
      { name: "Liza", age: 32 },
      { name: "John", age: 29 },
      { name: "Kate", age: 25 },
    ],
    schema: {
      fields: [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
      ],
    },
  });

  let result = rollup(table, {
    age: (values, includes) => mean(values, (v, i) => (includes(i) ? v : null)),
  });
  let table2 = query(table, {
    age: (v) => v > 27,
  });
  let result2 = rollup(table2, {
    age: (values, includes) => mean(values, (v, i) => (includes(i) ? v : null)),
  });
  expect(result).toEqual({ age: 28.25 });
  expect(result2).toEqual({ age: 30.5 });
});
