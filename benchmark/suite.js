import { createTable, query, sort, rollup, take } from "../modules/databundle.js";
import {
  createTable as createTable_opt,
  query as query_opt,
} from "../modules/databundle-optimized.js";
import * as aq from "arquero";
import { table } from "arquero";
import { ascending, descending } from "d3-array";

export function runFilterBench(env, Suite, size) {
  return new Promise((resolve, reject) => {
    let values = Array.from({ length: size }, () => {
      let number = (Math.random() * 100000) | 0;
      let string = ["ab", "bc", "cd", "de", "ef", "fg", "gh", "hi"][(Math.random() * 9) | 0];
      return {
        valueA: Math.random() > 0.2 ? number : null,
        valueB: Math.random() > 0.2 ? string : null,
      };
    });
    let numbers = values.map((r) => r.valueA);
    let strings = values.map((r) => r.valueB);
    let source = {
      data: values.slice(),
      schema: {
        fields: [
          { name: "valueA", type: "number" },
          { name: "valueB", type: "string" },
        ],
      },
    };
    let t = createTable(source);
    let t_opt = createTable_opt(source);
    let at = table({ valueA: numbers, valueB: strings });
    let out;

    let suite = new Suite();
    let results = [];

    suite
      .add("arquero", () => {
        out = at.filter(
          (d) =>
            d.valueA > 312 &&
            d.valueA < 7690 &&
            (d.valueB === "cd" || d.valueB === "ef" || d.valueB === "hi"),
        );
      })
      .add("databundle", () => {
        out = query(t, {
          valueA: (v) => v > 312 && v < 7690,
          valueB: (v) => v === "cd" || v === "ef" || v === "hi",
        });
      })
      .add("javascript", () => {
        out = values.filter(
          (d) =>
            d.valueA > 312 &&
            d.valueA < 7690 &&
            (d.valueB === "cd" || d.valueB === "ef" || d.valueB === "hi"),
        );
      })
      .add("precomputed", () => {
        out = query_opt(t_opt, {
          valueA: { min: 312, max: 7690 },
          valueB: ["cd", "ef", "hi"],
        });
      })
      .on("cycle", (event) => {
        results.push({ env, impl: event.target.name, rows: size, ops: event.target.hz | 0 });
      })
      .on("error", (event) => {
        reject(event);
      })
      .on("complete", () => {
        resolve(results);
      })
      .run();
  });
}

export function runSortBench(env, Suite, size) {
  return new Promise((resolve, reject) => {
    let values = Array.from({ length: size }, () => {
      let number = (Math.random() * 100000) | 0;
      let string = ["ab", "bc", "cd", "de", "ef", "fg", "gh", "hi"][(Math.random() * 9) | 0];
      return {
        valueA: Math.random() > 0.2 ? number : null,
        valueB: Math.random() > 0.2 ? string : null,
      };
    });
    let numbers = values.map((r) => r.valueA);
    let strings = values.map((r) => r.valueB);
    let source = {
      data: values.slice(),
      schema: {
        fields: [
          { name: "valueA", type: "number" },
          { name: "valueB", type: "string" },
        ],
      },
    };
    let t = createTable(source);
    let at = table({ valueA: numbers, valueB: strings });
    let out;

    let suite = new Suite();
    let results = [];

    suite
      .add("arquero", () => {
        out = at.orderby("valueA", aq.desc("valueB")).slice(0, 10);
      })
      .add("databundle", () => {
        out = take(sort(t, { valueA: ascending, valueB: descending }), { offset: 0, limit: 10 });
      })
      .add("javascript", () => {
        let order = Uint32Array.from({ length: values.length }, (_, i) => i);
        out = order.sort(
          (iA, iB) =>
            ascending(values[iA].valueA, values[iB].valueA) ||
            descending(values[iA].valueB, values[iB].valueB),
        );
      })
      .on("cycle", (event) => {
        results.push({ env, impl: event.target.name, rows: size, ops: event.target.hz | 0 });
      })
      .on("error", (event) => {
        reject(event);
      })
      .on("complete", () => {
        resolve(results);
      })
      .run();
  });
}

export function runAggrBench(env, Suite, size) {
  return new Promise((resolve, reject) => {
    let values = Array.from({ length: size }, () => {
      let number = (Math.random() * 100000) | 0;
      let string = ["ab", "bc", "cd", "de", "ef", "fg", "gh", "hi"][(Math.random() * 9) | 0];
      return {
        valueA: Math.random() > 0.2 ? number : null,
        valueB: Math.random() > 0.2 ? string : null,
      };
    });
    let numbers = values.map((r) => r.valueA);
    let strings = values.map((r) => r.valueB);
    let source = {
      data: values.slice(),
      schema: {
        fields: [
          { name: "valueA", type: "number" },
          { name: "valueB", type: "string" },
        ],
      },
    };
    let t = createTable(source);
    let at = table({ valueA: numbers, valueB: strings });
    let filtered_at = at.filter(
      (d) =>
        d.valueA > 312 &&
        d.valueA < 7690 &&
        (d.valueB === "cd" || d.valueB === "ef" || d.valueB === "hi"),
    );
    let filtered_t = query(t, {
      valueA: (v) => v > 312 && v < 7690,
      valueB: (v) => v === "cd" || v === "ef" || v === "hi",
    });
    let filtered_values = values.filter(
      (d) =>
        d.valueA > 312 &&
        d.valueA < 7690 &&
        (d.valueB === "cd" || d.valueB === "ef" || d.valueB === "hi"),
    );

    let out;

    let suite = new Suite();
    let results = [];

    suite
      .add("arquero", () => {
        let originalCount = at.rollup({ m: (d) => op.max(d.valueA) });
        let filteredCount = filtered_at.count({ m: (d) => op.max(d.valueA) });
        out = [originalCount, filteredCount];
      })
      .add("databundle", () => {
        let aggr = rollup(filtered_t, {
          valueA(values, includes) {
            return values.reduce(
              (acc, v, i) => {
                if (v > acc.uC) {
                  acc.uC = v;
                }
                if (includes(i)) {
                  if (v > acc.fC) {
                    acc.fC = v;
                  }
                }
                return acc;
              },
              { fC: 0, uC: 0 },
            );
          },
        });
        out = [aggr.uC, aggr.fC];
      })
      .add("javascript", () => {
        let originalCount = values.reduce((acc, v) => (v > acc ? v : acc), 0);
        let filteredCount = filtered_values.reduce((acc, v) => (v > acc ? v : acc), 0);
        out = [originalCount, filteredCount];
      })
      .on("cycle", (event) => {
        results.push({ env, impl: event.target.name, rows: size, ops: event.target.hz | 0 });
      })
      .on("error", (event) => {
        reject(event);
      })
      .on("complete", () => {
        resolve(results);
      })
      .run();
  });
}
