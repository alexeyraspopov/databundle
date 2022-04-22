import { createTable, query, take } from "./databundle.js";
import { table } from "arquero";

let values = Array.from({ length: 50_000 }, () => {
  let number = (Math.random() * 100000) | 0;
  return { value: Math.random() > 0.2 ? number : null };
});
let numbers = values.map((r) => +r.value);
let source = { data: values.slice(), schema: { fields: [{ name: "value", type: "number" }] } };
let t = createTable(source);
let at = table({ value: numbers });
let out;

let suite = new window.Benchmark.Suite();

suite
  .add("databundle", () => {
    out = query(query(t, { value: (v) => v > 312 }), { value: (v) => v < 7690 });
  })
  .add("arquero", () => {
    out = at.filter((d) => d.value > 312).filter((d) => d.value < 7690);
  })
  .on("cycle", (event) => {
    console.log(
      event.target.name,
      event.target.hz.toLocaleString("en-US", { maximumFractionDigits: 0 }) + " ops/sec",
    );
  })
  .on("error", (event) => {
    throw event.message;
  })
  .run();
