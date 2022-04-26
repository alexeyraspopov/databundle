import * as methods from "./suite.js";
import benchmark from "benchmark";
import { chromium, firefox, webkit } from "playwright";

let benchType = "runAggrBench";

let sizes = [
  100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000,
];

console.log("node");
let sample = await runNodeBench(benchType, sizes);
console.log(sample);

for (let browserType of [webkit, chromium, firefox]) {
  console.log(browserType.name());
  let sample = await runBrowserBench(browserType, benchType, sizes);
  console.log(sample);
}

async function runNodeBench(benchType, sizes) {
  let results = [];
  for (let size of sizes) {
    let sample = await methods[benchType]("node", benchmark.Suite, size);
    results.push(...sample);
  }
  return results;
}

async function runBrowserBench(browserType, benchType, sizes) {
  let results = [];
  let browser = await browserType.launch({ headless: true });

  let context = await browser.newContext();
  let page = await context.newPage();
  await page.goto("http://localhost:1234/");
  await page.waitForTimeout(1000);

  for (let size of sizes) {
    let sample = await page.evaluate(
      ([browserName, benchType, size]) => {
        return window[benchType](browserName, window.Benchmark.Suite, size);
      },
      [browserType.name(), benchType, size],
    );

    results.push(...sample);
  }

  await browser.close();

  return results;
}
