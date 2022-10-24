import "./style.css";
import * as d3 from "d3";

import { barChart } from "./bar-chart";
import { Int32, Table, Utf8 } from "apache-arrow";
import { db } from "./duckdb";
import parquet from "./pittsburgh-air-quality.parquet?url";

const app = document.querySelector("#app")!;

// Create the chart. The specific code here makes some assumptions that may not hold for you.
const chart = barChart();

async function update(City: string) {
  // Query DuckDB for the data we want to visualize.
  const data: Table<{ cnt: Int32; AQI: Utf8 }> = await conn.query(`
  SELECT AQI, count(*)::INT as cnt
  FROM pittsburgh-air-quality.parquet
  WHERE City = '${City}'
  GROUP BY AQI
  ORDER BY cnt DESC`);

  // Get the X and Y columns for the chart. Instead of using Parquet, DuckDB, and Arrow, we could also load data from CSV or JSON directly.
  const X = data.getChild("cnt")!.toArray();
  const Y = data
    .getChild("AQI")!
    .toJSON()
    .map((d) => `${d}`);

  chart.update(X, Y);
}

// Load a Parquet file and register it with DuckDB. We could request the data from a URL instead.
const res = await fetch(parquet);
await db.registerFileBuffer(
  "pittsburgh-air-quality.parquet",
  new Uint8Array(await res.arrayBuffer())
);

// Query DuckDB for the locations.
const conn = await db.connect();

const locations: Table<{ location: Utf8 }> = await conn.query(`
SELECT DISTINCT City
FROM pittsburgh-air-quality.parquet`);

// Create a select element for the locations.
const select = d3.select(app).append("select");
for (const City of Cities) {
  select.append("option").text(City.City);
}

select.on("change", () => {
  const City = select.property("value");
  update(City);
});

// Update the chart with the first location.
update("All");

// Add the chart to the DOM.
app.appendChild(chart.element);
