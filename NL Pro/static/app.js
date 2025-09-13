/* static/app.js v6 */

let chartInstance = null;
let lastSQL = "";
let lastResult = null; // { columns, types, rows }

const $ = (s) => document.querySelector(s);

function setText(el, txt) {
  el.textContent = txt ?? "";
}
function show(el) {
  el.classList.remove("hidden");
}
function hide(el) {
  el.classList.add("hidden");
}
function enable(el, on = true) {
  el.disabled = !on;
}

// ---------------- Schema UI ----------------
// ---------------- Schema UI ----------------
function populateSchemaUI(schema) {
  // schema is an object: { tableName: [{name, type}, ...], ... }
  const tables = Object.keys(schema).sort();

  // fill the multi-select
  const sel = $("#table-select");
  sel.innerHTML = "";
  tables.forEach((t) => {
    const o = document.createElement("option");
    o.value = t;
    o.textContent = t;
    sel.appendChild(o);
  });

  // initial state: show nothing until user selects
  renderSchemaTreeForTables(schema, []);

  // whenever selection changes, re-render the schema list
  sel.addEventListener("change", () => {
    const chosen = selectedTables();
    renderSchemaTreeForTables(schema, chosen);
  });
}

// Render only columns of selected tables
function renderSchemaTreeForTables(schema, tableNames) {
  const tree = $("#schema-tree");
  tree.innerHTML = "";

  if (!tableNames || tableNames.length === 0) {
    tree.innerHTML = `<div class="muted">Select one or more tables above to see their columns.</div>`;
    return;
  }

  tableNames.forEach((t) => {
    const cols = schema[t];
    if (!cols || !cols.length) return;

    const title = document.createElement("div");
    title.className = "tree-title";
    title.textContent = t;
    tree.appendChild(title);

    const ul = document.createElement("ul");
    cols.forEach((c) => {
      const li = document.createElement("li");
      li.textContent = `${c.name} — ${String(c.type || "").toLowerCase()}`;
      ul.appendChild(li);
    });
    tree.appendChild(ul);
  });
}

function selectedTables() {
  return Array.from($("#table-select").selectedOptions).map((o) => o.value);
}

// ---------------- History ----------------
async function refreshHistory() {
  const res = await fetch("/history");
  const data = await res.json();
  const container = $("#history-list");
  if (!data.ok || !data.items || !data.items.length) {
    container.textContent = "No history yet.";
    return;
  }
  container.innerHTML = "";
  data.items.forEach((item) => {
    const card = document.createElement("div");
    card.className = "hist-card";
    card.innerHTML = `
      <div class="hist-ts">${item.ts || ""}</div>
      <div class="hist-q">${item.question || ""}</div>
      <pre class="pre small">${item.sql || ""}</pre>
      <button class="btn btn-light hist-run">Run this SQL</button>
    `;
    card
      .querySelector(".hist-run")
      .addEventListener("click", () => runSQLDirect(item.sql || ""));
    container.appendChild(card);
  });
}

async function clearHistory() {
  await fetch("/history", { method: "DELETE" });
  refreshHistory();
}

// ---------------- Query/Run ----------------
async function runQueryFromQuestion() {
  const question = $("#question").value.trim();
  if (!question) {
    alert("Please enter a question.");
    return;
  }

  setText($("#sql-box"), "Generating SQL…");
  enable($("#export-csv"), false);
  enable($("#export-xlsx"), false);

  const res = await fetch("/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, tables: selectedTables() }),
  });
  const data = await res.json();

  if (!data.ok) {
    setText($("#sql-box"), `Error: ${data.error || "Unknown"}`);
    renderTable({ columns: [], rows: [] });
    destroyChart();
    hide($("#chart-controls"));
    return;
  }

  lastSQL = data.sql || "";
  lastResult = {
    columns: data.columns || [],
    types: data.types || {},
    rows: data.rows || [],
  };

  setText($("#sql-box"), data.sql || "");
  enable($("#export-csv"), true);
  enable($("#export-xlsx"), true);

  renderTable(lastResult);
  renderChartAuto(lastResult);
  refreshHistory();
}

async function runSQLDirect(sql) {
  // Use /query with sql_override to execute directly
  setText($("#sql-box"), sql || "");
  const res = await fetch("/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql_override: sql }),
  });
  const data = await res.json();

  if (!data.ok) {
    setText($("#sql-box"), `Error: ${data.error || "Unknown"}`);
    renderTable({ columns: [], rows: [] });
    destroyChart();
    hide($("#chart-controls"));
    return;
  }

  lastSQL = data.sql || "";
  lastResult = {
    columns: data.columns || [],
    types: data.types || {},
    rows: data.rows || [],
  };

  renderTable(lastResult);
  renderChartAuto(lastResult);
}

// ---------------- Export ----------------
async function exportFile(path) {
  if (!lastSQL) return;
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql: lastSQL }),
  });
  if (!res.ok) return;
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = path.endsWith("csv")
    ? "query_results.csv"
    : "query_results.xlsx";
  a.click();
  URL.revokeObjectURL(url);
}

// ---------------- Table ----------------
function renderTable(result) {
  const wrap = $("#table-wrap");
  const { columns = [], rows = [] } = result || {};
  if (!columns.length) {
    wrap.innerHTML = "<div class='muted'>No rows.</div>";
    return;
  }
  const thead = `<thead><tr>${columns
    .map((c) => `<th>${c}</th>`)
    .join("")}</tr></thead>`;
  const tbody = `<tbody>${rows
    .map(
      (r) =>
        `<tr>${r.map((v) => `<td>${v == null ? "" : v}</td>`).join("")}</tr>`
    )
    .join("")}</tbody>`;
  wrap.innerHTML = `<div class="table-scroller"><table class="table">${thead}${tbody}</table></div>`;
}

// ---------------- Charting ----------------
function destroyChart() {
  if (chartInstance) {
    chartInstance.destroy();
    chartInstance = null;
  }
}

function inferTypes(types) {
  const cols = Object.keys(types || {});
  return {
    dateCols: cols.filter((c) => types[c] === "date"),
    numCols: cols.filter((c) => types[c] === "number"),
    txtCols: cols.filter((c) => types[c] === "text"),
  };
}

function autoChartSuggestion(columns, types, rows) {
  const { dateCols, numCols, txtCols } = inferTypes(types);
  if (!columns.length || !rows.length) return { kind: "kpi", x: null, y: [] };

  if (columns.length === 1) {
    if (numCols.length === 1) return { kind: "hist", x: columns[0], y: [] };
    return { kind: "kpi", x: null, y: [] };
  }

  if (columns.length === 2) {
    const [a, b] = columns;
    const nums = [a, b].filter((c) => types[c] === "number");
    if (nums.length === 1) {
      const y = nums[0];
      const x = a === y ? b : a;
      const kind = types[x] === "date" ? "line" : "bar";
      return { kind, x, y: [y] };
    }
    if (nums.length === 2) return { kind: "scatter", x: a, y: [b] };
    return { kind: "bar-count", x: a, y: [] };
  }

  const dim = dateCols[0] || txtCols[0];
  if (dim && numCols.length >= 1) {
    const kind = types[dim] === "date" ? "line" : "bar";
    return { kind, x: dim, y: numCols.slice(0, 5) };
  }

  if (numCols.length >= 2)
    return { kind: "scatter", x: numCols[0], y: [numCols[1]] };
  if (numCols.length === 1) return { kind: "hist", x: numCols[0], y: [] };
  return { kind: "kpi", x: null, y: [] };
}

function buildChartConfig(kind, columns, types, rows, x, yList) {
  const toNum = (v) => (v == null || v === "" ? null : Number(v));

  if (kind === "kpi") return null;

  if (kind === "scatter") {
    const xi = columns.indexOf(x),
      yi = columns.indexOf(yList[0]);
    return {
      type: "scatter",
      data: {
        datasets: [
          {
            label: `${x} vs ${yList[0]}`,
            data: rows.map((r) => ({ x: toNum(r[xi]), y: toNum(r[yi]) })),
            pointRadius: 3,
          },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    };
  }

  if (kind === "hist") {
    const xi = columns.indexOf(x);
    const vals = rows
      .map((r) => toNum(r[xi]))
      .filter((v) => Number.isFinite(v));
    if (!vals.length) return null;
    const bins = 12,
      min = Math.min(...vals),
      max = Math.max(...vals);
    const step = (max - min) / (bins || 1);
    const counts = new Array(bins).fill(0);
    vals.forEach((v) => {
      const bi = Math.min(bins - 1, Math.floor((v - min) / (step || 1)));
      counts[bi] += 1;
    });
    const labels = counts.map(
      (_, i) =>
        `${(min + i * step).toFixed(1)}–${(min + (i + 1) * step).toFixed(1)}`
    );
    return {
      type: "bar",
      data: {
        labels,
        datasets: [{ label: `Histogram of ${x}`, data: counts }],
      },
      options: { responsive: true, maintainAspectRatio: false },
    };
  }

  if (kind === "bar-count") {
    const xi = columns.indexOf(x);
    const freq = {};
    rows.forEach((r) => {
      const k = r[xi];
      freq[k] = (freq[k] || 0) + 1;
    });
    const labels = Object.keys(freq);
    return {
      type: "bar",
      data: {
        labels,
        datasets: [
          { label: `Count of ${x}`, data: labels.map((k) => freq[k]) },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    };
  }

  if (kind === "line" || kind === "bar") {
    const labels = rows.map((r) => r[columns.indexOf(x)]);
    const datasets = yList.map((y) => ({
      label: y,
      data: rows.map((r) => toNum(r[columns.indexOf(y)])),
      borderWidth: 2,
      tension: 0.25,
    }));
    return {
      type: kind,
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: { y: { beginAtZero: true } },
      },
    };
  }

  return null;
}

function renderChartAuto(result) {
  const { columns, types, rows } = result;
  if (!columns.length || !rows.length) {
    hide($("#chart-controls"));
    destroyChart();
    return;
  }
  const s = autoChartSuggestion(columns, types, rows);

  // populate controls
  const xSel = $("#x-col");
  const ySel = $("#y-col");
  const chartSel = $("#chart-type");
  xSel.innerHTML = "";
  ySel.innerHTML = "";
  columns.forEach((c) => {
    const xo = document.createElement("option");
    xo.value = c;
    xo.textContent = c;
    xSel.appendChild(xo);
    const yo = document.createElement("option");
    yo.value = c;
    yo.textContent = c;
    ySel.appendChild(yo);
  });
  chartSel.value = "auto";
  xSel.value = s.x || columns[0];
  [...ySel.options].forEach((o) => {
    o.selected = s.y?.includes(o.value);
  });

  const kind = chartSel.value === "auto" ? s.kind : chartSel.value;
  const cfg = buildChartConfig(
    kind,
    columns,
    types,
    rows,
    xSel.value,
    [...ySel.selectedOptions].map((o) => o.value)
  );

  destroyChart();
  if (cfg) {
    show($("#chart-controls"));
    const ctx = $("#resultChart").getContext("2d");
    chartInstance = new Chart(ctx, cfg);
  } else {
    hide($("#chart-controls"));
  }
}

function manualPlot() {
  if (!lastResult) return;
  const { columns, types, rows } = lastResult;
  const kindSel = $("#chart-type").value;
  const x = $("#x-col").value;
  const y = [...$("#y-col").selectedOptions].map((o) => o.value);
  const kind =
    kindSel === "auto"
      ? autoChartSuggestion(columns, types, rows).kind
      : kindSel;

  const cfg = buildChartConfig(kind, columns, types, rows, x, y);
  destroyChart();
  if (cfg) {
    const ctx = $("#resultChart").getContext("2d");
    chartInstance = new Chart(ctx, cfg);
  }
}

// ---------------- Init ----------------
function bindEvents() {
  $("#run-btn").addEventListener("click", runQueryFromQuestion);
  $("#export-csv").addEventListener("click", () => exportFile("/export/csv"));
  $("#export-xlsx").addEventListener("click", () =>
    exportFile("/export/excel")
  );
  $("#plot-btn").addEventListener("click", manualPlot);

  $("#hist-refresh").addEventListener("click", refreshHistory);
  $("#hist-clear").addEventListener("click", clearHistory);
}

function init() {
  populateSchemaUI(window.APP_SCHEMA || {});
  bindEvents();
  refreshHistory();
}

document.addEventListener("DOMContentLoaded", init);
