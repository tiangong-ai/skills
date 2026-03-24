from __future__ import annotations

import argparse
import json
from html import escape
from pathlib import Path

from river_outfall_status_lib import load_workbook


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>__TITLE__</title>
  <style>
    :root {
      --bg: #eef2f7;
      --panel: #ffffff;
      --ink: #0f172a;
      --muted: #475569;
      --grid: #dbe4ef;
      --line: #94a3b8;
      --safe: #15803d;
      --partial: #ea580c;
      --submerged: #b91c1c;
      --unknown: #64748b;
      --left-bank: #0f766e;
      --right-bank: #1d4ed8;
      --riverbed: #8b5a2b;
      --levee: #64748b;
      --water-fill: rgba(59, 130, 246, 0.24);
      --shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      background: linear-gradient(180deg, #f8fbfd 0%, var(--bg) 100%);
      color: var(--ink);
      overflow-x: hidden;
    }

    .page {
      width: min(1700px, calc(100vw - 32px));
      margin: 18px auto 28px;
      display: grid;
      gap: 16px;
    }

    .page > *,
    .panel {
      min-width: 0;
      background: var(--panel);
      border-radius: 18px;
      box-shadow: var(--shadow);
      padding: 18px 20px;
    }

    .hero {
      display: grid;
      gap: 12px;
      background:
        radial-gradient(circle at top right, rgba(37, 99, 235, 0.12), transparent 32%),
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.12), transparent 28%),
        var(--panel);
    }

    .hero h1 {
      margin: 0;
      font-size: 28px;
      letter-spacing: 0.02em;
    }

    .hero p,
    .footer-note,
    .summary-box p,
    .summary-box ul {
      margin: 0;
      color: var(--muted);
      line-height: 1.65;
    }

    .note-row,
    .control-row,
    .legend-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 12px;
      align-items: center;
      min-width: 0;
    }

    .control-row .footer-note,
    .timeline-head .footer-note {
      min-width: 0;
      overflow-wrap: anywhere;
    }

    .control-row .footer-note {
      flex: 1 1 320px;
      text-align: right;
    }

    .chip,
    .chip-toggle {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid #dbe4ef;
      background: #f8fafc;
      color: var(--ink);
      font-size: 13px;
      line-height: 1;
      user-select: none;
    }

    .chip::before,
    .chip-toggle::before {
      content: "";
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: #94a3b8;
      flex: 0 0 auto;
    }

    .chip.safe::before { background: var(--safe); }
    .chip.partial::before { background: var(--partial); }
    .chip.submerged::before { background: var(--submerged); }
    .chip.unknown::before { background: var(--unknown); }
    .chip.line-chip::before {
      width: 18px;
      height: 3px;
      border-radius: 999px;
      background: currentColor;
    }
    .chip.fill-chip::before {
      width: 12px;
      height: 12px;
      border-radius: 4px;
      background: var(--water-fill);
      border: 1px solid rgba(37, 99, 235, 0.24);
    }

    .chip-toggle {
      cursor: pointer;
      transition: 140ms ease;
    }

    .chip-toggle.bank-left::before { background: var(--left-bank); }
    .chip-toggle.bank-right::before { background: var(--right-bank); }
    .chip-toggle.bank-unknown::before { background: var(--unknown); }
    .chip-toggle.off {
      background: #ffffff;
      color: #94a3b8;
      border-style: dashed;
    }
    .chip-toggle.off::before { opacity: 0.28; }

    .scenario-bar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .scenario-bar button {
      border: 1px solid #cbd5e1;
      background: #f8fafc;
      color: var(--ink);
      padding: 10px 14px;
      border-radius: 12px;
      cursor: pointer;
      font-size: 14px;
      transition: 160ms ease;
    }

    .scenario-bar button.active {
      color: #ffffff;
      border-color: transparent;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.14);
      transform: translateY(-1px);
    }

    .stats-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }

    .stat-card {
      padding: 16px;
      border-radius: 16px;
      background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
      border: 1px solid #e2e8f0;
      display: grid;
      gap: 8px;
    }

    .stat-card strong {
      font-size: 28px;
      line-height: 1;
    }

    .stat-card .label {
      color: var(--muted);
      font-size: 13px;
    }

    .chart-panel {
      display: grid;
      gap: 14px;
      min-width: 0;
    }

    .chart-host {
      position: relative;
      width: 100%;
      min-width: 0;
      overflow: hidden;
      border: 1px solid #dbe4ef;
      border-radius: 18px;
      background:
        linear-gradient(180deg, rgba(255,255,255,0.96) 0%, rgba(248,250,252,0.96) 100%);
      cursor: grab;
      user-select: none;
    }

    .chart-host.dragging {
      cursor: grabbing;
    }

    #chartCanvas,
    #timelineCanvas {
      display: block;
      width: 100%;
      max-width: 100%;
    }

    .timeline-shell {
      min-width: 0;
      border: 1px solid #dbe4ef;
      border-radius: 16px;
      background: linear-gradient(180deg, #f8fafc 0%, #ffffff 100%);
      padding: 10px 12px 12px;
      display: grid;
      gap: 8px;
    }

    .timeline-head {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      min-width: 0;
    }

    .summary-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      min-width: 0;
    }

    .summary-box {
      display: grid;
      gap: 12px;
    }

    .summary-box h2,
    .table-panel h2 {
      margin: 0;
      font-size: 18px;
    }

    .summary-box ul {
      padding-left: 20px;
    }

    .code-list {
      padding: 10px 12px;
      border-radius: 12px;
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      color: var(--ink);
      min-height: 48px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    th,
    td {
      padding: 10px 8px;
      border-bottom: 1px solid #e2e8f0;
      text-align: left;
      vertical-align: top;
    }

    thead th {
      background: #f8fafc;
      position: sticky;
      top: 0;
      z-index: 1;
    }

    .table-wrap {
      max-height: 360px;
      overflow: auto;
      border: 1px solid #e2e8f0;
      border-radius: 14px;
    }

    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 9px;
      border-radius: 999px;
      color: #fff;
      font-size: 12px;
      font-weight: 600;
    }

    @media (max-width: 1200px) {
      .summary-grid,
      .stats-grid {
        grid-template-columns: 1fr;
      }
    }

    @media (max-width: 900px) {
      .page {
        width: calc(100vw - 20px);
        margin: 10px auto 18px;
        gap: 12px;
      }

      .panel {
        padding: 14px 14px;
        border-radius: 16px;
      }

      .hero h1 {
        font-size: 22px;
      }

      .control-row {
        justify-content: flex-start !important;
      }

      .control-row .footer-note,
      .timeline-head .footer-note {
        flex-basis: 100%;
        text-align: left;
      }
    }
  </style>
</head>
<body>
  <main class="page">
    <section class="panel hero">
      <div>
        <h1>__TITLE__</h1>
        <p id="heroText"></p>
      </div>
      <div class="note-row" id="noteRow"></div>
    </section>

    <section class="panel">
      <div class="control-row" style="justify-content: space-between;">
        <div class="scenario-bar" id="scenarioBar"></div>
        <div class="footer-note">在主图区域滚动鼠标滚轮缩放，按住拖拽可平移，下方总览条用于快速定位。</div>
      </div>
    </section>

    <section class="panel stats-grid">
      <div class="stat-card">
        <span class="label">当前显示排口数</span>
        <strong id="totalCount">0</strong>
      </div>
      <div class="stat-card">
        <span class="label">左/右岸分布</span>
        <strong id="bankCount">0 / 0</strong>
      </div>
      <div class="stat-card">
        <span class="label">未受淹</span>
        <strong id="safeCount" style="color: var(--safe);">0</strong>
      </div>
      <div class="stat-card">
        <span class="label">部分受淹</span>
        <strong id="partialCount" style="color: var(--partial);">0</strong>
      </div>
      <div class="stat-card">
        <span class="label">完全淹没</span>
        <strong id="submergedCount" style="color: var(--submerged);">0</strong>
      </div>
    </section>

    <section class="panel chart-panel">
      <div class="legend-row" id="legendRow"></div>
      <div class="chart-host" id="chartHost">
        <canvas id="chartCanvas"></canvas>
      </div>
      <div class="timeline-shell">
        <div class="timeline-head">
          <strong>河道总览</strong>
          <div class="footer-note" id="timelineInfo"></div>
        </div>
        <canvas id="timelineCanvas"></canvas>
      </div>
    </section>

    <section class="summary-grid">
      <section class="panel summary-box">
        <h2>场景摘要</h2>
        <p id="scenarioSummary"></p>
        <div>
          <div class="footer-note">完全淹没排口</div>
          <div class="code-list" id="submergedCodes"></div>
        </div>
        <div>
          <div class="footer-note">部分受淹排口</div>
          <div class="code-list" id="partialCodes"></div>
        </div>
      </section>

      <section class="panel summary-box">
        <h2>数据提示</h2>
        <ul id="warningList"></ul>
      </section>
    </section>

    <section class="panel table-panel">
      <h2>排口明细</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>编号</th>
              <th>名称</th>
              <th>代码</th>
              <th>岸别</th>
              <th>尺寸</th>
              <th>里程</th>
              <th>底高程</th>
              <th>口顶高程</th>
              <th>河底高程</th>
              <th>堤顶高程</th>
              <th id="scenarioLevelHeader">当前水位</th>
              <th id="scenarioStatusHeader">当前状态</th>
            </tr>
          </thead>
          <tbody id="detailBody"></tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    const REPORT = __REPORT_DATA__;
    const STATUS_META = REPORT.status_meta;
    const BANK_META = REPORT.bank_meta;
    const CONTEXT_COLORS = {
      bed: "#8b5a2b",
      bedFill: "rgba(139, 90, 43, 0.14)",
      bedStripe: "rgba(110, 74, 33, 0.18)",
      levee: "#64748b",
      waterFill: "rgba(59, 130, 246, 0.24)",
    };

    let activeScenario = "__INITIAL_SCENARIO__";
    let horizontalScale = Number(__DEFAULT_HORIZONTAL_SCALE__);
    let panX = 0;
    const bankVisibility = { left: true, right: true, unknown: true };
    let chartDrag = null;
    let timelineDragging = false;
    let chartLayout = null;
    let timelineLayout = null;

    const chartHost = document.getElementById("chartHost");
    const canvas = document.getElementById("chartCanvas");
    const ctx = canvas.getContext("2d");
    const timelineCanvas = document.getElementById("timelineCanvas");
    const timelineCtx = timelineCanvas.getContext("2d");
    const scenarioBar = document.getElementById("scenarioBar");
    const legendRow = document.getElementById("legendRow");

    function formatNumber(value, digits = 2) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return "—";
      }
      const numeric = Number(value);
      if (Math.abs(numeric - Math.round(numeric)) < 1e-6) {
        return String(Math.round(numeric));
      }
      return numeric.toFixed(digits);
    }

    function niceStep(span, target = 7) {
      if (span <= 0) {
        return 1;
      }
      const rough = span / Math.max(target, 1);
      const exponent = Math.floor(Math.log10(rough));
      const fraction = rough / Math.pow(10, exponent);
      let niceFraction = 1;
      if (fraction <= 1) {
        niceFraction = 1;
      } else if (fraction <= 2) {
        niceFraction = 2;
      } else if (fraction <= 5) {
        niceFraction = 5;
      } else {
        niceFraction = 10;
      }
      return niceFraction * Math.pow(10, exponent);
    }

    function scenarioLineLabel(label) {
      return label.includes("水位") ? `${label}线` : `${label}水位线`;
    }

    function scenarioLevelLabel(label) {
      return label.includes("水位") ? label : `${label}水位`;
    }

    function usesKilometers() {
      return (REPORT.bounds.max_mileage - REPORT.bounds.min_mileage) >= 2000;
    }

    function formatMileage(mileage, digits = 1) {
      if (mileage === null || mileage === undefined) {
        return "—";
      }
      if (!usesKilometers()) {
        return `${formatNumber(mileage, 0)} m`;
      }
      const km = Number(mileage) / 1000;
      return `${km.toFixed(digits)} km`;
    }

    function axisMileageLabel(mileage) {
      if (!usesKilometers()) {
        return formatNumber(mileage, 0);
      }
      const km = Number(mileage) / 1000;
      return Math.abs(km - Math.round(km)) < 1e-6 ? `${Math.round(km)}` : km.toFixed(1);
    }

    function currentScenario() {
      return REPORT.scenarios.find((item) => item.key === activeScenario) || REPORT.scenarios[0];
    }

    function visibleOutfalls() {
      return REPORT.outfalls.filter((outfall) => bankVisibility[outfall.bank] !== false);
    }

    function selectedBankLabels() {
      const labels = [];
      ["left", "right", "unknown"].forEach((bankKey) => {
        if (bankVisibility[bankKey] && (REPORT.bank_counts?.[bankKey] || 0) > 0) {
          labels.push(BANK_META[bankKey].label);
        }
      });
      return labels;
    }

    function computeSummaryForScenario(outfalls, scenarioKey) {
      const counts = { safe: 0, partial: 0, submerged: 0, unknown: 0 };
      const submergedCodes = [];
      const partialCodes = [];
      outfalls.forEach((outfall) => {
        const statusInfo = outfall.statuses[scenarioKey] || { status_key: "unknown" };
        const statusKey = statusInfo.status_key || "unknown";
        counts[statusKey] += 1;
        if (statusKey === "submerged") {
          submergedCodes.push(outfall.code);
        } else if (statusKey === "partial") {
          partialCodes.push(outfall.code);
        }
      });
      return { counts, submergedCodes, partialCodes };
    }

    function setCanvasSize(targetCanvas, targetCtx, width, height) {
      const dpr = window.devicePixelRatio || 1;
      targetCanvas.width = Math.round(width * dpr);
      targetCanvas.height = Math.round(height * dpr);
      targetCanvas.style.width = `${width}px`;
      targetCanvas.style.height = `${height}px`;
      targetCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }

    function buildScenarioButtons() {
      scenarioBar.innerHTML = "";
      REPORT.scenarios.forEach((scenario) => {
        const button = document.createElement("button");
        button.textContent = scenario.label;
        button.className = scenario.key === activeScenario ? "active" : "";
        button.style.background = scenario.key === activeScenario ? scenario.color : "#f8fafc";
        button.addEventListener("click", () => {
          activeScenario = scenario.key;
          buildScenarioButtons();
          updateHero();
          updatePanels();
          renderAll();
        });
        scenarioBar.appendChild(button);
      });
    }

    function buildLegend() {
      legendRow.innerHTML = "";
      [["left", "bank-left"], ["right", "bank-right"], ["unknown", "bank-unknown"]].forEach(([bankKey, className]) => {
        if ((REPORT.bank_counts?.[bankKey] || 0) === 0) {
          return;
        }
        const button = document.createElement("button");
        button.type = "button";
        button.className = `chip-toggle ${className} ${bankVisibility[bankKey] ? "" : "off"}`.trim();
        button.textContent = BANK_META[bankKey].label;
        button.addEventListener("click", () => {
          bankVisibility[bankKey] = !bankVisibility[bankKey];
          buildLegend();
          updateHero();
          updatePanels();
          renderAll();
        });
        legendRow.appendChild(button);
      });

      if (REPORT.has_bed_profile) {
        const span = document.createElement("span");
        span.className = "chip line-chip";
        span.style.color = CONTEXT_COLORS.bed;
        span.textContent = "河底线";
        legendRow.appendChild(span);
      }
      if (REPORT.has_levee_profile) {
        const span = document.createElement("span");
        span.className = "chip line-chip";
        span.style.color = CONTEXT_COLORS.levee;
        span.textContent = "堤顶线";
        legendRow.appendChild(span);
      }
      if (REPORT.has_bed_profile) {
        const span = document.createElement("span");
        span.className = "chip fill-chip";
        span.textContent = "当前场景水体";
        legendRow.appendChild(span);
      }

      ["safe", "partial", "submerged", "unknown"].forEach((statusKey) => {
        const span = document.createElement("span");
        span.className = `chip ${statusKey}`;
        span.textContent = STATUS_META[statusKey].label;
        legendRow.appendChild(span);
      });

      REPORT.scenarios.forEach((scenario) => {
        const span = document.createElement("span");
        span.className = "chip line-chip";
        span.style.color = scenario.color;
        span.textContent = scenarioLineLabel(scenario.label);
        legendRow.appendChild(span);
      });
    }

    function updateHero() {
      const scenario = currentScenario();
      const channelText = REPORT.has_bed_profile || REPORT.has_levee_profile
        ? "图中同步叠加河底/堤顶背景剖面，当前场景的河道水体区间以蓝色填充。"
        : "源数据未提供河底/堤顶高程，主图仅显示水位线与排口状态。";
      const bankText = selectedBankLabels().length
        ? `当前显示 ${selectedBankLabels().join("、")}排口。`
        : "当前已取消全部岸别显示。";
      document.getElementById("heroText").textContent =
        `${REPORT.river_name}排口状态可视化图。高亮场景为 ${scenario.label}，` +
        `纵向始终使用真实高程；横向通过滚轮缩放与时间轴总览联动；` +
        `${channelText}${bankText}`;

      const noteRow = document.getElementById("noteRow");
      noteRow.innerHTML = "";
      REPORT.notes.forEach((note) => {
        const span = document.createElement("span");
        span.className = "chip";
        span.textContent = note;
        noteRow.appendChild(span);
      });
    }

    function updatePanels() {
      const filtered = visibleOutfalls();
      const scenario = currentScenario();
      const summary = computeSummaryForScenario(filtered, activeScenario);
      const counts = summary.counts;
      const visibleLeft = filtered.filter((item) => item.bank === "left").length;
      const visibleRight = filtered.filter((item) => item.bank === "right").length;

      document.getElementById("totalCount").textContent = filtered.length;
      document.getElementById("bankCount").textContent = `${visibleLeft} / ${visibleRight}`;
      document.getElementById("safeCount").textContent = counts.safe;
      document.getElementById("partialCount").textContent = counts.partial;
      document.getElementById("submergedCount").textContent = counts.submerged;

      const scopePrefix = filtered.length === REPORT.outfalls.length
        ? `${scenario.label}下共 ${filtered.length} 个排口`
        : `${scenario.label}下当前显示 ${filtered.length} / ${REPORT.outfalls.length} 个排口`;
      document.getElementById("scenarioSummary").textContent =
        `${scopePrefix}，未受淹 ${counts.safe} 个，部分受淹 ${counts.partial} 个，完全淹没 ${counts.submerged} 个，待补充 ${counts.unknown} 个。`;
      document.getElementById("submergedCodes").textContent =
        summary.submergedCodes.length ? summary.submergedCodes.join("、") : "无";
      document.getElementById("partialCodes").textContent =
        summary.partialCodes.length ? summary.partialCodes.join("、") : "无";
      document.getElementById("scenarioLevelHeader").textContent = scenarioLevelLabel(scenario.label);
      document.getElementById("scenarioStatusHeader").textContent = `${scenario.label}状态`;

      const warningList = document.getElementById("warningList");
      warningList.innerHTML = "";
      if (!REPORT.warnings.length) {
        const item = document.createElement("li");
        item.textContent = "未发现结构性告警。";
        warningList.appendChild(item);
      } else {
        REPORT.warnings.slice(0, 12).forEach((warning) => {
          const item = document.createElement("li");
          item.textContent = warning;
          warningList.appendChild(item);
        });
      }

      const body = document.getElementById("detailBody");
      body.innerHTML = "";
      filtered.forEach((outfall) => {
        const statusInfo = outfall.statuses[activeScenario] || {
          status_key: "unknown",
          status_label: "待补充",
          level: null,
        };
        const tr = document.createElement("tr");
        const pillColor = STATUS_META[statusInfo.status_key]?.color || STATUS_META.unknown.color;
        tr.innerHTML = `
          <td>${outfall.number || "—"}</td>
          <td>${outfall.name || "—"}</td>
          <td>${outfall.code}</td>
          <td>${outfall.bank_label}</td>
          <td>${outfall.size_text || "—"}</td>
          <td>${formatMileage(outfall.mileage, 2)}</td>
          <td>${formatNumber(outfall.base_elev, 2)}</td>
          <td>${formatNumber(outfall.crown_elev, 2)}</td>
          <td>${formatNumber(outfall.bed_elev, 2)}</td>
          <td>${formatNumber(outfall.levee_elev, 2)}</td>
          <td>${formatNumber(statusInfo.level, 2)}</td>
          <td><span class="status-pill" style="background:${pillColor};">${statusInfo.status_label}</span></td>
        `;
        body.appendChild(tr);
      });
    }

    function clampPan(nextPan) {
      if (!chartLayout) {
        return 0;
      }
      return Math.max(0, Math.min(nextPan, chartLayout.maxPanX));
    }

    function computeChartLayout() {
      const hostWidth = chartHost.clientWidth || chartHost.parentElement?.clientWidth || 0;
      const viewportWidth = Math.max(hostWidth, 320);
      const compact = viewportWidth < 900;
      const margin = compact
        ? { left: 68, right: 26, top: 60, bottom: 76 }
        : { left: 96, right: 88, top: 74, bottom: 92 };
      const viewportHeight = compact ? 660 : 840;
      const plotViewWidth = Math.max(viewportWidth - margin.left - margin.right, 180);
      const plotViewHeight = Math.max(viewportHeight - margin.top - margin.bottom, 220);
      const worldPlotWidth = plotViewWidth * horizontalScale;
      const maxPanX = Math.max(0, worldPlotWidth - plotViewWidth);
      panX = Math.max(0, Math.min(panX, maxPanX));

      setCanvasSize(canvas, ctx, viewportWidth, viewportHeight);
      chartLayout = {
        viewportWidth,
        viewportHeight,
        plotViewWidth,
        plotViewHeight,
        worldPlotWidth,
        maxPanX,
        pxPerElev: plotViewHeight / (REPORT.bounds.max_elev - REPORT.bounds.min_elev || 1),
        frame: {
          left: margin.left,
          right: viewportWidth - margin.right,
          top: margin.top,
          bottom: viewportHeight - margin.bottom,
          width: plotViewWidth,
          height: plotViewHeight,
        },
        xWorldForMileage(mileage) {
          const span = REPORT.bounds.max_mileage - REPORT.bounds.min_mileage || 1;
          return ((mileage - REPORT.bounds.min_mileage) / span) * worldPlotWidth;
        },
        xForMileage(mileage) {
          return margin.left + this.xWorldForMileage(mileage) - panX;
        },
        yForElev(elev) {
          const span = REPORT.bounds.max_elev - REPORT.bounds.min_elev || 1;
          return (viewportHeight - margin.bottom) - ((elev - REPORT.bounds.min_elev) / span) * plotViewHeight;
        },
      };
    }

    function computeTimelineLayout() {
      const hostWidth = chartHost.clientWidth || chartHost.parentElement?.clientWidth || 0;
      const width = Math.max(hostWidth, 320);
      const compact = width < 900;
      const height = compact ? 84 : 92;
      const margin = compact
        ? { left: 28, right: 12, top: 14, bottom: 22 }
        : { left: 50, right: 20, top: 14, bottom: 22 };
      setCanvasSize(timelineCanvas, timelineCtx, width, height);
      timelineLayout = {
        width,
        height,
        barLeft: margin.left,
        barRight: width - margin.right,
        barTop: 26,
        barHeight: 18,
        axisY: height - margin.bottom,
        barWidth: width - margin.left - margin.right,
      };
    }

    function drawRoundRect(targetCtx, x, y, width, height, radius) {
      const r = Math.min(radius, width / 2, height / 2);
      targetCtx.beginPath();
      targetCtx.moveTo(x + r, y);
      targetCtx.arcTo(x + width, y, x + width, y + height, r);
      targetCtx.arcTo(x + width, y + height, x, y + height, r);
      targetCtx.arcTo(x, y + height, x, y, r);
      targetCtx.arcTo(x, y, x + width, y, r);
      targetCtx.closePath();
    }

    function drawTag(text, centerX, centerY, options = {}) {
      if (!text) {
        return;
      }
      const fontSize = options.fontSize || 11;
      const paddingX = options.paddingX || 6;
      const paddingY = options.paddingY || 3;
      ctx.save();
      ctx.font = `${options.fontWeight || 600} ${fontSize}px Microsoft YaHei`;
      const textWidth = ctx.measureText(text).width;
      const boxWidth = textWidth + paddingX * 2;
      const boxHeight = fontSize + paddingY * 2;
      drawRoundRect(ctx, centerX - boxWidth / 2, centerY - boxHeight / 2, boxWidth, boxHeight, 7);
      ctx.fillStyle = options.background || "rgba(255,255,255,0.92)";
      ctx.fill();
      ctx.strokeStyle = options.border || "rgba(148,163,184,0.35)";
      ctx.lineWidth = 1;
      ctx.stroke();
      ctx.fillStyle = options.color || "#0f172a";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(text, centerX, centerY + 0.5);
      ctx.restore();
    }

    function buildSeriesPoints(sourcePoints, valueAccessor) {
      const rawPoints = (sourcePoints || [])
        .map((point) => {
          const value = valueAccessor(point);
          return {
            mileage: Number(point.mileage),
            value: value === null || value === undefined ? null : Number(value),
          };
        })
        .filter((point) => Number.isFinite(point.mileage) && Number.isFinite(point.value))
        .sort((a, b) => a.mileage - b.mileage);
      if (!rawPoints.length) {
        return [];
      }
      const deduped = [];
      rawPoints.forEach((point) => {
        if (deduped.length && Math.abs(deduped[deduped.length - 1].mileage - point.mileage) < 1e-6) {
          deduped[deduped.length - 1] = point;
        } else {
          deduped.push(point);
        }
      });
      const firstValue = deduped[0].value;
      const lastValue = deduped[deduped.length - 1].value;
      if (deduped[0].mileage > REPORT.bounds.min_mileage) {
        deduped.unshift({ mileage: REPORT.bounds.min_mileage, value: firstValue });
      } else {
        deduped[0] = { mileage: REPORT.bounds.min_mileage, value: firstValue };
      }
      if (deduped[deduped.length - 1].mileage < REPORT.bounds.max_mileage) {
        deduped.push({ mileage: REPORT.bounds.max_mileage, value: lastValue });
      } else {
        deduped[deduped.length - 1] = { mileage: REPORT.bounds.max_mileage, value: lastValue };
      }
      return deduped;
    }

    function buildStepPoints(scenarioKey) {
      return buildSeriesPoints(
        REPORT.profile_points,
        (point) => point.levels && point.levels[scenarioKey]
      );
    }

    function buildChannelPoints(fieldKey) {
      return buildSeriesPoints(REPORT.channel_points, (point) => point[fieldKey]);
    }

    function drawStepSeries(points, color, options = {}) {
      if (points.length < 2) {
        return;
      }
      ctx.save();
      ctx.beginPath();
      ctx.rect(
        chartLayout.frame.left,
        chartLayout.frame.top,
        chartLayout.frame.width,
        chartLayout.frame.height
      );
      ctx.clip();
      ctx.beginPath();
      const first = points[0];
      let previousValue = first.value;
      ctx.moveTo(chartLayout.xForMileage(first.mileage), chartLayout.yForElev(previousValue));
      for (let index = 1; index < points.length; index += 1) {
        const point = points[index];
        const x = chartLayout.xForMileage(point.mileage);
        const previousY = chartLayout.yForElev(previousValue);
        ctx.lineTo(x, previousY);
        if (Math.abs(point.value - previousValue) > 1e-6) {
          ctx.lineTo(x, chartLayout.yForElev(point.value));
        }
        previousValue = point.value;
      }
      ctx.strokeStyle = color;
      ctx.lineWidth = options.lineWidth || 2;
      ctx.globalAlpha = options.alpha ?? 1;
      if (options.dash?.length) {
        ctx.setLineDash(options.dash);
      }
      ctx.stroke();
      ctx.restore();
    }

    function drawStepLine(scenarioKey, color, isActive) {
      drawStepSeries(buildStepPoints(scenarioKey), color, {
        lineWidth: isActive ? 3 : 1.6,
        alpha: isActive ? 0.96 : 0.42,
        dash: isActive ? [] : [7, 5],
      });
    }

    function uniqueBreakpoints(pointSets) {
      const values = [];
      pointSets.forEach((points) => {
        (points || []).forEach((point) => {
          if (Number.isFinite(point.mileage)) {
            values.push(Number(point.mileage));
          }
        });
      });
      values.sort((a, b) => a - b);
      const unique = [];
      values.forEach((value) => {
        if (!unique.length || Math.abs(unique[unique.length - 1] - value) > 1e-6) {
          unique.push(value);
        }
      });
      return unique;
    }

    function stepValueAt(points, mileage) {
      if (!points.length) {
        return null;
      }
      let activeValue = points[0].value;
      for (let index = 1; index < points.length; index += 1) {
        if (mileage < points[index].mileage - 1e-6) {
          break;
        }
        activeValue = points[index].value;
      }
      return activeValue;
    }

    function drawActiveWaterFill() {
      if (!REPORT.has_bed_profile) {
        return;
      }
      const waterPoints = buildStepPoints(activeScenario);
      const bedPoints = buildChannelPoints("bed_elev");
      if (waterPoints.length < 2 || bedPoints.length < 2) {
        return;
      }
      const breakpoints = uniqueBreakpoints([waterPoints, bedPoints]);
      if (breakpoints.length < 2) {
        return;
      }

      ctx.save();
      ctx.beginPath();
      ctx.rect(
        chartLayout.frame.left,
        chartLayout.frame.top,
        chartLayout.frame.width,
        chartLayout.frame.height
      );
      ctx.clip();
      ctx.fillStyle = CONTEXT_COLORS.waterFill;

      for (let index = 0; index < breakpoints.length - 1; index += 1) {
        const startMileage = breakpoints[index];
        const endMileage = breakpoints[index + 1];
        if (endMileage - startMileage <= 1e-6) {
          continue;
        }
        const sampleMileage = startMileage + (endMileage - startMileage) / 2;
        const waterLevel = stepValueAt(waterPoints, sampleMileage);
        const bedLevel = stepValueAt(bedPoints, sampleMileage);
        if (!Number.isFinite(waterLevel) || !Number.isFinite(bedLevel) || waterLevel <= bedLevel) {
          continue;
        }
        const leftX = chartLayout.xForMileage(startMileage);
        const rightX = chartLayout.xForMileage(endMileage);
        const topY = chartLayout.yForElev(waterLevel);
        const bottomY = chartLayout.yForElev(bedLevel);
        ctx.fillRect(leftX, topY, Math.max(1, rightX - leftX), Math.max(1, bottomY - topY));
      }
      ctx.restore();
    }

    function drawRiverbedBackdrop() {
      if (!REPORT.has_bed_profile) {
        return;
      }
      const bedPoints = buildChannelPoints("bed_elev");
      if (bedPoints.length < 2) {
        return;
      }

      ctx.save();
      ctx.beginPath();
      ctx.rect(
        chartLayout.frame.left,
        chartLayout.frame.top,
        chartLayout.frame.width,
        chartLayout.frame.height
      );
      ctx.clip();

      for (let index = 0; index < bedPoints.length - 1; index += 1) {
        const startPoint = bedPoints[index];
        const endPoint = bedPoints[index + 1];
        const leftX = chartLayout.xForMileage(startPoint.mileage);
        const rightX = chartLayout.xForMileage(endPoint.mileage);
        const bedY = chartLayout.yForElev(startPoint.value);
        const segmentHeight = chartLayout.frame.bottom - bedY;
        if (segmentHeight <= 1 || rightX <= leftX) {
          continue;
        }

        ctx.fillStyle = CONTEXT_COLORS.bedFill;
        ctx.fillRect(leftX, bedY, rightX - leftX, segmentHeight);

        ctx.save();
        ctx.beginPath();
        ctx.rect(leftX, bedY, rightX - leftX, segmentHeight);
        ctx.clip();
        ctx.strokeStyle = CONTEXT_COLORS.bedStripe;
        ctx.lineWidth = 0.9;
        ctx.beginPath();
        const stripeGap = 14;
        for (let stripeX = leftX - segmentHeight; stripeX < rightX + segmentHeight; stripeX += stripeGap) {
          ctx.moveTo(stripeX, chartLayout.frame.bottom);
          ctx.lineTo(stripeX + segmentHeight, bedY);
        }
        ctx.stroke();
        ctx.restore();
      }
      ctx.restore();
    }

    function drawChannelProfiles() {
      if (REPORT.has_bed_profile) {
        drawStepSeries(buildChannelPoints("bed_elev"), CONTEXT_COLORS.bed, {
          lineWidth: 2.8,
          alpha: 0.95,
        });
      }
      if (REPORT.has_levee_profile) {
        drawStepSeries(buildChannelPoints("levee_elev"), CONTEXT_COLORS.levee, {
          lineWidth: 2.1,
          alpha: 0.9,
        });
      }
    }

    function outfallPath(geom) {
      ctx.beginPath();
      if (geom.shape === "circle") {
        ctx.arc(geom.centerX, geom.centerY, geom.heightPx / 2, 0, Math.PI * 2);
      } else {
        ctx.rect(geom.left, geom.topY, geom.widthPx, geom.heightPx);
      }
    }

    function computeOutfallGeometry(outfall) {
      if (outfall.base_elev === null || outfall.crown_elev === null) {
        return null;
      }
      const heightPx = Math.max((outfall.crown_elev - outfall.base_elev) * chartLayout.pxPerElev, 8);
      let widthPx = heightPx;
      if (outfall.shape === "rect" && outfall.width_m) {
        widthPx = Math.max(outfall.width_m * chartLayout.pxPerElev, 8);
      }
      const anchorX = chartLayout.xForMileage(outfall.mileage);
      const bankShiftBase = Math.max(22, Math.min(widthPx / 2 + 10, 46));
      const bankOffset = outfall.bank === "left" ? -bankShiftBase : outfall.bank === "right" ? bankShiftBase : 0;
      const centerX = anchorX + bankOffset;
      const topY = chartLayout.yForElev(outfall.crown_elev);
      const bottomY = chartLayout.yForElev(outfall.base_elev);
      return {
        shape: outfall.shape,
        anchorX,
        centerX,
        centerY: (topY + bottomY) / 2,
        topY,
        bottomY,
        left: centerX - widthPx / 2,
        right: centerX + widthPx / 2,
        widthPx,
        heightPx,
      };
    }

    function drawOutfalls() {
      const filtered = visibleOutfalls();
      const activeStatusKey = activeScenario;
      const deferredLabels = [];
      const lanes = { left: [], right: [], unknown: [] };

      function nextLane(bank, centerX, widthPx) {
        const track = lanes[bank] || lanes.unknown;
        const threshold = Math.max(70, widthPx * 0.7);
        const nearby = track.filter((entry) => Math.abs(entry - centerX) < threshold).length;
        track.push(centerX);
        return nearby % 3;
      }

      filtered.forEach((outfall) => {
        const geom = computeOutfallGeometry(outfall);
        if (!geom) {
          return;
        }
        if (geom.right < chartLayout.frame.left - 220 || geom.left > chartLayout.frame.right + 220) {
          return;
        }
        const statusInfo = outfall.statuses[activeStatusKey] || { status_key: "unknown", level: null };
        const strokeColor = STATUS_META[statusInfo.status_key]?.color || STATUS_META.unknown.color;

        ctx.save();
        ctx.beginPath();
        ctx.rect(
          chartLayout.frame.left,
          chartLayout.frame.top,
          chartLayout.frame.width,
          chartLayout.frame.height
        );
        ctx.clip();

        ctx.strokeStyle = outfall.bank_accent || BANK_META.unknown.accent;
        ctx.lineWidth = 1.6;
        ctx.beginPath();
        const connectorEnd = outfall.bank === "left" ? geom.right : outfall.bank === "right" ? geom.left : geom.centerX;
        ctx.moveTo(geom.anchorX, geom.bottomY);
        ctx.lineTo(connectorEnd, geom.bottomY);
        ctx.stroke();

        outfallPath(geom);
        ctx.fillStyle = "rgba(248,250,252,0.95)";
        ctx.fill();

        if (statusInfo.level !== null && statusInfo.level > outfall.base_elev) {
          const cappedLevel = Math.min(statusInfo.level, outfall.crown_elev);
          const waterY = chartLayout.yForElev(cappedLevel);
          ctx.save();
          outfallPath(geom);
          ctx.clip();
          ctx.fillStyle = CONTEXT_COLORS.waterFill;
          ctx.fillRect(geom.left - 2, waterY, geom.widthPx + 4, geom.bottomY - waterY + 2);
          ctx.restore();
        }
        ctx.restore();

        deferredLabels.push({ outfall, geom, strokeColor, lane: nextLane(outfall.bank, geom.centerX, geom.widthPx) });
      });

      REPORT.scenarios.forEach((scenario) => {
        drawStepLine(scenario.key, scenario.color, scenario.key === activeStatusKey);
      });

      deferredLabels.forEach(({ outfall, geom, strokeColor, lane }) => {
        outfallPath(geom);
        ctx.strokeStyle = strokeColor;
        ctx.lineWidth = 2.4;
        ctx.stroke();

        const primaryLabel = outfall.code;
        const secondaryLabel = outfall.size_text || "";
        const canFitPrimaryInside = geom.widthPx >= 54 && geom.heightPx >= 24;
        const canFitSecondaryInside = geom.widthPx >= 44 && geom.heightPx >= 40;

        if (canFitPrimaryInside) {
          drawTag(primaryLabel, geom.centerX, geom.centerY - (canFitSecondaryInside ? 8 : 0), {
            fontSize: 11,
            background: "rgba(255,255,255,0.86)",
          });
        } else {
          drawTag(primaryLabel, geom.centerX, geom.topY - 10 - lane * 16, {
            fontSize: 11,
            background: "rgba(255,255,255,0.94)",
          });
        }

        if (secondaryLabel) {
          if (canFitSecondaryInside) {
            drawTag(secondaryLabel, geom.centerX, geom.centerY + 10, {
              fontSize: 10,
              background: "rgba(248,250,252,0.78)",
              color: "#334155",
            });
          } else {
            drawTag(secondaryLabel, geom.centerX, geom.bottomY + 12, {
              fontSize: 10,
              background: "rgba(248,250,252,0.9)",
              color: "#334155",
            });
          }
        }
      });
    }

    function renderChart() {
      computeChartLayout();
      ctx.clearRect(0, 0, chartLayout.viewportWidth, chartLayout.viewportHeight);
      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, chartLayout.viewportWidth, chartLayout.viewportHeight);
      drawRiverbedBackdrop();
      drawActiveWaterFill();

      const yStep = niceStep(REPORT.bounds.max_elev - REPORT.bounds.min_elev, 8);
      const xStep = niceStep(REPORT.bounds.max_mileage - REPORT.bounds.min_mileage, 8);
      const firstY = Math.ceil(REPORT.bounds.min_elev / yStep) * yStep;
      const firstX = Math.ceil(REPORT.bounds.min_mileage / xStep) * xStep;

      ctx.strokeStyle = "#dbe4ef";
      ctx.lineWidth = 1;
      ctx.font = "12px Microsoft YaHei";
      ctx.fillStyle = "#334155";
      ctx.textAlign = "right";

      for (let elev = firstY; elev <= REPORT.bounds.max_elev + 1e-6; elev += yStep) {
        const y = chartLayout.yForElev(elev);
        ctx.beginPath();
        ctx.moveTo(chartLayout.frame.left, y);
        ctx.lineTo(chartLayout.frame.right, y);
        ctx.stroke();
        ctx.fillText(formatNumber(elev, 2), chartLayout.frame.left - 12, y + 4);
      }

      ctx.textAlign = "center";
      for (let mileage = firstX; mileage <= REPORT.bounds.max_mileage + 1e-6; mileage += xStep) {
        const x = chartLayout.xForMileage(mileage);
        if (x < chartLayout.frame.left - 30 || x > chartLayout.frame.right + 30) {
          continue;
        }
        ctx.beginPath();
        ctx.moveTo(x, chartLayout.frame.top);
        ctx.lineTo(x, chartLayout.frame.bottom);
        ctx.stroke();
        ctx.fillText(axisMileageLabel(mileage), x, chartLayout.frame.bottom + 22);
      }

      ctx.strokeStyle = "#0f172a";
      ctx.lineWidth = 1.8;
      ctx.beginPath();
      ctx.moveTo(chartLayout.frame.left, chartLayout.frame.top);
      ctx.lineTo(chartLayout.frame.left, chartLayout.frame.bottom);
      ctx.lineTo(chartLayout.frame.right, chartLayout.frame.bottom);
      ctx.stroke();

      ctx.save();
      ctx.translate(34, chartLayout.frame.top + chartLayout.frame.height / 2);
      ctx.rotate(-Math.PI / 2);
      ctx.font = "14px Microsoft YaHei";
      ctx.textAlign = "center";
      ctx.fillStyle = "#0f172a";
      ctx.fillText("高程 (m)", 0, 0);
      ctx.restore();

      ctx.textAlign = "center";
      ctx.font = "14px Microsoft YaHei";
      ctx.fillStyle = "#0f172a";
      ctx.fillText(usesKilometers() ? "里程 (km)" : "里程 (m)", chartLayout.frame.left + chartLayout.frame.width / 2, chartLayout.viewportHeight - 22);

      REPORT.controls.forEach((control) => {
        const x = chartLayout.xForMileage(control.mileage);
        if (x < chartLayout.frame.left - 120 || x > chartLayout.frame.right + 120) {
          return;
        }
        ctx.save();
        ctx.setLineDash([5, 4]);
        ctx.strokeStyle = "#475569";
        ctx.lineWidth = 1.2;
        ctx.beginPath();
        ctx.moveTo(x, chartLayout.frame.top);
        ctx.lineTo(x, chartLayout.frame.bottom);
        ctx.stroke();
        ctx.restore();
        drawTag(control.name, x, chartLayout.frame.top - 14, {
          fontSize: 10,
          background: "rgba(255,255,255,0.88)",
          color: "#334155",
        });
      });

      drawChannelProfiles();
      drawOutfalls();
    }

    function renderTimeline() {
      computeTimelineLayout();
      timelineCtx.clearRect(0, 0, timelineLayout.width, timelineLayout.height);
      timelineCtx.fillStyle = "#ffffff";
      timelineCtx.fillRect(0, 0, timelineLayout.width, timelineLayout.height);

      const barY = timelineLayout.barTop;
      drawRoundRect(timelineCtx, timelineLayout.barLeft, barY, timelineLayout.barWidth, timelineLayout.barHeight, 9);
      timelineCtx.fillStyle = "#e2e8f0";
      timelineCtx.fill();

      const xForMileage = (mileage) =>
        timelineLayout.barLeft + ((mileage - REPORT.bounds.min_mileage) / (REPORT.bounds.max_mileage - REPORT.bounds.min_mileage || 1)) * timelineLayout.barWidth;

      REPORT.controls.forEach((control) => {
        const x = xForMileage(control.mileage);
        timelineCtx.strokeStyle = "#475569";
        timelineCtx.lineWidth = 1;
        timelineCtx.beginPath();
        timelineCtx.moveTo(x, barY - 6);
        timelineCtx.lineTo(x, barY + timelineLayout.barHeight + 14);
        timelineCtx.stroke();
      });

      REPORT.outfalls.forEach((outfall) => {
        if (!bankVisibility[outfall.bank]) {
          return;
        }
        const x = xForMileage(outfall.mileage);
        timelineCtx.strokeStyle = outfall.bank === "left" ? "#0f766e" : outfall.bank === "right" ? "#1d4ed8" : "#64748b";
        timelineCtx.lineWidth = 1.6;
        timelineCtx.beginPath();
        timelineCtx.moveTo(x, barY + 2);
        timelineCtx.lineTo(x, barY + timelineLayout.barHeight - 2);
        timelineCtx.stroke();
      });

      const visibleRatio = chartLayout.plotViewWidth / chartLayout.worldPlotWidth;
      const startRatio = panX / chartLayout.worldPlotWidth;
      const brushX = timelineLayout.barLeft + timelineLayout.barWidth * startRatio;
      const brushW = Math.max(22, timelineLayout.barWidth * visibleRatio);
      drawRoundRect(timelineCtx, brushX, barY - 4, brushW, timelineLayout.barHeight + 8, 10);
      timelineCtx.fillStyle = "rgba(37, 99, 235, 0.14)";
      timelineCtx.fill();
      timelineCtx.strokeStyle = "#2563eb";
      timelineCtx.lineWidth = 1.8;
      timelineCtx.stroke();

      const xStep = niceStep(REPORT.bounds.max_mileage - REPORT.bounds.min_mileage, 6);
      const firstX = Math.ceil(REPORT.bounds.min_mileage / xStep) * xStep;
      timelineCtx.font = "11px Microsoft YaHei";
      timelineCtx.textAlign = "center";
      timelineCtx.fillStyle = "#334155";
      for (let mileage = firstX; mileage <= REPORT.bounds.max_mileage + 1e-6; mileage += xStep) {
        const x = xForMileage(mileage);
        timelineCtx.beginPath();
        timelineCtx.moveTo(x, timelineLayout.axisY - 8);
        timelineCtx.lineTo(x, timelineLayout.axisY - 2);
        timelineCtx.strokeStyle = "#cbd5e1";
        timelineCtx.lineWidth = 1;
        timelineCtx.stroke();
        timelineCtx.fillText(axisMileageLabel(mileage), x, timelineLayout.axisY + 10);
      }

      const viewStartMileage = REPORT.bounds.min_mileage + (panX / chartLayout.worldPlotWidth) * (REPORT.bounds.max_mileage - REPORT.bounds.min_mileage);
      const viewEndMileage = REPORT.bounds.min_mileage + ((panX + chartLayout.plotViewWidth) / chartLayout.worldPlotWidth) * (REPORT.bounds.max_mileage - REPORT.bounds.min_mileage);
      document.getElementById("timelineInfo").textContent =
        `当前视窗: ${formatMileage(viewStartMileage, 2)} - ${formatMileage(viewEndMileage, 2)} / 全长 ${formatMileage(REPORT.bounds.max_mileage - REPORT.bounds.min_mileage, 2)}`;
    }

    function renderAll() {
      renderChart();
      renderTimeline();
    }

    function handleWheel(event) {
      event.preventDefault();
      if (!chartLayout) {
        return;
      }
      const rect = chartHost.getBoundingClientRect();
      const localX = event.clientX - rect.left - chartLayout.frame.left;
      const anchor = Math.max(0, Math.min(chartLayout.plotViewWidth, localX));
      const previousWorldWidth = chartLayout.worldPlotWidth;
      const anchorWorldX = panX + anchor;
      const zoomFactor = event.deltaY < 0 ? 1.12 : 1 / 1.12;
      const nextScale = Math.max(1, Math.min(8, horizontalScale * zoomFactor));
      if (Math.abs(nextScale - horizontalScale) < 1e-4) {
        return;
      }
      horizontalScale = nextScale;
      const nextWorldWidth = chartLayout.plotViewWidth * horizontalScale;
      panX = ((anchorWorldX / previousWorldWidth) * nextWorldWidth) - anchor;
      panX = clampPan(panX);
      renderAll();
    }

    function panToClientX(clientX) {
      if (!timelineLayout || !chartLayout) {
        return;
      }
      const rect = timelineCanvas.getBoundingClientRect();
      const localX = Math.max(timelineLayout.barLeft, Math.min(clientX - rect.left, timelineLayout.barRight));
      const ratio = (localX - timelineLayout.barLeft) / timelineLayout.barWidth;
      panX = clampPan(ratio * chartLayout.worldPlotWidth - chartLayout.plotViewWidth / 2);
      renderAll();
    }

    chartHost.addEventListener("wheel", handleWheel, { passive: false });
    chartHost.addEventListener("mousedown", (event) => {
      chartDrag = { startX: event.clientX, startPanX: panX };
      chartHost.classList.add("dragging");
      event.preventDefault();
    });

    timelineCanvas.addEventListener("mousedown", (event) => {
      timelineDragging = true;
      panToClientX(event.clientX);
      event.preventDefault();
    });

    window.addEventListener("mousemove", (event) => {
      if (chartDrag) {
        panX = clampPan(chartDrag.startPanX - (event.clientX - chartDrag.startX));
        renderAll();
      } else if (timelineDragging) {
        panToClientX(event.clientX);
      }
    });

    window.addEventListener("mouseup", () => {
      chartDrag = null;
      timelineDragging = false;
      chartHost.classList.remove("dragging");
    });

    buildScenarioButtons();
    buildLegend();
    updateHero();
    updatePanels();
    renderAll();
    window.addEventListener("resize", renderAll);
  </script>
</body>
</html>
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render a standalone HTML river outfall status report from an Excel workbook."
    )
    parser.add_argument("--input", required=True, help="Path to the input .xlsx workbook.")
    parser.add_argument("--output", required=True, help="Path to the output HTML file.")
    parser.add_argument("--title", help="Optional report title override.")
    parser.add_argument(
        "--initial-scenario",
        choices=["current", "normal", "flood20", "flood50"],
        help="Initial highlighted scenario. Defaults to current when present.",
    )
    parser.add_argument(
        "--default-horizontal-scale",
        type=float,
        default=1.0,
        help="Initial horizontal scale factor for the report UI.",
    )
    return parser


def build_html(
    report: dict[str, object],
    *,
    title: str | None = None,
    initial_scenario: str | None = None,
    default_horizontal_scale: float = 1.0,
) -> str:
    scenario_keys = {scenario["key"] for scenario in report["scenarios"]}
    resolved_initial = (
        initial_scenario
        if initial_scenario in scenario_keys
        else report.get("default_scenario") or next(iter(scenario_keys))
    )
    resolved_title = title or f"{report['river_name']}排口状态可视化图"
    report_json = json.dumps(report, ensure_ascii=False).replace("</", "<\\/")
    return (
        HTML_TEMPLATE.replace("__TITLE__", escape(resolved_title))
        .replace("__REPORT_DATA__", report_json)
        .replace("__INITIAL_SCENARIO__", resolved_initial)
        .replace("__DEFAULT_HORIZONTAL_SCALE__", f"{default_horizontal_scale:.2f}")
    )


def main() -> int:
    args = build_parser().parse_args()
    report = load_workbook(args.input)
    output_html = build_html(
        report,
        title=args.title,
        initial_scenario=args.initial_scenario,
        default_horizontal_scale=args.default_horizontal_scale,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_html, encoding="utf-8")
    print(f"Rendered HTML report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
