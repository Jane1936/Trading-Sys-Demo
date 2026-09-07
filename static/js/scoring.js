(() => {
  'use strict';
  let scoreTrendChart = null;
  const refreshScoreTrendChartLayout = () => scoreTrendChart?.resize();

  const buttons = Array.from(document.querySelectorAll('.sidebar .tab-btn[data-tab]'));
  const panels = Array.from(document.querySelectorAll('.content > .panel'));
  buttons.forEach(button => button.addEventListener('click', () => {
    buttons.forEach(item => item.classList.toggle('active', item === button));
    panels.forEach(panel => panel.classList.toggle('active', panel.id === button.dataset.tab));
    if (button.dataset.tab === 'tab-score-trend') requestAnimationFrame(refreshScoreTrendChartLayout);
  }));

  const scoreButtons = Array.from(document.querySelectorAll('.score-rule-tab'));
  const scorePanels = Array.from(document.querySelectorAll('.score-rule-panel'));
  scoreButtons.forEach(button => button.addEventListener('click', () => {
    scoreButtons.forEach(item => item.classList.toggle('active', item === button));
    scorePanels.forEach(panel => panel.classList.toggle('active', panel.id === button.dataset.scoreTab));
  }));

  const strategyButtons = Array.from(document.querySelectorAll('[data-strategy-tab]'));
  const strategyPanels = Array.from(document.querySelectorAll('.strategy-subpanel'));
  strategyButtons.forEach(button => button.addEventListener('click', () => {
    strategyButtons.forEach(item => {
      const active = item === button;
      item.classList.toggle('active', active);
      item.setAttribute('aria-selected', String(active));
    });
    strategyPanels.forEach(panel => panel.classList.toggle('active', panel.id === button.dataset.strategyTab));
  }));

  document.querySelectorAll('[data-collapsible]').forEach(section => {
    const button = section.querySelector('.collapsible-toggle');
    if (!button) return;
    const update = () => {
      const collapsed = section.classList.contains('is-collapsed');
      button.textContent = collapsed ? '展开全部' : '收起至10条';
      button.setAttribute('aria-expanded', String(!collapsed));
    };
    button.addEventListener('click', () => { section.classList.toggle('is-collapsed'); update(); });
    update();
  });

  const scoreBandElement = document.getElementById('score-band-data');
  const SCORE_BANDS = JSON.parse(scoreBandElement?.textContent || '[]').map(band => ({
    min: band.lower, max: band.upper, name: band.label,
    color: band.chart_color, borderColor: band.chart_border_color,
  })).sort((a, b) => b.min - a.min);
function buildScoreTrendOption(rawRows, symbolName) {
  const hasRows = rawRows.length > 0;
  const labels = rawRows.map(r => new Date(r[0]).toISOString().slice(5, 16).replace('T', ' '));
  const values = rawRows.map(r => r[1]);
  const bandMarkAreas = SCORE_BANDS.map((band) => ([
    {
      yAxis: band.min,
      itemStyle: { color: band.color, borderColor: band.borderColor, borderWidth: 1 },
      label: {
        show: true,
        position: 'insideRight',
        color: '#374151',
        fontWeight: 700,
        fontSize: 12,
        formatter: `${band.name}\n${band.min}-${band.max === 100 ? '100+' : band.max}分`,
      },
    },
    { yAxis: band.max + 1 },
  ]));

  if (!hasRows) {
    return {
      title: { text: '暂无总分趋势数据', left: 'center', top: 'middle', textStyle: { color: '#6b7280', fontSize: 16 } },
      grid: { left: 62, right: 142, top: 28, bottom: 58 },
      xAxis: { type: 'category', data: [] },
      yAxis: {
        type: 'value',
        name: '总分 / 单型区间',
        min: 60,
        max: 101,
        interval: 4,
        splitLine: { lineStyle: { color: '#e5e7eb', type: 'dashed' } },
      },
      series: [{ type: 'line', data: [], markArea: { silent: true, data: bandMarkAreas } }]
    };
  }

  return {
    animation: false,
    grid: { left: 62, right: 142, top: 28, bottom: 58 },
    tooltip: {
      trigger: 'axis',
      formatter: function (params) {
        const p = params[0];
        const row = rawRows[p.dataIndex];
        return `${new Date(row[0]).toISOString().replace('T', ' ').slice(0, 19)} UTC<br/>总分：${row[1]}`;
      }
    },
    xAxis: { type: 'category', data: labels, boundaryGap: false },
    yAxis: {
      type: 'value',
      name: '总分 / 单型区间',
      min: function (value) { return Math.min(60, Math.floor(value.min / 5) * 5); },
      max: function (value) { return Math.max(101, Math.ceil(value.max / 5) * 5); },
      interval: 4,
      axisLabel: { color: '#374151', fontWeight: 600 },
      axisLine: { show: true, lineStyle: { color: '#94a3b8' } },
      splitLine: { lineStyle: { color: '#e5e7eb', type: 'dashed' } },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', bottom: 12, height: 18, start: 0, end: 100 }
    ],
    series: [{
      name: `${symbolName || 'Symbol'} 总分`,
      type: 'line',
      data: values,
      smooth: true,
      symbol: 'circle',
      symbolSize: 7,
      lineStyle: { width: 3, color: '#2563eb' },
      itemStyle: { color: '#2563eb', borderColor: '#ffffff', borderWidth: 2 },
      areaStyle: { color: 'rgba(37, 99, 235, 0.12)' },
      markArea: { silent: true, data: bandMarkAreas },
      markLine: {
        silent: true,
        symbol: 'none',
        lineStyle: { color: '#94a3b8', type: 'dashed', width: 1 },
        label: { show: false },
        data: SCORE_BANDS.map((band) => ({ yAxis: band.min }))
      }
    }]
  };
}

function setScoreTrendStatus(symbolName, count, isLoading) {
  const statusEl = document.getElementById('score-trend-status');
  if (!statusEl) return;
  if (isLoading) {
    statusEl.textContent = `正在加载 ${symbolName || ''} 的趋势数据...`;
    return;
  }
  statusEl.innerHTML = symbolName
    ? `当前 symbol：<span id="score-trend-current-symbol"></span>；数据点数量：<span id="score-trend-count"></span>。`
    : '当前暂无可查询 symbol。';
  const symbolEl = document.getElementById('score-trend-current-symbol');
  const countEl = document.getElementById('score-trend-count');
  if (symbolEl) symbolEl.textContent = symbolName;
  if (countEl) countEl.textContent = count;
}

function updateScoreTrendChart(rawRows, symbolName) {
  const chartEl = document.getElementById('score-trend-chart');
  if (!chartEl || !window.echarts) return;
  if (!scoreTrendChart) {
    scoreTrendChart = echarts.init(chartEl);
    window.addEventListener('resize', refreshScoreTrendChartLayout);
  }
  scoreTrendChart.setOption(buildScoreTrendOption(rawRows, symbolName), true);
  requestAnimationFrame(refreshScoreTrendChartLayout);
}

(function initScoreTrendAjax() {
  const form = document.getElementById('score-trend-form');
  const select = document.getElementById('score-trend-symbol');
  if (!form || !select) return;
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const symbolName = select.value;
    setScoreTrendStatus(symbolName, 0, true);
    try {
      const response = await fetch(`/api/safety/score-trend?symbol=${encodeURIComponent(symbolName)}&days=3`, {
        headers: { 'Accept': 'application/json' },
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      const rows = (payload.rows || []).map(row => [row.decision_round_ts, row.total_score]);
      updateScoreTrendChart(rows, payload.symbol || symbolName);
      setScoreTrendStatus(payload.symbol || symbolName, payload.count || rows.length, false);
      const url = new URL(window.location.href);
      url.searchParams.set('active_tab', 'tab-score-trend');
      if (symbolName) url.searchParams.set('score_trend_symbol', symbolName);
      window.history.replaceState({}, '', url.toString());
    } catch (error) {
      setScoreTrendStatus(symbolName, 0, false);
      alert(`趋势数据加载失败：${error.message}`);
    }
  });
})();
})();
