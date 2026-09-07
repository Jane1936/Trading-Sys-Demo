(() => {
  'use strict';

  let btcChart = null;

  const marketTabButtons = Array.from(document.querySelectorAll('[data-market-tab]'));
  const marketPanels = Array.from(document.querySelectorAll('.content .panel'));

  function selectMarketTab(tabId, updateUrl = true) {
    const selectedButton = marketTabButtons.find(button => button.dataset.marketTab === tabId);
    const selectedPanel = document.getElementById(tabId);
    if (!selectedButton || !selectedPanel) return;

    marketTabButtons.forEach(button => {
      const selected = button === selectedButton;
      button.classList.toggle('active', selected);
      button.setAttribute('aria-selected', String(selected));
    });
    marketPanels.forEach(panel => {
      const selected = panel === selectedPanel;
      panel.classList.toggle('active', selected);
      panel.hidden = !selected;
    });

    if (updateUrl) window.history.replaceState(null, '', `#${tabId}`);
    if (tabId === 'tab-btc') requestAnimationFrame(refreshBtcChartLayout);
  }

  function formatMsDatetime(tsMs) {
    const ts = Number(tsMs);
    if (!Number.isFinite(ts) || ts <= 0) return '-';
    const date = new Date(ts);
    const pad = value => String(value).padStart(2, '0');
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())} UTC`;
  }

  function refreshBtcChartLayout() {
    if (btcChart) btcChart.resize();
  }

  function renderBtcKlineChart(rawRowsDesc) {
    const chartEl = document.getElementById('btc-kline-chart');
    if (!chartEl || !window.echarts) return;
    if (!rawRowsDesc.length) {
      if (btcChart) btcChart.dispose();
      btcChart = null;
      chartEl.innerHTML = '<div class="empty">暂无 BTC 5分钟K线图表数据</div>';
      return;
    }
    const rows = rawRowsDesc.slice().reverse();
    const categoryData = rows.map(row => new Date(row[0]).toISOString().slice(5, 16).replace('T', ' '));
    if (!btcChart) {
      chartEl.innerHTML = '';
      btcChart = window.echarts.init(chartEl);
      window.addEventListener('resize', refreshBtcChartLayout);
    }
    btcChart.setOption({
      animation: false,
      grid: [{ left: 50, right: 20, top: 12, height: '62%' }, { left: 50, right: 20, top: '78%', height: '14%' }],
      tooltip: { trigger: 'axis' },
      xAxis: [
        { type: 'category', data: categoryData, boundaryGap: true, axisLine: { onZero: false } },
        { type: 'category', gridIndex: 1, data: categoryData, boundaryGap: true, axisLabel: { show: false }, axisTick: { show: false } },
      ],
      yAxis: [
        { scale: true, splitArea: { show: true } },
        { scale: true, gridIndex: 1, splitNumber: 2, axisLabel: { show: false }, splitLine: { show: false } },
      ],
      dataZoom: [
        { type: 'inside', xAxisIndex: [0, 1], start: 92, end: 100 },
        { type: 'slider', xAxisIndex: [0, 1], bottom: 0, height: 18, start: 92, end: 100 },
      ],
      series: [
        { name: 'BTCUSDT 5m', type: 'candlestick', data: rows.map(row => [row[1], row[4], row[3], row[2]]), itemStyle: { color: '#ef4444', color0: '#22c55e', borderColor: '#ef4444', borderColor0: '#22c55e' } },
        { name: 'Volume', type: 'bar', xAxisIndex: 1, yAxisIndex: 1, data: rows.map(row => row[5]), itemStyle: { color: '#93c5fd' } },
      ],
    }, true);
    requestAnimationFrame(refreshBtcChartLayout);
  }

  function setBtcLoading(isLoading) {
    const button = document.getElementById('refresh-btc-data');
    const status = document.getElementById('btc-refresh-status');
    if (button) {
      button.disabled = isLoading;
      button.textContent = isLoading ? '刷新中...' : '刷新BTC数据并渲染图表';
    }
    if (status && isLoading) status.textContent = '正在读取最近3天 BTC 5分钟K线...';
  }

  function updateBtcPager(payload) {
    const page = Number(payload.page || 1);
    const totalPages = Number(payload.total_pages || 1);
    const previous = document.getElementById('btc-prev-page');
    const next = document.getElementById('btc-next-page');
    if (previous) {
      previous.disabled = page <= 1;
      previous.dataset.page = String(Math.max(1, page - 1));
    }
    if (next) {
      next.disabled = page >= totalPages;
      next.dataset.page = String(Math.min(totalPages, page + 1));
    }
    const info = document.getElementById('btc-pager-info');
    if (info) info.textContent = `第 ${page} / ${totalPages} 页，共 ${payload.total_rows || 0} 条（最近3天）`;
  }

  function renderBtcTable(rows) {
    const body = document.getElementById('btc-5m-rows');
    if (!body) return;
    if (!rows.length) {
      body.innerHTML = '<tr><td class="empty" colspan="7">暂无 BTC 5分钟K线数据</td></tr>';
      return;
    }
    body.innerHTML = rows.map(row => {
      const cssClass = Number(row[4]) >= Number(row[1]) ? 'k-up' : 'k-down';
      return `<tr><td>${formatMsDatetime(row[0])}</td><td>${formatMsDatetime(row[6])}</td><td class="${cssClass}">${Number(row[1]).toFixed(6)}</td><td class="${cssClass}">${Number(row[2]).toFixed(6)}</td><td class="${cssClass}">${Number(row[3]).toFixed(6)}</td><td class="${cssClass}">${Number(row[4]).toFixed(6)}</td><td>${Number(row[5]).toFixed(6)}</td></tr>`;
    }).join('');
  }

  async function refreshBtcData(page = 1) {
    setBtcLoading(true);
    const status = document.getElementById('btc-refresh-status');
    try {
      const response = await fetch(`/api/btc/5m?page=${encodeURIComponent(page)}`, { headers: { Accept: 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderBtcTable(payload.table_rows || []);
      renderBtcKlineChart(payload.chart_rows || []);
      updateBtcPager(payload);
      if (status) status.textContent = `刷新完成：${formatMsDatetime(payload.queried_at)}；图表数据 ${payload.chart_rows?.length || 0} 条。`;
    } catch (error) {
      if (status) status.textContent = `刷新失败：${error.message}`;
    } finally {
      setBtcLoading(false);
    }
  }

  document.querySelectorAll('[data-collapsible]').forEach(section => {
    const button = section.querySelector('.collapsible-toggle');
    if (!button) return;
    button.addEventListener('click', () => {
      const collapsed = section.classList.toggle('is-collapsed');
      button.setAttribute('aria-expanded', String(!collapsed));
      button.textContent = collapsed ? '展开全部' : '收起';
    });
  });

  const refresh = document.getElementById('refresh-btc-data');
  const previous = document.getElementById('btc-prev-page');
  const next = document.getElementById('btc-next-page');
  if (refresh) refresh.addEventListener('click', () => refreshBtcData(1));
  if (previous) previous.addEventListener('click', () => refreshBtcData(Number(previous.dataset.page || 1)));
  if (next) next.addEventListener('click', () => refreshBtcData(Number(next.dataset.page || 1)));

  marketTabButtons.forEach(button => {
    button.addEventListener('click', () => selectMarketTab(button.dataset.marketTab));
  });
  selectMarketTab(window.location.hash.slice(1) || 'tab-btc', false);
})();
