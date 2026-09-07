(() => {
  'use strict';

  const byId = id => document.getElementById(id);
  const escapeHtml = value => String(value ?? '').replace(/[&<>'"]/g, character => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;'
  })[character]);
  const formatTime = value => {
    const timestamp = Number(value);
    return Number.isFinite(timestamp) && timestamp > 0
      ? `${new Date(timestamp).toISOString().replace('T', ' ').slice(0, 19)} UTC`
      : '-';
  };

  function activate(button, buttons, panels, attribute) {
    buttons.forEach(item => {
      const selected = item === button;
      item.classList.toggle('active', selected);
      item.setAttribute('aria-selected', String(selected));
    });
    panels.forEach(panel => panel.classList.toggle('active', panel.id === button.dataset[attribute]));
  }

  const liveButtons = [...document.querySelectorAll('.bookmark-tab[data-live-tab]')];
  const livePanels = [...document.querySelectorAll('.live-subpanel')];
  liveButtons.forEach(button => button.addEventListener('click', () => {
    activate(button, liveButtons, livePanels, 'liveTab');
    if (button.dataset.liveTab === 'live-account') requestAnimationFrame(resizeEquityChart);
  }));

  document.querySelectorAll('.holding-module-tab[data-holding-module-tab]').forEach(button => {
    button.addEventListener('click', () => {
      const container = button.closest('.live-subpanel');
      if (!container) return;
      activate(button, [...container.querySelectorAll('.holding-module-tab')],
        [...container.querySelectorAll('.holding-module-panel')], 'holdingModuleTab');
    });
  });

  document.querySelectorAll('[data-collapsible]').forEach(section => {
    section.querySelector('.collapsible-toggle')?.addEventListener('click', event => {
      const collapsed = section.classList.toggle('is-collapsed');
      event.currentTarget.setAttribute('aria-expanded', String(!collapsed));
      event.currentTarget.textContent = collapsed ? '展开全部' : '收起';
    });
  });

  function showError(errorId, statusId, message) {
    const error = byId(errorId);
    if (error) { error.textContent = message; error.style.display = 'block'; }
    if (byId(statusId)) byId(statusId).textContent = '查询失败';
  }
  function clearError(id) {
    const error = byId(id);
    if (error) { error.textContent = ''; error.style.display = 'none'; }
  }

  byId('live-query-balance')?.addEventListener('click', async event => {
    const button = event.currentTarget;
    button.disabled = true; button.textContent = '查询中...';
    byId('live-balance-status').textContent = '正在请求 Binance 实盘 REST API...';
    try {
      const response = await fetch('/api/live/account/balance', { headers: { Accept: 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      clearError('live-balance-error');
      const balances = payload.balances || [];
      const usdt = balances.find(row => row.asset === 'USDT') || {};
      byId('live-usdt-balance').textContent = usdt.balance || '-';
      byId('live-usdt-available').textContent = usdt.available_balance || '-';
      byId('live-usdt-pnl').textContent = usdt.cross_un_pnl || '-';
      byId('live-balance-status').textContent = `Real 查询完成：${formatTime(payload.queried_at)}`;
      byId('live-balance-rows').innerHTML = balances.length ? balances.map(row => `<tr><td>${escapeHtml(row.asset)}</td><td>${escapeHtml(row.balance)}</td><td>${escapeHtml(row.available_balance)}</td><td>${escapeHtml(row.cross_wallet_balance)}</td><td>${escapeHtml(row.cross_un_pnl)}</td><td>${escapeHtml(row.account_alias)}</td></tr>`).join('') : '<tr><td class="empty" colspan="6">接口未返回余额数据</td></tr>';
    } catch (error) { showError('live-balance-error', 'live-balance-status', `实盘账户余额查询失败：${error.message}`); }
    finally { button.disabled = false; button.textContent = '查询账户余额'; }
  });

  let displayedOrders = [];
  let latestOrders = null;
  const scoreBands = JSON.parse(byId('live-score-band-data')?.textContent || '[]');
  function matchesBand(row, selected) {
    if (selected === 'all') return true;
    if (selected === 'unknown') return !row.open_score_band;
    const band = scoreBands.find(item => `${item.lower}-${item.upper}` === selected);
    const score = Number(row.open_total_score);
    return band && Number.isFinite(score) && score >= band.lower && score <= band.upper;
  }
  function renderOrders(payload) {
    latestOrders = payload;
    displayedOrders = (payload.orders || []).filter(row => matchesBand(row, byId('live-filled-score-band')?.value || 'all'));
    byId('live-export-filled').disabled = !displayedOrders.length;
    clearError('live-filled-error');
    byId('live-filled-status').textContent = `Real 查询完成：${formatTime(payload.queried_at)}；当前显示 ${displayedOrders.length} 条`;
    byId('live-filled-rows').innerHTML = displayedOrders.length ? displayedOrders.map(row => `<tr><td>${formatTime(row.time)}</td><td><strong>${escapeHtml(row.symbol)}</strong></td><td>${escapeHtml(row.open_score_band || '-')}</td><td>${escapeHtml(row.open_leverage ?? '-')}</td><td>${escapeHtml(row.open_total_score ?? '-')}</td><td>${escapeHtml(row.exit_reason || '-')}</td><td>${escapeHtml(row.side || '-')}</td><td>${escapeHtml(row.order_id)}</td><td>${escapeHtml(row.price)}</td><td>${escapeHtml(row.quantity)}</td><td>${escapeHtml(row.quote_quantity)}</td><td>${escapeHtml(row.realized_pnl || '0')}</td><td>${escapeHtml(row.commission || '0')} ${escapeHtml(row.commission_asset || '')}</td>${Array.from({length: 18}, (_, index) => `<td>${escapeHtml(row[`open_rule${index + 1}_score`] ?? '-')}</td>`).join('')}<td>${row.maker ? '是' : '否'}</td><td>${escapeHtml(row.trade_id)}</td></tr>`).join('') : '<tr><td class="empty" colspan="33">所选范围暂无已成交订单</td></tr>';
  }
  async function queryOrders(query) {
    try {
      const response = await fetch(`/api/live/account/filled-orders?${query}`, { headers: { Accept: 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderOrders(payload);
    } catch (error) { showError('live-filled-error', 'live-filled-status', `实盘已成交订单查询失败：${error.message}`); }
  }
  const days = byId('live-filled-days');
  days?.addEventListener('change', () => { byId('live-query-filled').textContent = `查询近${days.value}天已成交订单`; });
  byId('live-query-filled')?.addEventListener('click', () => queryOrders(new URLSearchParams({ days: days.value })));
  byId('live-filled-score-band')?.addEventListener('change', () => latestOrders && renderOrders(latestOrders));
  const end = new Date(); end.setMinutes(0, 0, 0);
  const localHour = date => new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 13) + ':00';
  if (byId('live-filled-end')) byId('live-filled-end').value = localHour(end);
  if (byId('live-filled-start')) byId('live-filled-start').value = localHour(new Date(end.getTime() - 7 * 86400000));
  byId('live-query-filled-range')?.addEventListener('click', () => {
    const start = new Date(byId('live-filled-start').value).getTime();
    const finish = new Date(byId('live-filled-end').value).getTime();
    if (start >= finish) return showError('live-filled-error', 'live-filled-status', '请选择有效的起止时间');
    queryOrders(new URLSearchParams({ start_time: start, end_time: finish }));
  });
  byId('live-export-filled')?.addEventListener('click', async () => {
    const response = await fetch('/api/account/filled-orders/export', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ orders: displayedOrders }) });
    if (!response.ok) return showError('live-filled-error', 'live-filled-status', 'Excel 导出失败');
    const link = document.createElement('a'); link.href = URL.createObjectURL(await response.blob()); link.download = 'live_filled_orders.xlsx'; link.click(); URL.revokeObjectURL(link.href);
  });

  document.querySelectorAll('.live-module-refresh').forEach(button => button.addEventListener('click', async () => {
    const key = button.dataset.liveModule;
    const status = button.parentElement.querySelector('.live-module-refresh-status');
    const url = key === 'holding-increase' ? '/api/live/holding-increase/summary' : `/api/live/high-frequency/${encodeURIComponent(key)}/summary`;
    button.disabled = true;
    try {
      const response = await fetch(url, { headers: {Accept: 'application/json'} });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      status.textContent = `刷新完成：判断结果 ${(payload.checks || []).length} 条，操作记录 ${(payload.records || []).length} 条`;
    } catch (error) { status.textContent = `刷新失败：${error.message}`; }
    finally { button.disabled = false; }
  }));

  let equityChart = null;
  function resizeEquityChart() { equityChart?.resize(); }
  const chartElement = byId('live-experiment-equity-trend-chart');
  if (chartElement && window.echarts) {
    const rows = JSON.parse(byId('live-equity-trend-data')?.textContent || '[]');
    equityChart = window.echarts.init(chartElement);
    equityChart.setOption({ xAxis: {type: 'category', data: rows.map(row => formatTime(row[0]))}, yAxis: {type: 'value'}, series: [{type: 'line', data: rows.map(row => Number(row[1])), smooth: true}] });
    window.addEventListener('resize', resizeEquityChart);
  }
})();
