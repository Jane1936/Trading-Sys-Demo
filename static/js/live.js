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
    const score = Number(row.open_total_score);
    if (selected === 'unknown') return row.open_total_score === null || row.open_total_score === undefined || row.open_total_score === '' || !Number.isFinite(score);
    const band = scoreBands.find(item => `${item.lower}-${item.upper}` === selected);
    return band && Number.isFinite(score) && score >= band.lower && score <= band.upper;
  }
  function pnlClass(value) {
    const numeric = Number(value) || 0;
    return numeric > 0 ? 'pnl-positive' : numeric < 0 ? 'pnl-negative' : 'pnl-zero';
  }
  function formatAmount(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return '0';
    return numeric.toFixed(8).replace(/(\.\d*?[1-9])0+$/, '$1').replace(/\.0+$/, '');
  }
  function analyzeOrders(orders) {
    const rowGroups = orders.map(() => []);
    const openGroups = new Map();
    const groups = new Map();
    let nextGroupId = 1;
    orders.map((row, index) => ({ ...row, index, numericTime: Number(row.time) || 0 }))
      .sort((a, b) => a.numericTime - b.numericTime || a.index - b.index)
      .forEach(row => {
        const symbolGroups = openGroups.get(row.symbol) || [];
        openGroups.set(row.symbol, symbolGroups);
        const quantity = Math.abs(Number(row.quantity) || 0);
        if (row.side === 'BUY') {
          const groupId = nextGroupId++;
          symbolGroups.push({ groupId, remaining: quantity });
          groups.set(groupId, { buyQty: quantity, sellQty: 0, realizedPnl: 0 });
          rowGroups[row.index].push(groupId);
        } else if (row.side === 'SELL') {
          let remaining = quantity;
          while (remaining > 1e-12 && symbolGroups.length) {
            const open = symbolGroups[0];
            const matched = Math.min(remaining, open.remaining);
            const group = groups.get(open.groupId);
            if (!rowGroups[row.index].includes(open.groupId)) rowGroups[row.index].push(open.groupId);
            group.sellQty += matched;
            group.realizedPnl += quantity ? (Number(row.realized_pnl) || 0) * matched / quantity : 0;
            open.remaining -= matched;
            remaining -= matched;
            if (open.remaining <= 1e-12) symbolGroups.shift();
          }
        }
      });
    const completed = [...groups.values()].filter(group => group.buyQty > 0 && Math.abs(group.buyQty - group.sellQty) <= 1e-8);
    const totalProfit = completed.reduce((sum, group) => sum + Math.max(group.realizedPnl, 0), 0);
    const totalLoss = completed.reduce((sum, group) => sum + Math.min(group.realizedPnl, 0), 0);
    const profitCount = completed.filter(group => group.realizedPnl > 0).length;
    const lossCount = completed.filter(group => group.realizedPnl < 0).length;
    const averageProfit = profitCount ? totalProfit / profitCount : 0;
    const averageLoss = lossCount ? totalLoss / lossCount : 0;
    const profitLossRatio = averageLoss ? averageProfit / Math.abs(averageLoss) : 0;
    const winRate = completed.length ? profitCount / completed.length : 0;
    return { rowGroups, summary: { completedCount: completed.length, totalProfit, profitCount, winRate, totalLoss, lossCount, averageProfit, averageLoss, profitLossRatio, expectancy: winRate * profitLossRatio - (1 - winRate) } };
  }
  function renderOrderSummary(summary = {}) {
    const values = {
      'live-completed-count': summary.completedCount || 0,
      'live-profit-count': summary.profitCount || 0,
      'live-win-rate': `${((summary.winRate || 0) * 100).toFixed(2)}%`,
      'live-loss-count': summary.lossCount || 0,
      'live-profit-loss-ratio': formatAmount(summary.profitLossRatio || 0),
    };
    Object.entries(values).forEach(([id, value]) => { if (byId(id)) byId(id).textContent = value; });
    [['live-total-profit', summary.totalProfit], ['live-total-loss', summary.totalLoss], ['live-average-profit', summary.averageProfit], ['live-average-loss', summary.averageLoss], ['live-expectancy', summary.expectancy]].forEach(([id, value]) => {
      const element = byId(id);
      if (!element) return;
      element.textContent = formatAmount(value || 0);
      element.classList.remove('pnl-positive', 'pnl-negative', 'pnl-zero');
      element.classList.add(pnlClass(value));
    });
  }
  function bindOrderHighlights() {
    const rows = [...document.querySelectorAll('#live-filled-rows .filled-order-row')];
    rows.forEach(row => row.addEventListener('click', () => {
      const selected = row.dataset.groupIds.split(',').filter(Boolean);
      rows.forEach(candidate => {
        const groups = candidate.dataset.groupIds.split(',').filter(Boolean);
        candidate.classList.toggle('is-linked', selected.length > 0 && groups.some(group => selected.includes(group)));
        candidate.classList.toggle('is-active', candidate === row);
      });
    }));
  }
  function renderOrders(payload) {
    latestOrders = payload;
    displayedOrders = (payload.orders || []).filter(row => matchesBand(row, byId('live-filled-score-band')?.value || 'all'));
    const analysis = analyzeOrders(displayedOrders);
    byId('live-export-filled').disabled = !displayedOrders.length;
    clearError('live-filled-error');
    byId('live-filled-status').textContent = `Real 查询完成：${formatTime(payload.queried_at)}；当前显示 ${displayedOrders.length} 条`;
    renderOrderSummary(analysis.summary);
    byId('live-filled-rows').innerHTML = displayedOrders.length ? displayedOrders.map((row, index) => {
      const groupIds = analysis.rowGroups[index];
      const badge = groupIds.length ? `<span class="filled-order-group-badge">组${escapeHtml(groupIds.join('/'))}</span>` : '';
      return `<tr class="filled-order-row" data-group-ids="${escapeHtml(groupIds.join(','))}" title="点击高亮同组买卖订单"><td>${formatTime(row.time)}</td><td><strong>${escapeHtml(row.symbol)}</strong>${badge}</td><td>${escapeHtml(row.open_score_band || '-')}</td><td>${escapeHtml(row.open_leverage ?? '-')}</td><td>${escapeHtml(row.open_total_score ?? '-')}</td><td>${escapeHtml(row.exit_reason || '-')}</td><td><span class="status-badge ${row.side === 'BUY' ? 'status-ok' : 'status-fail'}">${escapeHtml(row.side || '-')}</span></td><td>${escapeHtml(row.order_id)}</td><td>${escapeHtml(row.price)}</td><td>${escapeHtml(row.quantity)}</td><td>${escapeHtml(row.quote_quantity)}</td><td><span class="${pnlClass(row.realized_pnl)}">${escapeHtml(row.realized_pnl || '0')}</span></td><td>${escapeHtml(row.commission || '0')} ${escapeHtml(row.commission_asset || '')}</td>${Array.from({length: 18}, (_, ruleIndex) => `<td>${escapeHtml(row[`open_rule${ruleIndex + 1}_score`] ?? '-')}</td>`).join('')}<td>${row.maker ? '是' : '否'}</td><td>${escapeHtml(row.trade_id)}</td></tr>`;
    }).join('') : '<tr><td class="empty" colspan="33">所选范围暂无已成交订单</td></tr>';
    bindOrderHighlights();
  }
  async function queryOrders(query) {
    const buttons = [byId('live-query-filled'), byId('live-query-filled-range')].filter(Boolean);
    buttons.forEach(button => { button.disabled = true; });
    renderOrderSummary();
    try {
      const response = await fetch(`/api/live/account/filled-orders?${query}`, { headers: { Accept: 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderOrders(payload);
    } catch (error) { showError('live-filled-error', 'live-filled-status', `实盘已成交订单查询失败：${error.message}`); }
    finally { buttons.forEach(button => { button.disabled = false; }); }
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
    if (!Number.isFinite(start) || !Number.isFinite(finish) || start >= finish) return showError('live-filled-error', 'live-filled-status', '请选择有效的起止时间');
    queryOrders(new URLSearchParams({ start_time: start, end_time: finish }));
  });
  byId('live-export-filled')?.addEventListener('click', async () => {
    const response = await fetch('/api/account/filled-orders/export', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ orders: displayedOrders }) });
    if (!response.ok) return showError('live-filled-error', 'live-filled-status', 'Excel 导出失败');
    const link = document.createElement('a'); link.href = URL.createObjectURL(await response.blob()); link.download = 'live_filled_orders.xlsx'; link.click(); URL.revokeObjectURL(link.href);
  });

  function renderModuleRows(body, rows, columns, emptyText) {
    if (!body) return;
    body.innerHTML = rows.length ? rows.map((row, index) => `<tr class="${index >= 10 ? 'collapsed-extra' : ''}">${columns.map(column => {
      const [, key, type] = column;
      const value = row[key];
      if (type === 'time') return `<td>${formatTime(value)}</td>`;
      if (type === 'bool') return `<td>${value ? '是' : '否'}</td>`;
      return `<td>${escapeHtml(value === null || value === undefined || value === '' ? '-' : value)}</td>`;
    }).join('')}</tr>`).join('') : `<tr><td class="empty" colspan="${columns.length}">${emptyText}</td></tr>`;
  }
  function renderHoldingIncrease(panel, payload) {
    const checks = payload.checks || [];
    const records = payload.records || [];
    const checksBody = panel.querySelector('#live-holding-increase-checks-body');
    const recordsBody = panel.querySelector('#live-holding-increase-records-body');
    if (checksBody) checksBody.innerHTML = checks.length ? checks.map(row => `<tr><td>${escapeHtml(row.symbol)}</td><td>${formatTime(row.decision_round_ts)}</td><td>${escapeHtml(row.tag || '-')}</td><td>${escapeHtml(row.current_price || '-')}</td><td>${escapeHtml(row.unrealized_pnl || '-')} / ${escapeHtml(row.one_r_usdt || '-')}</td><td>${escapeHtml(row.latest_total_score || '-')}</td><td>${escapeHtml(row.previous_total_score || '-')}</td><td>${escapeHtml(row.latest_reduction_price || '无，已跳过条件3')}</td><td>${formatTime(row.open_trade_created_at)}</td><td>${escapeHtml(row.reason || '-')}</td><td>${formatTime(row.checked_at)}</td></tr>`).join('') : '<tr><td class="empty" colspan="11">暂无加仓条件模块执行结果</td></tr>';
    if (recordsBody) recordsBody.innerHTML = records.length ? records.map((row, index) => `<tr class="${index >= 10 ? 'collapsed-extra' : ''}"><td>${formatTime(row.created_at)}</td><td>${escapeHtml(row.symbol)}</td><td>${formatTime(row.decision_round_ts)}</td><td>${escapeHtml(row.action_name || '-')}</td><td>${escapeHtml(row.current_price || '-')}</td><td>${escapeHtml(row.unrealized_pnl || '-')} / ${escapeHtml(row.one_r_usdt || '-')}</td><td>${escapeHtml(row.latest_total_score || '-')} / ${escapeHtml(row.previous_total_score || '-')}</td><td>${escapeHtml(row.latest_reduction_price || '-')}</td><td>${escapeHtml(row.increased_quantity || '-')} / ${escapeHtml(row.required_margin_usdt || '-')} / ${escapeHtml(row.available_experiment_usdt || '-')}</td><td>${escapeHtml(row.status || '-')}</td><td>${escapeHtml(row.reason || '-')}</td></tr>`).join('') : '<tr><td class="empty" colspan="11">最近7天暂无加仓操作记录</td></tr>';
    const note = panel.querySelector('#live-holding-increase-round-note');
    if (note) note.textContent = payload.round_ts ? `最近判断轮次：${formatTime(payload.round_ts)}` : '暂无加仓条件模块执行结果。';
  }

  document.querySelectorAll('.live-module-refresh').forEach(button => button.addEventListener('click', async () => {
    const key = button.dataset.liveModule;
    const panel = button.closest('.holding-module-panel');
    const status = panel?.querySelector('.live-module-refresh-status');
    const url = key === 'holding-increase' ? '/api/live/holding-increase/summary' : `/api/live/high-frequency/${encodeURIComponent(key)}/summary`;
    button.disabled = true;
    button.textContent = '刷新中...';
    try {
      const response = await fetch(url, { headers: {Accept: 'application/json'} });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      if (key === 'holding-increase') renderHoldingIncrease(panel, payload);
      else {
        renderModuleRows(panel?.querySelector('.live-high-frequency-checks-body'), payload.checks || [], payload.tables.check_columns, `暂无实盘${payload.label}判断结果`);
        renderModuleRows(panel?.querySelector('.live-high-frequency-records-body'), payload.records || [], payload.tables.record_columns, `暂无实盘${payload.label}操作记录`);
        const roundNote = panel?.querySelector('.live-high-frequency-round-note');
        if (roundNote) roundNote.textContent = `最近轮次：${formatTime(payload.round_ts)}`;
      }
      if (status) status.textContent = `刷新完成：判断结果 ${(payload.checks || []).length} 条，操作记录 ${(payload.records || []).length} 条`;
    } catch (error) { if (status) status.textContent = `刷新失败：${error.message}`; }
    finally { button.disabled = false; button.textContent = '刷新'; }
  }));

  let equityChart = null;
  function resizeEquityChart() { equityChart?.resize(); }
  const chartElement = byId('live-experiment-equity-trend-chart');
  if (chartElement && window.echarts) {
    const rows = JSON.parse(byId('live-equity-trend-data')?.textContent || '[]');
    equityChart = window.echarts.init(chartElement);
    const hasRows = rows.length > 0;
    equityChart.setOption(hasRows ? {
      animation: false,
      grid: { left: 64, right: 28, top: 32, bottom: 58 },
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'category', data: rows.map(row => new Date(row[0]).toISOString().slice(5, 16).replace('T', ' ')), boundaryGap: false },
      yAxis: { type: 'value', name: 'USDT净值', scale: true },
      dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 12, height: 18 }],
      series: [{ name: '实验组USDT净值', type: 'line', data: rows.map(row => Number(row[1])), smooth: true, symbol: 'circle', symbolSize: 6, lineStyle: { width: 3, color: '#f97316' }, itemStyle: { color: '#f97316' }, areaStyle: { color: 'rgba(249, 115, 22, 0.14)' } }]
    } : {
      title: { text: '暂无近7天实验组USDT净值数据', left: 'center', top: 'middle', textStyle: { color: '#6b7280', fontSize: 16 } },
      xAxis: { type: 'category', data: [] }, yAxis: { type: 'value', name: 'USDT净值' }, series: [{ type: 'line', data: [] }]
    }, true);
    window.addEventListener('resize', resizeEquityChart);
    requestAnimationFrame(resizeEquityChart);
  }
})();
