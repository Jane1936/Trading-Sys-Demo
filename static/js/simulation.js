(() => {
  'use strict';

  function activate(button, buttons, panels, attribute) {
    buttons.forEach(item => {
      const selected = item === button;
      item.classList.toggle('active', selected);
      item.setAttribute('aria-selected', String(selected));
    });
    panels.forEach(panel => panel.classList.toggle('active', panel.id === button.dataset[attribute]));
  }

  function initSimulationTabs() {
    const buttons = [...document.querySelectorAll('.bookmark-tab[data-simulation-tab]')];
    const panels = [...document.querySelectorAll('.simulation-subpanel')];
    buttons.forEach(button => button.addEventListener('click', () => {
      activate(button, buttons, panels, 'simulationTab');
      if (button.dataset.simulationTab === 'strategy-account') {
        requestAnimationFrame(refreshExperimentEquityTrendChartLayout);
      }
    }));
  }

  function initHoldingModuleTabs() {
    document.querySelectorAll('.holding-module-tab[data-holding-module-tab]').forEach(button => {
      button.addEventListener('click', () => {
        const container = button.closest('.simulation-subpanel');
        if (!container) return;
        activate(
          button,
          [...container.querySelectorAll('.holding-module-tab[data-holding-module-tab]')],
          [...container.querySelectorAll('.holding-module-panel')],
          'holdingModuleTab'
        );
      });
    });
  }

  function initCollapsibles() {
    document.querySelectorAll('[data-collapsible]').forEach(section => {
      const button = section.querySelector('.collapsible-toggle');
      if (!button) return;
      button.addEventListener('click', () => {
        const collapsed = section.classList.toggle('is-collapsed');
        button.setAttribute('aria-expanded', String(!collapsed));
        button.textContent = collapsed ? '展开全部' : '收起';
      });
    });
  }


  const DYNAMIC_PROFIT_PROTECTION_RULE_NOTE = '每分钟刷新动态利润保护扫描结果与操作记录。';
  const TRAILING_STOP_RULE_NOTE = '每分钟刷新移动追踪止盈扫描结果与操作记录。';
  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>'"]/g, character => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[character]);
  }
const EXPERIMENT_UNINVESTED_USDT = 4000;

function formatUsdtValue(value) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return '-';
  return `${numericValue.toFixed(2)} U`;
}

function renderExperimentUsdtEquity(usdtBalance) {
  const equityEl = document.getElementById('experiment-usdt-equity');
  if (!equityEl) return;
  const numericBalance = Number(usdtBalance);
  equityEl.textContent = Number.isFinite(numericBalance)
    ? formatUsdtValue(numericBalance - EXPERIMENT_UNINVESTED_USDT)
    : '-';
}

function setAccountBalanceLoading(isLoading) {
  const btn = document.getElementById('query-account-balance');
  const status = document.getElementById('account-balance-status');
  if (btn) {
    btn.disabled = isLoading;
    btn.textContent = isLoading ? '查询中...' : '查询账户余额';
  }
  if (status && isLoading) status.textContent = '正在请求 Binance REST API...';
}

function renderAccountBalance(payload) {
  const rowsEl = document.getElementById('account-balance-rows');
  const statusEl = document.getElementById('account-balance-status');
  const errorEl = document.getElementById('account-balance-error');
  const balances = payload.balances || [];
  const usdt = balances.find(row => row.asset === 'USDT') || balances[0] || {};

  if (errorEl) {
    errorEl.style.display = 'none';
    errorEl.textContent = '';
  }
  document.getElementById('account-usdt-balance').textContent = usdt.balance || '-';
  document.getElementById('account-usdt-available').textContent = usdt.available_balance || '-';
  document.getElementById('account-usdt-pnl').textContent = usdt.cross_un_pnl || '-';
  renderExperimentUsdtEquity(usdt.balance);

  if (statusEl) {
    const queriedAt = payload.queried_at ? new Date(payload.queried_at).toISOString().replace('T', ' ').slice(0, 19) + ' UTC' : '未知时间';
    statusEl.textContent = `${payload.testnet ? 'Demo/Testnet' : 'Real'} 查询完成：${queriedAt}`;
  }

  const escapeHtml = (value) => String(value ?? '').replace(/[&<>'"]/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
  if (!rowsEl) return;
  if (!balances.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="6">接口未返回余额数据</td></tr>';
    return;
  }
  rowsEl.innerHTML = balances.map(row => `
    <tr>
      <td>${escapeHtml(row.asset)}</td>
      <td>${escapeHtml(row.balance || '0')}</td>
      <td>${escapeHtml(row.available_balance || '0')}</td>
      <td>${escapeHtml(row.cross_wallet_balance || '0')}</td>
      <td>${escapeHtml(row.cross_un_pnl || '0')}</td>
      <td>${escapeHtml(row.account_alias)}</td>
    </tr>
  `).join('');
}

function renderAccountBalanceError(message) {
  const errorEl = document.getElementById('account-balance-error');
  const statusEl = document.getElementById('account-balance-status');
  if (errorEl) {
    errorEl.textContent = message;
    errorEl.style.display = 'block';
  }
  if (statusEl) statusEl.textContent = '查询失败';
  renderExperimentUsdtEquity(null);
}

(function initAccountBalanceQuery() {
  const btn = document.getElementById('query-account-balance');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    setAccountBalanceLoading(true);
    try {
      const response = await fetch('/api/account/balance', {
        headers: { 'Accept': 'application/json' },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderAccountBalance(payload);
    } catch (error) {
      renderAccountBalanceError(`账户余额查询失败：${error.message}`);
    } finally {
      setAccountBalanceLoading(false);
    }
  });
})();

function pnlClass(value) {
  const numericValue = Number(value);
  if (Number.isFinite(numericValue) && numericValue > 0) return 'pnl-positive';
  if (Number.isFinite(numericValue) && numericValue < 0) return 'pnl-negative';
  return 'pnl-zero';
}

function formatMsDatetime(ms) {
  const numericValue = Number(ms);
  if (!Number.isFinite(numericValue) || numericValue <= 0) return '-';
  return new Date(numericValue).toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
}


function statusBadge(value, passText = '是', failText = '否') {
  return `<span class="status-badge ${value ? 'status-pass' : 'status-fail'}">${value ? passText : failText}</span>`;
}


const BREAK_EVEN_RULE_NOTE = 'R = 实验组USDT净值 × 1%。每分钟扫描当前持仓；若未变现盈亏 ≥ R，则取消原止损单并按开仓价新建 reduceOnly STOP_MARKET 条件止损单，等待价格回落到保本价时触发市价平仓。';

function breakEvenTriggeredCell(row) {
  if (row.reason === 'break_even_already_completed') {
    return '<span class="status-badge status-pass">已完成保本</span>';
  }
  return statusBadge(row.triggered);
}

function renderBreakEvenChecks(rows) {
  const rowsEl = document.getElementById('break-even-checks-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="9">暂无保本止盈扫描结果</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map(row => `
    <tr>
      <td>${formatMsDatetime(row.checked_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${escapeHtml(row.account_equity_usdt)}</td>
      <td>${escapeHtml(row.r_usdt)}</td>
      <td>${escapeHtml(row.unrealized_pnl)}</td>
      <td>${escapeHtml(row.entry_price)}</td>
      <td>${escapeHtml(row.position_amt)}</td>
      <td>${breakEvenTriggeredCell(row)}</td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderBreakEvenRecords(rows) {
  const rowsEl = document.getElementById('break-even-records-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="11">暂无保本止盈止损单记录</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map((row, index) => `
    <tr class="${index >= 10 ? 'collapsed-extra' : ''}">
      <td>${formatMsDatetime(row.checked_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${escapeHtml(row.side)}</td>
      <td>${escapeHtml(row.position_amt)}</td>
      <td>${escapeHtml(row.entry_price)}</td>
      <td>${escapeHtml(row.r_usdt)} / ${escapeHtml(row.unrealized_pnl)}</td>
      <td>${escapeHtml(row.old_stop_loss_order_id || '-')}</td>
      <td>${escapeHtml(row.new_stop_loss_order_id || '-')}</td>
      <td>${escapeHtml(row.stop_loss_price)}</td>
      <td><span class="status-badge ${row.status === 'submitted' ? 'status-pass' : 'status-fail'}">${escapeHtml(row.status)}</span></td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderBreakEvenSummary(payload) {
  const checks = payload.checks || [];
  const records = payload.records || [];
  const roundNoteEl = document.getElementById('break-even-round-note');
  if (roundNoteEl) {
    roundNoteEl.textContent = payload.round_ts ? `最近判断轮次：${formatMsDatetime(payload.round_ts)}。${BREAK_EVEN_RULE_NOTE}` : '暂无保本止盈扫描结果。';
  }
  const chipsEl = document.getElementById('break-even-triggered-chips');
  if (chipsEl) {
    const triggeredRows = checks.filter(row => row.triggered);
    chipsEl.innerHTML = triggeredRows.length
      ? triggeredRows.map(row => `<span class="chip" title="${escapeHtml(row.reason)}">${escapeHtml(row.symbol)}</span>`).join('')
      : '<span class="chip">本轮无触发保本止盈symbol</span>';
  }
  renderBreakEvenChecks(checks);
  renderBreakEvenRecords(records);
}

(function initBreakEvenRefresh() {
  const btn = document.getElementById('refresh-break-even');
  const statusEl = document.getElementById('break-even-refresh-status');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    btn.disabled = true;
    btn.textContent = '刷新中...';
    if (statusEl) statusEl.textContent = '正在获取保本止盈最新扫描结果与新建止损单记录...';
    try {
      const response = await fetch('/api/break-even/summary', { headers: { 'Accept': 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderBreakEvenSummary(payload);
      if (statusEl) statusEl.textContent = `刷新完成：扫描结果 ${payload.checks?.length || 0} 条，止损单记录 ${payload.records?.length || 0} 条。`;
    } catch (error) {
      if (statusEl) statusEl.textContent = `刷新失败：${error.message}`;
    } finally {
      btn.disabled = false;
      btn.textContent = '刷新';
    }
  });
})();


const PARTIAL_TAKE_PROFIT_RULE_NOTE = 'R = 实验组USDT净值 × 1%。每分钟在保本止盈执行完成后扫描当前持仓；常态下未变现盈亏 ≥ 2R 卖出30%，市场弱势时动态调整为 ≥ 1.4R 卖出50%。';

function partialTakeProfitTriggeredCell(row) {
  if (row.reason === 'partial_take_profit_already_completed') {
    return '<span class="status-badge status-pass">已完成分批止盈</span>';
  }
  return statusBadge(row.triggered);
}

function renderPartialTakeProfitChecks(rows) {
  const rowsEl = document.getElementById('partial-take-profit-checks-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="9">暂无分批止盈扫描结果</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map(row => `
    <tr>
      <td>${formatMsDatetime(row.checked_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${escapeHtml(row.account_equity_usdt)}</td>
      <td>${escapeHtml(row.r_usdt)} / ${escapeHtml(row.trigger_r_usdt)}</td>
      <td>${escapeHtml(row.unrealized_pnl)}</td>
      <td>${escapeHtml(row.entry_price)}</td>
      <td>${escapeHtml(row.position_amt)}</td>
      <td>${partialTakeProfitTriggeredCell(row)}</td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderPartialTakeProfitRecords(rows) {
  const rowsEl = document.getElementById('partial-take-profit-records-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="10">暂无分批止盈卖出记录</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map((row, index) => `
    <tr class="${index >= 10 ? 'collapsed-extra' : ''}">
      <td>${formatMsDatetime(row.checked_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${escapeHtml(row.side)}</td>
      <td>${escapeHtml(row.position_amt)}</td>
      <td>${escapeHtml(row.take_profit_quantity)}</td>
      <td>${escapeHtml(row.entry_price)}</td>
      <td>${escapeHtml(row.r_usdt)} / ${escapeHtml(row.trigger_r_usdt)} / ${escapeHtml(row.unrealized_pnl)}</td>
      <td>${escapeHtml(row.take_profit_order_id || '-')} / ${escapeHtml(row.trigger_label || '-')}</td>
      <td><span class="status-badge ${row.status === 'submitted' ? 'status-pass' : 'status-fail'}">${escapeHtml(row.status)}</span></td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderPartialTakeProfitErrors(rows) {
  const rowsEl = document.getElementById('partial-take-profit-errors-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="8">暂无分批止盈卖出错误记录</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map((row, index) => `
    <tr class="${index >= 10 ? 'collapsed-extra' : ''}">
      <td>${formatMsDatetime(row.occurred_at)}</td><td>${escapeHtml(row.symbol || '-')}</td>
      <td>${escapeHtml(row.source)}</td><td>${escapeHtml(row.stage)}</td><td>${escapeHtml(row.error_type)}</td>
      <td>${escapeHtml(row.position_amt || '-')} / ${escapeHtml(row.entry_price || '-')}</td>
      <td>${escapeHtml(row.r_usdt || '-')} / ${escapeHtml(row.trigger_r_usdt || '-')} / ${escapeHtml(row.unrealized_pnl || '-')}</td>
      <td>${escapeHtml(row.error_message)}</td>
    </tr>`).join('');
}

function renderPartialTakeProfitSummary(payload) {
  const roundNoteEl = document.getElementById('partial-take-profit-round-note');
  if (roundNoteEl) roundNoteEl.textContent = payload.round_ts ? `最近判断轮次：${formatMsDatetime(payload.round_ts)}。${PARTIAL_TAKE_PROFIT_RULE_NOTE}` : '暂无分批止盈扫描结果。';
  const chipsEl = document.getElementById('partial-take-profit-triggered-chips');
  if (chipsEl) {
    const triggeredRows = (payload.checks || []).filter(row => row.triggered);
    chipsEl.innerHTML = triggeredRows.length ? triggeredRows.map(row => `<span class="chip" title="${escapeHtml(row.reason)}">${escapeHtml(row.symbol)}</span>`).join('') : '<span class="chip">本轮无触发分批止盈symbol</span>';
  }
  renderPartialTakeProfitChecks(payload.checks || []);
  renderPartialTakeProfitRecords(payload.records || []);
  renderPartialTakeProfitErrors(payload.errors || []);
}

(function initPartialTakeProfitRefresh() {
  const btn = document.getElementById('refresh-partial-take-profit');
  const statusEl = document.getElementById('partial-take-profit-refresh-status');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    btn.disabled = true;
    btn.textContent = '刷新中...';
    if (statusEl) statusEl.textContent = '正在获取分批止盈最新扫描结果与卖出记录...';
    try {
      const response = await fetch('/api/partial-take-profit/summary', { headers: { 'Accept': 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderPartialTakeProfitSummary(payload);
      if (statusEl) statusEl.textContent = `刷新完成：扫描结果 ${payload.checks?.length || 0} 条，卖出记录 ${payload.records?.length || 0} 条，错误记录 ${payload.errors?.length || 0} 条。`;
    } catch (error) {
      if (statusEl) statusEl.textContent = `刷新失败：${error.message}`;
    } finally {
      btn.disabled = false;
      btn.textContent = '刷新';
    }
  });
})();


function renderDynamicProfitProtectionChecks(rows) {
  const rowsEl = document.getElementById('dynamic-profit-protection-checks-body');
  if (!rowsEl) return;
  if (!rows.length) { rowsEl.innerHTML = '<tr><td class="empty" colspan="17">暂无动态利润保护扫描结果</td></tr>'; return; }
  rowsEl.innerHTML = rows.map(row => `<tr><td>${formatMsDatetime(row.checked_at)}</td><td>${escapeHtml(row.symbol)}</td><td>${escapeHtml(row.entry_price)}</td><td>${escapeHtml(row.position_amt)}</td><td>${escapeHtml(row.unrealized_pnl)} / ${escapeHtml(row.profit_r_multiple)}R</td><td>${escapeHtml(row.realized_pnl_since_open || '-')}</td><td>${escapeHtml(row.cycle_total_pnl || '-')}</td><td>${escapeHtml(row.latest_1m_high)} / ${escapeHtml(row.latest_1m_close)}</td><td>${escapeHtml(row.highest_since_open)}</td><td>${escapeHtml(row.highest_cycle_total_pnl)}</td><td>${row.highest_profit_at ? formatMsDatetime(row.highest_profit_at) : '-'}</td><td>${escapeHtml(row.current_tier || '未达档')}</td><td>${escapeHtml(row.profit_drawdown_ratio)} / ${escapeHtml(row.drawdown_threshold)}</td><td>${statusBadge(row.triggered)}</td><td>${escapeHtml(row.close_quantity)} / ${escapeHtml(row.close_order_id || '-')} / ${escapeHtml(row.close_status)}</td><td>${statusBadge(row.eligible)}</td><td>${escapeHtml(row.reason)}</td></tr>`).join('');
}

function renderDynamicProfitProtectionRecords(rows) {
  const rowsEl = document.getElementById('dynamic-profit-protection-records-body');
  if (!rowsEl) return;
  if (!rows.length) { rowsEl.innerHTML = '<tr><td class="empty" colspan="15">暂无动态利润保护记录</td></tr>'; return; }
  rowsEl.innerHTML = rows.map((row, index) => `<tr class="${index >= 10 ? 'collapsed-extra' : ''}"><td>${formatMsDatetime(row.checked_at)}</td><td>${escapeHtml(row.symbol)}</td><td>${escapeHtml(row.position_amt)}</td><td>${escapeHtml(row.entry_price)}</td><td>${escapeHtml(row.r_usdt)} / ${escapeHtml(row.profit_r_multiple)}R</td><td>${escapeHtml(row.realized_pnl_since_open || '-')}</td><td>${escapeHtml(row.cycle_total_pnl || '-')}</td><td>${escapeHtml(row.latest_1m_close)} / ${escapeHtml(row.highest_since_open)}</td><td>${escapeHtml(row.highest_cycle_total_pnl)}</td><td>${row.highest_profit_at ? formatMsDatetime(row.highest_profit_at) : '-'}</td><td>${escapeHtml(row.current_tier || '未达档')}</td><td>${escapeHtml(row.profit_drawdown_ratio)} / ${escapeHtml(row.drawdown_threshold)}</td><td>${escapeHtml(row.close_quantity)}</td><td>${escapeHtml(row.close_order_id || '-')} / ${escapeHtml(row.close_status)}</td><td>${escapeHtml(row.reason)}</td></tr>`).join('');
}

function renderHardTakeProfitSummary(payload) {
  const note = document.getElementById('hard-take-profit-round-note');
  if (note) note.textContent = payload.round_ts ? `最近判断轮次：${formatMsDatetime(payload.round_ts)}。独立扫描全部已持仓 symbol；未变现盈利率达到默认 20% 时，调用 reduceOnly MARKET 市价单全部平仓。` : '暂无硬止盈扫描结果。';
  const checks = payload.checks || [];
  const records = payload.records || [];
  const checksBody = document.getElementById('hard-take-profit-checks-body');
  const recordsBody = document.getElementById('hard-take-profit-records-body');
  if (checksBody) checksBody.innerHTML = checks.length ? checks.map(row => `<tr><td>${formatMsDatetime(row.checked_at)}</td><td>${escapeHtml(row.symbol)}</td><td>${escapeHtml(row.entry_price)}</td><td>${escapeHtml(row.position_amt)}</td><td>${escapeHtml(row.unrealized_pnl)}</td><td>${escapeHtml(row.position_notional)}</td><td>${escapeHtml(row.profit_ratio)} / ${escapeHtml(row.profit_threshold)}</td><td>${row.triggered ? '是' : '否'}</td><td>${escapeHtml(row.close_quantity)} / ${escapeHtml(row.close_order_id || '-')} / ${escapeHtml(row.close_status)}</td><td>${escapeHtml(row.reason)}</td></tr>`).join('') : '<tr><td class="empty" colspan="10">暂无硬止盈扫描结果</td></tr>';
  if (recordsBody) recordsBody.innerHTML = records.length ? records.map((row, index) => `<tr class="${index >= 10 ? 'collapsed-extra' : ''}"><td>${formatMsDatetime(row.checked_at)}</td><td>${escapeHtml(row.symbol)}</td><td>${escapeHtml(row.position_amt)}</td><td>${escapeHtml(row.entry_price)}</td><td>${escapeHtml(row.unrealized_pnl)}</td><td>${escapeHtml(row.profit_ratio)} / ${escapeHtml(row.profit_threshold)}</td><td>${escapeHtml(row.close_quantity)}</td><td>${escapeHtml(row.close_order_id || '-')} / ${escapeHtml(row.close_status)}</td><td>${escapeHtml(row.reason)}</td></tr>`).join('') : '<tr><td class="empty" colspan="9">暂无硬止盈操作记录</td></tr>';
}

(function initHardTakeProfitRefresh() {
  const btn = document.getElementById('refresh-hard-take-profit');
  const status = document.getElementById('hard-take-profit-refresh-status');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    btn.disabled = true; btn.textContent = '刷新中...';
    if (status) status.textContent = '正在获取硬止盈模块最新信息...';
    try {
      const response = await fetch('/api/hard-take-profit/summary', {headers: {'Accept': 'application/json'}});
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderHardTakeProfitSummary(payload);
      if (status) status.textContent = `刷新完成：扫描结果 ${payload.checks?.length || 0} 条，操作记录 ${payload.records?.length || 0} 条。`;
    } catch (error) { if (status) status.textContent = `刷新失败：${error.message}`; }
    finally { btn.disabled = false; btn.textContent = '刷新'; }
  });
})();

function renderDynamicProfitProtectionSummary(payload) {
  const roundNoteEl = document.getElementById('dynamic-profit-protection-round-note');
  if (roundNoteEl) roundNoteEl.textContent = payload.round_ts ? `最近判断轮次：${formatMsDatetime(payload.round_ts)}。${DYNAMIC_PROFIT_PROTECTION_RULE_NOTE}` : '暂无动态利润保护扫描结果。';
  const chipsEl = document.getElementById('dynamic-profit-protection-eligible-chips');
  if (chipsEl) {
    const eligibleRows = (payload.checks || []).filter(row => row.eligible);
    chipsEl.innerHTML = eligibleRows.length ? eligibleRows.map(row => `<span class="chip" title="周期累积盈亏 ${escapeHtml(row.cycle_total_pnl)}，R倍数 ${escapeHtml(row.profit_r_multiple)}">${escapeHtml(row.symbol)}</span>`).join('') : '<span class="chip">本轮无满足动态利润保护前提的symbol</span>';
  }
  renderDynamicProfitProtectionChecks(payload.checks || []);
  renderDynamicProfitProtectionRecords(payload.records || []);
}


(function initDynamicProfitProtectionRefresh() {
  const btn = document.getElementById('refresh-dynamic-profit-protection');
  const statusEl = document.getElementById('dynamic-profit-protection-refresh-status');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    btn.disabled = true;
    btn.textContent = '刷新中...';
    if (statusEl) statusEl.textContent = '正在获取动态利润保护板块与动态利润保护记录最新信息...';
    try {
      const response = await fetch('/api/dynamic-profit-protection/summary', { headers: { 'Accept': 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderDynamicProfitProtectionSummary(payload);
      if (statusEl) statusEl.textContent = `刷新完成：扫描结果 ${payload.checks?.length || 0} 条，保护记录 ${payload.records?.length || 0} 条。`;
    } catch (error) {
      if (statusEl) statusEl.textContent = `刷新失败：${error.message}`;
    } finally {
      btn.disabled = false;
      btn.textContent = '刷新';
    }
  });
})();

function renderTrailingStopChecks(rows) {
  const rowsEl = document.getElementById('trailing-stop-checks-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="18">暂无移动追踪止盈扫描结果</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map(row => `
    <tr>
      <td>${formatMsDatetime(row.checked_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${escapeHtml(row.entry_price)}</td>
      <td>${escapeHtml(row.position_amt)}</td>
      <td>${escapeHtml(row.holding_hours)}</td>
      <td>${escapeHtml(row.kline_high)}</td>
      <td>${escapeHtml(row.latest_1m_close)}</td>
      <td>${escapeHtml(row.highest_since_open)}</td>
      <td>${escapeHtml(row.price_drawdown)} / ${escapeHtml(row.drawdown_threshold)}</td>
      <td>${escapeHtml(row.atr14)}</td>
      <td>${escapeHtml(row.volatility || '-')}</td>
      <td>${escapeHtml(row.tag || '-')}</td>
      <td>${statusBadge(row.trailing_stop_triggered)}</td>
      <td>${escapeHtml(row.cancel_take_profit_order_id || '-')} / ${escapeHtml(row.cancel_status)}</td>
      <td>${escapeHtml(row.close_quantity)} / ${escapeHtml(row.close_order_id || '-')} / ${escapeHtml(row.close_status)}</td>
      <td>${formatMsDatetime(row.max_unrealized_pnl_at)}</td>
      <td>${statusBadge(row.eligible)}</td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderTrailingStopRecords(rows) {
  const rowsEl = document.getElementById('trailing-stop-records-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="12">暂无移动追踪止盈操作记录</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map((row, index) => `
    <tr class="${index >= 10 ? 'collapsed-extra' : ''}">
      <td>${formatMsDatetime(row.checked_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${escapeHtml(row.position_amt)}</td>
      <td>${escapeHtml(row.entry_price)}</td>
      <td>${escapeHtml(row.atr14)}</td>
      <td>${escapeHtml(row.volatility || '-')}</td>
      <td>${escapeHtml(row.unrealized_pnl_at_high)} / ${escapeHtml(row.max_unrealized_pnl)}</td>
      <td>${escapeHtml(row.price_drawdown)} / ${escapeHtml(row.drawdown_threshold)}</td>
      <td>${escapeHtml(row.cancel_take_profit_order_id || '-')} / ${escapeHtml(row.cancel_status)}</td>
      <td>${escapeHtml(row.close_quantity)}</td>
      <td>${escapeHtml(row.close_order_id || '-')} / ${escapeHtml(row.close_status)}</td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderTrailingStopSummary(payload) {
  const roundNoteEl = document.getElementById('trailing-stop-round-note');
  if (roundNoteEl) {
    roundNoteEl.textContent = payload.round_ts
      ? `最近判断轮次：${formatMsDatetime(payload.round_ts)}。${TRAILING_STOP_RULE_NOTE}`
      : '暂无移动追踪止盈扫描结果。';
  }
  const chipsEl = document.getElementById('trailing-stop-eligible-chips');
  if (chipsEl) {
    const eligibleRows = (payload.checks || []).filter(row => row.eligible);
    chipsEl.innerHTML = eligibleRows.length
      ? eligibleRows.map(row => `<span class="chip" title="最大未变现盈利 ${escapeHtml(row.max_unrealized_pnl)}">${escapeHtml(row.symbol)}</span>`).join('')
      : '<span class="chip">本轮无满足移动追踪止盈前提的symbol</span>';
  }
  renderTrailingStopChecks(payload.checks || []);
  renderTrailingStopRecords(payload.records || []);
}

function setTrailingStopRefreshLoading(isLoading) {
  const btn = document.getElementById('refresh-trailing-stop');
  const statusEl = document.getElementById('trailing-stop-refresh-status');
  if (btn) {
    btn.disabled = isLoading;
    btn.textContent = isLoading ? '刷新中...' : '刷新';
  }
  if (statusEl && isLoading) statusEl.textContent = '正在更新本轮“预触发移动追踪止盈”symbol的开仓以来最高价、最新close与回撤幅度...';
}

const HOLDING_INCREASE_RULE_NOTE = '该15分钟模块在“减仓条件模块”之后执行，并在每轮执行前从“动态加仓阈值（每15分钟执行）”获取本轮加仓阈值；若当前未变现盈利 ≥ 本轮加仓阈值、最新总分 ≥ 上一轮总分 - 5、且最近一次本生命周期内减仓价为空或当前标记价格 ≥ 该减仓价，则触发“第一次加仓”，并立即调用市价单接口买入当前持仓数量的50%（杠杆沿用当前持仓）。每个 symbol 的单次开仓生命周期内最多记录一次非 skipped 的第一次加仓；原规则四（最新总分 ≥ 69）已取消；若打上“预触发”tag，后台每1分钟查询一次最新标记价格；无减仓记录时刷新后满足条件1即可触发，有减仓记录时刷新后仍满足条件3且满足条件1才触发“第一次加仓”并买入当前持仓数量的50%；加仓前要求当前已用保证金 + 本次加仓保证金不超过当前实验组USDT净值，若因实验组净值预算不足或可用金额不足跳过，则不会标记为已完成，下一轮仍继续参与评估。';

function holdingIncreasePnlCell(unrealizedPnl, oneRUsdt) {
  const pnl = Number(unrealizedPnl);
  const oneR = Number(oneRUsdt);
  const multiple = Number.isFinite(pnl) && Number.isFinite(oneR) && oneR > 0 ? pnl / oneR : 0;
  const cls = multiple >= 1.3 ? 'reduction-pnl-strong-profit' : multiple >= 1 ? 'reduction-pnl-profit' : 'reduction-metric-neutral';
  return `<span class="reduction-metric ${cls}">${escapeHtml(unrealizedPnl)}</span> / ${escapeHtml(oneRUsdt)}`;
}

function holdingIncreaseTagClass(tag) {
  return tag === '已完成第一次加仓' ? 'reduction-tag-increase-completed' : 'reduction-tag-default';
}

function holdingIncreaseTagCell(row) {
  const parts = [];
  if (row.tag) parts.push(`<span class="reduction-tag ${holdingIncreaseTagClass(row.tag)}">${escapeHtml(row.tag)}</span>`);
  if (row.latest_pretrigger_round_ts) parts.push(`<span class="reduction-tag reduction-tag-stale-pretrigger">触发轮次 ${formatMsDatetime(row.latest_pretrigger_round_ts)} 预触发</span>`);
  return parts.length ? parts.join('') : '-';
}

function renderHoldingIncreaseChecks(rows) {
  const rowsEl = document.getElementById('holding-increase-checks-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="11">暂无加仓条件模块执行结果</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map(row => `
    <tr>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${formatMsDatetime(row.decision_round_ts)}</td>
      <td>${holdingIncreaseTagCell(row)}</td>
      <td>${escapeHtml(row.current_price)}</td>
      <td>${holdingIncreasePnlCell(row.unrealized_pnl, row.one_r_usdt)}</td>
      <td>${row.latest_total_score || '-'}</td>
      <td>${row.previous_total_score || '-'}</td>
      <td>${row.latest_reduction_price ? escapeHtml(row.latest_reduction_price) : '无，已跳过条件3'}</td>
      <td>${row.open_trade_created_at ? formatMsDatetime(row.open_trade_created_at) : '-'}</td>
      <td>${escapeHtml(row.reason)}</td>
      <td>${formatMsDatetime(row.checked_at)}</td>
    </tr>
  `).join('');
}

function renderHoldingIncreaseRecords(rows) {
  const rowsEl = document.getElementById('holding-increase-records-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="11">最近7天暂无加仓操作记录</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map((row, index) => `
    <tr class="${index >= 10 ? 'collapsed-extra' : ''}">
      <td>${formatMsDatetime(row.created_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${formatMsDatetime(row.decision_round_ts)}</td>
      <td><span class="reduction-tag reduction-tag-default">${escapeHtml(row.action_name)}</span></td>
      <td>${escapeHtml(row.current_price)}</td>
      <td>${holdingIncreasePnlCell(row.unrealized_pnl, row.one_r_usdt)}</td>
      <td>${escapeHtml(row.latest_total_score)} / ${escapeHtml(row.previous_total_score)}</td>
      <td>${row.latest_reduction_price ? escapeHtml(row.latest_reduction_price) : '-'}</td>
      <td>${row.increased_quantity || '-'} / ${row.required_margin_usdt || '-'} / ${row.available_experiment_usdt || '-'}</td>
      <td><span class="status-badge ${row.status === 'submitted' ? 'status-pass' : row.status === 'skipped' ? 'status-warn' : 'status-fail'}">${escapeHtml(row.status)}</span></td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderHoldingIncreaseSummary(payload) {
  const checks = payload.checks || [];
  const records = payload.action_records || [];
  const roundNoteEl = document.getElementById('holding-increase-round-note');
  if (roundNoteEl) {
    roundNoteEl.textContent = payload.round_ts ? `最近判断轮次：${formatMsDatetime(payload.round_ts)}。${HOLDING_INCREASE_RULE_NOTE}` : '暂无加仓条件模块执行结果。';
  }
  const triggeredChipsEl = document.getElementById('holding-increase-triggered-chips');
  if (triggeredChipsEl) {
    const triggeredRows = checks.filter(row => row.triggered);
    triggeredChipsEl.innerHTML = triggeredRows.length
      ? triggeredRows.map(row => `<span class="reduction-tags" title="${escapeHtml(row.reason)}"><span class="chip">${escapeHtml(row.symbol)}</span><span class="reduction-tag reduction-tag-default">第一次加仓</span></span>`).join('')
      : '<span class="chip">本轮无触发第一次加仓symbol</span>';
  }
  const pretriggerChipsEl = document.getElementById('holding-increase-pretrigger-chips');
  if (pretriggerChipsEl) {
    const pretriggerRows = checks.filter(row => row.tag === '预触发');
    pretriggerChipsEl.style.display = pretriggerRows.length ? '' : 'none';
    pretriggerChipsEl.innerHTML = pretriggerRows.map(row => `<span class="reduction-tags" title="${escapeHtml(row.reason)}"><span class="chip">${escapeHtml(row.symbol)}</span><span class="reduction-tag reduction-tag-default">预触发</span></span>`).join('');
  }
  renderHoldingIncreaseChecks(checks);
  renderHoldingIncreaseRecords(records);
}

const TRAILING_REDUCTION_RULE_NOTE = '每个 symbol 会先检查最近一次开仓后的“分批止盈卖出记录”；如已有成功记录，则标记灰色 tag“已触发过分批止盈”，并跳过预触发、结构破位及减仓检查。其余 symbol 才继续执行移动追踪减仓逻辑。';

function trailingReductionTagCell(row) {
  if (row.tag === '已触发过分批止盈') {
    return `<span class="reduction-tag reduction-tag-partial-take-profit">${escapeHtml(row.tag)}</span>`;
  }
  if (row.latest_pretrigger_round_ts) {
    const tagClass = Number(row.latest_pretrigger_round_ts) === Number(row.decision_round_ts)
      ? 'reduction-tag-trailing-pretrigger-current'
      : 'reduction-tag-trailing-pretrigger';
    return `<span class="reduction-tag ${tagClass}">触发轮次 ${formatMsDatetime(row.latest_pretrigger_round_ts)} 预触发结构破位</span>`;
  }
  if (row.tag) {
    const tagClass = row.tag === '已触发过分批止盈' ? 'reduction-tag-partial-take-profit' : 'reduction-tag-trailing-pretrigger';
    return `<span class="reduction-tag ${tagClass}">${escapeHtml(row.tag)}</span>`;
  }
  return '-';
}

function trailingReductionCurrentPriceCell(row) {
  const currentPrice = Number(row.current_price);
  const lowest = Number(row.lowest_15m_low);
  const dangerClass = Number.isFinite(currentPrice) && Number.isFinite(lowest) && lowest > 0 && currentPrice < lowest
    ? 'reduction-current-price-danger'
    : '';
  return `<span class="${dangerClass}">${escapeHtml(row.current_price)}</span>`;
}

function renderTrailingReductionChecks(rows) {
  const rowsEl = document.getElementById('trailing-reduction-checks-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="18">暂无移动追踪减仓扫描结果</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map(row => `
    <tr>
      <td>${formatMsDatetime(row.checked_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${trailingReductionTagCell(row)}</td>
      <td>${escapeHtml(row.r_usdt)} / ${escapeHtml(row.trigger_r_usdt)}</td>
      <td>${escapeHtml(row.unrealized_pnl)}</td>
      <td>${trailingReductionCurrentPriceCell(row)}</td>
      <td>${escapeHtml(row.latest_15m_low)} / ${escapeHtml(row.second_15m_low)}</td>
      <td>${escapeHtml(row.lowest_15m_low)}</td>
      <td>${escapeHtml(row.atr14 || '-')}</td>
      <td>${escapeHtml(row.latest_1m_high || '-')} / ${escapeHtml(row.latest_1m_close || '-')}</td>
      <td>${escapeHtml(row.highest_since_open || '-')}</td>
      <td>${escapeHtml(row.price_drawdown || '-')}</td>
      <td>${escapeHtml(row.entry_price)}</td>
      <td>${escapeHtml(row.position_amt)}</td>
      <td>${statusBadge(row.eligible)}</td>
      <td>${statusBadge(row.pretriggered)}</td>
      <td>${statusBadge(row.structure_break_triggered)}</td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderTrailingReductionRecords(rows) {
  const rowsEl = document.getElementById('trailing-reduction-records-body');
  if (!rowsEl) return;
  if (!rows.length) {
    rowsEl.innerHTML = '<tr><td class="empty" colspan="11">近7天暂无移动追踪减仓记录</td></tr>';
    return;
  }
  rowsEl.innerHTML = rows.map((row, index) => `
    <tr class="${index >= 10 ? 'collapsed-extra' : ''}">
      <td>${formatMsDatetime(row.checked_at)}</td>
      <td>${escapeHtml(row.symbol)}</td>
      <td>${escapeHtml(row.latest_1m_high)} / ${escapeHtml(row.latest_1m_close)}</td>
      <td>${escapeHtml(row.highest_since_open)} / ${escapeHtml(row.atr14)} / ${escapeHtml(row.price_drawdown)}</td>
      <td>${escapeHtml(row.reduction_percent)}</td>
      <td>${escapeHtml(row.original_quantity)} / ${escapeHtml(row.reduced_quantity)} / ${escapeHtml(row.remaining_quantity)}</td>
      <td>${escapeHtml(row.market_order_id || '-')}</td>
      <td>${escapeHtml(row.take_profit_order_id || '-')}</td>
      <td>${escapeHtml(row.stop_loss_order_id || '-')}</td>
      <td><span class="status-badge ${row.status === 'submitted' ? 'status-pass' : 'status-fail'}">${escapeHtml(row.status)}</span></td>
      <td>${escapeHtml(row.reason)}</td>
    </tr>
  `).join('');
}

function renderTrailingReductionSummary(payload) {
  const checks = payload.checks || [];
  const records = payload.records || [];
  const roundNoteEl = document.getElementById('trailing-reduction-round-note');
  if (roundNoteEl) {
    roundNoteEl.textContent = payload.round_ts
      ? `最近判断轮次：${formatMsDatetime(payload.round_ts)}。${TRAILING_REDUCTION_RULE_NOTE}`
      : '暂无移动追踪减仓扫描结果。';
  }
  const chipsEl = document.getElementById('trailing-reduction-pretrigger-chips');
  if (chipsEl) {
    const pretriggerRows = checks.filter(row => row.pretriggered);
    chipsEl.innerHTML = pretriggerRows.length
      ? pretriggerRows.map(row => `<span class="chip" title="${escapeHtml(row.reason)}">${escapeHtml(row.symbol)} · ${escapeHtml(row.tag || '预触发结构破位')}</span>`).join('')
      : '<span class="chip">本轮无预触发结构破位symbol</span>';
  }
  renderTrailingReductionChecks(checks);
  renderTrailingReductionRecords(records);
}

(function initHoldingIncreaseRefresh() {
  const btn = document.getElementById('refresh-holding-increase');
  const statusEl = document.getElementById('holding-increase-refresh-status');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    btn.disabled = true;
    btn.textContent = '刷新中...';
    if (statusEl) statusEl.textContent = '正在刷新“预触发”symbol的 current_price...';
    try {
      const response = await fetch('/api/holding-increase/refresh-pretrigger', {
        method: 'POST',
        headers: { 'Accept': 'application/json' },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      if (statusEl) {
        statusEl.textContent = `刷新完成：预触发刷新 ${payload.refreshed || 0} 个，触发 ${payload.triggered || 0} 个，加仓记录 ${payload.created_records || payload.records || 0} 条；已更新加仓模块。`;
      }
      renderHoldingIncreaseSummary(payload);
    } catch (error) {
      if (statusEl) statusEl.textContent = `刷新失败：${error.message}`;
    } finally {
      btn.disabled = false;
      btn.textContent = '刷新';
    }
  });
})();


(function initTrailingReductionRefresh() {
  const btn = document.getElementById('refresh-trailing-reduction');
  const statusEl = document.getElementById('trailing-reduction-refresh-status');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    btn.disabled = true;
    btn.textContent = '刷新中...';
    if (statusEl) statusEl.textContent = '正在更新“预触发结构破位”symbol的回撤幅度与开仓以来最高价...';
    try {
      const response = await fetch('/api/trailing-reduction/refresh-pretrigger', { method: 'POST', headers: { 'Accept': 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      if (statusEl) statusEl.textContent = `刷新完成：更新 ${payload.refreshed || 0} 个，结构破位 ${payload.triggered || 0} 个，减仓记录 ${payload.created_records || 0} 条。`;
      renderTrailingReductionSummary(payload);
    } catch (error) {
      if (statusEl) statusEl.textContent = `刷新失败：${error.message}`;
    } finally {
      btn.disabled = false;
      btn.textContent = '刷新';
    }
  });
})();

(function initTrailingStopRefresh() {
  const btn = document.getElementById('refresh-trailing-stop');
  const statusEl = document.getElementById('trailing-stop-refresh-status');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    setTrailingStopRefreshLoading(true);
    try {
      const response = await fetch('/api/trailing-stop/refresh-pretrigger', {
        method: 'POST',
        headers: { 'Accept': 'application/json' },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      renderTrailingStopSummary(payload);
      if (statusEl) statusEl.textContent = `刷新完成：更新 ${payload.refreshed || 0} 个预触发symbol，触发 ${payload.triggered || 0} 个，操作记录 ${payload.created_records || 0} 条。`;
    } catch (error) {
      if (statusEl) statusEl.textContent = `刷新失败：${error.message}`;
    } finally {
      setTrailingStopRefreshLoading(false);
    }
  });
})();
function getFilledOrdersDays() {
  const select = document.getElementById('filled-orders-days');
  const days = Number(select?.value || 7);
  if (!Number.isFinite(days)) return 7;
  return Math.max(1, Math.min(Math.trunc(days), 30));
}

function setFilledOrdersButtonLabel() {
  const btn = document.getElementById('query-filled-sell-orders');
  if (btn) btn.textContent = `查询近${getFilledOrdersDays()}天已成交订单`;
}

function setFilledSellOrdersLoading(isLoading, rangeLabel = '') {
  const btn = document.getElementById('query-filled-sell-orders');
  const rangeBtn = document.getElementById('query-filled-orders-by-time');
  const daysSelect = document.getElementById('filled-orders-days');
  const scoreBandSelect = document.getElementById('filled-orders-score-band');
  const startInput = document.getElementById('filled-orders-start-time');
  const endInput = document.getElementById('filled-orders-end-time');
  const status = document.getElementById('filled-sell-orders-status');
  if (btn) {
    btn.disabled = isLoading;
    btn.textContent = isLoading ? '查询中...' : `查询近${getFilledOrdersDays()}天已成交订单`;
  }
  if (rangeBtn) {
    rangeBtn.disabled = isLoading;
    rangeBtn.textContent = isLoading && rangeLabel ? '查询中...' : '按起止时间查询';
  }
  if (daysSelect) daysSelect.disabled = isLoading;
  if (scoreBandSelect) scoreBandSelect.disabled = isLoading;
  if (startInput) startInput.disabled = isLoading;
  if (endInput) endInput.disabled = isLoading;
  if (status && isLoading) status.textContent = `正在请求 Binance REST API（${rangeLabel || `近${getFilledOrdersDays()}天`}）...`;
  if (isLoading) renderFilledOrdersSummary(null);
}

function getFilledOrdersScoreBandFilter() {
  const select = document.getElementById('filled-orders-score-band');
  return select?.value || 'all';
}

function orderMatchesScoreBandFilter(row, scoreBandFilter) {
  if (!scoreBandFilter || scoreBandFilter === 'all') return true;
  const score = Number(row.open_total_score);
  if (scoreBandFilter === 'unknown') return !Number.isFinite(score);
  const [lower, upper] = scoreBandFilter.split('-').map(Number);
  return Number.isFinite(score) && score >= lower && score <= upper;
}

function buildFilledOrderAnalysis(orders) {
  const groupByRow = new Map();
  const openGroupsBySymbol = new Map();
  const groups = new Map();
  let nextGroupId = 1;
  const ordered = orders
    .map((row, index) => ({ ...row, index, numericTime: Number(row.time) || 0, numericQty: Math.abs(Number(row.quantity) || 0) }))
    .sort((a, b) => a.numericTime - b.numericTime || a.index - b.index);

  const appendGroup = (rowIndex, groupId) => {
    if (!groupByRow.has(rowIndex)) groupByRow.set(rowIndex, new Set());
    groupByRow.get(rowIndex).add(groupId);
  };

  ordered.forEach((row) => {
    const symbol = row.symbol || '';
    if (!openGroupsBySymbol.has(symbol)) openGroupsBySymbol.set(symbol, []);
    const openGroups = openGroupsBySymbol.get(symbol);
    if (row.side === 'BUY') {
      const groupId = nextGroupId++;
      openGroups.push({ groupId, remainingQty: row.numericQty });
      groups.set(groupId, { groupId, symbol, buyQty: row.numericQty, sellQty: 0, realizedPnl: 0 });
      appendGroup(row.index, groupId);
      return;
    }
    if (row.side !== 'SELL') return;
    let remainingSellQty = row.numericQty;
    const rowPnl = Number(row.realized_pnl) || 0;
    while (remainingSellQty > 0 && openGroups.length) {
      const group = openGroups[0];
      const matchedQty = Math.min(remainingSellQty, group.remainingQty);
      const groupSummary = groups.get(group.groupId);
      appendGroup(row.index, group.groupId);
      if (groupSummary) {
        groupSummary.sellQty += matchedQty;
        // 同一完成订单组可能包含多笔卖出成交；按组累计这些卖出成交的全部已实现盈亏。
        groupSummary.realizedPnl += rowPnl;
      }
      group.remainingQty -= matchedQty;
      remainingSellQty -= matchedQty;
      if (group.remainingQty <= 1e-12) openGroups.shift();
    }
  });

  const rowGroups = orders.map((_, index) => Array.from(groupByRow.get(index) || []));
  const completedGroups = Array.from(groups.values()).filter(group => group.buyQty > 0 && Math.abs(group.buyQty - group.sellQty) <= 1e-8);
  const totalProfit = completedGroups.reduce((total, group) => total + Math.max(group.realizedPnl, 0), 0);
  const totalLoss = completedGroups.reduce((total, group) => total + Math.min(group.realizedPnl, 0), 0);
  const profitCount = completedGroups.filter(group => group.realizedPnl > 0).length;
  const lossCount = completedGroups.filter(group => group.realizedPnl < 0).length;
  const averageProfit = profitCount ? totalProfit / profitCount : 0;
  const averageLoss = lossCount ? totalLoss / lossCount : 0;
  const profitLossRatio = Math.abs(averageLoss) > 0 ? averageProfit / Math.abs(averageLoss) : 0;
  const winRate = completedGroups.length ? profitCount / completedGroups.length : 0;
  const expectancy = winRate * profitLossRatio - (1 - winRate);
  return {
    rowGroups,
    summary: {
      completedCount: completedGroups.length,
      totalProfit,
      profitCount,
      totalLoss,
      lossCount,
      averageProfit,
      averageLoss,
      profitLossRatio,
      winRate,
      expectancy,
    },
  };
}


function formatPnlAmount(value) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return '0.00000000';
  return numericValue.toFixed(8).replace(/(\.\d*?[1-9])0+$/, '$1').replace(/\.0+$/, '');
}

function renderFilledOrdersSummary(summary) {
  const setText = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  };
  const setPnl = (id, value) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = formatPnlAmount(value);
    el.classList.remove('pnl-positive', 'pnl-negative', 'pnl-zero');
    el.classList.add(pnlClass(value));
  };
  setText('filled-completed-count', String(summary?.completedCount ?? 0));
  setPnl('filled-total-profit', summary?.totalProfit ?? 0);
  setText('filled-profit-count', String(summary?.profitCount ?? 0));
  setText('filled-win-rate', `${((summary?.winRate ?? 0) * 100).toFixed(2)}%`);
  setPnl('filled-total-loss', summary?.totalLoss ?? 0);
  setText('filled-loss-count', String(summary?.lossCount ?? 0));
  setPnl('filled-average-profit', summary?.averageProfit ?? 0);
  setPnl('filled-average-loss', summary?.averageLoss ?? 0);
  setText('filled-profit-loss-ratio', formatPnlAmount(summary?.profitLossRatio ?? 0));
  setPnl('filled-expectancy', summary?.expectancy ?? 0);
}

function bindFilledOrderHighlights(rowsEl) {
  rowsEl.querySelectorAll('.filled-order-row').forEach((rowEl) => {
    rowEl.addEventListener('click', () => {
      const selectedGroups = (rowEl.dataset.groupIds || '').split(',').filter(Boolean);
      rowsEl.querySelectorAll('.filled-order-row').forEach((candidate) => {
        const candidateGroups = (candidate.dataset.groupIds || '').split(',').filter(Boolean);
        const isLinked = selectedGroups.length && candidateGroups.some(groupId => selectedGroups.includes(groupId));
        candidate.classList.toggle('is-linked', isLinked);
        candidate.classList.toggle('is-active', candidate === rowEl);
      });
    });
  });
}

function renderFilledSellOrders(payload) {
  const rowsEl = document.getElementById('filled-sell-orders-rows');
  const statusEl = document.getElementById('filled-sell-orders-status');
  const errorEl = document.getElementById('filled-sell-orders-error');
  const allOrders = payload.orders || [];
  const scoreBandFilter = getFilledOrdersScoreBandFilter();
  const orders = allOrders.filter(row => orderMatchesScoreBandFilter(row, scoreBandFilter));
  currentDisplayedFilledOrders = orders;
  const exportBtn = document.getElementById('export-filled-orders');
  if (exportBtn) exportBtn.disabled = !orders.length;
  const queriedRange = payload.days
    ? `近${payload.days}天`
    : `${formatMsDatetime(payload.start_time)} 至 ${formatMsDatetime(payload.end_time)}`;
  const escapeHtml = (value) => String(value ?? '').replace(/[&<>'"]/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));

  if (errorEl) {
    errorEl.style.display = 'none';
    errorEl.textContent = '';
  }
  if (statusEl) {
    const queriedAt = payload.queried_at ? formatMsDatetime(payload.queried_at) : '未知时间';
    const filterText = scoreBandFilter === 'all' ? '全部开仓评分档位' : (document.getElementById('filled-orders-score-band')?.selectedOptions?.[0]?.textContent || scoreBandFilter);
    statusEl.textContent = `${payload.testnet ? 'Demo/Testnet' : 'Real'} 查询完成：${queriedAt}；${queriedRange}已成交合并记录 ${allOrders.length} 条；当前筛选 ${filterText} 后 ${orders.length} 条`;
  }
  if (!rowsEl) return;
  if (!orders.length) {
    rowsEl.innerHTML = `<tr><td class="empty" colspan="33">${queriedRange}当前评分档位下暂无已成交订单</td></tr>`;
    renderFilledOrdersSummary({ completedCount: 0, totalProfit: 0, profitCount: 0, totalLoss: 0, lossCount: 0, averageProfit: 0, averageLoss: 0, profitLossRatio: 0, winRate: 0, expectancy: 0 });
    return;
  }
  const filledOrderAnalysis = buildFilledOrderAnalysis(orders);
  const groupsByRow = filledOrderAnalysis.rowGroups;
  renderFilledOrdersSummary(filledOrderAnalysis.summary);
  rowsEl.innerHTML = orders.map((row, index) => {
    const groupIds = groupsByRow[index];
    const groupLabel = groupIds.length ? `<span class="filled-order-group-badge">组${escapeHtml(groupIds.join('/'))}</span>` : '';
    return `
    <tr class="filled-order-row" data-group-ids="${escapeHtml(groupIds.join(','))}" title="点击高亮同一 symbol 同一组买卖订单">
      <td>${formatMsDatetime(row.time)}</td>
      <td>${escapeHtml(row.symbol)}${groupLabel}</td>
      <td>${escapeHtml(row.open_score_band || '-')}</td>
      <td>${escapeHtml(row.open_leverage ?? '-')}</td>
      <td>${row.open_total_score ?? '-'}</td>
      <td>${escapeHtml(row.exit_reason || '-')}</td>
      <td><span class="status-badge ${row.side === 'BUY' ? 'status-ok' : 'status-fail'}">${escapeHtml(row.side || '-')}</span></td>
      <td>${escapeHtml(row.order_id)}</td>
      <td>${escapeHtml(row.price)}</td>
      <td>${escapeHtml(row.quantity)}</td>
      <td>${escapeHtml(row.quote_quantity)}</td>
      <td><span class="${pnlClass(row.realized_pnl)}">${escapeHtml(row.realized_pnl || '0')}</span></td>
      <td>${escapeHtml(row.commission || '0')} ${escapeHtml(row.commission_asset || '')}</td>
      ${Array.from({ length: 18 }, (_, ruleIndex) => `<td>${escapeHtml(row[`open_rule${ruleIndex + 1}_score`] ?? '-')}</td>`).join('')}
      <td>${row.maker ? '是' : '否'}</td>
      <td>${escapeHtml(row.trade_id)}</td>
    </tr>`;
  }).join('');
  bindFilledOrderHighlights(rowsEl);
}

function renderFilledSellOrdersError(message) {
  const errorEl = document.getElementById('filled-sell-orders-error');
  const statusEl = document.getElementById('filled-sell-orders-status');
  if (errorEl) {
    errorEl.textContent = message;
    errorEl.style.display = 'block';
  }
  if (statusEl) statusEl.textContent = '查询失败';
  renderFilledOrdersSummary(null);
}

let latestFilledOrdersPayload = null;
let currentDisplayedFilledOrders = [];

(function initFilledSellOrdersQuery() {
  const btn = document.getElementById('query-filled-sell-orders');
  const rangeBtn = document.getElementById('query-filled-orders-by-time');
  const exportBtn = document.getElementById('export-filled-orders');
  if (!btn || !rangeBtn || !exportBtn) return;
  setFilledOrdersButtonLabel();
  const toLocalHourValue = (date) => {
    const localDate = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
    return localDate.toISOString().slice(0, 13) + ':00';
  };
  const endDate = new Date();
  endDate.setMinutes(0, 0, 0);
  document.getElementById('filled-orders-end-time').value = toLocalHourValue(endDate);
  document.getElementById('filled-orders-start-time').value = toLocalHourValue(new Date(endDate.getTime() - 7 * 24 * 60 * 60 * 1000));
  const daysSelect = document.getElementById('filled-orders-days');
  if (daysSelect) daysSelect.addEventListener('change', setFilledOrdersButtonLabel);
  const scoreBandSelect = document.getElementById('filled-orders-score-band');
  if (scoreBandSelect) scoreBandSelect.addEventListener('change', () => {
    if (latestFilledOrdersPayload) renderFilledSellOrders(latestFilledOrdersPayload);
  });
  exportBtn.addEventListener('click', async () => {
    if (!currentDisplayedFilledOrders.length) return;
    exportBtn.disabled = true;
    exportBtn.textContent = '正在导出...';
    try {
      const response = await fetch('/api/account/filled-orders/export', {
        method: 'POST',
        headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
        body: JSON.stringify({ orders: currentDisplayedFilledOrders }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || `HTTP ${response.status}`);
      }
      const blob = await response.blob();
      const disposition = response.headers.get('Content-Disposition') || '';
      const filenameMatch = disposition.match(/filename="?([^";]+)"?/i);
      const link = document.createElement('a');
      link.href = URL.createObjectURL(blob);
      link.download = filenameMatch?.[1] || 'filled_orders.xlsx';
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(link.href);
    } catch (error) {
      renderFilledSellOrdersError(`Excel 导出失败：${error.message}`);
    } finally {
      exportBtn.disabled = !currentDisplayedFilledOrders.length;
      exportBtn.textContent = '导出 Excel 到本地';
    }
  });
  btn.addEventListener('click', async () => {
    setFilledSellOrdersLoading(true);
    try {
      const days = getFilledOrdersDays();
      const response = await fetch(`/api/account/filled-sell-orders?days=${encodeURIComponent(days)}`, {
        headers: { 'Accept': 'application/json' },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      latestFilledOrdersPayload = payload;
      renderFilledSellOrders(payload);
    } catch (error) {
      renderFilledSellOrdersError(`已成交订单查询失败：${error.message}`);
    } finally {
      setFilledSellOrdersLoading(false);
    }
  });
  rangeBtn.addEventListener('click', async () => {
    const startValue = document.getElementById('filled-orders-start-time').value;
    const endValue = document.getElementById('filled-orders-end-time').value;
    const startTime = new Date(startValue).getTime();
    const endTime = new Date(endValue).getTime();
    if (!Number.isFinite(startTime) || !Number.isFinite(endTime)) {
      renderFilledSellOrdersError('请同时选择开始时间和截止时间');
      return;
    }
    if (startTime >= endTime) {
      renderFilledSellOrdersError('开始时间必须早于截止时间');
      return;
    }
    const rangeLabel = `${formatMsDatetime(startTime)} 至 ${formatMsDatetime(endTime)}`;
    setFilledSellOrdersLoading(true, rangeLabel);
    try {
      const query = new URLSearchParams({ start_time: String(startTime), end_time: String(endTime) });
      const response = await fetch(`/api/account/filled-sell-orders?${query}`, { headers: { 'Accept': 'application/json' } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      latestFilledOrdersPayload = payload;
      renderFilledSellOrders(payload);
    } catch (error) {
      renderFilledSellOrdersError(`已成交订单查询失败：${error.message}`);
    } finally {
      setFilledSellOrdersLoading(false);
    }
  });
})();


  let experimentEquityTrendChart = null;

  function refreshExperimentEquityTrendChartLayout() {
    if (experimentEquityTrendChart) experimentEquityTrendChart.resize();
  }

  function buildExperimentEquityTrendOption(rawRows) {
    const hasRows = rawRows.length > 0;
    const labels = rawRows.map(row => new Date(row[0]).toISOString().slice(5, 16).replace('T', ' '));
    const values = rawRows.map(row => Number(row[1]));
    if (!hasRows) {
      return {
        title: { text: '暂无近7天实验组USDT净值数据', left: 'center', top: 'middle', textStyle: { color: '#6b7280', fontSize: 16 } },
        xAxis: { type: 'category', data: [] },
        yAxis: { type: 'value', name: 'USDT净值' },
        series: [{ type: 'line', data: [] }]
      };
    }
    return {
      animation: false,
      grid: { left: 64, right: 28, top: 32, bottom: 58 },
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'category', data: labels, boundaryGap: false },
      yAxis: { type: 'value', name: 'USDT净值', scale: true },
      dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 12, height: 18 }],
      series: [{ name: '实验组USDT净值', type: 'line', data: values, smooth: true, symbol: 'circle', symbolSize: 6, lineStyle: { width: 3, color: '#f97316' }, itemStyle: { color: '#f97316' }, areaStyle: { color: 'rgba(249, 115, 22, 0.14)' } }]
    };
  }

  function initExperimentEquityTrendChart() {
    const chartEl = document.getElementById('experiment-equity-trend-chart');
    const dataEl = document.getElementById('simulation-equity-trend-data');
    if (!chartEl || !dataEl || !window.echarts) return;
    const rawRows = JSON.parse(dataEl.textContent);
    experimentEquityTrendChart = window.echarts.init(chartEl);
    experimentEquityTrendChart.setOption(buildExperimentEquityTrendOption(rawRows), true);
    window.addEventListener('resize', refreshExperimentEquityTrendChartLayout);
    requestAnimationFrame(refreshExperimentEquityTrendChartLayout);
  }

  initSimulationTabs();
  initHoldingModuleTabs();
  initCollapsibles();
  initExperimentEquityTrendChart();
})();
