function formatMsDatetime(tsMs) {
  const ts = Number(tsMs);
  if (!Number.isFinite(ts) || ts <= 0) return '-';
  const d = new Date(ts);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())} UTC`;
}

function setFeatureFlagMessage(message, isError = false) {
  const el = document.getElementById('feature-flag-message');
  if (!el) return;
  el.textContent = message || '';
  el.classList.toggle('error', Boolean(isError));
}

function renderFeatureFlagRow(flag) {
  const row = document.querySelector(`tr[data-feature-key="${CSS.escape(flag.key)}"]`);
  if (!row) return;
  const status = row.querySelector('.feature-flag-status');
  if (status) {
    status.textContent = flag.enabled ? '开启' : '关闭';
    status.classList.toggle('enabled', Boolean(flag.enabled));
    status.classList.toggle('disabled', !flag.enabled);
  }
  const updatedAt = row.querySelector('[data-feature-updated-at]');
  if (updatedAt) {
    updatedAt.dataset.featureUpdatedAt = String(flag.updated_at || 0);
    updatedAt.textContent = formatMsDatetime(flag.updated_at);
  }
  const button = row.querySelector('.feature-flag-toggle');
  if (button) {
    button.dataset.featureEnabled = flag.enabled ? '1' : '0';
    button.textContent = flag.enabled ? '关闭' : '开启';
    button.classList.toggle('btn-danger', Boolean(flag.enabled));
    button.classList.toggle('btn-success', !flag.enabled);
  }
}

(function initFeatureFlagToggles() {
  const buttons = document.querySelectorAll('.feature-flag-toggle');
  buttons.forEach((button) => {
    button.addEventListener('click', async () => {
      const key = button.dataset.featureKey;
      const currentlyEnabled = button.dataset.featureEnabled === '1';
      const nextEnabled = !currentlyEnabled;
      button.disabled = true;
      setFeatureFlagMessage(`正在${nextEnabled ? '开启' : '关闭'}功能开关...`);
      try {
        const response = await fetch(`/api/feature-flags/${encodeURIComponent(key)}`, {
          method: 'POST',
          headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
          body: JSON.stringify({ enabled: nextEnabled }),
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
        renderFeatureFlagRow(payload.flag);
        setFeatureFlagMessage(`${payload.flag.name}已${payload.flag.enabled ? '开启' : '关闭'}，将在对应模块下一次 job 执行前生效。`);
      } catch (err) {
        setFeatureFlagMessage(`功能开关更新失败：${err.message || err}`, true);
      } finally {
        button.disabled = false;
      }
    });
  });
})();

(function initMarginBudgetSettingsForm() {
  const form = document.getElementById('margin-budget-settings-form'); const message = document.getElementById('margin-budget-settings-message');
  if (!form || !message) return;
  form.addEventListener('submit', async event => { event.preventDefault(); if (!form.reportValidity()) return;
    try { const response = await fetch('/api/margin-budget-settings', {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({simulation_max_margin_cost_usdt:document.getElementById('simulation-max-margin-cost').value, live_max_margin_cost_usdt:document.getElementById('live-max-margin-cost').value})}); const result=await response.json(); if(!response.ok) throw new Error(result.error); message.textContent='总仓位成本配置已保存。'; } catch(err) { message.textContent=`总仓位成本配置保存失败：${err.message||err}`; message.classList.add('error'); }
  });
})();

(function initDynamicOpenThresholdForm() {
  const form = document.getElementById('dynamic-open-threshold-form');
  const message = document.getElementById('dynamic-open-threshold-message');
  if (!form || !message) return;
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const button = form.querySelector('button[type="submit"]');
    const payload = {
      window_hours: Number(document.getElementById('dynamic-open-window-hours').value),
      unrestricted_score: Number(document.getElementById('dynamic-open-unrestricted-score').value),
      restricted_score_floor: Number(document.getElementById('dynamic-open-restricted-floor').value),
      min_open_total_score: Number(document.getElementById('dynamic-open-min-total-score').value),
    };
    button.disabled = true;
    message.textContent = '正在保存动态开仓门槛配置...';
    message.classList.remove('error');
    try {
      const response = await fetch('/api/dynamic-open-threshold-settings', {
        method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload),
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
      message.textContent = `配置已保存：统计最近 ${result.window_hours} 小时；最高分 ≥ ${result.unrestricted_score} 放开限制，${result.restricted_score_floor}–${result.unrestricted_score - 1} 分时要求本轮总分 ≥ ${result.min_open_total_score}。`;
    } catch (err) {
      message.textContent = `动态开仓门槛配置保存失败：${err.message || err}`;
      message.classList.add('error');
    } finally { button.disabled = false; }
  });
})();

(function initPositionLimitSettingsForm() {
  const form = document.getElementById('position-limit-settings-form');
  const message = document.getElementById('position-limit-settings-message');
  if (!form || !message) return;
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const button = form.querySelector('button[type="submit"]');
    const payload = {
      simulation_max_open_positions: Number(document.getElementById('simulation-max-open-positions').value),
      live_max_open_positions: Number(document.getElementById('live-max-open-positions').value),
      max_new_positions_per_round: Number(document.getElementById('max-new-positions-per-round').value),
    };
    button.disabled = true;
    message.textContent = '正在保存最大持仓配置...';
    message.classList.remove('error');
    try {
      const response = await fetch('/api/position-limit-settings', {
        method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload),
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
      message.textContent = `配置已保存：模拟盘最多 ${result.simulation_max_open_positions} 个仓位，实盘最多 ${result.live_max_open_positions} 个仓位，每轮最多新开 ${result.max_new_positions_per_round} 个仓位。`;
    } catch (err) {
      message.textContent = `最大持仓配置保存失败：${err.message || err}`;
      message.classList.add('error');
    } finally { button.disabled = false; }
  });
})();

(function initHardTakeProfitSettingsForm() {
  const form = document.getElementById('hard-take-profit-settings-form');
  const message = document.getElementById('hard-take-profit-settings-message');
  if (!form || !message) return;
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const button = form.querySelector('button[type="submit"]');
    const payload = {profit_ratio: Number(document.getElementById('hard-take-profit-ratio').value) / 100};
    button.disabled = true; message.textContent = '正在保存硬止盈配置…'; message.classList.remove('error');
    try {
      const response = await fetch('/api/hard-take-profit-settings', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
      message.textContent = `配置已保存：未变现盈利率达到 ${result.profit_ratio * 100}% 时全部平仓。`;
    } catch (err) { message.textContent = `硬止盈配置保存失败：${err.message || err}`; message.classList.add('error'); }
    finally { button.disabled = false; }
  });
})();

(function initReductionModuleSettingsForm() {
  const form = document.getElementById('reduction-module-settings-form');
  const message = document.getElementById('reduction-module-settings-message');
  if (!form || !message) return;
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const button = form.querySelector('button[type="submit"]');
    const payload = {
      rule2: {enabled: document.getElementById('reduction-rule2-enabled').checked, reduction_fraction: Number(document.getElementById('reduction-rule2-percent').value) / 100},
      rule5: {enabled: document.getElementById('reduction-rule5-enabled').checked, reduction_fraction: Number(document.getElementById('reduction-rule5-percent').value) / 100},
    };
    button.disabled = true; message.textContent = '正在保存减仓模块配置…'; message.classList.remove('error');
    try {
      const response = await fetch('/api/reduction-module-settings', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
      message.textContent = `配置已保存：规则二${result.rule2.enabled ? '开启' : '关闭'}、减仓 ${result.rule2.reduction_fraction * 100}%；规则五${result.rule5.enabled ? '开启' : '关闭'}、减仓 ${result.rule5.reduction_fraction * 100}%。`;
    } catch (err) { message.textContent = `减仓模块配置保存失败：${err.message || err}`; message.classList.add('error'); }
    finally { button.disabled = false; }
  });
})();

(function initDynamicProfitProtectionSettingsForm() {
  const form = document.getElementById('dynamic-profit-protection-settings-form');
  const message = document.getElementById('dynamic-profit-protection-settings-message');
  if (!form || !message) return;
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const button = form.querySelector('button[type="submit"]');
    const payload = {
      enabled: document.getElementById('dynamic-profit-protection-enabled').checked,
      tier_2_min_r: Number(document.getElementById('dynamic-profit-tier-2-r').value),
      tier_3_min_r: Number(document.getElementById('dynamic-profit-tier-3-r').value),
      tier_4_min_r: Number(document.getElementById('dynamic-profit-tier-4-r').value),
      tier_2_drawdown_ratio: Number(document.getElementById('dynamic-profit-tier-2-drawdown').value) / 100,
      tier_3_drawdown_ratio: Number(document.getElementById('dynamic-profit-tier-3-drawdown').value) / 100,
      tier_4_drawdown_ratio: Number(document.getElementById('dynamic-profit-tier-4-drawdown').value) / 100,
    };
    button.disabled = true;
    message.textContent = '正在保存动态利润保护配置…';
    message.classList.remove('error');
    try {
      const response = await fetch('/api/dynamic-profit-protection-settings', {
        method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload),
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
      message.textContent = `配置已保存并同时应用于模拟盘和实盘：保护${result.enabled ? '开启' : '关闭'}；(${result.tier_2_min_r}R, ${result.tier_3_min_r}R] 回撤≥${result.tier_2_drawdown_ratio * 100}%，(${result.tier_3_min_r}R, ${result.tier_4_min_r}R] 回撤≥${result.tier_3_drawdown_ratio * 100}%，${result.tier_4_min_r}R以上回撤≥${result.tier_4_drawdown_ratio * 100}%。`;
    } catch (err) {
      message.textContent = `动态利润保护配置保存失败：${err.message || err}`;
      message.classList.add('error');
    } finally { button.disabled = false; }
  });
})();

(function initMarketFilterSettingsForm() {
  const form = document.getElementById('market-filter-settings-form');
  const message = document.getElementById('market-filter-settings-message');
  if (!form || !message) return;
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const button = form.querySelector('button[type="submit"]');
    const payload = {
      btc_siphon_threshold: Number(document.getElementById('market-filter-btc-threshold').value) / 100,
      market_crash_threshold: Number(document.getElementById('market-filter-crash-threshold').value) / 100,
      allusdt_24h_drop_threshold: Number(document.getElementById('market-filter-allusdt-24h-threshold').value) / 100,
      block_duration_minutes: Number(document.getElementById('market-filter-block-minutes').value),
    };
    button.disabled = true;
    message.textContent = '正在保存独立市场过滤配置...';
    message.classList.remove('error');
    try {
      const response = await fetch('/api/market-filter-settings', {
        method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload),
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
      message.textContent = `配置已保存：BTC吸血阈值 ${result.btc_siphon_threshold * 100}%，大盘暴跌阈值 ${result.market_crash_threshold * 100}%，ALLUSDT 最近24h涨跌幅低于 ${result.allusdt_24h_drop_threshold * 100}% 时触发，触发后禁止新开仓 ${result.block_duration_minutes} 分钟。`;
    } catch (err) {
      message.textContent = `独立市场过滤配置保存失败：${err.message || err}`;
      message.classList.add('error');
    } finally { button.disabled = false; }
  });
})();

(function initWeakMarketProfitForm() {
  const form = document.getElementById('weak-market-profit-form');
  const message = document.getElementById('weak-market-profit-message');
  if (!form || !message) return;
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const button = form.querySelector('button[type="submit"]');
    button.disabled = true;
    message.textContent = '正在保存弱势市场止盈配置...';
    message.classList.remove('error');
    try {
      const response = await fetch('/api/weak-market-profit-settings', {
        method: 'PUT',
        headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
        body: JSON.stringify({
          trigger_r_multiple: Number(document.getElementById('weak-market-trigger-r').value),
          take_profit_fraction: Number(document.getElementById('weak-market-take-profit-percent').value) / 100,
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      message.textContent = `配置已保存：未变现盈亏 ≥ ${payload.trigger_r_multiple}R 时卖出当前仓位 ${(payload.take_profit_fraction * 100).toFixed(1)}%。`;
    } catch (err) {
      message.textContent = `弱势市场止盈配置保存失败：${err.message || err}`;
      message.classList.add('error');
    } finally {
      button.disabled = false;
    }
  });
})();

(function initScoreWeightForm() {
  const form = document.getElementById('score-weight-form');
  const message = document.getElementById('score-weight-message');
  const saveButton = document.getElementById('save-score-weights');
  const inputs = Array.from(document.querySelectorAll('.score-weight-input'));
  if (!form || !message) return;
  const showMessage = (text, isError = false) => {
    message.textContent = text;
    message.classList.toggle('error', isError);
  };
  document.getElementById('reset-score-weights')?.addEventListener('click', () => {
    inputs.forEach(input => { input.value = input.dataset.defaultWeight; });
    showMessage('已填入文件默认值，请点击“保存全部权重”确认生效。');
  });
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const rules = inputs.map(input => ({
      rule_id: Number(input.dataset.ruleId),
      weight: Number(input.value),
    }));
    saveButton.disabled = true;
    showMessage('正在校验并保存全部评分权重...');
    try {
      const response = await fetch('/api/scoring-rule-weights', {
        method: 'PUT',
        headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
        body: JSON.stringify({ rules }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      payload.rules.forEach(rule => {
        const input = inputs.find(item => Number(item.dataset.ruleId) === rule.rule_id);
        if (input) input.value = rule.weight;
      });
      showMessage('18 条评分规则权重已保存，将从下一轮评分开始生效。');
    } catch (err) {
      showMessage(`评分权重保存失败：${err.message || err}`, true);
    } finally {
      saveButton.disabled = false;
    }
  });
})();

(function initScoreElectionForm() {
  const form = document.getElementById('score-election-form');
  const message = document.getElementById('score-election-message');
  const saveButton = document.getElementById('save-score-election');
  if (!form) return;
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const configurations = Array.from(form.querySelectorAll('.score-election-config')).map(container => {
      const rules = Array.from(container.querySelectorAll('.score-election-select')).map(select => ({
        rule_id: Number(select.dataset.ruleId), status: select.value,
      }));
      return {key: container.dataset.configKey,
        enabled: container.querySelector('.score-election-enabled').checked,
        optional_min: Number(container.querySelector('.score-election-optional-min').value), rules};
    });
    for (const config of configurations) {
      const optionalCount = config.rules.filter(rule => rule.status === 'optional').length;
      if (config.optional_min > optionalCount) {
        message.textContent = `配置 ${config.key} 的 N 不能大于当前可选规则数量 ${optionalCount}。`;
        message.classList.add('error'); return;
      }
    }
    if (!configurations.some(config => config.enabled)) {
      message.textContent = '请至少启用一套配置。'; message.classList.add('error'); return;
    }
    const combination_mode = document.getElementById('score-election-combination-mode').value;
    const automation = {
      enabled: document.getElementById('score-election-auto-enabled').checked,
      threshold_percent: Number(document.getElementById('score-election-auto-threshold').value),
      config_key: document.getElementById('score-election-auto-config').value,
    };
    saveButton.disabled = true;
    message.textContent = '正在保存评分规则选举…'; message.classList.remove('error');
    try {
      const response = await fetch('/api/scoring-rule-election', {
        method: 'PUT', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({configurations, combination_mode, automation}),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      message.textContent = '评分规则选举已保存，将从下一轮可开仓检查开始生效。';
    } catch (err) {
      message.textContent = `评分规则选举保存失败：${err.message || err}`; message.classList.add('error');
    } finally { saveButton.disabled = false; }
  });
})();

(function initOpenableSettingsForm() {
  const form = document.getElementById('openable-settings-form');
  const message = document.getElementById('openable-settings-message');
  if (!form) return;
  form.addEventListener('submit', async event => {
    event.preventDefault(); if (!form.reportValidity()) return;
    const tier_min_percent = Number(form.querySelector('#tier-min-percent-input').value);
    const tier_max_percent = Object.fromEntries(Array.from(form.querySelectorAll('.tier-limit-input')).map(input => [input.dataset.tier, Number(input.value)]));
    const bands = Array.from(form.querySelectorAll('.mapping-band-row')).map(row => ({
      label: row.dataset.label,
      lower: Number(row.querySelector('[data-field="lower"]').value), upper: Number(row.querySelector('[data-field="upper"]').value),
      distance_threshold_percent: Number(row.querySelector('[data-field="distance_threshold_percent"]').value),
      leverages: Object.fromEntries(Array.from(row.querySelectorAll('[data-leverage]')).map(input => [input.dataset.leverage, Number(input.value)])),
    }));
    message.textContent = '正在保存配置…'; message.classList.remove('error');
    try {
      const response = await fetch('/api/openable-symbol-settings', {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({tier_min_percent, tier_max_percent, bands})});
      const payload = await response.json(); if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      message.textContent = '止损距离档位与总分映射已保存，将从下一轮可开仓评估开始生效。';
    } catch (err) { message.textContent = `配置保存失败：${err.message || err}`; message.classList.add('error'); }
  });
})();
