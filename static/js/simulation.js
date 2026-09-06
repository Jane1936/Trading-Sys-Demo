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
    buttons.forEach(button => button.addEventListener('click', () =>
      activate(button, buttons, panels, 'simulationTab')));
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

  initSimulationTabs();
  initHoldingModuleTabs();
  initCollapsibles();
})();
