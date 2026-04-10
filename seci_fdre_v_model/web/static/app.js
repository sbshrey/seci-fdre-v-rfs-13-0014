function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderChartCards(cards) {
  const chartGrid = document.querySelector("[data-chart-grid]");
  if (!chartGrid) return;
  if (!cards.length) {
    chartGrid.innerHTML = '<p class="empty-state">No charts available for this dataset.</p>';
    return;
  }
  chartGrid.innerHTML = cards
    .map(
      (card) => `
        <article class="chart-card">
          <div class="panel-header panel-header-stack">
            <div>
              <h4>${escapeHtml(card.title)}</h4>
              <p class="section-note">${escapeHtml(card.subtitle)}</p>
            </div>
          </div>
          <div class="chart-wrap">${card.svg}</div>
        </article>
      `
    )
    .join("");
}

function updateStartButtons(isActive) {
  document.querySelectorAll("[data-start-study-button]").forEach((button) => {
    button.disabled = isActive;
    button.textContent = isActive ? "Study Running" : "Run Study";
  });
}

function updateJobCard({
  rootSelector,
  stageSelector,
  pctSelector,
  fillSelector,
  detailSelector,
  caseSelector,
  runSelector,
  statusSelector,
  openSelector,
  cancelSelector,
  deleteSelector,
}) {
  const root = document.querySelector(rootSelector);
  if (!root) return;

  const stageNode = document.querySelector(stageSelector);
  const pctNode = document.querySelector(pctSelector);
  const fillNode = document.querySelector(fillSelector);
  const detailNode = document.querySelector(detailSelector);
  const caseNode = document.querySelector(caseSelector);
  const runNode = document.querySelector(runSelector);
  const statusNode = document.querySelector(statusSelector);
  const openLink = document.querySelector(openSelector);
  const cancelForm = document.querySelector(cancelSelector);
  const deleteForm = document.querySelector(deleteSelector);

  return function applyJobState(job) {
    if (!stageNode || !pctNode || !fillNode || !detailNode || !runNode || !statusNode) return;

    if (!job) {
      root.hidden = true;
      return;
    }

    const pct = Math.max(0, Math.min(100, Number(job.pct || 0)));
    root.hidden = false;
    stageNode.textContent = job.stage || "Study";
    pctNode.textContent = `${pct.toFixed(0)}%`;
    fillNode.style.width = `${pct}%`;
    detailNode.textContent = job.detail || "";
    if (caseNode) {
      const hasCaseProgress = job.completed_cases !== null && job.completed_cases !== undefined
        && job.total_cases !== null && job.total_cases !== undefined;
      caseNode.hidden = !hasCaseProgress;
      caseNode.textContent = hasCaseProgress
        ? `${job.stage || "Stage"} • ${job.completed_cases}/${job.total_cases} cases`
        : "";
    }
    runNode.textContent = job.run_id ? `Run ${job.run_id}` : "No active run";
    statusNode.textContent = job.status || "unknown";
    statusNode.className = `status-pill status-${job.status || "failed"}`;

    if (openLink) {
      openLink.hidden = !job.run_url;
      if (job.run_url) {
        openLink.href = job.run_url;
      }
    }
    if (cancelForm) {
      cancelForm.hidden = !job.is_active;
    }
    if (deleteForm) {
      deleteForm.hidden = job.is_active || !job.delete_url;
      if (job.delete_url) {
        deleteForm.action = job.delete_url;
      }
    }
  };
}

const applyFloatingJobState = updateJobCard({
  rootSelector: "[data-job-shell]",
  stageSelector: "[data-progress-stage]",
  pctSelector: "[data-progress-pct]",
  fillSelector: "[data-progress-fill]",
  detailSelector: "[data-progress-detail]",
  caseSelector: "[data-progress-cases]",
  runSelector: "[data-progress-run-id]",
  statusSelector: "[data-progress-status]",
  openSelector: "[data-progress-open]",
  cancelSelector: "[data-job-cancel-form]",
  deleteSelector: "[data-job-delete-form]",
});

const applyDashboardJobState = updateJobCard({
  rootSelector: "[data-dashboard-job-card]",
  stageSelector: "[data-dashboard-job-stage]",
  pctSelector: "[data-dashboard-job-pct]",
  fillSelector: "[data-dashboard-job-fill]",
  detailSelector: "[data-dashboard-job-detail]",
  caseSelector: "[data-dashboard-job-cases]",
  runSelector: "[data-dashboard-job-run-id]",
  statusSelector: "[data-dashboard-job-status]",
  openSelector: "[data-dashboard-job-open]",
  cancelSelector: "[data-dashboard-job-cancel]",
  deleteSelector: "[data-dashboard-job-delete]",
});

function updateJobShell(job) {
  if (applyFloatingJobState) {
    applyFloatingJobState(job);
  }
  if (applyDashboardJobState) {
    applyDashboardJobState(job);
  }
  updateStartButtons(Boolean(job && job.is_active));
}

async function pollJobStatus() {
  const shell = document.querySelector("[data-job-shell]");
  const dashboardCard = document.querySelector("[data-dashboard-job-card]");
  if (!shell && !dashboardCard) return;
  try {
    const response = await fetch("/api/job-status", {
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    if (!response.ok) return;
    const payload = await response.json();
    updateJobShell(payload.job);
  } catch (_error) {
    // Keep the last rendered state if polling fails.
  }
}

async function handleChartDatasetClick(event) {
  const button = event.currentTarget;
  const dataset = button.dataset.chartDataset;
  const runId = button.dataset.runId;
  if (!dataset || !runId) return;

  const response = await fetch(`/api/charts/${encodeURIComponent(runId)}/${encodeURIComponent(dataset)}`);
  if (!response.ok) return;
  const cards = await response.json();
  document.querySelectorAll("[data-chart-dataset]").forEach((node) => node.classList.remove("active"));
  button.classList.add("active");
  renderChartCards(cards);
}

function syncProfileModeFields(mode) {
  const templateOnlyFields = document.querySelectorAll("[data-template-only]");
  const flatOnlyFields = document.querySelectorAll("[data-flat-only]");
  const templateLabels = document.querySelectorAll('[data-mode-field="template_only"]');
  const flatLabels = document.querySelectorAll('[data-mode-field="flat_only"]');

  const isFlat = mode === "flat";

  templateOnlyFields.forEach((field) => {
    field.disabled = isFlat;
  });
  flatOnlyFields.forEach((field) => {
    field.disabled = !isFlat;
  });

  templateLabels.forEach((label) => {
    label.classList.toggle("mode-field-disabled", isFlat);
    label.classList.toggle("mode-field-active", !isFlat);
  });
  flatLabels.forEach((label) => {
    label.classList.toggle("mode-field-disabled", !isFlat);
    label.classList.toggle("mode-field-active", isFlat);
  });
}

function setupProfileModeControls() {
  const modeSelect = document.querySelector("[data-profile-mode-select]");
  if (!modeSelect) return;

  syncProfileModeFields(modeSelect.value);
  modeSelect.addEventListener("change", () => {
    syncProfileModeFields(modeSelect.value);
  });
}

document.querySelectorAll("[data-chart-dataset]").forEach((button) => {
  button.addEventListener("click", handleChartDatasetClick);
});

setupProfileModeControls();
pollJobStatus();
window.setInterval(pollJobStatus, 2500);
