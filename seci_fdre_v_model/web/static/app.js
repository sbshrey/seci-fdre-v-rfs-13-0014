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

function updateJobShell(job) {
  const shell = document.querySelector("[data-job-shell]");
  const stageNode = document.querySelector("[data-progress-stage]");
  const pctNode = document.querySelector("[data-progress-pct]");
  const fillNode = document.querySelector("[data-progress-fill]");
  const detailNode = document.querySelector("[data-progress-detail]");
  const runNode = document.querySelector("[data-progress-run-id]");
  const statusNode = document.querySelector("[data-progress-status]");
  const openLink = document.querySelector("[data-progress-open]");
  const cancelForm = document.querySelector("[data-job-cancel-form]");
  const deleteForm = document.querySelector("[data-job-delete-form]");
  if (!shell || !stageNode || !pctNode || !fillNode || !detailNode || !runNode || !statusNode) return;

  if (!job) {
    shell.hidden = true;
    updateStartButtons(false);
    return;
  }

  const pct = Math.max(0, Math.min(100, Number(job.pct || 0)));
  shell.hidden = false;
  stageNode.textContent = job.stage || "Study";
  pctNode.textContent = `${pct.toFixed(0)}%`;
  fillNode.style.width = `${pct}%`;
  detailNode.textContent = job.detail || "";
  runNode.textContent = job.run_id || "No active run";
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

  updateStartButtons(Boolean(job.is_active));
}

async function pollJobStatus() {
  const shell = document.querySelector("[data-job-shell]");
  if (!shell) return;
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
