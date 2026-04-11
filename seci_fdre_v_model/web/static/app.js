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
      (card, index) => `
        <article
          class="chart-card chart-card-expandable"
          data-chart-index="${index}"
          role="button"
          tabindex="0"
          title="Click to enlarge">
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

function getChartModalElements() {
  const root = document.querySelector("[data-chart-modal]");
  if (!root) return null;
  return {
    root,
    title: root.querySelector("[data-chart-modal-title]"),
    subtitle: root.querySelector("[data-chart-modal-subtitle]"),
    body: root.querySelector("[data-chart-modal-body]"),
  };
}

function closeChartModal() {
  const els = getChartModalElements();
  if (!els) return;
  els.root.hidden = true;
  els.body.innerHTML = "";
  document.body.classList.remove("chart-modal-open");
}

function openChartModal(payload) {
  const els = getChartModalElements();
  if (!els) return;
  els.title.textContent = payload.title || "";
  els.subtitle.textContent = payload.subtitle || "";
  els.body.innerHTML = payload.svg || "";
  els.root.hidden = false;
  document.body.classList.add("chart-modal-open");
  els.root.querySelector(".chart-modal-close")?.focus();
}

async function expandChartCard(runId, dataset, index) {
  const url = `/api/charts/${encodeURIComponent(runId)}/${encodeURIComponent(dataset)}?expanded=1&index=${index}`;
  const response = await fetch(url, { headers: { Accept: "application/json" }, cache: "no-store" });
  if (!response.ok) return;
  const payload = await response.json();
  if (!payload || typeof payload.svg !== "string") return;
  openChartModal(payload);
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

    if (!job.is_active) {
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

let jobPollIntervalId = null;

function stopJobPolling() {
  if (jobPollIntervalId !== null) {
    window.clearInterval(jobPollIntervalId);
    jobPollIntervalId = null;
  }
}

function jobPollingTargetsPresent() {
  return Boolean(document.querySelector("[data-job-shell]") || document.querySelector("[data-dashboard-job-card]"));
}

/** True when server rendered a visible job shell or dashboard job card (active or queued run). */
function serverHintsAtVisibleJob() {
  const shell = document.querySelector("[data-job-shell]");
  const dashboardCard = document.querySelector("[data-dashboard-job-card]");
  const shellVisible = shell && !shell.hasAttribute("hidden");
  const dashboardVisible = dashboardCard && !dashboardCard.hasAttribute("hidden");
  return Boolean(shellVisible || dashboardVisible);
}

async function pollJobStatus() {
  if (!jobPollingTargetsPresent()) {
    stopJobPolling();
    return;
  }
  try {
    const response = await fetch("/api/job-status", {
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    if (!response.ok) return;
    const payload = await response.json();
    updateJobShell(payload.job);
    const active = Boolean(payload.job && payload.job.is_active);
    if (active) {
      if (jobPollIntervalId === null) {
        jobPollIntervalId = window.setInterval(pollJobStatus, 10000);
      }
    } else {
      stopJobPolling();
    }
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
  const chartGrid = document.querySelector("[data-chart-grid]");
  if (chartGrid) {
    chartGrid.dataset.runId = runId;
    chartGrid.dataset.chartDataset = dataset;
  }
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

/**
 * On the Config page, mirror the selected study profile (workspace vs ideal) into the
 * project form so users see the parameters that will be snapshotted for Run study.
 */
function setupConfigStudyProfileFormMirror() {
  const form = document.querySelector("[data-config-form]");
  const studySelect = document.querySelector('.run-study-form select[name="study_profile"]');
  if (!(form instanceof HTMLFormElement) || !(studySelect instanceof HTMLSelectElement)) {
    return Promise.resolve();
  }

  const hint = document.querySelector("[data-study-profile-form-hint]");
  const saveButton = form.querySelector("[data-config-save-button]");

  async function applyPreview(profile) {
    try {
      const response = await fetch(
        `/api/config-form-preview?study_profile=${encodeURIComponent(profile)}`,
        {
          headers: { Accept: "application/json" },
          cache: "no-store",
        }
      );
      if (!response.ok) return;
      const data = await response.json();
      const fields = data.fields && typeof data.fields === "object" ? data.fields : {};

      for (const [name, raw] of Object.entries(fields)) {
        const target = form.querySelector(`[name="${name}"]`);
        if (!(target instanceof HTMLElement)) continue;
        if (target instanceof HTMLInputElement && target.type === "checkbox") {
          target.checked = Boolean(raw);
        } else if (target instanceof HTMLSelectElement) {
          target.value = raw === null || raw === undefined ? "" : String(raw);
        } else if (target instanceof HTMLTextAreaElement || target instanceof HTMLInputElement) {
          target.value = raw === null || raw === undefined ? "" : String(raw);
        }
      }

      const modeSelect = form.querySelector("[data-profile-mode-select]");
      if (modeSelect instanceof HTMLSelectElement) {
        syncProfileModeFields(modeSelect.value);
      }

      const isWorkspace = data.study_profile === "workspace";
      if (saveButton instanceof HTMLButtonElement) {
        saveButton.disabled = !isWorkspace;
      }
      if (hint instanceof HTMLElement) {
        if (isWorkspace) {
          hint.hidden = true;
          hint.textContent = "";
        } else {
          hint.hidden = false;
          hint.textContent =
            "Showing Ideal 1 MW example parameters in the form (preview). Save is disabled — choose Workspace (saved YAML) to edit what gets saved.";
        }
      }
    } catch (_err) {
      // Leave the server-rendered form as-is if preview fails.
    }
  }

  studySelect.addEventListener("change", () => {
    void applyPreview(studySelect.value);
  });

  const initial = form.dataset.initialStudyProfile || studySelect.value;
  if (initial !== "workspace") {
    return applyPreview(initial);
  }
  if (saveButton instanceof HTMLButtonElement) {
    saveButton.disabled = false;
  }
  if (hint instanceof HTMLElement) {
    hint.hidden = true;
    hint.textContent = "";
  }
  return Promise.resolve();
}

async function bootUi() {
  await setupConfigStudyProfileFormMirror();
  setupProfileModeControls();
  setupAlignedEnergyReport();
  if (serverHintsAtVisibleJob()) {
    pollJobStatus();
  }
}

document.querySelectorAll("[data-chart-dataset]").forEach((button) => {
  button.addEventListener("click", handleChartDatasetClick);
});

document.addEventListener("click", (event) => {
  const closeEl = event.target.closest("[data-chart-modal-close]");
  if (closeEl) {
    closeChartModal();
    return;
  }
  const card = event.target.closest(".chart-card-expandable[data-chart-index]");
  if (!card) return;
  const grid = card.closest("[data-chart-grid]");
  if (!grid) return;
  const runId = grid.dataset.runId;
  const dataset = grid.dataset.chartDataset;
  const index = Number.parseInt(card.dataset.chartIndex ?? "", 10);
  if (!runId || !dataset || Number.isNaN(index)) return;
  void expandChartCard(runId, dataset, index);
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeChartModal();
    return;
  }
  if (event.key !== "Enter" && event.key !== " ") return;
  const t = event.target;
  if (!(t instanceof HTMLElement)) return;
  const card = t.closest(".chart-card-expandable[data-chart-index]");
  if (!card) return;
  event.preventDefault();
  card.click();
});

function setupAlignedEnergyReport() {
  const root = document.querySelector("[data-aligned-report]");
  if (!root) return;
  const pre = root.querySelector("[data-aligned-report-pre]");
  const status = root.querySelector("[data-aligned-report-status]");
  const btn = root.querySelector("[data-aligned-report-refresh]");
  if (!pre || !status || !btn) return;

  async function refresh() {
    status.hidden = false;
    status.textContent = "Loading...";
    pre.hidden = true;
    try {
      const excessInput = root.querySelector("[data-alignment-excess-fraction]");
      const excessRaw = excessInput && excessInput.value !== "" ? excessInput.value : "0.08";
      const response = await fetch(
        `/api/aligned-energy-report?excess_fraction=${encodeURIComponent(excessRaw)}`,
        {
          headers: { Accept: "application/json" },
          cache: "no-store",
        }
      );
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        status.textContent = payload.error || "Request failed.";
        return;
      }
      const s = payload.summary;
      const g = payload.suggestions;
      const lines = [
        `Aligned horizon: ${Number(s.minutes).toLocaleString()} minutes`,
        "",
        "Annualised energy (kWh, sums / 60):",
        `  Solar:              ${Number(s.solar_kwh).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
        `  Wind:               ${Number(s.wind_kwh).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
        `  Solar + wind:       ${Number(s.generation_kwh).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
        `  Output profile:     ${Number(s.output_profile_kwh).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
        `  Aux:                ${Number(s.aux_kwh).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
        `  Total consumption:  ${Number(s.consumption_kwh).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
        `  Net (gen - load):   ${Number(s.net_generation_minus_load_kwh).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
        "",
        "Minute balance (before battery):",
        `  Surplus minutes: ${Number(s.surplus_minutes).toLocaleString()}`,
        `  Deficit minutes: ${Number(s.deficit_minutes).toLocaleString()}`,
        "",
        "Heuristic scales (annual kWh, not minute-optimal):",
        `  Load / generation ratio: ${Number(g.annual_load_to_generation_ratio).toPrecision(4)}`,
        `  Uniform RE scale (cap ${500}): ${Number(g.uniform_renewable_scale).toPrecision(5)}`,
        `  Profile mult scale (<=1): ${Number(g.profile_multiplier_scale).toPrecision(5)}`,
        `  Implied solar mult: ${Number(g.implied_next_solar_multiplier).toPrecision(5)}`,
        `  Implied wind mult:  ${Number(g.implied_next_wind_multiplier).toPrecision(5)}`,
        `  Implied profile mult: ${Number(g.implied_next_profile_multiplier).toPrecision(5)}`,
      ];
      if (g.notes) {
        lines.push("", String(g.notes));
      }
      pre.textContent = lines.join("\n");
      pre.hidden = false;
      status.textContent = "Loaded (pre-BESS alignment + suggestions).";
    } catch (_err) {
      status.textContent = "Could not load alignment report.";
    }
  }

  btn.addEventListener("click", () => {
    void refresh();
  });

  root.querySelectorAll("form.apply-alignment-form").forEach((form) => {
    form.addEventListener("submit", () => {
      const xf = root.querySelector("[data-alignment-excess-fraction]");
      const v = xf && xf.value !== "" ? xf.value : "0.08";
      form.querySelectorAll("input.alignment-excess-hidden").forEach((hidden) => {
        if (hidden instanceof HTMLInputElement) {
          hidden.value = v;
        }
      });
    });
  });
}

void bootUi();
