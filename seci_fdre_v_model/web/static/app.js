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

async function handleStudySubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const shell = document.querySelector("[data-progress-shell]");
  const stageNode = document.querySelector("[data-progress-stage]");
  const pctNode = document.querySelector("[data-progress-pct]");
  const fillNode = document.querySelector("[data-progress-fill]");
  const detailNode = document.querySelector("[data-progress-detail]");
  if (!shell || !stageNode || !pctNode || !fillNode || !detailNode) return;

  shell.hidden = false;
  stageNode.textContent = "Preparing run…";
  pctNode.textContent = "0%";
  fillNode.style.width = "0%";
  detailNode.textContent = "Starting background execution.";

  const response = await fetch(form.action, { method: "POST" });
  if (!response.ok || !response.body) {
    detailNode.textContent = "Failed to start the study.";
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";
    for (const line of lines) {
      if (!line.trim()) continue;
      const payload = JSON.parse(line);
      if (payload.error) {
        stageNode.textContent = "Run failed";
        detailNode.textContent = payload.error;
        fillNode.style.width = "100%";
        pctNode.textContent = "Failed";
        return;
      }
      if (payload.done && payload.redirect) {
        stageNode.textContent = "Completed";
        detailNode.textContent = "Opening finished run.";
        fillNode.style.width = "100%";
        pctNode.textContent = "100%";
        window.location.assign(payload.redirect);
        return;
      }
      if (payload.stage) {
        stageNode.textContent = payload.stage;
        detailNode.textContent = payload.detail || "";
        const pct = Math.max(0, Math.min(100, Number(payload.pct || 0)));
        pctNode.textContent = `${pct.toFixed(0)}%`;
        fillNode.style.width = `${pct}%`;
      }
    }
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

document.querySelectorAll("[data-study-form]").forEach((form) => {
  form.addEventListener("submit", handleStudySubmit);
});

document.querySelectorAll("[data-chart-dataset]").forEach((button) => {
  button.addEventListener("click", handleChartDatasetClick);
});

setupProfileModeControls();
