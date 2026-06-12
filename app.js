const stageContent = {
  profile: {
    number: "01",
    file: "profiling.py",
    title: "Know the data before changing it.",
    description: "Profiling establishes a measurable baseline for the source dataset and exposes structural or content problems before transformation.",
    operations: ["Print inferred Spark schema", "Count null and empty values", "Find duplicate business keys", "Measure distinct value frequencies"],
    stats: [["22,330", "missing pack sizes found"], ["260", "duplicate business rows"]]
  },
  clean: {
    number: "02",
    file: "cleaning.py",
    title: "Create a consistent analytical foundation.",
    description: "Cleaning standardizes naming, null behavior, types, and duplicate handling before any business features are calculated.",
    operations: ["Convert headers to snake_case", "Trim and normalize strings", "Cast price, size, IDs, and booleans", "Keep one row per business key"],
    stats: [["253,713", "rows after deduplication"], ["15", "source fields normalized"]]
  },
  parse: {
    number: "03",
    file: "medicine_processing.py",
    title: "Turn medicine text into structured fields.",
    description: "Regular expressions and Spark expressions extract clean product names, strengths, units, dosage forms, and combination status.",
    operations: ["Parse numeric strength", "Normalize mg, mcg, ml, g, and percent", "Standardize dosage forms", "Identify multi-ingredient products"],
    stats: [["1,586", "distinct primary ingredients"], ["15", "raw dosage forms"]]
  },
  price: {
    number: "04",
    file: "price_transforms.py",
    title: "Make price comparable and contextual.",
    description: "Raw prices become unit prices, relative measures, and data-driven price bands that support product and manufacturer analysis.",
    operations: ["Calculate price per package unit", "Build approximate tertile bands", "Compare with manufacturer average", "Compare with category average"],
    stats: [["3", "price bands"], ["0-safe", "division with try_divide"]]
  },
  window: {
    number: "05",
    file: "window_functions.py",
    title: "Analyze products inside their business groups.",
    description: "Window functions preserve row-level detail while adding rankings, category boundaries, running totals, and previous-row comparisons.",
    operations: ["Rank within manufacturer", "Find category min and max", "Calculate running totals", "Compare with previous product price"],
    stats: [["7,648", "manufacturer partitions"], ["5", "window-derived features"]]
  },
  quality: {
    number: "06",
    file: "quality_checks.py",
    title: "Convert assumptions into visible quality rules.",
    description: "Each record receives one deterministic quality flag, making issues easy to count, inspect, and filter downstream.",
    operations: ["Detect missing product names", "Flag non-positive or extreme prices", "Find missing oral-solid strengths", "Detect missing strength units"],
    stats: [["251,495", "healthy records"], ["2,218", "records requiring review"]]
  },
  publish: {
    number: "07",
    file: "pipeline.py",
    title: "Publish a stable, curated contract.",
    description: "The pipeline selects an explicit final schema and writes either one local CSV or distributed Spark output for AWS Glue.",
    operations: ["Select 17 final columns", "Write local pandas CSV", "Write distributed Glue output", "Print analytical summaries"],
    stats: [["17", "curated columns"], ["31.5 MB", "verified local CSV"]]
  }
};

const stageDetail = document.querySelector("#stage-detail");
const stageButtons = document.querySelectorAll(".stage-button");

function renderStage(key) {
  const stage = stageContent[key];
  stageDetail.innerHTML = `
    <div class="detail-number">${stage.number}</div>
    <span class="detail-kicker">${stage.file}</span>
    <h3>${stage.title}</h3>
    <p>${stage.description}</p>
    <div class="detail-grid">
      <div>
        <span>Operations</span>
        <ul>${stage.operations.map(item => `<li>${item}</li>`).join("")}</ul>
      </div>
      <div>
        <span>Key output</span>
        ${stage.stats.map(([value, label]) => `<div class="detail-stat"><strong>${value}</strong><small>${label}</small></div>`).join("")}
      </div>
    </div>`;
}

stageButtons.forEach(button => {
  button.addEventListener("click", () => {
    stageButtons.forEach(item => {
      item.classList.remove("active");
      item.setAttribute("aria-selected", "false");
    });
    button.classList.add("active");
    button.setAttribute("aria-selected", "true");
    renderStage(button.dataset.stage);
  });
});

const revealObserver = new IntersectionObserver(entries => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add("visible");
      revealObserver.unobserve(entry.target);
    }
  });
}, { threshold: 0.12 });

document.querySelectorAll(".reveal").forEach(element => revealObserver.observe(element));

const themeToggle = document.querySelector("#theme-toggle");
const storedTheme = localStorage.getItem("pharmaflow-theme");
if (storedTheme) document.documentElement.dataset.theme = storedTheme;

themeToggle.addEventListener("click", () => {
  const nextTheme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = nextTheme;
  localStorage.setItem("pharmaflow-theme", nextTheme);
});

document.querySelectorAll("[data-copy]").forEach(button => {
  button.addEventListener("click", async () => {
    await navigator.clipboard.writeText(button.dataset.copy);
    const original = button.textContent;
    button.textContent = "Copied";
    window.setTimeout(() => { button.textContent = original; }, 1200);
  });
});
