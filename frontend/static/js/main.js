/**
 * StatsBomb Viz — Dashboard JavaScript
 * Fetches competitions/matches from API, loads all charts in parallel.
 */

// Use relative path so the page works when served by uvicorn at http://127.0.0.1:8000/
// Falls back to absolute localhost URL if opened directly as file://
const API = (location.protocol === "file:")
  ? "http://127.0.0.1:8000/api/v1"
  : "/api/v1";

// ── DOM references ────────────────────────────────────────────────────────────
const compSelect  = document.getElementById("comp-select");
const matchSelect = document.getElementById("match-select");
const loadBtn     = document.getElementById("load-btn");
const scoreBanner = document.getElementById("score-banner");
const chartGrid   = document.getElementById("chart-grid");

// ── State ─────────────────────────────────────────────────────────────────────
let matchMeta = null; // current match summary

// ── Init ──────────────────────────────────────────────────────────────────────
(async () => {
  await loadCompetitions();
})();

// ── Competitions ──────────────────────────────────────────────────────────────
async function loadCompetitions() {
  try {
    const res  = await fetch(`${API}/competitions`);
    const data = await res.json();

    compSelect.innerHTML = '<option value="">— Select competition —</option>';
    data.forEach(c => {
      const opt = document.createElement("option");
      opt.value = JSON.stringify({ competition_id: c.competition_id, season_id: c.season_id });
      opt.textContent = `${c.competition_name} — ${c.season_name}`;
      compSelect.appendChild(opt);
    });
    compSelect.disabled = false;
  } catch (e) {
    compSelect.innerHTML = '<option>Failed to load competitions</option>';
    console.error("loadCompetitions error:", e);
  }
}

compSelect.addEventListener("change", async () => {
  matchSelect.innerHTML = '<option value="">— Select match —</option>';
  matchSelect.disabled = true;
  loadBtn.disabled = true;
  scoreBanner.style.display = "none";
  chartGrid.innerHTML = "";

  const raw = compSelect.value;
  if (!raw) return;

  const { competition_id, season_id } = JSON.parse(raw);
  await loadMatches(competition_id, season_id);
});

async function loadMatches(competition_id, season_id) {
  try {
    const res  = await fetch(`${API}/competitions/${competition_id}/${season_id}/matches`);
    const data = await res.json();

    matchSelect.innerHTML = '<option value="">— Select match —</option>';
    data.forEach(m => {
      const opt = document.createElement("option");
      opt.value = m.match_id;
      opt.textContent =
        `${m.match_date}  ${m.home_team} ${m.home_score}–${m.away_score} ${m.away_team}  [${m.stage}]`;
      matchSelect.appendChild(opt);
    });
    matchSelect.disabled = false;
  } catch (e) {
    matchSelect.innerHTML = '<option>Failed to load matches</option>';
    console.error("loadMatches error:", e);
  }
}

matchSelect.addEventListener("change", () => {
  loadBtn.disabled = !matchSelect.value;
});

// ── Load dashboard ────────────────────────────────────────────────────────────
loadBtn.addEventListener("click", async () => {
  const matchId = matchSelect.value;
  if (!matchId) return;

  loadBtn.disabled = true;
  loadBtn.textContent = "Loading…";
  scoreBanner.style.display = "none";
  chartGrid.innerHTML = "";

  try {
    // Fetch match summary (teams + score)
    const res  = await fetch(`${API}/matches/${matchId}/summary`);
    matchMeta  = await res.json();

    renderScoreBanner(matchMeta);
    renderChartGrid(matchId, matchMeta);
    loadAllCharts(matchId, matchMeta);
  } catch (e) {
    chartGrid.innerHTML = `<p class="error-msg">Failed to load match data: ${e.message}</p>`;
    console.error("load dashboard error:", e);
  } finally {
    loadBtn.disabled = false;
    loadBtn.textContent = "Load Dashboard";
  }
});

// ── Score banner ──────────────────────────────────────────────────────────────
function renderScoreBanner(meta) {
  scoreBanner.innerHTML = `
    <div>
      <span class="home">${meta.home_team}</span>
      <span class="score">${meta.home_score} – ${meta.away_score}</span>
      <span class="away">${meta.away_team}</span>
    </div>
    <div class="meta">${meta.match_date} · ${meta.stadium} · ${meta.stage}</div>
  `;
  scoreBanner.style.display = "block";
}

// ── Chart grid skeleton ───────────────────────────────────────────────────────
function renderChartGrid(matchId, meta) {
  const teams = meta.teams || [];
  const homeTeam = teams.find(t => t.team_name === meta.home_team) || teams[0];
  const awayTeam = teams.find(t => t.team_name === meta.away_team) || teams[1];

  const cards = [
    { id: "formation-home",  title: `${meta.home_team} Formation`,      pill: "home",  endpoint: `charts/${matchId}/formation/${homeTeam?.team_id}` },
    { id: "formation-away",  title: `${meta.away_team} Formation`,      pill: "away",  endpoint: `charts/${matchId}/formation/${awayTeam?.team_id}` },
    { id: "passnet-home",    title: `${meta.home_team} Pass Network`,   pill: "home",  endpoint: `charts/${matchId}/pass_network/${homeTeam?.team_id}` },
    { id: "passnet-away",    title: `${meta.away_team} Pass Network`,   pill: "away",  endpoint: `charts/${matchId}/pass_network/${awayTeam?.team_id}` },
    { id: "shot-map",        title: "Shot Map",                          pill: "both",  endpoint: `charts/${matchId}/shot_map`, fullWidth: true },
    { id: "xg-timeline",     title: "xG Timeline",                      pill: "both",  endpoint: `charts/${matchId}/xg_timeline`, fullWidth: true },
    { id: "pressure-home",   title: `${meta.home_team} Pressure`,       pill: "home",  endpoint: `charts/${matchId}/pressure_heatmap/${homeTeam?.team_id}` },
    { id: "pressure-away",   title: `${meta.away_team} Pressure`,       pill: "away",  endpoint: `charts/${matchId}/pressure_heatmap/${awayTeam?.team_id}` },
  ];

  chartGrid.innerHTML = "";
  cards.forEach(c => {
    const card = document.createElement("div");
    card.className = `chart-card${c.fullWidth ? " full-width" : ""}`;
    card.id = `card-${c.id}`;
    card.innerHTML = `
      <div class="card-header">
        <span class="card-title">${c.title}</span>
        <span class="team-pill ${c.pill}-pill">${
          c.pill === "home" ? meta.home_team :
          c.pill === "away" ? meta.away_team : "Both Teams"
        }</span>
      </div>
      <div class="card-body" id="body-${c.id}">
        <div class="placeholder">
          <div class="spinner"></div>
          <p>Generating chart…</p>
        </div>
      </div>
    `;
    chartGrid.appendChild(card);
    // store endpoint for later
    card.dataset.endpoint = c.endpoint;
  });

  return cards;
}

// ── Parallel chart loading ────────────────────────────────────────────────────
function loadAllCharts(matchId, meta) {
  const cards = chartGrid.querySelectorAll(".chart-card");
  const promises = Array.from(cards).map(card => {
    const cardId  = card.id.replace("card-", "");
    const endpoint = card.dataset.endpoint;
    if (!endpoint || endpoint.includes("undefined")) {
      setCardError(cardId, "Team data unavailable");
      return Promise.resolve();
    }
    return fetchChart(cardId, endpoint);
  });
  Promise.all(promises).then(() => console.log("All charts loaded."));
}

async function fetchChart(cardId, endpoint) {
  try {
    const res  = await fetch(`${API}/${endpoint}`);
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || res.statusText);
    }
    const data = await res.json();
    setCardImage(cardId, data.image);
  } catch (e) {
    setCardError(cardId, e.message);
    console.error(`fetchChart [${endpoint}] error:`, e);
  }
}

function setCardImage(cardId, base64) {
  const body = document.getElementById(`body-${cardId}`);
  if (!body) return;
  body.innerHTML = `<img src="data:image/png;base64,${base64}" alt="chart" loading="lazy" />`;
}

function setCardError(cardId, msg) {
  const body = document.getElementById(`body-${cardId}`);
  if (!body) return;
  body.innerHTML = `<div class="error-msg">⚠ ${msg}</div>`;
}
