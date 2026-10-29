const $ = (id) => document.getElementById(id);
const TICKS = 100;
const MAX_TRADES = 40;

let socket = null;
let reconnectTimer = null;
let maxBookQty = 1;

const price = (ticks) => (ticks / TICKS).toFixed(2);
const qty = (value) => value.toLocaleString("en-US");

function setConnection(connected, text) {
  $("connection").className = `dot ${connected ? "good" : "bad"}`;
  $("connection-text").textContent = text;
}

function connect() {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  socket = new WebSocket(`${scheme}://${location.host}/ws`);
  socket.onopen = () => setConnection(true, "live");
  socket.onclose = () => {
    setConnection(false, "reconnecting");
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connect, 1000);
  };
  socket.onerror = () => socket.close();
  socket.onmessage = (event) => {
    let state;
    try {
      state = JSON.parse(event.data);
    } catch {
      return;
    }
    render(state);
  };
}

function levelRow(level, side) {
  const width = Math.max(2, Math.round((level.qty / maxBookQty) * 100));
  return `
    <div class="row ${side}">
      <span class="bar" style="width:${width}%"></span>
      <span class="price">${price(level.price)}</span>
      <span class="qty">${qty(level.qty)}</span>
    </div>`;
}

function render(state) {
  $("md_seq").textContent = `md_seq ${state.md_seq}`;
  const sync = $("sync");
  sync.textContent = state.synced ? "synced" : "syncing";
  sync.className = `pill ${state.synced ? "good" : "bad"}`;

  maxBookQty = Math.max(
    1,
    ...state.bids.map((l) => l.qty),
    ...state.asks.map((l) => l.qty),
  );

  // The view lists both sides best first. The ladder shows the worst ask at
  // the top and the best ask next to the spread; bids descend from the best.
  const asks = state.asks.slice().reverse();
  $("asks").innerHTML = asks.map((l) => levelRow(l, "ask")).join("") ||
    '<div class="empty">no asks</div>';
  $("bids").innerHTML = state.bids.map((l) => levelRow(l, "bid")).join("") ||
    '<div class="empty">no bids</div>';

  const bestBid = state.bids.length ? state.bids[0].price : null;
  const bestAsk = state.asks.length ? state.asks[0].price : null;
  $("best").textContent =
    bestBid === null && bestAsk === null
      ? "— / —"
      : `${bestBid === null ? "—" : price(bestBid)} / ${bestAsk === null ? "—" : price(bestAsk)}`;
  $("spread").textContent =
    bestBid !== null && bestAsk !== null ? price(bestAsk - bestBid) : "—";

  $("trades_total").textContent = `${state.trades_total} total`;
  const trades = state.trades.slice(0, MAX_TRADES);
  $("trades").innerHTML = trades
    .map(
      (trade) => `
      <li class="trade ${trade.aggressor}">
        <span class="seq">${trade.md_seq}</span>
        <span class="price">${price(trade.price)}</span>
        <span class="qty">${qty(trade.qty)}</span>
        <span class="side">${trade.aggressor}</span>
      </li>`,
    )
    .join("") || '<li class="empty">no trades yet</li>';
}

async function refreshLatency() {
  try {
    const response = await fetch("/api/latency", { cache: "no-store" });
    const latency = await response.json();
    renderLatency(latency);
  } catch {
    // The next interval tries again.
  }
}

function renderLatency(latency) {
  if (!latency.available) {
    $("latency-note").textContent = "run deploy/bench.sh to fill this panel";
    drawChart([]);
    return;
  }
  $("p50").textContent = `${latency.p50_us.toFixed(0)} µs`;
  $("p90").textContent = `${latency.p90_us.toFixed(0)} µs`;
  $("p99").textContent = `${latency.p99_us.toFixed(0)} µs`;
  $("rate").textContent = `${Math.round(latency.rate_per_s).toLocaleString("en-US")}/s`;
  $("completed").textContent = qty(latency.completed);
  $("latency-note").textContent =
    "client-side RTT, queue → first report; the latest benchmark run";
  drawChart([
    { label: "p50", value: latency.p50_us },
    { label: "p90", value: latency.p90_us },
    { label: "p99", value: latency.p99_us },
  ]);
}

// Set when a kill was refused; a polled status must not hide the reason.
let controlError = null;

function describeControl(control) {
  const parts = [];
  if (!control.connected) {
    parts.push("cluster not connected (a kill waits in the queue)");
  }
  if (control.kill_all) {
    parts.push("killed: all accounts");
  }
  if (control.killed_accounts.length > 0) {
    parts.push(`killed: account ${control.killed_accounts.join(", ")}`);
  }
  if (control.pending > 0) {
    parts.push(`${control.pending} queued`);
  }
  return parts.length === 0 ? "connected · no kill submitted" : parts.join(" · ");
}

async function refreshControl() {
  try {
    const response = await fetch("/api/control", { cache: "no-store" });
    if (!response.ok) {
      return;
    }
    const control = await response.json();
    $("kill-status").textContent =
      controlError === null ? describeControl(control) : controlError;
  } catch {
    // The next interval tries again.
  }
}

async function kill(account) {
  try {
    const response = await fetch("/api/kill", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(account === null ? {} : { account }),
    });
    const result = await response.json().catch(() => ({}));
    if (response.ok) {
      controlError = null;
      $("kill-status").textContent =
        account === null ? "queued: all accounts" : `queued: account ${account}`;
      refreshControl();
    } else {
      controlError = result.error || `refused (HTTP ${response.status})`;
      $("kill-status").textContent = controlError;
    }
  } catch (error) {
    controlError = `the admin is unreachable: ${error.message}`;
    $("kill-status").textContent = controlError;
  }
}

$("kill-all").addEventListener("click", () => kill(null));
$("kill-one").addEventListener("click", () => {
  const value = $("kill-account").value.trim();
  if (value === "") {
    controlError = "enter an account id";
    $("kill-status").textContent = controlError;
    return;
  }
  kill(Number(value));
});

function drawChart(bars) {
  const canvas = $("chart");
  const context = canvas.getContext("2d");
  const { width, height } = canvas;
  context.clearRect(0, 0, width, height);

  context.strokeStyle = "#1c2330";
  context.lineWidth = 1;
  for (let line = 1; line <= 3; line += 1) {
    const y = (height / 4) * line;
    context.beginPath();
    context.moveTo(0, y);
    context.lineTo(width, y);
    context.stroke();
  }
  if (bars.length === 0) {
    return;
  }

  const max = Math.max(...bars.map((bar) => bar.value), 1);
  const slot = width / bars.length;
  const barWidth = Math.min(120, slot * 0.5);
  const colors = ["#4ade80", "#fbbf24", "#ff6b6b"];
  bars.forEach((bar, index) => {
    const barHeight = (bar.value / max) * (height - 28);
    const x = slot * index + (slot - barWidth) / 2;
    const y = height - 22 - barHeight;
    context.fillStyle = colors[index % colors.length];
    context.fillRect(x, y, barWidth, barHeight);
    context.fillStyle = "#d7dce5";
    context.font = "12px ui-monospace, monospace";
    context.textAlign = "center";
    context.fillText(`${bar.label} ${bar.value.toFixed(0)}µs`, x + barWidth / 2, height - 6);
  });
}

connect();
refreshLatency();
refreshControl();
setInterval(refreshLatency, 2000);
setInterval(refreshControl, 2000);

