package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>EMA Pullback — Live Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background: #0f172a; color: #e2e8f0; font-family: ui-sans-serif, system-ui, sans-serif; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 0.75rem; }
    .pos-long { color: #4ade80; }
    .pos-short { color: #f87171; }
    table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
    th, td { padding: 0.5rem 0.75rem; text-align: left; border-bottom: 1px solid #334155; }
    th { color: #94a3b8; font-weight: 600; text-transform: uppercase; font-size: 0.7rem; letter-spacing: 0.05em; }
    tr:hover td { background: #33415533; }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
    .connected { background: #22c55e; box-shadow: 0 0 8px #22c55e; }
    .disconnected { background: #ef4444; }
  </style>
</head>
<body class="min-h-screen p-4 md:p-6">
  <header class="mb-6">
    <div class="flex flex-wrap items-center justify-between gap-4">
      <div>
        <h1 class="text-2xl font-bold text-white">20/50 EMA First-Candle Pullback</h1>
        <p class="text-slate-400 text-sm mt-1">Binance Futures · 15m · Paper · 5x · BTC · ETH · SOL · BNB</p>
      </div>
      <div class="flex items-center gap-2 text-sm">
        <span id="wsStatus" class="status-dot disconnected"></span>
        <span id="wsLabel" class="text-slate-400">Connecting…</span>
      </div>
    </div>
    <div id="globalMetrics" class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3 mt-6"></div>
  </header>

  <section class="card p-4 mb-6">
    <h2 class="text-lg font-semibold text-white mb-3">Active Positions</h2>
    <div class="overflow-x-auto">
      <table>
        <thead>
          <tr>
            <th>Symbol</th><th>Direction</th><th>Entry</th><th>Current</th>
            <th>Leverage</th><th>Margin</th><th>Live PnL ($)</th><th>Live ROE (%)</th>
          </tr>
        </thead>
        <tbody id="activeBody"><tr><td colspan="8" class="text-slate-500">No open positions</td></tr></tbody>
      </table>
    </div>
  </section>

  <section class="card p-4">
    <h2 class="text-lg font-semibold text-white mb-3">Historical Positions Ledger</h2>
    <div class="overflow-x-auto max-h-96 overflow-y-auto">
      <table>
        <thead class="sticky top-0 bg-slate-800">
          <tr>
            <th>Timestamp</th><th>Symbol</th><th>Direction</th><th>Entry</th><th>Exit</th>
            <th>Fees ($)</th><th>Net PnL ($)</th><th>ROE (%)</th>
          </tr>
        </thead>
        <tbody id="historyBody"><tr><td colspan="8" class="text-slate-500">No closed trades yet</td></tr></tbody>
      </table>
    </div>
  </section>

  <script>
    const fmt = (n, d = 2) => (n == null || isNaN(n)) ? '—' : Number(n).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
    const fmtUsd = (n) => {
      const s = fmt(n, 2);
      if (s === '—') return s;
      const v = Number(n);
      const cls = v > 0 ? 'text-green-400' : v < 0 ? 'text-red-400' : 'text-slate-300';
      return '<span class="' + cls + '">' + (v >= 0 ? '+' : '') + s + '</span>';
    };

    function renderGlobal(d) {
      const el = document.getElementById('globalMetrics');
      const symbols = d.symbols || [];
      let html = '';
      symbols.forEach(s => {
        html += '<div class="card p-3"><div class="text-xs text-slate-400">' + s.symbol + ' Wallet</div>' +
          '<div class="text-lg font-semibold text-white">$' + fmt(s.balance, 2) + '</div>' +
          '<div class="text-xs text-slate-500">EMA20 ' + fmt(s.ema20, 2) + ' · EMA50 ' + fmt(s.ema50, 2) + '</div></div>';
      });
      html += '<div class="card p-3 col-span-2"><div class="text-xs text-slate-400">Cumulative System PnL</div>' +
        '<div class="text-lg font-semibold">' + fmtUsd(d.globalPnL) + '</div></div>';
      html += '<div class="card p-3"><div class="text-xs text-slate-400">Total Fees Paid</div>' +
        '<div class="text-lg font-semibold text-amber-400">$' + fmt(d.globalFees, 2) + '</div></div>';
      html += '<div class="card p-3"><div class="text-xs text-slate-400">Total Equity</div>' +
        '<div class="text-lg font-semibold text-white">$' + fmt(d.globalBalance, 2) + '</div></div>';
      el.innerHTML = html;
    }

    function renderActive(positions) {
      const body = document.getElementById('activeBody');
      if (!positions || positions.length === 0) {
        body.innerHTML = '<tr><td colspan="8" class="text-slate-500">No open positions</td></tr>';
        return;
      }
      body.innerHTML = positions.map(p => {
        const dirCls = p.direction === 'LONG' ? 'pos-long' : 'pos-short';
        return '<tr><td>' + p.symbol + '</td><td class="' + dirCls + ' font-semibold">' + p.direction +
          '</td><td>' + fmt(p.entryPrice, 4) + '</td><td>' + fmt(p.currentPrice, 4) +
          '</td><td>' + fmt(p.leverage, 0) + 'x</td><td>$' + fmt(p.marginUsed, 2) +
          '</td><td>' + fmtUsd(p.livePnL) + '</td><td>' + fmtUsd(p.liveROE) + '%</td></tr>';
      }).join('');
    }

    function renderHistory(trades) {
      const body = document.getElementById('historyBody');
      if (!trades || trades.length === 0) {
        body.innerHTML = '<tr><td colspan="8" class="text-slate-500">No closed trades yet</td></tr>';
        return;
      }
      const sorted = [...trades].sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      body.innerHTML = sorted.map(t => {
        const dirCls = t.direction === 'LONG' ? 'pos-long' : 'pos-short';
        const ts = new Date(t.timestamp).toLocaleString();
        return '<tr><td class="text-slate-400 text-xs">' + ts + '</td><td>' + t.symbol +
          '</td><td class="' + dirCls + '">' + t.direction + '</td><td>' + fmt(t.entryPrice, 4) +
          '</td><td>' + fmt(t.exitPrice, 4) + '</td><td>$' + fmt(t.totalFees, 4) +
          '</td><td>' + fmtUsd(t.netPnL) + '</td><td>' + fmt(t.roePercent, 2) + '%</td></tr>';
      }).join('');
    }

    function applyPayload(d) {
      renderGlobal(d);
      renderActive(d.activePositions);
      renderHistory(d.closedTrades);
    }

    function connect() {
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(proto + '//' + location.host + '/ws/ui');
      ws.onopen = () => {
        document.getElementById('wsStatus').className = 'status-dot connected';
        document.getElementById('wsLabel').textContent = 'Live';
      };
      ws.onclose = () => {
        document.getElementById('wsStatus').className = 'status-dot disconnected';
        document.getElementById('wsLabel').textContent = 'Reconnecting…';
        setTimeout(connect, 2000);
      };
      ws.onmessage = (ev) => {
        try { applyPayload(JSON.parse(ev.data)); } catch (e) { console.error(e); }
      };
    }
    connect();
  </script>
</body>
</html>`

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
}

// Hub broadcasts dashboard snapshots to UI clients.
type Hub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]bool
	engines []*SymbolEngine
}

func NewHub(engines []*SymbolEngine) *Hub {
	return &Hub{
		clients: make(map[*websocket.Conn]bool),
		engines: engines,
	}
}

func (h *Hub) BuildPayload() DashboardPayload {
	h.mu.RLock()
	engines := h.engines
	h.mu.RUnlock()

	symbols := make([]SymbolSnapshot, 0, len(engines))
	active := make([]PositionSnapshot, 0, 4)
	var closed []ClosedTrade
	var globalBalance, globalFees float64
	initial := InitialBalancePerSymbol * float64(len(engines))

	for _, e := range engines {
		snap := e.Snapshot()
		symbols = append(symbols, snap)
		globalFees += snap.TotalFees
		globalBalance += snap.Balance

		e.mu.RLock()
		for _, t := range e.ClosedTrades {
			closed = append(closed, t)
		}
		e.mu.RUnlock()

		if snap.Position != nil {
			active = append(active, *snap.Position)
		}
	}

	return DashboardPayload{
		Type:            "dashboard",
		Timestamp:       time.Now().UTC(),
		Symbols:         symbols,
		ActivePositions: active,
		ClosedTrades:    closed,
		GlobalBalance:   roundFloat(globalBalance, 2),
		GlobalPnL:       roundFloat(globalBalance-initial, 2),
		GlobalFees:      roundFloat(globalFees, 2),
		InitialCapital:  initial,
	}
}

func (h *Hub) Broadcast() {
	payload := h.BuildPayload()
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("broadcast marshal: %v", err)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if err := c.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("ws write: %v", err)
			c.Close()
			delete(h.clients, c)
		}
	}
}

func (h *Hub) Register(conn *websocket.Conn) {
	h.mu.Lock()
	h.clients[conn] = true
	h.mu.Unlock()

	data, _ := json.Marshal(h.BuildPayload())
	_ = conn.WriteMessage(websocket.TextMessage, data)
}

func (h *Hub) Unregister(conn *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, conn)
	h.mu.Unlock()
	conn.Close()
}

func StartHTTPServer(addr string, hub *Hub) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(dashboardHTML))
	})
	mux.HandleFunc("/ws/ui", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("ws upgrade: %v", err)
			return
		}
		hub.Register(conn)
		go func() {
			defer hub.Unregister(conn)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	})

	log.Printf("dashboard listening on http://%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
