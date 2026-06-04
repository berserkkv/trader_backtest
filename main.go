package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	engines := make([]*SymbolEngine, len(symbols))
	pipelines := make([]*SymbolPipeline, len(symbols))

	for i, sym := range symbols {
		eng := NewSymbolEngine(sym)
		engines[i] = eng
	}

	hub := NewHub(engines)

	addr := ":8090"
	if p := os.Getenv("HTTP_ADDR"); p != "" {
		addr = p
	}

	go StartHTTPServer(addr, hub)

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			hub.Broadcast()
		}
	}()

	for i, sym := range symbols {
		sym := sym
		eng := engines[i]
		p := NewSymbolPipeline(sym, eng, func(events []EngineEvent) {
			for _, ev := range events {
				switch ev.Type {
				case EventTradeOpen:
					log.Printf("[TRADE OPEN] %s %s @ %.4f margin=%.2f fee=%.4f",
						ev.Symbol, ev.Direction, ev.Price, ev.Margin, ev.Fee)
				case EventTradeClose:
					log.Printf("[TRADE CLOSE] %s %s @ %.4f netPnL=%.4f roe=%.2f%% fees=%.4f — %s",
						ev.Symbol, ev.Direction, ev.Price, ev.NetPnL, ev.ROE, ev.Fees, ev.Message)
				case EventSignal:
					log.Printf("[SIGNAL] %s: %s", ev.Symbol, ev.Message)
				}
			}
			hub.Broadcast()
		})
		pipelines[i] = p
		p.Start()
	}

	log.Printf("started %d symbol pipelines; initial capital $%.0f per symbol ($%.0f total)",
		len(symbols), InitialBalancePerSymbol, InitialBalancePerSymbol*float64(len(symbols)))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down…")
	ticker.Stop()
	for _, p := range pipelines {
		p.Stop()
	}
}
