package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	binanceFuturesREST = "https://fapi.binance.com"
	binanceFuturesWS   = "wss://fstream.binance.com/ws"
	klineInterval      = "15m"
	historyLimit       = 100
)

type KlineHandler func(c Candle, isFinal bool)

// SymbolPipeline runs REST bootstrap then resilient WS for one symbol.
type SymbolPipeline struct {
	Symbol  string
	Engine  *SymbolEngine
	Handler func([]EngineEvent)

	ctx    context.Context
	cancel context.CancelFunc
}

func NewSymbolPipeline(symbol string, engine *SymbolEngine, handler func([]EngineEvent)) *SymbolPipeline {
	ctx, cancel := context.WithCancel(context.Background())
	return &SymbolPipeline{
		Symbol:  symbol,
		Engine:  engine,
		Handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (p *SymbolPipeline) Start() {
	go p.run()
}

func (p *SymbolPipeline) Stop() {
	p.cancel()
}

func (p *SymbolPipeline) run() {
	if err := p.bootstrapHistory(); err != nil {
		log.Printf("[%s] history bootstrap failed: %v", p.Symbol, err)
	} else {
		log.Printf("[%s] loaded %d historical candles", p.Symbol, len(p.Engine.Candles))
	}

	backoff := time.Second
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		err := p.streamKlines()
		if p.ctx.Err() != nil {
			return
		}
		log.Printf("[%s] websocket disconnected: %v — reconnect in %s", p.Symbol, err, backoff)
		select {
		case <-time.After(backoff):
		case <-p.ctx.Done():
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (p *SymbolPipeline) bootstrapHistory() error {
	candles, err := FetchFuturesKlines(p.Symbol, klineInterval, historyLimit)
	if err != nil {
		return err
	}
	p.Engine.PreloadCandles(candles)
	return nil
}

func (p *SymbolPipeline) streamKlines() error {
	stream := strings.ToLower(p.Symbol) + "@kline_" + klineInterval
	url := binanceFuturesWS + "/" + stream

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}
	conn, _, err := dialer.DialContext(p.ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	log.Printf("[%s] websocket connected", p.Symbol)

	conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	pingDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(10*time.Second)); err != nil {
					return
				}
			case <-pingDone:
				return
			case <-p.ctx.Done():
				return
			}
		}
	}()
	defer close(pingDone)

	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))

		c, isFinal, err := parseKlineMessage(msg)
		if err != nil {
			log.Printf("[%s] parse kline: %v", p.Symbol, err)
			continue
		}

		events := p.Engine.OnKlineTick(c, isFinal)
		if p.Handler != nil {
			p.Handler(events)
		}
	}
}

type wsKlineEvent struct {
	EventType string `json:"e"`
	Symbol    string `json:"s"`
	Kline     struct {
		StartTime int64  `json:"t"`
		EndTime   int64  `json:"T"`
		Symbol    string `json:"s"`
		Interval  string `json:"i"`
		Open      string `json:"o"`
		Close     string `json:"c"`
		High      string `json:"h"`
		Low       string `json:"l"`
		Volume    string `json:"v"`
		IsFinal   bool   `json:"x"`
	} `json:"k"`
}

func parseKlineMessage(data []byte) (Candle, bool, error) {
	var ev wsKlineEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return Candle{}, false, err
	}
	k := ev.Kline
	o, err := strconv.ParseFloat(k.Open, 64)
	if err != nil {
		return Candle{}, false, err
	}
	c, err := strconv.ParseFloat(k.Close, 64)
	if err != nil {
		return Candle{}, false, err
	}
	h, err := strconv.ParseFloat(k.High, 64)
	if err != nil {
		return Candle{}, false, err
	}
	l, err := strconv.ParseFloat(k.Low, 64)
	if err != nil {
		return Candle{}, false, err
	}
	v, err := strconv.ParseFloat(k.Volume, 64)
	if err != nil {
		return Candle{}, false, err
	}
	candle := Candle{
		OpenTime: time.UnixMilli(k.StartTime).UTC(),
		Open:     o,
		High:     h,
		Low:      l,
		Close:    c,
		Volume:   v,
		Closed:   k.IsFinal,
	}
	return candle, k.IsFinal, nil
}

// FetchFuturesKlines loads historical candles from Binance Futures REST API.
func FetchFuturesKlines(symbol, interval string, limit int) ([]Candle, error) {
	url := fmt.Sprintf("%s/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		binanceFuturesREST, symbol, interval, limit)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("binance klines %s: %s", resp.Status, string(body))
	}

	var raw [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	candles := make([]Candle, 0, len(raw))
	for _, row := range raw {
		if len(row) < 7 {
			continue
		}
		openTimeMs, _ := row[0].(float64)
		o, _ := strconv.ParseFloat(fmt.Sprint(row[1]), 64)
		h, _ := strconv.ParseFloat(fmt.Sprint(row[2]), 64)
		l, _ := strconv.ParseFloat(fmt.Sprint(row[3]), 64)
		c, _ := strconv.ParseFloat(fmt.Sprint(row[4]), 64)
		v, _ := strconv.ParseFloat(fmt.Sprint(row[5]), 64)
		candles = append(candles, Candle{
			OpenTime: time.UnixMilli(int64(openTimeMs)).UTC(),
			Open:     o,
			High:     h,
			Low:      l,
			Close:    c,
			Volume:   v,
			Closed:   true,
		})
	}
	return candles, nil
}
