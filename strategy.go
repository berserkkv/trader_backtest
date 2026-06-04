package main

import (
	"math"
	"sync"
	"time"
)

const (
	InitialBalancePerSymbol = 1000.0
	Leverage                  = 5.0
	MarginBuffer              = 0.98
	TakerFeeRate              = 0.0005
	EMAFastPeriod             = 20
	EMASlowPeriod             = 50
	MaxCandles                = 200
)

type Direction string

const (
	DirLong  Direction = "LONG"
	DirShort Direction = "SHORT"
)

type Candle struct {
	OpenTime time.Time
	Open     float64
	High     float64
	Low      float64
	Close    float64
	Volume   float64
	Closed   bool
}

type Position struct {
	Symbol      string
	Direction   Direction
	EntryPrice  float64
	EntryTime   time.Time
	Tokens      float64
	MarginUsed  float64
	EntryNominal float64
	EntryFee    float64
	Leverage    float64
}

type ClosedTrade struct {
	Timestamp  time.Time   `json:"timestamp"`
	Symbol     string      `json:"symbol"`
	Direction  Direction   `json:"direction"`
	EntryPrice float64     `json:"entryPrice"`
	ExitPrice  float64     `json:"exitPrice"`
	TotalFees  float64     `json:"totalFees"`
	NetPnL     float64     `json:"netPnL"`
	ROEPercent float64     `json:"roePercent"`
	MarginUsed float64     `json:"marginUsed"`
}

// SymbolEngine holds isolated per-symbol strategy and ledger state.
type SymbolEngine struct {
	mu sync.RWMutex

	Symbol string

	Candles []Candle
	EMA20   float64
	EMA50   float64

	LongPullbackActive  bool
	ShortPullbackActive bool
	PendingLongEntry    bool
	PendingShortEntry   bool

	Position     *Position
	Balance      float64
	TotalFees    float64
	ClosedTrades []ClosedTrade

	CurrentPrice float64
	LastKline    *Candle
}

func NewSymbolEngine(symbol string) *SymbolEngine {
	return &SymbolEngine{
		Symbol:       symbol,
		Balance:      InitialBalancePerSymbol,
		Candles:      make([]Candle, 0, MaxCandles),
		ClosedTrades: make([]ClosedTrade, 0, 64),
	}
}

func (e *SymbolEngine) Snapshot() SymbolSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.snapshotLocked()
}

func (e *SymbolEngine) snapshotLocked() SymbolSnapshot {
	snap := SymbolSnapshot{
		Symbol:        e.Symbol,
		Balance:       roundFloat(e.equityLocked(), 2),
		TotalFees:     e.TotalFees,
		EMA20:         e.EMA20,
		EMA50:         e.EMA50,
		CurrentPrice:  e.CurrentPrice,
		LongPullback:  e.LongPullbackActive,
		ShortPullback: e.ShortPullbackActive,
	}
	if e.Position != nil {
		livePnL, roe := e.liveMetricsLocked()
		snap.Position = &PositionSnapshot{
			Symbol:       e.Position.Symbol,
			Direction:    string(e.Position.Direction),
			EntryPrice:   e.Position.EntryPrice,
			CurrentPrice: e.CurrentPrice,
			Leverage:     e.Position.Leverage,
			MarginUsed:   e.Position.MarginUsed,
			LivePnL:      livePnL,
			LiveROE:      roe,
		}
	}
	return snap
}

func (e *SymbolEngine) liveMetricsLocked() (pnl, roe float64) {
	if e.Position == nil || e.CurrentPrice <= 0 {
		return 0, 0
	}
	p := e.Position
	var gross float64
	if p.Direction == DirLong {
		gross = (e.CurrentPrice - p.EntryPrice) * p.Tokens
	} else {
		gross = (p.EntryPrice - e.CurrentPrice) * p.Tokens
	}
	exitNominal := p.Tokens * e.CurrentPrice
	estExitFee := exitNominal * TakerFeeRate
	pnl = gross - p.EntryFee - estExitFee
	if p.MarginUsed > 0 {
		roe = (pnl / p.MarginUsed) * 100
	}
	return pnl, roe
}

func (e *SymbolEngine) PreloadCandles(candles []Candle) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Candles = append(e.Candles[:0], candles...)
	if len(e.Candles) > MaxCandles {
		e.Candles = e.Candles[len(e.Candles)-MaxCandles:]
	}
	e.recomputeEMAsLocked()
	if n := len(e.Candles); n > 0 {
		last := e.Candles[n-1]
		e.CurrentPrice = last.Close
		e.LastKline = &last
	}
}

// OnKlineTick processes a kline update. closed=true means the candle just finalized.
func (e *SymbolEngine) OnKlineTick(c Candle, isFinal bool) []EngineEvent {
	e.mu.Lock()
	defer e.mu.Unlock()

	var events []EngineEvent

	c.Closed = isFinal
	e.CurrentPrice = c.Close
	e.LastKline = &c

	if isFinal {
		e.appendCandleLocked(c)
		e.recomputeEMAsLocked()
		events = append(events, e.onCandleClosedLocked(c)...)
	} else {
		if e.PendingLongEntry {
			events = append(events, e.executeLongLocked(c.Close)...)
			e.PendingLongEntry = false
		}
		if e.PendingShortEntry {
			events = append(events, e.executeShortLocked(c.Close)...)
			e.PendingShortEntry = false
		}
		if e.Position != nil {
			livePnL, liveROE := e.liveMetricsLocked()
			events = append(events, EngineEvent{Type: EventPositionUpdate, Symbol: e.Symbol, LivePnL: livePnL, LiveROE: liveROE})
		}
	}

	return events
}

func (e *SymbolEngine) appendCandleLocked(c Candle) {
	if len(e.Candles) > 0 {
		last := e.Candles[len(e.Candles)-1]
		if last.OpenTime.Equal(c.OpenTime) {
			e.Candles[len(e.Candles)-1] = c
			return
		}
	}
	e.Candles = append(e.Candles, c)
	if len(e.Candles) > MaxCandles {
		e.Candles = e.Candles[len(e.Candles)-MaxCandles:]
	}
}

func (e *SymbolEngine) recomputeEMAsLocked() {
	if len(e.Candles) == 0 {
		e.EMA20, e.EMA50 = 0, 0
		return
	}
	closes := make([]float64, len(e.Candles))
	for i, c := range e.Candles {
		closes[i] = c.Close
	}
	e.EMA20 = calcEMA(closes, EMAFastPeriod)
	e.EMA50 = calcEMA(closes, EMASlowPeriod)
}

func calcEMA(values []float64, period int) float64 {
	if len(values) < period {
		return 0
	}
	k := 2.0 / float64(period+1)
	var sum float64
	for i := 0; i < period; i++ {
		sum += values[i]
	}
	ema := sum / float64(period)
	for i := period; i < len(values); i++ {
		ema = values[i]*k + ema*(1-k)
	}
	return ema
}

func (e *SymbolEngine) onCandleClosedLocked(c Candle) []EngineEvent {
	var events []EngineEvent

	if e.Position != nil {
		if ev := e.checkStructuralExitLocked(c); ev != nil {
			events = append(events, *ev)
		}
	}

	bullish := e.EMA20 > e.EMA50
	bearish := e.EMA20 < e.EMA50

	if bullish {
		e.ShortPullbackActive = false
	}
	if bearish {
		e.LongPullbackActive = false
	}

	if bullish {
		if c.Low <= e.EMA20 && c.Close >= e.EMA50 {
			e.LongPullbackActive = true
		}
	}
	if bearish {
		if c.High >= e.EMA20 && c.Low <= e.EMA50 {
			e.ShortPullbackActive = true
		}
	}

	if e.Position == nil {
		if e.LongPullbackActive && c.Close > c.Open {
			e.LongPullbackActive = false
			e.PendingLongEntry = true
			events = append(events, EngineEvent{Type: EventSignal, Symbol: e.Symbol, Message: "long entry armed — next tick"})
		}
		if e.ShortPullbackActive && c.Close < c.Open {
			e.ShortPullbackActive = false
			e.PendingShortEntry = true
			events = append(events, EngineEvent{Type: EventSignal, Symbol: e.Symbol, Message: "short entry armed — next tick"})
		}
	}

	return events
}

func (e *SymbolEngine) checkStructuralExitLocked(c Candle) *EngineEvent {
	if e.Position == nil {
		return nil
	}
	p := e.Position
	switch p.Direction {
	case DirLong:
		if c.Close < e.EMA50 {
			events := e.closePositionLocked(c.Close, "structural long exit")
			if len(events) > 0 {
				return &events[0]
			}
		}
	case DirShort:
		if c.Close > e.EMA50 {
			events := e.closePositionLocked(c.Close, "structural short exit")
			if len(events) > 0 {
				return &events[0]
			}
		}
	}
	return nil
}

func (e *SymbolEngine) executeLongLocked(price float64) []EngineEvent {
	if e.Position != nil || price <= 0 {
		return nil
	}
	return e.openPositionLocked(DirLong, price)
}

func (e *SymbolEngine) executeShortLocked(price float64) []EngineEvent {
	if e.Position != nil || price <= 0 {
		return nil
	}
	return e.openPositionLocked(DirShort, price)
}

func (e *SymbolEngine) openPositionLocked(dir Direction, price float64) []EngineEvent {
	marginUsed := e.Balance * MarginBuffer
	if marginUsed <= 0 {
		return nil
	}
	nominal := marginUsed * Leverage
	tokens := nominal / price
	entryFee := nominal * TakerFeeRate

	e.Balance -= entryFee
	e.TotalFees += entryFee

	e.Position = &Position{
		Symbol:       e.Symbol,
		Direction:    dir,
		EntryPrice:   price,
		EntryTime:    time.Now().UTC(),
		Tokens:       tokens,
		MarginUsed:   marginUsed,
		EntryNominal: nominal,
		EntryFee:     entryFee,
		Leverage:     Leverage,
	}

	return []EngineEvent{{
		Type:      EventTradeOpen,
		Symbol:    e.Symbol,
		Direction: string(dir),
		Price:     price,
		Margin:    marginUsed,
		Fee:       entryFee,
	}}
}

func (e *SymbolEngine) closePositionLocked(exitPrice float64, reason string) []EngineEvent {
	if e.Position == nil {
		return nil
	}
	p := e.Position
	exitNominal := p.Tokens * exitPrice
	exitFee := exitNominal * TakerFeeRate
	totalFees := p.EntryFee + exitFee

	var gross float64
	if p.Direction == DirLong {
		gross = (exitPrice - p.EntryPrice) * p.Tokens
	} else {
		gross = (p.EntryPrice - exitPrice) * p.Tokens
	}
	netPnL := gross - totalFees
	roe := 0.0
	if p.MarginUsed > 0 {
		roe = (netPnL / p.MarginUsed) * 100
	}

	e.Balance += netPnL
	e.TotalFees += exitFee

	trade := ClosedTrade{
		Timestamp:  time.Now().UTC(),
		Symbol:     e.Symbol,
		Direction:  p.Direction,
		EntryPrice: p.EntryPrice,
		ExitPrice:  exitPrice,
		TotalFees:  totalFees,
		NetPnL:     netPnL,
		ROEPercent: roe,
		MarginUsed: p.MarginUsed,
	}
	e.ClosedTrades = append(e.ClosedTrades, trade)
	e.Position = nil

	return []EngineEvent{{
		Type:      EventTradeClose,
		Symbol:    e.Symbol,
		Direction: string(p.Direction),
		Price:     exitPrice,
		NetPnL:    netPnL,
		Fees:      totalFees,
		ROE:       roe,
		Message:   reason,
		Trade:     &trade,
	}}
}

func (e *SymbolEngine) equityLocked() float64 {
	if e.Position == nil {
		return e.Balance
	}
	livePnL, _ := e.liveMetricsLocked()
	return e.Balance + livePnL
}

func (e *SymbolEngine) Equity() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.equityLocked()
}

// EngineEvent notifies the UI layer of state changes.
type EngineEvent struct {
	Type      string
	Symbol    string
	Direction string
	Price     float64
	Margin    float64
	Fee       float64
	NetPnL    float64
	Fees      float64
	ROE       float64
	LivePnL   float64
	LiveROE   float64
	Message   string
	Trade     *ClosedTrade
}

const (
	EventSignal          = "signal"
	EventTradeOpen       = "trade_open"
	EventTradeClose      = "trade_close"
	EventPositionUpdate  = "position_update"
)

type SymbolSnapshot struct {
	Symbol        string  `json:"symbol"`
	Balance       float64 `json:"balance"`
	TotalFees     float64 `json:"totalFees"`
	EMA20         float64 `json:"ema20"`
	EMA50         float64 `json:"ema50"`
	CurrentPrice  float64 `json:"currentPrice"`
	LongPullback  bool    `json:"longPullback"`
	ShortPullback bool    `json:"shortPullback"`
	Position      *PositionSnapshot `json:"position,omitempty"`
}

type PositionSnapshot struct {
	Symbol       string  `json:"symbol"`
	Direction    string  `json:"direction"`
	EntryPrice   float64 `json:"entryPrice"`
	CurrentPrice float64 `json:"currentPrice"`
	Leverage     float64 `json:"leverage"`
	MarginUsed   float64 `json:"marginUsed"`
	LivePnL      float64 `json:"livePnL"`
	LiveROE      float64 `json:"liveROE"`
}

type DashboardPayload struct {
	Type             string              `json:"type"`
	Timestamp        time.Time           `json:"timestamp"`
	Symbols          []SymbolSnapshot    `json:"symbols"`
	ActivePositions  []PositionSnapshot  `json:"activePositions"`
	ClosedTrades     []ClosedTrade       `json:"closedTrades"`
	GlobalBalance    float64             `json:"globalBalance"`
	GlobalPnL        float64             `json:"globalPnL"`
	GlobalFees       float64             `json:"globalFees"`
	InitialCapital   float64             `json:"initialCapital"`
}

func roundFloat(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}
