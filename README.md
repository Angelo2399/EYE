# EYE

**EYE** is an AI-driven intraday market intelligence and alerting project designed to transform **economic, geopolitical, and market-moving information** into structured operational reasoning.

It is not just a signal engine.  
It is a live technical experiment focused on how an AI Agent can:

- interpret events
- reason across market context
- adapt to time zones and trading sessions
- generate structured alerts
- avoid useless noise
- behave like a real operational system

---

## Vision

Financial markets do not move only because of price.

They move because of:

- macroeconomic releases
- central bank decisions
- Treasury dynamics
- oil and inventory data
- geopolitical tensions
- regulatory actions
- institutional announcements
- changes in market structure and volatility

EYE was created to test whether an AI Agent can **connect those inputs to market behavior**, and translate them into **clear intraday monitoring, briefings, and alerts**.

---

## What EYE is

EYE is a modular AI Agent for **intraday market reasoning**.

Its role is to:

- monitor selected assets
- process market structure and event context
- classify the current environment
- decide whether the situation supports:
  - **BUY**
  - **SELL**
  - **WAIT**
  - **NO TRADE**
- explain the reasoning in a structured way
- distribute updates automatically through Telegram

The system is built to privilege **clarity, selectivity, and context-awareness** over noise.

---

## Current asset focus

EYE currently focuses on a small set of high-priority assets:

- **Nasdaq 100 (NDX)**
- **S&P 500 (SPX)**
- **WTI Crude Oil (WTI)**

This limited scope is intentional.

The objective is not to cover everything.  
The objective is to cover a few relevant assets **well**, with stronger logic and more credible signal flow.

---

## Why this project is technically interesting

EYE is not a simple alert bot.

It combines several layers:

### 1. Market data reasoning
The system collects price data, derives features, classifies regime, evaluates setup quality, and estimates possible intraday behavior.

### 2. External intelligence
The architecture integrates institutional and first-party sources such as:

- central banks
- Treasury / macro data
- official market sources
- regulatory and index-related sources
- oil, inventory, and commodity-related sources
- volatility and market structure sources

The purpose is to let AI reason not only on candles, but also on **why markets may move**.

### 3. Timezone-aware automation
EYE is built to behave differently depending on:

- local user timezone
- market session
- official trading calendar
- quiet hours
- briefing windows
- hourly follow-up logic

This is one of the core parts of the project:
**the AI Agent is expected to reason not only on data, but also on timing.**

### 4. Event-driven Telegram delivery
The project includes a Telegram delivery layer capable of:

- sending structured market briefings
- generating follow-up updates
- avoiding duplicate noise
- respecting configured quiet hours
- reacting to meaningful market changes

### 5. Operational discipline
EYE is intentionally designed for **intraday logic**, not for random overtrading.

If the setup is weak, the AI must be able to say:

- **WAIT**
- **NO TRADE**

and justify that decision based on current context.

---

## What EYE tries to demonstrate

This repository is also a **practical proof of competence**.

It demonstrates my ability to design and connect:

- AI-driven reasoning flows
- market context interpretation
- timezone-aware automation
- event-based alert systems
- structured Telegram delivery
- modular service architecture
- testing and validation across complex operational flows

In other words, EYE is both:

- a working intraday AI Agent prototype
- a demonstration of engineering, architecture, and operational reasoning skills

---

## Operational behavior

When active, EYE is designed to:

- send scheduled briefings
- monitor market conditions continuously
- generate structured asset updates
- provide clear directional context
- explain why a signal is valid or why no action is justified
- suppress non-essential messages during configured quiet hours

The intent is to simulate the behavior of an AI Agent that does not just “speak”, but **operates with timing, filters, and structure**.

---

## Example of expected output

A typical alert is meant to stay readable and operational:

- asset
- signal
- scenario
- confidence
- reason
- entry area
- stop loss
- take profit

The goal is not verbosity.  
The goal is **usable decision support**.

---

## Telegram bot

Live alert flow and project updates are distributed through Telegram:

**@eye_intraday_alerts_bot**

This bot is used to test:

- automated briefings
- structured intraday alerts
- hourly follow-up logic
- event-aware messaging behavior

---

## Architecture overview

The project includes:

- **FastAPI** backend
- signal generation engine
- regime and probability logic
- risk planning
- briefing builder and runner
- market calendar logic
- market intelligence connectors
- Telegram alert service
- hourly update scheduling
- test coverage across services and flows

It is built as a modular system rather than a single script, because the objective is to reflect how a serious AI operational agent should be structured.

---

## Repository safety

This public repository does **not** include:

- private API keys
- `.env` secrets
- local databases
- runtime caches
- local logs
- confidential operational data

Sensitive and local-only files are intentionally excluded from version control.

---

## Disclaimer

EYE is a **research, testing, and demonstration project**.

Its purpose is to explore how an AI Agent can interpret current events, market structure, and operational timing, and translate them into structured alerts and intraday reasoning.

It does **not** provide financial advice, portfolio management, or guaranteed outcomes.

Trading and investing involve significant risk.  
Any market decision remains entirely the responsibility of the user.

---

## Project status

EYE is under active refinement.

The current evolution focuses on:

- better reasoning quality
- stronger event interpretation
- cleaner Telegram delivery
- more reliable automation behavior
- clearer intraday communication
- more robust market context integration

---

## Contact / follow-up

For live alerts and project follow-up:

**Telegram:** @eye_intraday_alerts_bot
