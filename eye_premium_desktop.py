# -*- coding: utf-8 -*-

from datetime import datetime, time as dt_time
from email.utils import parsedate_to_datetime
import json
import subprocess
import threading
from pathlib import Path
from tkinter import Canvas, Frame, Label, StringVar, Tk
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET
from tkinter import ttk
from zoneinfo import ZoneInfo


BG = "#06101d"
CARD = "#0b1728"
TEXT = "#eef4ff"
MUTED = "#9fb0c9"
GREEN = "#19c37d"
RED = "#ff5c5c"
TICKER_BG = "#081a2b"
TICKER_BORDER = "#1d3a57"
TICKER_TEXT = "#eaf3ff"
PROJECT_ROOT = Path(r"C:\Users\angel\Projects\EYE")
VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
CHART_BG = "#091321"
CHART_GRID = "#18324f"

NASDAQ_MARKETS_RSS = "https://www.nasdaq.com/feed/rssoutbound?category=Markets"


def get_market_phase() -> tuple[str, str]:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    weekday = now_et.weekday()
    current_time = now_et.time()

    if weekday >= 5:
        return "Mercato chiuso", "Weekend. Modalita osservazione attiva."

    if dt_time(4, 0) <= current_time < dt_time(9, 30):
        return "Pre-market", "Monitoraggio news e contesto prima dell'apertura."

    if dt_time(9, 30) <= current_time < dt_time(16, 0):
        return "Mercato aperto", "Monitoraggio live attivo."

    if dt_time(16, 0) <= current_time < dt_time(20, 0):
        return "Post-market", "Monitoraggio dopo la chiusura regolare."

    return "Mercato chiuso", "Modalita osservazione attiva."


def get_main_message_for_phase(phase_label: str) -> tuple[str, str]:
    if phase_label == "Mercato aperto":
        return (
            "Mercato aperto: monitoraggio live attivo.",
            "EYE puo mostrare segnali, contesto e sviluppi rilevanti in tempo reale.",
        )

    if phase_label == "Pre-market":
        return (
            "Pre-market: preparazione apertura.",
            "EYE osserva notizie e possibili impatti prima della sessione regolare.",
        )

    if phase_label == "Post-market":
        return (
            "Post-market: osservazione dopo la chiusura.",
            "EYE continua a monitorare sviluppi utili per la prossima finestra operativa.",
        )

    return (
        "Mercato chiuso: modalita osservazione.",
        "EYE non sta dando un segnale live. Sta preparando il quadro per la prossima apertura.",
    )

class NewsTicker:
    def __init__(self, canvas: Canvas) -> None:
        self.canvas = canvas
        self.text_id = None
        self.running = False
        self.speed = 2
        self.text = ""

    def set_items(self, items: list[str]) -> None:
        cleaned = [item.strip() for item in items if item and item.strip()]
        if not cleaned:
            cleaned = ["Nessuna news disponibile."]
        self.text = "     |     ".join(cleaned)

        self.canvas.delete("all")
        self.canvas.update_idletasks()

        width = max(self.canvas.winfo_width(), 200)
        height = max(self.canvas.winfo_height(), 44)

        self.text_id = self.canvas.create_text(
            width,
            height // 2,
            text=self.text,
            fill=TICKER_TEXT,
            font=("Segoe UI", 12, "bold"),
            anchor="w",
        )

        if not self.running:
            self.running = True
            self._tick()

    def _tick(self) -> None:
        if not self.running or self.text_id is None:
            return

        self.canvas.move(self.text_id, -self.speed, 0)
        bbox = self.canvas.bbox(self.text_id)

        if bbox is not None and bbox[2] < 0:
            width = max(self.canvas.winfo_width(), 200)
            height = max(self.canvas.winfo_height(), 44)
            self.canvas.coords(self.text_id, width, height // 2)

        self.canvas.after(25, self._tick)


def fetch_nasdaq_markets_news(limit: int = 12) -> list[str]:
    request = Request(
        NASDAQ_MARKETS_RSS,
        headers={"User-Agent": "EYE Premium/1.0"},
    )

    with urlopen(request, timeout=12) as response:
        xml_text = response.read().decode("utf-8", errors="replace")

    root_xml = ET.fromstring(xml_text)
    channel = root_xml.find("channel")
    if channel is None:
        return ["Feed Nasdaq non disponibile."]

    items: list[str] = []

    for item in channel.findall("item")[:limit]:
        title = clean_news_text((item.findtext("title") or "").strip())
        pub_date = (item.findtext("pubDate") or "").strip()

        if not title:
            continue

        try:
            dt = parsedate_to_datetime(pub_date)
            stamp = dt.strftime("%H:%M")
        except Exception:
            stamp = "--:--"

        items.append(f"NASDAQ  {stamp}  {title}")

    return items or ["Nessuna notizia Nasdaq disponibile."]

def clean_news_text(text: str) -> str:
    cleaned = (text or "").replace("?", "").replace("  ", " ").strip()
    cleaned = cleaned.replace("â€™", "'").replace("â€“", "-").replace("â€œ", '"').replace("â€", '"')
    return cleaned


def refresh_nasdaq_ticker() -> None:
    try:
        items = fetch_nasdaq_markets_news(limit=12)
        root.news_ticker.set_items(items)
        news_status.set("Ticker news live da Nasdaq.")
    except Exception:
        root.news_ticker.set_items(
            ["Feed Nasdaq temporaneamente non disponibile."]
        )
        news_status.set("Ticker Nasdaq non disponibile in questo momento.")

    root.after(60000, refresh_nasdaq_ticker)


def fetch_live_chart_payload(symbol_code: str, points_limit: int = 80) -> dict:
    python_code = r"""
import json
import sys
import yfinance as yf

symbol_code = sys.argv[1]
points_limit = int(sys.argv[2])

provider_map = {
    "NDX": "^NDX",
    "SPX": "^GSPC",
}

provider_symbol = provider_map[symbol_code]

df = yf.download(
    provider_symbol,
    period="5d",
    interval="60m",
    auto_adjust=False,
    progress=False,
    threads=False,
    prepost=False,
)

if df is None or df.empty:
    print(json.dumps({"symbol": symbol_code, "values": [], "error": "empty"}))
    raise SystemExit

if hasattr(df.columns, "levels"):
    df.columns = [str(c[0]).strip().lower() for c in df.columns]
else:
    df.columns = [str(c).strip().lower() for c in df.columns]

close_col = "close" if "close" in df.columns else df.columns[-1]
values = [float(x) for x in df[close_col].dropna().tolist()][-points_limit:]

payload = {
    "symbol": symbol_code,
    "values": values,
}
print(json.dumps(payload))
"""

    result = subprocess.run(
        [
            str(VENV_PYTHON),
            "-c",
            python_code,
            symbol_code,
            str(points_limit),
        ],
        capture_output=True,
        text=True,
        timeout=25,
        cwd=str(PROJECT_ROOT),
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Errore recupero grafico.")

    output = (result.stdout or "").strip()
    if not output:
        raise RuntimeError("Nessun output dal grafico live.")

    return json.loads(output)


def draw_live_chart(symbol_code: str, values: list[float]) -> None:
    chart_canvas.delete("all")
    chart_canvas.update_idletasks()

    width = max(chart_canvas.winfo_width(), 640)
    height = max(chart_canvas.winfo_height(), 260)

    chart_canvas.create_rectangle(0, 0, width, height, fill=CHART_BG, outline="")

    if len(values) < 2:
        chart_canvas.create_text(
            width // 2,
            height // 2,
            text="Dati grafico non disponibili.",
            fill=MUTED,
            font=("Segoe UI", 11),
        )
        chart_last_var.set("-")
        chart_change_var.set("-")
        chart_high_var.set("-")
        chart_low_var.set("-")
        chart_range_var.set("-")
        chart_history_var.set("-")
        return

    left = 56
    top = 18
    right = width - 18
    bottom = height - 38

    min_v = min(values)
    max_v = max(values)
    span = max(max_v - min_v, 0.0001)

    # griglia orizzontale + etichette asse Y
    for i in range(5):
        y = top + ((bottom - top) / 4) * i
        value = max_v - ((max_v - min_v) / 4) * i

        chart_canvas.create_line(
            left,
            y,
            right,
            y,
            fill=CHART_GRID,
            width=1,
        )

        chart_canvas.create_text(
            left - 8,
            y,
            text=f"{value:.2f}",
            fill=MUTED,
            font=("Segoe UI", 9),
            anchor="e",
        )

    points = []
    for index, value in enumerate(values):
        x = left + ((right - left) * index / max(len(values) - 1, 1))
        y = bottom - ((value - min_v) / span) * (bottom - top)
        points.extend([x, y])

    first_value = values[0]
    last_value = values[-1]
    high_value = max_v
    low_value = min_v
    change = last_value - first_value
    pct = (change / first_value * 100) if first_value else 0.0

    line_color = GREEN if change >= 0 else RED
    value_color = GREEN if change >= 0 else RED

    # area morbida sotto la linea
    area_points = [points[0], bottom] + points + [points[-2], bottom]
    chart_canvas.create_polygon(
        area_points,
        fill="#0f2741",
        outline="",
    )

    chart_canvas.create_line(
        points,
        fill=line_color,
        width=3,
        smooth=True,
    )

    # ultimo punto evidenziato
    chart_canvas.create_oval(
        points[-2] - 4,
        points[-1] - 4,
        points[-2] + 4,
        points[-1] + 4,
        fill=line_color,
        outline=line_color,
    )

    chart_canvas.create_text(
        left,
        height - 16,
        text=f"{symbol_code}",
        anchor="w",
        fill=TEXT,
        font=("Segoe UI", 10, "bold"),
    )

    chart_canvas.create_text(
        right,
        height - 16,
        text=f"{last_value:.2f}   {pct:+.2f}%",
        anchor="e",
        fill=value_color,
        font=("Segoe UI", 10, "bold"),
    )

    chart_last_var.set(f"{last_value:.2f}")
    chart_change_var.set(f"{change:+.2f} / {pct:+.2f}%")
    chart_high_var.set(f"{high_value:.2f}")
    chart_low_var.set(f"{low_value:.2f}")
    chart_range_var.set(f"{(high_value - low_value):.2f}")

    recent_values = list(reversed(values[-6:]))
    chart_history_var.set(" | ".join(f"{v:.2f}" for v in recent_values))


def schedule_chart_refresh() -> None:
    existing_job = getattr(root, "_chart_refresh_job", None)
    if existing_job:
        try:
            root.after_cancel(existing_job)
        except Exception:
            pass

    root._chart_refresh_job = root.after(60000, refresh_live_chart)


def apply_chart_payload(payload: dict) -> None:
    symbol_code = payload.get("symbol", "NDX")
    values = payload.get("values", [])

    draw_live_chart(symbol_code, values)

    chart_status.set(
        f"Aggiornato {datetime.now().strftime('%H:%M:%S')}"
    )

    chart_mode_var.set(market_status.get())

    try:
        last_value = values[-1]
        first_value = values[0]
        change = last_value - first_value
        change_color = GREEN if change >= 0 else RED
        chart_change_value_label.configure(fg=change_color, bg="#0d2238")
    except Exception:
        chart_change_value_label.configure(fg=TEXT, bg="#0d2238")

    schedule_chart_refresh()


def refresh_live_chart() -> None:
    symbol_code = chart_symbol_var.get().strip()
    chart_status.set(f"Caricamento grafico live {symbol_code}...")

    def worker() -> None:
        try:
            payload = fetch_live_chart_payload(symbol_code)
            root.after(0, lambda: apply_chart_payload(payload))
        except Exception:
            root.after(
                0,
                lambda: chart_status.set(f"Grafico live {symbol_code} non disponibile."),
            )

    threading.Thread(target=worker, daemon=True).start()

root = Tk()
root.title("EYE Premium")
root.configure(bg=BG)

window_width = 1120
window_height = 640

screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

position_x = max((screen_width - window_width) // 2, 40)
position_y = max((screen_height - window_height) // 2, 40)

root.geometry(f"{window_width}x{window_height}+{position_x}+{position_y}")
root.minsize(980, 560)

style = ttk.Style()
style.theme_use("clam")
style.configure(".", background=BG, foreground=TEXT)
style.configure("Card.TFrame", background=CARD)
style.configure("Main.TFrame", background=BG)
style.configure("Title.TLabel", background=CARD, foreground=TEXT, font=("Segoe UI", 24, "bold"))
style.configure("Sub.TLabel", background=CARD, foreground=MUTED, font=("Segoe UI", 10))
style.configure("Section.TLabel", background=CARD, foreground=TEXT, font=("Segoe UI", 15, "bold"))
style.configure("Value.TLabel", background=CARD, foreground=TEXT, font=("Segoe UI", 18, "bold"))
style.configure("Small.TLabel", background=CARD, foreground=MUTED, font=("Segoe UI", 10))
style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"), padding=9)
style.configure("Secondary.TButton", font=("Segoe UI", 10, "bold"), padding=9)

main = ttk.Frame(root, style="Main.TFrame", padding=16)
main.pack(fill="both", expand=True)

header = ttk.Frame(main, style="Card.TFrame", padding=18)
header.pack(fill="x", pady=(0, 14))

ttk.Label(header, text="EYE Premium", style="Title.TLabel").pack(anchor="w")
ttk.Label(
    header,
    text="Interfaccia chiara, ordinata e comprensibile.",
    style="Sub.TLabel",
).pack(anchor="w", pady=(6, 0))

body = ttk.Frame(main, style="Main.TFrame")
body.pack(fill="both", expand=True, pady=(0, 12))

left = ttk.Frame(body, style="Card.TFrame", padding=16)
left.pack(side="left", fill="y", padx=(0, 14))

right = ttk.Frame(body, style="Main.TFrame")
right.pack(side="right", fill="both", expand=True)

market_status = StringVar()
market_hint = StringVar()
main_message = StringVar()
main_sub = StringVar()
news_status = StringVar(value="Connessione ticker Nasdaq in avvio...")
chart_status = StringVar(value="Grafico live in inizializzazione...")
chart_last_var = StringVar(value="-")
chart_change_var = StringVar(value="-")
chart_high_var = StringVar(value="-")
chart_low_var = StringVar(value="-")
chart_range_var = StringVar(value="-")
chart_history_var = StringVar(value="-")
chart_mode_var = StringVar(value="-")
history_status = StringVar(value="Lo storico verra collegato nel prossimo passo.")
lang_status = StringVar(value="Lingua: Italiano")
chart_symbol_var = StringVar(value="NDX")

status_value, hint_value = get_market_phase()
message_value, sub_value = get_main_message_for_phase(status_value)

market_status.set(status_value)
market_hint.set(hint_value)
main_message.set(message_value)
main_sub.set(sub_value)

ttk.Label(left, text="Controlli rapidi", style="Section.TLabel").pack(anchor="w", pady=(0, 12))
ttk.Label(left, textvariable=market_status, style="Value.TLabel", wraplength=240).pack(anchor="w", pady=(0, 8))
ttk.Label(left, textvariable=market_hint, style="Sub.TLabel", wraplength=240).pack(anchor="w", pady=(0, 10))
ttk.Label(left, textvariable=lang_status, style="Small.TLabel").pack(anchor="w", pady=(0, 18))

ttk.Button(left, text="Avvia EYE", style="Primary.TButton").pack(fill="x", pady=4)
ttk.Button(left, text="Analizza NDX", style="Secondary.TButton").pack(fill="x", pady=4)
ttk.Button(left, text="Analizza SPX", style="Secondary.TButton").pack(fill="x", pady=4)
ttk.Button(left, text="Aggiorna news", style="Secondary.TButton").pack(fill="x", pady=4)
ttk.Button(left, text="Apri dashboard web", style="Secondary.TButton").pack(fill="x", pady=4)

top = ttk.Frame(right, style="Main.TFrame")
top.pack(fill="x", pady=(0, 14))

main_card = ttk.Frame(top, style="Card.TFrame", padding=16)
main_card.pack(side="left", fill="both", expand=True, padx=(0, 7))

ttk.Label(main_card, text="Messaggio principale", style="Section.TLabel").pack(anchor="w")
ttk.Label(main_card, textvariable=main_message, style="Value.TLabel", wraplength=560).pack(anchor="w", pady=(12, 8))
ttk.Label(main_card, textvariable=main_sub, style="Sub.TLabel", wraplength=560).pack(anchor="w")

alerts_card = ttk.Frame(top, style="Card.TFrame", padding=16)
alerts_card.pack(side="right", fill="both", expand=True, padx=(7, 0))

ttk.Label(alerts_card, text="Allerte", style="Section.TLabel").pack(anchor="w")
Label(
    alerts_card,
    text=(
        "1. Stato mercato collegato\n"
        "2. Messaggio principale collegato\n"
        "3. News ufficiali nel prossimo passo"
    ),
    fg=TEXT,
    bg=CARD,
    justify="left",
    anchor="nw",
    font=("Segoe UI", 11),
).pack(fill="both", expand=True, pady=(12, 0))

news_card = ttk.Frame(right, style="Card.TFrame", padding=16)
news_card.pack(fill="x", pady=(0, 14))

top_news = Frame(news_card, bg=CARD)
top_news.pack(fill="x", pady=(0, 10))

ttk.Label(top_news, text="News finanziarie live", style="Section.TLabel").pack(side="left")

Label(
    top_news,
    text=" LIVE ",
    bg=GREEN,
    fg="white",
    font=("Segoe UI", 9, "bold"),
    padx=8,
    pady=3,
).pack(side="left", padx=(12, 6))

Label(
    top_news,
    text=" RISK ",
    bg=RED,
    fg="white",
    font=("Segoe UI", 9, "bold"),
    padx=8,
    pady=3,
).pack(side="left")

Label(
    news_card,
    textvariable=news_status,
    bg=CARD,
    fg="#8fd3b6",
    font=("Segoe UI", 10, "bold"),
    anchor="w",
).pack(fill="x", pady=(0, 10))

news_canvas = Canvas(
    news_card,
    bg=TICKER_BG,
    height=52,
    highlightthickness=1,
    highlightbackground=TICKER_BORDER,
    relief="flat",
)
news_canvas.pack(fill="x")

news_canvas.create_rectangle(
    0, 0, 1600, 52,
    outline="",
    fill=TICKER_BG,
)

root.news_ticker = NewsTicker(news_canvas)
root.after(600, refresh_nasdaq_ticker)

bottom = ttk.Frame(right, style="Main.TFrame")
bottom.pack(fill="x", expand=False, pady=(0, 12))

chart_card = ttk.Frame(bottom, style="Card.TFrame", padding=0)
chart_card.pack(side="left", fill="both", expand=True, padx=(0, 0))

chart_shell = Frame(chart_card, bg="#071320")
chart_shell.pack(fill="both", expand=True)

# HEADER
chart_header = Frame(chart_shell, bg="#0a1a2d", height=54)
chart_header.pack(fill="x")
chart_header.pack_propagate(False)

Label(
    chart_header,
    text="TRADING PANEL",
    bg="#0a1a2d",
    fg=TEXT,
    font=("Segoe UI", 11, "bold"),
).pack(side="left", padx=16)

Label(
    chart_header,
    text="USA INDEX",
    bg="#0a1a2d",
    fg="#7ea1c7",
    font=("Segoe UI", 9, "bold"),
).pack(side="left", padx=(0, 14))

chart_selector = ttk.Combobox(
    chart_header,
    textvariable=chart_symbol_var,
    values=["NDX", "SPX"],
    state="readonly",
    width=10,
)
chart_selector.pack(side="right", padx=14, pady=10)
chart_selector.bind("<<ComboboxSelected>>", lambda event: refresh_live_chart())

# TOP QUOTE BAR
quote_bar = Frame(chart_shell, bg="#0d2238", height=96)
quote_bar.pack(fill="x")
quote_bar.pack_propagate(False)

quote_left = Frame(quote_bar, bg="#0d2238")
quote_left.pack(side="left", fill="y", padx=18, pady=12)

Label(
    quote_left,
    textvariable=chart_symbol_var,
    bg="#0d2238",
    fg="#8fb3d9",
    font=("Segoe UI", 10, "bold"),
).pack(anchor="w")

Label(
    quote_left,
    textvariable=chart_last_var,
    bg="#0d2238",
    fg=TEXT,
    font=("Segoe UI", 26, "bold"),
).pack(anchor="w")

quote_mid = Frame(quote_bar, bg="#0d2238")
quote_mid.pack(side="left", fill="both", expand=True, padx=(16, 12), pady=14)

Label(
    quote_mid,
    text="Stato mercato",
    bg="#0d2238",
    fg=MUTED,
    font=("Segoe UI", 8, "bold"),
    anchor="w",
).pack(anchor="w")

Label(
    quote_mid,
    textvariable=chart_mode_var,
    bg="#0d2238",
    fg=TEXT,
    font=("Segoe UI", 12, "bold"),
    anchor="w",
).pack(anchor="w", pady=(3, 0))

Label(
    quote_mid,
    textvariable=chart_status,
    bg="#0d2238",
    fg=MUTED,
    font=("Segoe UI", 9),
    anchor="w",
).pack(anchor="w", pady=(6, 0))

quote_right = Frame(quote_bar, bg="#0d2238")
quote_right.pack(side="right", fill="y", padx=18, pady=14)

Label(
    quote_right,
    text="Variazione",
    bg="#0d2238",
    fg=MUTED,
    font=("Segoe UI", 8, "bold"),
    anchor="e",
).pack(anchor="e")

chart_change_value_label = Label(
    quote_right,
    textvariable=chart_change_var,
    bg="#0d2238",
    fg=TEXT,
    font=("Segoe UI", 16, "bold"),
    anchor="e",
)
chart_change_value_label.pack(anchor="e", pady=(4, 0))

# STATS BAR
stats_bar = Frame(chart_shell, bg="#09192c", height=74)
stats_bar.pack(fill="x")
stats_bar.pack_propagate(False)

def stat_box(parent, title_text, value_var):
    box = Frame(parent, bg="#09192c", width=160)
    box.pack(side="left", fill="y", padx=12, pady=10)
    box.pack_propagate(False)

    Label(
        box,
        text=title_text,
        bg="#09192c",
        fg=MUTED,
        font=("Segoe UI", 8, "bold"),
    ).pack(anchor="w")

    Label(
        box,
        textvariable=value_var,
        bg="#09192c",
        fg=TEXT,
        font=("Segoe UI", 12, "bold"),
    ).pack(anchor="w", pady=(4, 0))

stat_box(stats_bar, "ULTIMO", chart_last_var)
stat_box(stats_bar, "VARIAZIONE", chart_change_var)
stat_box(stats_bar, "MASSIMO", chart_high_var)
stat_box(stats_bar, "MINIMO", chart_low_var)
stat_box(stats_bar, "RANGE", chart_range_var)

# MAIN BODY
chart_body = Frame(chart_shell, bg="#071320")
chart_body.pack(fill="both", expand=True, padx=12, pady=12)

chart_left = Frame(chart_body, bg="#071320")
chart_left.pack(side="left", fill="both", expand=True, padx=(0, 12))

chart_canvas = Canvas(
    chart_left,
    bg=CHART_BG,
    height=360,
    highlightthickness=1,
    highlightbackground="#223d5b",
    relief="flat",
)
chart_canvas.pack(fill="both", expand=True)

chart_right = Frame(chart_body, bg="#0b1c2d", width=240)
chart_right.pack(side="right", fill="y")
chart_right.pack_propagate(False)

Label(
    chart_right,
    text="LIVE STATISTICS",
    bg="#0b1c2d",
    fg=TEXT,
    font=("Segoe UI", 10, "bold"),
).pack(anchor="w", padx=14, pady=(14, 10))

def make_side_stat(parent, title_text, value_var):
    block = Frame(parent, bg="#0b1c2d")
    block.pack(fill="x", padx=14, pady=8)

    Label(
        block,
        text=title_text,
        bg="#0b1c2d",
        fg=MUTED,
        font=("Segoe UI", 8, "bold"),
        anchor="w",
    ).pack(anchor="w")

    Label(
        block,
        textvariable=value_var,
        bg="#0b1c2d",
        fg=TEXT,
        font=("Segoe UI", 12, "bold"),
        anchor="w",
    ).pack(anchor="w", pady=(3, 0))

make_side_stat(chart_right, "Ultimo", chart_last_var)
make_side_stat(chart_right, "Massimo", chart_high_var)
make_side_stat(chart_right, "Minimo", chart_low_var)
make_side_stat(chart_right, "Range", chart_range_var)
make_side_stat(chart_right, "Modalita", chart_mode_var)

Label(
    chart_right,
    text="MARKET NOTE",
    bg="#0b1c2d",
    fg=TEXT,
    font=("Segoe UI", 10, "bold"),
).pack(anchor="w", padx=14, pady=(16, 8))

Label(
    chart_right,
    textvariable=market_hint,
    bg="#0b1c2d",
    fg=MUTED,
    font=("Segoe UI", 9),
    justify="left",
    wraplength=200,
    anchor="nw",
).pack(fill="x", padx=14)

# FOOTER
chart_footer = Frame(chart_shell, bg="#0a1a2d", height=56)
chart_footer.pack(fill="x")
chart_footer.pack_propagate(False)

Label(
    chart_footer,
    text="Storico recente",
    bg="#0a1a2d",
    fg="#8fb3d9",
    font=("Segoe UI", 9, "bold"),
).pack(side="left", padx=16)

Label(
    chart_footer,
    textvariable=chart_history_var,
    bg="#0a1a2d",
    fg=MUTED,
    font=("Segoe UI", 9),
).pack(side="right", padx=16)

# LO STORICO DECISIONI ORA NON DEVE RUBARE SPAZIO
# (applicato dopo la creazione effettiva di history_card)
history_card = ttk.Frame(bottom, style="Card.TFrame", padding=16)
history_card.pack(side="right", fill="both", expand=True, padx=(7, 0))
history_card.pack_forget()


ttk.Label(history_card, text="Storico decisioni", style="Section.TLabel").pack(anchor="w")
ttk.Label(history_card, textvariable=history_status, style="Sub.TLabel", wraplength=380).pack(anchor="w", pady=(10, 0))

chart_card.configure(height=300)
history_card.configure(height=300)
chart_card.pack_propagate(False)
history_card.pack_propagate(False)

root.after(900, refresh_live_chart)

root.mainloop()









