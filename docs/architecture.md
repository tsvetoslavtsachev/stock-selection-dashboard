# Architecture

Пълно описание на петслойната архитектура на Stock Selection Dashboard.

---

## Принципи на дизайна

- **No paid backend** — всичко работи с безплатни услуги (Yahoo Finance чрез `yfinance`,
  каноничния price-archive, GitHub Actions, GitHub Pages)
- **Statically served frontend** — браузърът чете само готови JSON файлове; не прави API calls
- **Modular Python** — всеки слой е независим модул; лесна замяна на data source
- **Fail-soft pipeline** — грешка в един job не спира публикуването с наличните данни
- **Commit-as-deploy** — pipeline-ът commit-ва резултатите; GitHub Pages засича промяната автоматично

---

## 5-те слоя

### Слой 1 — Ingestion (`src/jobs/fetch_prices.py`)

Отговорност: Изтегля суровите данни от външни sources и ги записва локално.

| Source | Какво | Output | Auth |
|---|---|---|---|
| price-archive (каноничен, INIT-22 P9) | split-adjusted, drift-proof total-return close, base-first през `collectors.price.consumer.load_ohlcv_base_first` | `data/raw/prices/{symbol}.csv` | DATACORE_ROOT (private) |
| Yahoo Finance (`yfinance`) | CLOSED fallback за липсващи в архива тикери (дневен close) | `data/raw/prices/{symbol}.csv` | — |
| Yahoo Finance (`yfinance` `.info`) | фундаментали (виж Слой 2) | in-memory при `compute_factors` | — |

Цените идват **base-first от каноничния price-archive**; per-ticker yfinance pull-ът е
запазен само като CLOSED fallback за тикери, липсващи в архива (RIV-2 capstone: base-ът
съвпада с `yfinance auto_adjust=True` до ~1e-6).

Поведение при повторно изпълнение:
- `fetch_prices` — skip ако файлът е по-млад от `price_max_age_days` (default 1 ден)

> `src/jobs/fetch_sec.py` + `src/lib/sec_client.py` (SEC EDGAR XBRL) и
> `src/lib/alpha_vantage_client.py` съществуват, но **НЕ са част от живия pipeline**
> (`run_pipeline` вика само `fetch_prices → compute_factors → publish_site_data`).
> SEC fetching е опционален инструмент за дълбок XBRL анализ, не production вход.

---

### Слой 2 — Normalization (`src/jobs/compute_factors.py`, частично)

Отговорност: Парсира суровите JSON файлове и извлича числови факторни входове.

**Цени (price-archive base-first → `ret_12_1`, `ret_13w`, `volatility_26w`):**
- Чете каноничния total-return close (`data/raw/prices/{symbol}.csv`), resample-нат до седмичен
- Изчислява total returns между последната и N-та bar назад
- Annualised volatility = σ(log weekly returns) × √52

**Фундаментали (Yahoo Finance `info` + тримесечни отчети):**
- `src/lib/yfinance_client.py:get_fundamentals` чете `yf.Ticker(symbol).info` dict
- Директни полета: `trailingPE`, `priceToBook`, `enterpriseToEbitda`, `returnOnEquity`,
  `debtToEquity` (÷100 → десетична), `operatingMargins`, `revenueGrowth`, `dividendYield`
  (÷100 → десетична; версийно-зависима конвенция — `yfinance` е **пинат** в requirements), `beta`
- Апроксимации от тримесечните отчети (не директно в `info`):
  - `roic` ≈ EBIT(TTM) / (Total Assets − Current Liabilities), най-скорошното тримесечие
  - `fcf_margin_ttm` ≈ `freeCashflow / totalRevenue`, fallback `OCF_ttm − |CapEx_ttm|`
  - `gpa` = Gross Profit(TTM) / Total Assets (Novy-Marx), fallback `totalRevenue × grossMargins`
  - `net_payout_yield` = (Cash Dividends Paid + Repurchase Of Capital Stock)_TTM / market_cap
    (абсолютни стойности; липса на ЕДНОТО = 0, липса на ДВЕТЕ = `None`)
- `ev_ebit` е **премахнат** (беше дубликат на EV/EBITDA — 472/472 идентични)
- Липсваща стойност → `None` (обработва се надолу: coverage-aware, НЕ 0.0-fill)

> Историческият дизайн предвиждаше SEC EDGAR XBRL (`facts.us-gaap.{concept}`) като
> източник на фундаментали, но живият pipeline го замени с Yahoo `info`. `sec_client.py`
> остава за опционален дълбок XBRL анализ, не за production scoring.

---

### Слой 3 — Scoring (`src/lib/scoring.py`)  ·  INIT-22 M1 rework

Отговорност: Нормализира факторните входове **секторно-неутрално** и изчислява composite.

#### Нормализация

Всеки под-фактор се изразява като **сигнал „по-високо = по-добре"** и минава през един
общ конвейер — има точно **едно** място, където се прилага посока (`_signal`), затова
разместен знак се хваща от directional тест:

1. **Посока** — рисковите входове (vol / debt / beta) се негат (по-ниско = по-безопасно =
   по-добре); двата скорирани value множителя се обръщат в **доходности** (E/P, EBITDA/EV =
   1/множител), така че по-евтин множител *и* отрицателен множител (отрицателни печалби /
   EBITDA = лошо) излизат с правилния знак. net_payout_yield вече е доходност (не се обръща).
   P/B **не се скорира** в M2 (отрицателна книга при buyback-тежка large-cap не е нито „евтина",
   нито дистрес-сигнал — затова B/P просто не участва). Останалото вече е „по-високо = по-добре".
2. **Gaussianize** — inverse-normal (rankit): `percentile rank → Φ⁻¹`. Робъстно като ранг,
   но z-скалирано, така че опашките носят magnitude (там е сигналът).
3. **Секторна неутрализация** — пълна within-GICS-сектор стандартизация (de-mean + de-vol).
   Композитът НЕ носи секторен облог („най-атрактивното *в сектора си*"; секторният наклон
   е отделна, по-късна attribution). Сектор с < 10 члена → fallback към universe.

#### Липсващи данни

Комбинирането е coverage-aware per акция: липсващ под-фактор се изхвърля и теглото му се
преразпределя, но бъкет се скорира само при **≥ 50 % налично тегло**, иначе → **неутралната
среда 0** (след неутрализация 0 е секторната средна) — НЕ 0.5, което би било над реални акции.
Present-but-partial бъкети пазят честната претеглена средна на наличните сигнали — **няма**
coverage shrink (той би върнал тихо същия missing-data bias, който този дизайн премахва, и е
излишен покрай единично-дисперсионната ре-стандартизация при composite-а).

#### Факторни бъкети (сектор-неутрален z) — M2

```
Trend   = z(ret_12_1)                                               # само 12-1 skip-month
Quality = 0.25·z(roe) + 0.25·z(oper_margin) + 0.25·z(fcf_margin) + 0.25·z(gpa)
Value   = 0.3334·z(E/P) + 0.3333·z(EBITDA/EV) + 0.3333·z(net_payout_yield)   # yield-form
Risk    = 0.3334·(−z)(vol) + 0.3333·(−z)(debt) + 0.3333·(−z)(beta)   # РЕЖИМНА ЛЕЩА

Composite = Trend + Quality + Value  (Risk НЕ участва — отделна режимна леща)
            → 3-те бъкета се ре-стандартизират до единична дисперсия, после се смесват с
              equal-contribution (ERC) тегла върху ковариацията им
```

**Composite = Trend + Quality + Value** (M2). Risk продължава да се смята и показва, но
**не влиза в composite-а** — той е отделна режимна леща (режимният облог живее във VRM,
не в този cross-sectional ранкер). ret_13w, P/B, dividend_yield и ROIC **вече не се скорират**
(остават видими UI колони): ret_13w не носи incremental сигнал; P/B е одитираният дефект
(quality inversion + секторен шум); dividend_yield е доминиран от net_payout_yield; ROIC е
шумен proxy (~0.85 z-корелация с ROE, недокументирани bias-и).

Composite-ните тегла в `config/scoring.yml` (равни трети) са **целевите дялове на принос**,
не сурови коефициенти: scoring.py решава equal-contribution (ERC) тегла върху ковариацията на
бъкетите (портнатият Spinu 2013 солвър от backtest рамката), така че всеки бъкет носи равен
ДЯЛ от дисперсията на composite-а. „Равни тегла = равно влияние" така е вярно по конструкция,
не претенция — корелирани бъкети вече не доминират тихо обикновена претеглена средна. При
изродена ковариация (напр. едно-секторна вселена) composite-ът пада към номиналните трети.

Скоровете са **z-стойности** (средно ~0, могат да са отрицателни), не `[0, 1]`.

---

### Слой 4 — Publish (`src/jobs/publish_site_data.py`)

Отговорност: Чете `data/processed/ranks.csv` и записва 3 JSON файла в `app/data/`.

| Файл | Съдържание |
|---|---|
| `ranked_stocks.json` | Пълен universe — масив от обекти, сортирани по composite_score desc |
| `market_summary.json` | Агрегирани метрики: universe_size, top_symbol, median_composite, as_of |
| `leaders.json` | Top N (default 10) по composite_score |

Всички `NaN` и `Inf` стойности се заменят с `null` преди сериализация.

---

### Слой 5 — Frontend (`app/stock-selection-dashboard.html`)

Отговорност: Статичен dashboard, сервиран от GitHub Pages.

- Зарежда само `./data/*.json` — никакви live API calls
- Вградени CSS и JS в един файл — нулеви external зависимости (освен Google Fonts CDN)
- Функционалности: KPI bar, leaders strip, sortable/filterable таблица, row drill-down, light/dark toggle

---

## Data Flow диаграма

```
┌────────────────────────────────────────────────────────────────────┐
│                    GitHub Actions (cron)                           │
│                      22:15 UTC Mon–Fri                             │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
              python -m src.jobs.run_pipeline
                           │
                           ▼                │
                   ┌──────────────┐         │
                   │ fetch_prices │         │
                   │              │         │
                   │ price-archive│         │
                   │ (base-first) │         │
                   │ yfinance     │         │
                   │ (CLOSED f/b) │         │
                   └──────┬───────┘         │
                          │                 │
                          ▼                 │
                   data/raw/prices/         │
                    {symbol}.csv            │
                          │                 │
                          ▼                 │
        ┌──────────────────┐               │
        │  compute_factors │               │
        │                  │               │
        │  parse prices    │               │
        │  yfinance .info  │  ← фундаментали │
        │  percentile_rank │               │
        │  build_scores    │               │
        └────────┬─────────┘               │
                 │                         │
                 ▼                         │
        data/processed/ranks.csv           │
                 │                         │
                 ▼                         │
        ┌──────────────────┐               │
        │ publish_site_data│               │
        └────────┬─────────┘               │
                 │                         │
                 ▼                         │
        app/data/                          │
        ├── ranked_stocks.json             │
        ├── market_summary.json            │
        └── leaders.json                   │
                 │                         │
                 └─────────────────────────┘
                           │
                   git commit + push
                           │
                           ▼
                   GitHub Pages
                           │
                           ▼
              Browser ← fetch('./data/*.json')
              stock-selection-dashboard.html
```

---

## GitHub Actions роля

Файл: `.github/workflows/update-data.yml`

```
Triggers:
  schedule: cron '15 22 * * 1-5'   ← 22:15 UTC всеки делник
  workflow_dispatch                  ← ръчно от GitHub UI

Permissions:
  contents: write                    ← за git push

Steps:
  1. checkout (fetch-depth: 0)
  2. Python 3.11 + pip cache
  3. pip install -r requirements.txt
  4. python -m pytest tests/ -q
  5. cp config/settings.example.yml config/settings.yml
  6. checkout data-core + price-archive + collectors (само ако двата RO PAT-а са зададени)
     → base-first канонични цени; иначе fetch_prices пада към DAILY yfinance fallback
  7. python -m src.jobs.run_pipeline
     env: DATACORE_ROOT, PYTHONPATH (сочат checkout-натите base репота)
  8. python scripts/assert_base_sourced.py  (RED при тих mass-fallback към yfinance)
  9. git add app/data/ price_source.json
 10. git commit (само ако има промени) + push
```

Секрети: `DATACORE_RO_PAT` + `PRICE_ARCHIVE_RO_PAT` (read-only checkout на каноничните
base репота). Няма `ALPHA_VANTAGE_API_KEY` — Alpha Vantage не е част от живия pipeline.

`config/settings.yml` се генерира в стъпка 4 от settings.example.yml.  
Никога не се commit-ва — само се ползва runtime в CI.

---

## JSON Schema

### `ranked_stocks.json`

Масив от обекти. Сортиран по `composite_score` descending.

```json
[
  {
    "rank":              1,
    "ticker":            "NVDA",
    "name":              "NVIDIA Corp.",
    "sector":            "Technology",
    "composite_score":   0.9800,
    "trend_score":       1.4200,
    "quality_score":     1.1000,
    "value_score":      -0.3500,
    "risk_score":       -0.2100,
    "ret_12_1":          0.4450,
    "ret_13w":           0.3120,
    "volatility_26w":    0.4200,
    "revenue_growth_ttm":0.1220,
    "oper_margin_ttm":   0.5540,
    "fcf_margin_ttm":    0.4810
  },
  ...
]
```

| Поле | Тип | Описание |
|---|---|---|
| `rank` | integer | Ранг в universe (1 = най-добър) |
| `ticker` | string | Stock symbol |
| `name` | string | Company name |
| `sector` | string | GICS сектор |
| `composite_score` | float (z) | ERC-претеглена комбинация на 3-те composite бъкета (Trend+Quality+Value; Risk е отделна леща) |
| `trend_score` | float (z) | Сектор-неутрален momentum (12-1 + 13w) |
| `quality_score` | float (z) | Сектор-неутрален profitability |
| `value_score` | float (z) | Сектор-неутрален yield-form valuation |
| `risk_score` | float (z) | Сектор-неутрален (обърнат) риск |
| `ret_12_1` | float | 12-1 skip-month momentum (decimal) |
| `ret_13w` | float | 13-week price return (decimal) |
| `volatility_26w` | float | Annualised 26-week volatility (decimal) |
| `revenue_growth_ttm` | float | TTM revenue YoY growth (decimal) |
| `oper_margin_ttm` | float | TTM operating margin (decimal) |
| `fcf_margin_ttm` | float | TTM FCF margin (decimal) |

---

### `market_summary.json`

```json
{
  "universe_size":    20,
  "top_symbol":       "NVDA",
  "top_score":        0.8124,
  "top_sector":       "Technology",
  "bottom_symbol":    "XOM",
  "bottom_score":     0.2710,
  "median_composite": 0.5230,
  "as_of":            "2026-04-11T21:00:00Z"
}
```

---

### `leaders.json`

Същата схема като `ranked_stocks.json`, но само top N записи (default 10).

---

## Разширяване

### Нов data source (напр. алтернативен фундаментален feed)

1. Създай `src/lib/{source}_client.py` по аналогия с `yfinance_client.py`
2. Добави job `src/jobs/fetch_{source}.py`
3. Включи го в `run_pipeline._ALL_STEPS` преди `compute_factors`
4. Не е нужна промяна в scoring или publish слоевете

### Нов фактор (напр. Momentum Acceleration)

1. Добави изчислението в `compute_factors.py` → нова колона в `ranks.csv`
2. Добави percentile rank в `scoring.py` → нова `*_score` колона
3. Добави тегло в `settings.yml` → `scoring.composite`
4. Добави колоната в `publish_site_data.py` → ще се появи в JSON-ите
5. Добави колона в таблицата на dashboard-а → `app/stock-selection-dashboard.html`
