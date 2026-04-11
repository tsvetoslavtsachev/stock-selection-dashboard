# Architecture

Пълно описание на петслойната архитектура на Stock Selection Dashboard.

---

## Принципи на дизайна

- **No paid backend** — всичко работи с безплатни услуги (SEC EDGAR, GitHub Actions, GitHub Pages)
- **Statically served frontend** — браузърът чете само готови JSON файлове; не прави API calls
- **Modular Python** — всеки слой е независим модул; лесна замяна на data source
- **Fail-soft pipeline** — грешка в един job не спира публикуването с наличните данни
- **Commit-as-deploy** — pipeline-ът commit-ва резултатите; GitHub Pages засича промяната автоматично

---

## 5-те слоя

### Слой 1 — Ingestion (`src/jobs/fetch_sec.py`, `src/jobs/fetch_prices.py`)

Отговорност: Изтегля суровите данни от外部 sources и ги записва локално.

| Source | Endpoint | Output | Auth |
|---|---|---|---|
| SEC EDGAR | `data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json` | `data/raw/sec/{symbol}/companyfacts.json` | User-Agent header |
| SEC EDGAR | `data.sec.gov/submissions/CIK{cik10}.json` | `data/raw/sec/{symbol}/submissions.json` | User-Agent header |
| Alpha Vantage | `TIME_SERIES_WEEKLY_ADJUSTED` | `data/raw/prices/{symbol}.json` | API key (env var) |

Поведение при повторно изпълнение:
- `fetch_sec` — skip ако файловете вече съществуват (освен `--force`)
- `fetch_prices` — skip ако файлът е по-млад от `price_max_age_days` (default 1 ден)

---

### Слой 2 — Normalization (`src/jobs/compute_factors.py`, частично)

Отговорност: Парсира суровите JSON файлове и извлича числови факторни входове.

**Цени (Alpha Vantage → `ret_13w`, `ret_26w`, `ret_52w`, `volatility_26w`):**
- Чете `"Weekly Adjusted Time Series"` → adjusted close prices
- Изчислява total returns между последната и N-та bar назад
- Annualised volatility = σ(log weekly returns) × √52

**Фундаментали (SEC EDGAR → `revenue_growth_ttm`, `oper_margin_ttm`, `fcf_margin_ttm`):**
- Парсира `facts.us-gaap.{concept}.units.USD` от companyfacts JSON
- Сумира последните 4 тримесечни стойности за TTM
- Поддържани концепти:
  - Revenue: `Revenues`, `RevenueFromContractWithCustomerExcludingAssessedTax`, `SalesRevenueNet`
  - Operating Income: `OperatingIncomeLoss`
  - Operating Cash Flow: `NetCashProvidedByUsedInOperatingActivities`
  - CapEx: `PaymentsToAcquirePropertyPlantAndEquipment`
- Липсващи данни → попълва с `0.0`

---

### Слой 3 — Scoring (`src/lib/scoring.py`)

Отговорност: Нормализира всички факторни входове и изчислява composite score.

#### Нормализация

Всеки факторен вход се преобразува в **percentile rank [0, 1]** спрямо целия universe:

```
percentile_rank(series) = series.rank(method="average", pct=True)
```

Това осигурява, че всеки фактор допринася на еднаква скала, независимо от мерните единици.

#### Факторни формули

```
Trend score   = 0.40 × rank(ret_13w)
              + 0.30 × rank(ret_26w)
              + 0.30 × rank(ret_52w)

Quality score = 0.40 × rank(revenue_growth_ttm)
              + 0.30 × rank(oper_margin_ttm)
              + 0.30 × rank(fcf_margin_ttm)

Value score   = 1 − rank(ev_ebit)           ← инверсия: по-нисък EV/EBIT = по-добър
Risk score    = 1 − rank(volatility_26w)    ← инверсия: по-ниска волатилност = по-добър

Composite     = 0.35 × Trend
              + 0.30 × Quality
              + 0.20 × Value
              + 0.15 × Risk
```

Всички тегла са конфигурируеми в `config/settings.yml`.

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
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               │
   ┌──────────────┐ ┌──────────────┐       │
   │  fetch_sec   │ │ fetch_prices │       │
   │              │ │              │       │
   │ SEC EDGAR    │ │ Alpha Vantage│       │
   │ companyfacts │ │ weekly adj.  │       │
   │ submissions  │ │ time series  │       │
   └──────┬───────┘ └──────┬───────┘       │
          │                │               │
          ▼                ▼               │
   data/raw/sec/     data/raw/prices/      │
   {symbol}/          {symbol}.json        │
   companyfacts.json                       │
   submissions.json                        │
          │                │               │
          └───────┬─────────┘              │
                  │                        │
                  ▼                        │
        ┌──────────────────┐               │
        │  compute_factors │               │
        │                  │               │
        │  parse prices    │               │
        │  parse XBRL      │               │
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
  4. cp config/settings.example.yml config/settings.yml
  5. python -m src.jobs.run_pipeline
     env: ALPHA_VANTAGE_API_KEY (от GitHub Secret)
  6. git add app/data/ data/processed/ data/raw/
  7. git commit (само ако има промени)
  8. git push
```

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
    "composite_score":   0.8124,
    "trend_score":       0.9100,
    "quality_score":     0.8800,
    "value_score":       0.5500,
    "risk_score":        0.4200,
    "ret_13w":           0.3120,
    "ret_26w":           0.5180,
    "ret_52w":           0.8710,
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
| `composite_score` | float [0,1] | Претеглена сума на 4-те фактора |
| `trend_score` | float [0,1] | Momentum percentile rank |
| `quality_score` | float [0,1] | Profitability percentile rank |
| `value_score` | float [0,1] | Inverted valuation percentile rank |
| `risk_score` | float [0,1] | Inverted volatility percentile rank |
| `ret_13w` | float | 13-week price return (decimal) |
| `ret_26w` | float | 26-week price return (decimal) |
| `ret_52w` | float | 52-week price return (decimal) |
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

### Нов data source (напр. финансови данни от Yahoo Finance)

1. Създай `src/lib/yahoo_client.py` по аналогия с `alpha_vantage_client.py`
2. Добави job `src/jobs/fetch_yahoo.py`
3. Включи го в `run_pipeline.py` между `fetch_sec` и `compute_factors`
4. Не е нужна промяна в scoring или publish слоевете

### Нов фактор (напр. Momentum Acceleration)

1. Добави изчислението в `compute_factors.py` → нова колона в `ranks.csv`
2. Добави percentile rank в `scoring.py` → нова `*_score` колона
3. Добави тегло в `settings.yml` → `scoring.composite`
4. Добави колоната в `publish_site_data.py` → ще се появи в JSON-ите
5. Добави колона в таблицата на dashboard-а → `app/stock-selection-dashboard.html`
