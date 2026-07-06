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

**Фундаментали (Yahoo Finance `info` → `roe`, `oper_margin_ttm`, `fcf_margin_ttm`, `roic`,
множители, `beta`, `debt_equity`, `dividend_yield`):**
- `src/lib/yfinance_client.py:get_fundamentals` чете `yf.Ticker(symbol).info` dict
- Директни полета: `trailingPE`, `priceToBook`, `enterpriseToEbitda`, `returnOnEquity`,
  `debtToEquity` (÷100 → десетична), `operatingMargins`, `revenueGrowth`, `dividendYield`
  (÷100 → десетична), `beta`
- Апроксимации от тримесечните отчети (не директно в `info`):
  - `roic` ≈ EBIT(TTM) / (Total Assets − Current Liabilities), най-скорошното тримесечие
  - `fcf_margin_ttm` ≈ `freeCashflow / totalRevenue`, fallback `OCF_ttm − |CapEx_ttm|`
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
   по-добре); value множителите се обръщат в **доходности** (E/P, EBITDA/EV, B/P = 1/множител),
   така че по-евтин множител *и* отрицателен множител (отрицателна книга / EBITDA = лошо)
   излизат с правилния знак. Останалото вече е „по-високо = по-добре".
2. **Gaussianize** — inverse-normal (rankit): `percentile rank → Φ⁻¹`. Робъстно като ранг,
   но z-скалирано, така че опашките носят magnitude (там е сигналът).
3. **Секторна неутрализация** — пълна within-GICS-сектор стандартизация (de-mean + de-vol).
   Композитът НЕ носи секторен облог („най-атрактивното *в сектора си*"; секторният наклон
   е отделна, по-късна attribution). Сектор с < 10 члена → fallback към universe.

#### Липсващи данни

Комбинирането е coverage-aware per акция: липсващ под-фактор се изхвърля и теглото му се
преразпределя, но бъкет се скорира само при **≥ 50 % налично тегло**, иначе → **неутралната
среда 0** (след неутрализация 0 е секторната средна) — НЕ 0.5, което би било над реални акции.
Ниско-покритите бъкети се свиват към 0 пропорционално на покритието.

#### Факторни бъкети (сектор-неутрален z)

```
Trend   = 0.5·z(ret_12_1) + 0.5·z(ret_13w)                          # 12-1 skip-month + 13w
Quality = 0.25·z(roe) + 0.25·z(oper_margin) + 0.25·z(fcf_margin) + 0.25·z(roic)
Value   = 0.25·z(E/P) + 0.25·z(EBITDA/EV) + 0.25·z(B/P) + 0.25·z(div_yield)   # yield-form
Risk    = 0.3334·(−z)(vol) + 0.3333·(−z)(debt) + 0.3333·(−z)(beta)

Composite = равнопретеглена комбинация на 4-те бъкета
            (Етап D: bucket re-стандартизация + тегла от committed `config/scoring.yml`)
```

**Всички тегла са equal-weight** (виж `config/scoring.yml`) — на ниво бъкети (0.25 ×4)
и на ниво под-фактори. При липса на валидиран information coefficient (M1 rework-ът
не съдържа backtest) equal-weight е честният no-information prior; ръчно настроени
тегла биха твърдели точност, която не е измерена. Понеже бъкетите се ре-стандартизират
до единична дисперсия преди composite-а, „equal weight" значи равно ВЛИЯНИЕ, не просто
равни коефициенти.

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
| `composite_score` | float (z) | Равнопретеглена комбинация на 4-те сектор-неутрални бъкета |
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
