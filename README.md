# Stock Selection Dashboard

A fully static, factor-scored equity dashboard that runs on GitHub Actions and is served via GitHub Pages — no paid backend, no live API calls from the browser.

---

## Цел

Автоматичен pipeline, който всеки делник след US market close:

1. Изтегля фундаментални данни от **SEC EDGAR** (безплатен, без API key)
2. Изтегля седмични цени от **Alpha Vantage**
3. Изчислява **Trend / Quality / Value / Risk** factor scores за всеки тикър
4. Публикува компактни **JSON snapshots** в `app/data/`
5. **GitHub Pages** сервира статичния dashboard, който чете само тези JSON файлове

---

## Архитектура

```
GitHub Actions (cron: weekdays 22:15 UTC)
        │
        ▼
  run_pipeline.py
  ┌─────────────────────────────────────────────────────┐
  │  1. fetch_sec.py      → data/raw/sec/{symbol}/      │
  │  2. fetch_prices.py   → data/raw/prices/{symbol}.json│
  │  3. compute_factors.py→ data/processed/ranks.csv    │
  │  4. publish_site_data → app/data/*.json             │
  └─────────────────────────────────────────────────────┘
        │
        ▼
  git commit + push (app/data/, data/processed/, data/raw/)
        │
        ▼
  GitHub Pages → app/stock-selection-dashboard.html
```

Виж [`docs/architecture.md`](docs/architecture.md) за подробно описание.

---

## Файлова структура

```
stock-selection-dashboard/
│
├── .github/workflows/
│   └── update-data.yml         # Cron + manual trigger
│
├── config/
│   ├── settings.example.yml    # Шаблон — копирай като settings.yml
│   └── universe.csv            # Тикъри с CIK, сектор, industry, enabled флаг
│
├── src/
│   ├── lib/
│   │   ├── sec_client.py       # SEC EDGAR API wrapper
│   │   ├── alpha_vantage_client.py  # Alpha Vantage API wrapper
│   │   ├── io_utils.py         # Пътища, read_universe(), write_json()
│   │   └── scoring.py          # percentile_rank(), build_scores()
│   └── jobs/
│       ├── fetch_sec.py        # Job 1: изтегля SEC данни
│       ├── fetch_prices.py     # Job 2: изтегля цени
│       ├── compute_factors.py  # Job 3: изчислява фактори
│       ├── publish_site_data.py# Job 4: записва JSON за frontend
│       └── run_pipeline.py     # Orchestrator
│
├── data/                       # .gitignore-нато — не се commit-ва
│   ├── raw/
│   │   ├── sec/{symbol}/       # companyfacts.json, submissions.json
│   │   └── prices/{symbol}.json
│   └── processed/
│       └── ranks.csv
│
├── app/
│   ├── stock-selection-dashboard.html  # Статичен dashboard
│   └── data/                   # JSON snapshots — commit-ват се
│       ├── ranked_stocks.json
│       ├── market_summary.json
│       └── leaders.json
│
├── docs/
│   └── architecture.md
│
├── requirements.txt
└── README.md
```

---

## Как работи update-ът

Всеки делник в **22:15 UTC** (след US market close в 21:00 UTC):

1. GitHub Actions стартира `update-data.yml`
2. Python 3.11 се инсталира с кешрани зависимости
3. `python -m src.jobs.run_pipeline` изпълнява 4-те стъпки последователно
4. Ако дадена стъпка се провали, pipeline-ът продължава с предупреждение
5. Обновените файлове в `app/data/` се commit-ват и push-ват
6. GitHub Pages засича промяната и обновява сайта автоматично

Може да се стартира и ръчно от **Actions → Update Dashboard Data → Run workflow**.

---

## Как да инсталирам локално

```bash
# 1. Клонирай repo-то
git clone https://github.com/YOUR_USERNAME/stock-selection-dashboard.git
cd stock-selection-dashboard

# 2. Създай виртуална среда
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 3. Инсталирай зависимостите
pip install -r requirements.txt

# 4. Копирай config шаблона
cp config/settings.example.yml config/settings.yml

# 5. Редактирай config/settings.yml
#    Смени sec_user_agent с твоето реално ime и email

# 6. Задай API key като environment variable
export ALPHA_VANTAGE_API_KEY="your_key_here"   # Linux/macOS
# или
set ALPHA_VANTAGE_API_KEY=your_key_here        # Windows CMD

# 7. Пусни pipeline-а
python -m src.jobs.run_pipeline

# 8. Отвори dashboard-а локално
# Нужен е HTTP сървър (поради fetch() CORS ограниченията):
python -m http.server 8000 --directory app
# Отвори http://localhost:8000/stock-selection-dashboard.html
```

---

## Как да деплойна в GitHub

```bash
# 1. Създай нов repo в GitHub (публичен за безплатен GitHub Pages)
#    Например: https://github.com/YOUR_USERNAME/stock-selection-dashboard

# 2. Инициализирай и push-ни
git init
git add .
git commit -m "feat: initial project structure"
git remote add origin https://github.com/YOUR_USERNAME/stock-selection-dashboard.git
git push -u origin main
```

---

## Стъпки за GitHub Pages

1. Отиди в **repo → Settings → Pages**
2. Source: **Deploy from a branch**
3. Branch: `main`, Folder: `/app`
4. Запази — след ~1 минута dashboard-ът е достъпен на:
   `https://YOUR_USERNAME.github.io/stock-selection-dashboard/stock-selection-dashboard.html`

> **Важно:** GitHub Pages трябва да сервира от `app/` директорията,  
> за да може `./data/ranked_stocks.json` да се резолвира правилно.

---

## Environment variables и secrets

| Variable | Откъде | Описание |
|---|---|---|
| `ALPHA_VANTAGE_API_KEY` | GitHub Secret | API key за Alpha Vantage |

### Добавяне на secret в GitHub

1. Repo → **Settings → Secrets and variables → Actions**
2. **New repository secret**
3. Name: `ALPHA_VANTAGE_API_KEY`
4. Value: твоят API key от [alphavantage.co](https://www.alphavantage.co/support/#api-key)

> SEC EDGAR не изисква API key — само коректен `User-Agent` header,  
> конфигуриран в `config/settings.yml` → `api.sec_user_agent`.

---

## Как да разширя universe-а

1. Отвори `config/universe.csv`
2. Добави нов ред по формата:
   ```
   TICKER,CIK_NUMBER,Company Name,Sector,Industry,1
   ```
3. CIK номера намираш на [efts.sec.gov/LATEST/search-index?q=%22TICKER%22&dateRange=custom](https://efts.sec.gov/LATEST/search-index?q=%22AAPL%22&dateRange=custom)  
   или директно: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=COMPANY_NAME&type=10-K`
4. Задай `enabled=1` — `enabled=0` изключва тикъра без да го трива
5. Pipeline-ът ще го включи при следващото изпълнение

> Лимит: `pipeline.max_universe_size: 100` в settings.yml  
> Alpha Vantage free tier: 25 заявки/ден — намали universe-а  
> или надгради до premium key за по-голям universe

---

## TODO

- [ ] Добавяне на EV/EBIT изчисление (изисква market cap source)
- [ ] FCF margin от SEC за всички тикъри (coverage varies)
- [ ] Sector heatmap визуализация в dashboard-а
- [ ] Исторически snapshot архив (rolling 52-week history)
- [ ] Backtesting модул за factor performance validation
- [ ] Поддръжка на ETF overlays (SPY, QQQ, XLK като benchmarks)
- [ ] Export to CSV бутон в dashboard-а
- [ ] Email/Slack нотификация при значима промяна в rankings
