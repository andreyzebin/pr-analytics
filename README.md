# pr_analytics.py

Инструмент для анализа Pull Request из **Bitbucket Server (On-Premise)**. Кеширует PR, комментарии и реакции в локальную SQLite-базу. Все аналитические команды работают без сети.

## Требования

- Python 3.9+
- Зависимости: `requests`, `pandas`, `matplotlib`, `tabulate`, `pyyaml`, `pytest`
- Клиентский TLS-сертификат и CA-бандл (если требует инстанс)

## Установка

```bash
git clone ...
cd pr-analytics
bash setup_venv.sh
```

Скрипт создаёт `.venv/`, устанавливает зависимости и печатает инструкции по настройке.

## Тесты

```bash
.venv/bin/pytest tests/ -v
```

59 тестов без сетевого доступа: конфиг, БД (schema, upsert, threading, реакции), команды (sql, find-repos, status, review-feedback).

## Конфигурация

Конфигурация читается послойно — каждый слой переопределяет предыдущий:

```
config.yaml          ← база, хранится в VCS
config.local.yaml    ← локальные переопределения, в .gitignore
переменные окружения ← наивысший приоритет
CLI-аргументы        ← переопределяют config, но не env vars
```

### config.yaml (база, коммитится)

```yaml
bitbucket:
  url: ""
  token: "${BB_TOKEN}"
  ca_bundle: "${REQUESTS_CA_BUNDLE}"
  client_cert: "${BITBUCKET_SERVER_CLIENT_CERT}"

cache:
  db: bitbucket_cache.db
  concurrency: 4
```

Строки вида `${VAR}` раскрываются из переменных окружения.

### config.local.yaml (локально, не коммитится)

Переопределяет нужные поля. Например:

```yaml
bitbucket:
  url: "https://bitbucket.example.com"
  ca_bundle: "/home/user/certs/ca-bundle.pem"
  client_cert: "/home/user/certs/client.pem"

cache:
  concurrency: 8
```

### .env (секреты, не коммитится)

```bash
cp .env.example .env
# заполните значения
source .env
```

### Переменные окружения

| Переменная | Описание |
|---|---|
| `BB_TOKEN` / `BITBUCKET_SERVER_BEARER_TOKEN` | Personal Access Token |
| `BB_URL` | Базовый URL инстанса |
| `BB_DB` | Путь к SQLite-файлу |
| `REQUESTS_CA_BUNDLE` | Путь к CA-бандлу |
| `BITBUCKET_SERVER_CLIENT_CERT` | Путь к клиентскому PEM (mTLS) |

> В CI/CD достаточно выставить только `BB_TOKEN` — остальное берётся из `config.yaml`.

---

## Команды

Все команды поддерживают `--db <path>` и `--log-level DEBUG|INFO|WARNING|ERROR`.

### `cache` — загрузить данные из Bitbucket

```bash
.venv/bin/python pr_analytics.py cache \
  --token $BB_TOKEN \
  --url https://bitbucket.example.com \
  --projects PROJ1,PROJ2 \
  --since 2026-01-01 --until 2026-03-31 \
  --concurrency 8
```

| Параметр | Описание |
|---|---|
| `--token` | Personal Access Token |
| `--url` | Базовый URL инстанса |
| `--since` / `--until` | Диапазон по дате создания PR (`YYYY-MM-DD`) |
| `--projects` | Comma-separated ключи проектов |
| `--repos` | Comma-separated `PROJ/repo`. Приоритет выше `--projects` |
| `--concurrency` | Параллельных потоков (по умолчанию: 4) |
| `--no-comments` | Не загружать комментарии и реакции (быстрый режим) |

Без `--projects` и `--repos` загружаются все доступные проекты.

При повторном запуске PR и комментарии перезаписываются (upsert).

---

### `plot` — графики метрик PR

**Типы вывода (`--type`):**

| Тип | Описание |
|---|---|
| `box` | Boxplot cycle time по серии (по умолчанию) |
| `trend` | Линейный/столбчатый график метрик по времени |
| `points` | Отсортированный список значений в stdout, файл не создаётся |

**Доступные метрики (`--metrics`, только для `trend`):**

| Метрика | Описание |
|---|---|
| `cycle_time` | Медианное время PR от создания до закрытия (часы) |
| `acceptance_rate` | MERGED / (MERGED + DECLINED) × 100% |
| `throughput` | Количество смерженных PR за период |

Метрики можно комбинировать через запятую.

#### Примеры

```bash
# Boxplot cycle time
.venv/bin/python pr_analytics.py plot \
  --repos "PROJ1/backend,PROJ2/frontend" \
  --since 2026-01-01 --until 2026-03-31 \
  --state MERGED --output output/chart.png

# Тренд одной метрики по месяцам
.venv/bin/python pr_analytics.py plot \
  --projects PROJ1 --since 2025-01-01 --state MERGED \
  --type trend --period month --metrics cycle_time \
  --output output/cycle_time.png

# Две метрики стопкой (два subplot'а)
.venv/bin/python pr_analytics.py plot \
  --projects PROJ1 --since 2025-01-01 --state MERGED \
  --type trend --period month \
  --metrics cycle_time,acceptance_rate \
  --layout stack --output output/metrics.png

# Две метрики на одном графике с двойной осью Y
.venv/bin/python pr_analytics.py plot \
  --projects PROJ1 --since 2025-01-01 --state MERGED \
  --type trend --period month \
  --metrics cycle_time,acceptance_rate \
  --layout overlay --output output/overlay.png

# Эффект AI-агента: split на два когорта по наличию ревьювера
.venv/bin/python pr_analytics.py plot \
  --projects MTRAVEL --since 2025-01-01 --state MERGED \
  --type trend --period month \
  --metrics cycle_time,acceptance_rate \
  --split reviewer:ai-review-bot \
  --layout stack --output output/ai_effect.png

# Сырые точки для отладки
.venv/bin/python pr_analytics.py plot \
  --repos "PROJ1/backend" --since 2026-01-01 \
  --type points
```

**Параметры:**

| Параметр | Описание |
|---|---|
| `--repos` / `--projects` / `--repos-file` | Источник репозиториев |
| `--state` | `MERGED` (по умолчанию), `DECLINED`, `OPEN` |
| `--type` | `box` / `trend` / `points` |
| `--metrics` | Comma-separated метрики для trend (default: `cycle_time`) |
| `--period` | `month` (по умолчанию) или `week` — для trend |
| `--layout` | `stack` (subplot'ы, по умолчанию) или `overlay` (dual y-axis, только 2 метрики) |
| `--split` | `reviewer:<slug>` — разделить на два когорта по наличию ревьювера |
| `--reviewer` | `include:<slug>` или `exclude:<slug>` — фильтр датасета (не разбивает) |
| `--output` | `.png` или `.svg` |

**Про `--split`:** все репозитории агрегируются в два когорта — PR где указанный аккаунт был ревьювером и PR где его не было. На графике — две линии на каждой метрике. Основной сценарий: показать влияние AI-агента код-ревью на метрики процесса.

Cycle Time = `closed_date − created_date` в часах. Все trend-метрики группируются по `closed_date`.

**Добавить новую метрику** (DORA, PDLC и др.): написать функцию `(rows, period, state) -> dict[str, float]` и добавить запись в `METRICS` в `pa/metrics.py`. Рендер-слой менять не нужно.

---

### `find-repos` — репозитории по ревьюверу

Возвращает все репозитории, где пользователь был формальным ревьювером.

```bash
.venv/bin/python pr_analytics.py find-repos \
  --reviewer ivan.ivanov \
  --state MERGED \
  --since 2026-01-01 \
  --output repos.txt
```

Вывод — `PROJ/repo`, по одному в строке. Файл совместим с `plot --repos-file`.

---

### `sql` — произвольный SELECT

```bash
.venv/bin/python pr_analytics.py sql \
  --query "SELECT state, COUNT(*) FROM pull_requests GROUP BY state" \
  --format table
```

```bash
.venv/bin/python pr_analytics.py sql \
  --file query.sql \
  --output result.csv \
  --format csv \
  --limit 0
```

- Разрешены только `SELECT` и `WITH`. Модифицирующие запросы → exit code `5`.
- `--limit 0` снимает ограничение (по умолчанию: 10 000 строк).
- Форматы: `table` (по умолчанию), `csv`, `json`.

---

### `status` — состояние кэша

```bash
.venv/bin/python pr_analytics.py status
```

Выводит число проектов, репозиториев, PR, комментариев, реакций, диапазон дат и размер файла БД.

---

### `review-feedback` — обратная связь AI-агента

Выгружает корневые комментарии указанного автора вместе с реакциями людей и ответами в треде. Предназначена для анализа качества код-ревью AI-агента.

```bash
.venv/bin/python pr_analytics.py review-feedback \
  --author ai-review-bot \
  --since 2026-01-01 --until 2026-03-31 \
  --state MERGED \
  --min-reactions 1 \
  --output feedback.csv \
  --format csv
```

```bash
# Полная структура для загрузки в LLM
.venv/bin/python pr_analytics.py review-feedback \
  --author ai-review-bot \
  --since 2026-01-01 \
  --format json \
  --output feedback_for_llm.json
```

| Параметр | Описание |
|---|---|
| `--author` | Slug автора (AI-агент) |
| `--min-reactions` | Показывать только комментарии с ≥ N реакциями |
| `--state` | Фильтр по статусу PR |
| `--repos` / `--projects` / `--repos-file` | Фильтр по репозиториям |
| `--format` | `table` (по умолчанию), `csv`, `json` |

**Поля вывода:** `repo`, `pr_id`, `pr_title`, `comment_id`, `created_date`, `file_path`, `line_from`, `severity`, `comment_text`, `reactions_positive`, `reactions_negative`, `reactions_other`, `reactions_detail`, `replies_count`, `replies`.

Классификация реакций:
- Позитивные: `+1`, `thumbsup`, `heart`, `tada`
- Негативные: `-1`, `thumbsdown`
- Прочие: всё остальное

---

## Примеры сценариев

### Кеш + график за квартал

```bash
export BB_TOKEN=...
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/company-ca.pem
export BITBUCKET_SERVER_CLIENT_CERT=~/.certs/client.pem

.venv/bin/python pr_analytics.py cache \
  --url https://bitbucket.example.com \
  --projects MYPROJ \
  --since 2026-01-01 --until 2026-03-31 \
  --concurrency 8

.venv/bin/python pr_analytics.py find-repos \
  --reviewer alex.smith --state MERGED --since 2026-01-01 \
  --output repos.txt

.venv/bin/python pr_analytics.py plot \
  --repos-file repos.txt \
  --since 2026-01-01 --until 2026-03-31 \
  --output chart.png
```

### Самые оспариваемые комментарии агента

```bash
.venv/bin/python pr_analytics.py sql --query "
SELECT
    r.project_key || '/' || r.slug AS repo,
    c.pr_id,
    c.id AS comment_id,
    c.text AS agent_comment,
    COUNT(DISTINCT cr.author) FILTER (WHERE cr.emoji IN ('-1','thumbsdown')) AS negative,
    COUNT(DISTINCT replies.id) AS replies
FROM pr_comments c
JOIN repos r ON r.id = c.repo_id
LEFT JOIN comment_reactions cr ON cr.comment_id = c.id
LEFT JOIN pr_comments replies ON replies.parent_id = c.id AND replies.author != c.author
WHERE c.author = 'ai-review-bot' AND c.parent_id IS NULL
GROUP BY c.id
HAVING negative > 0 OR replies > 1
ORDER BY negative DESC, replies DESC
LIMIT 20
" --format table
```

---

## Коды выхода

| Код | Ситуация |
|---|---|
| `0` | Успех |
| `1` | Ошибка аргументов |
| `2` | Ошибка аутентификации (401, 403) |
| `3` | Сетевая ошибка (таймаут после повторных попыток) |
| `4` | Данные отсутствуют в БД |
| `5` | Запрещённая SQL-операция |

## Структура БД

```
bitbucket_cache.db
├── projects          key, name, cache_date
├── repos             id, project_key, slug, name
├── pull_requests     repo_id, pr_id, title, author, created_date,
│                     closed_date, updated_date, state, reviewers (JSON)
├── pr_comments       id, repo_id, pr_id, parent_id, author, text,
│                     created_date, updated_date, severity, state,
│                     file_path, line, line_type, file_type
└── comment_reactions comment_id, author, emoji
```

`reviewers` — JSON-массив slug'ов, доступен через `json_each()` в SQL-запросах.
