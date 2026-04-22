# pr_analytics.py

Инструмент для анализа Pull Request из **Bitbucket Server (On-Premise)**. Кеширует PR, комментарии и реакции в локальную SQLite-базу. Все аналитические команды работают без сети.

## Содержание

- [Требования](#требования)
- [Установка](#установка)
- [Тесты](#тесты)
- [Конфигурация](#конфигурация)
  - [config.yaml](#configyaml-база-коммитится)
  - [config.local.yaml](#configlocalyaml-локально-не-коммитится)
  - [.env](#env-секреты-не-коммитится)
  - [Переменные окружения](#переменные-окружения)
- [SSL/TLS и mTLS](#ssltls-и-mtls)
  - [Корпоративные прокси (CheckPoint, Zscaler)](#корпоративные-прокси)
  - [Mutual TLS (клиентский сертификат)](#mutual-tls-клиентский-сертификат)
  - [Конвертация P12 → PEM](#конвертация-p12--pem)
  - [Где найти клиентский сертификат на Windows](#где-найти-клиентский-сертификат-на-windows)
- [Команды](#команды)
  - [cache](#cache--загрузить-данные-из-bitbucket)
  - [plot](#plot--графики-метрик-pr)
  - [find-repos](#find-repos--репозитории-по-ревьюверу-или-комментатору)
  - [find-prs](#find-prs--список-pr-по-фильтрам)
  - [find-comments](#find-comments--список-комментариев-по-фильтрам)
  - [sql](#sql--произвольный-select)
  - [status](#status--состояние-кэша)
  - [analyze-feedback](#analyze-feedback--llm-оценка-замечаний-ai-агента)
  - [analyze-merges](#analyze-merges--проверка-влияния-комментариев-на-код-через-diff)
  - [acceptance](#acceptance--метрики-по-поколению-агента-diffgraph)
  - [review-feedback](#review-feedback--обратная-связь-ai-агента)
  - [select-golden](#select-golden--отбор-эталонных-pr-для-бенчмарка)
- [Примеры сценариев](#примеры-сценариев)
- [Коды выхода](#коды-выхода)
- [Структура БД](#структура-бд)

## Требования

- Python 3.9+
- Зависимости: `requests`, `pandas`, `matplotlib`, `tabulate`, `pyyaml`, `truststore`, `pytest`
- Клиентский TLS-сертификат (если требует инстанс); CA-бандл опционален — по умолчанию используются сертификаты ОС

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
| `REQUESTS_CA_BUNDLE` | Путь к CA-бандлу (если не задан — системные сертификаты ОС через `truststore`) |
| `BITBUCKET_SERVER_CLIENT_CERT` | Путь к клиентскому PEM (mTLS) |
| `ANTHROPIC_API_KEY` | API-ключ Anthropic (для LLM-судьи) |
| `DEEPSEEK_API_KEY` | API-ключ DeepSeek (для LLM-судьи) |

> В CI/CD достаточно выставить только `BB_TOKEN` — остальное берётся из `config.yaml`.

---

## SSL/TLS и mTLS

### Корпоративные прокси

При запуске автоматически подключается библиотека `truststore`, которая использует хранилище сертификатов ОС (Windows Certificate Store, macOS Keychain, Linux `/etc/ssl/certs`). Это решает проблему с корпоративными прокси (CheckPoint, Zscaler), которые подменяют TLS-сертификаты — их CA автоматически подхватывается без явного указания `REQUESTS_CA_BUNDLE`.

Если автоматическое определение не работает — укажите CA-бандл явно:

```yaml
# config.local.yaml
bitbucket:
  ca_bundle: "/path/to/corporate-ca.crt"
```

### Mutual TLS (клиентский сертификат)

Некоторые корпоративные Bitbucket Server требуют клиентский сертификат (mTLS) в дополнение к Bearer-токену.

```yaml
# config.local.yaml
bitbucket:
  ca_bundle: "/path/to/corporate-ca.crt"
  client_cert: "/home/user/certs/client.pem"
```

Или через переменные окружения:

```bash
export REQUESTS_CA_BUNDLE=/path/to/corporate-ca.crt
export BITBUCKET_SERVER_CLIENT_CERT=/path/to/client.pem
```

### Конвертация P12 → PEM

Корпоративные сертификаты часто выдаются в формате `.p12` (PKCS#12). Python `requests` работает с PEM. Конвертация:

```bash
# Linux / macOS
openssl pkcs12 -in client.p12 -out client.pem -nodes -passin pass:<password>
chmod 600 client.pem

# Windows (Git Bash или WSL)
openssl pkcs12 -in client.p12 -out client.pem -nodes -passin pass:<password>

# Windows (PowerShell, если openssl установлен через chocolatey/scoop)
openssl pkcs12 -in .\client.p12 -out .\client.pem -nodes -passin pass:<password>
```

Если `openssl` не установлен на Windows — можно установить через `choco install openssl` или `scoop install openssl`.

### Где найти клиентский сертификат на Windows

1. **Диспетчер сертификатов:** Win+R → `certmgr.msc` → «Личное» → «Сертификаты»
2. Найти сертификат выданный вашей организацией (обычно содержит ваше ФИО или логин)
3. Правый клик → «Все задачи» → «Экспорт…»
4. Выбрать «Да, экспортировать закрытый ключ» → формат PKCS#12 (.p12)
5. Задать пароль → сохранить файл
6. Сконвертировать в PEM командой выше

Альтернативно, если сертификат выдан через корпоративный портал или VPN-клиент — файл `.p12` может лежать в:
- `%APPDATA%\` или `%LOCALAPPDATA%\`
- Папке VPN-клиента (CheckPoint, Cisco AnyConnect)
- Директории, указанной администратором

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

| Метрика | Y-ось | Описание |
|---|---|---|
| `cycle_time` | лог | Медианное время PR от создания до закрытия (часы) |
| `acceptance_rate` | линейная | MERGED / (MERGED + DECLINED) × 100% |
| `throughput` | линейная | Количество смерженных PR за период |
| `total_prs` | линейная | Всего PR (MERGED + DECLINED) за период |
| `total_repos` | bar | Активных репозиториев за период (уникальные репы с MERGED PR, по `created_date`). С `--split reviewer/commenter` — "+агент" = репы где ≥1 PR с агентом, "-агент" = репы с 0 PR с агентом (для отслеживания adoption) |
| `time_to_first_comment` | лог | Медианное время до первого комментария от не-автора (часы) |
| `agent_comments` | bar | Суммарное число корневых замечаний AI-агента за период (требует `--author`) |
| `feedback_rate` | линейная | % замечаний агента, на которые отреагировали (реакция или ответ), требует `--author` |
| `feedback_acceptance_rate` | линейная | yes/(yes+no) — среди замечаний с фидбеком (LLM-судья, требует `--author`) |
| `feedback_acceptance_rate_all` | линейная | yes/все_замечания — знаменатель включает замечания без фидбека (LLM-судья, требует `--author`) |
| `merge_acceptance_rate` | линейная | (YES + 0.5×PARTIAL) / total — комментарии, подтверждённые diff'ом (требует `analyze-merges` + `--author`) |
| `feedback_yes` | bar | Принятых (yes) по фидбеку за период |
| `feedback_no` | bar | Отклонённых (no) по фидбеку за период |
| `feedback_unclear` | bar | Неопределённых (unclear) за период |
| `merge_yes` | bar | Подтверждённых diff'ом (YES) за период |
| `merge_partial` | bar | Частично подтверждённых (PARTIAL) за период |
| `merge_yes_partial` | bar | YES + PARTIAL суммарно за период |
| `merge_no` | bar | Не подтверждённых diff'ом (NO) за период |

Метрики можно комбинировать через запятую.

Воронка эффективности AI-агента:
```
agent_comments → feedback_rate → feedback_acceptance_rate     (по фидбеку)
                               → merge_acceptance_rate        (по diff — реальные изменения в коде)
                                 feedback_acceptance_rate_all  (реальное влияние на весь поток)
```

**Формулы:**
- `cycle_time` = `median((closed_date - created_date) / 3600000)` часы, по PR с `state=<--state>`
- `acceptance_rate` = `count(MERGED) / count(MERGED + DECLINED) × 100`
- `throughput` = `count(MERGED)` за период
- `total_prs` = `count(MERGED + DECLINED)` за период
- `total_repos` = `count(DISTINCT repo_id)` среди MERGED PR с `created_date` в периоде. При `--split reviewer:<slug>` / `commenter:<slug>` "-" серия исключает репы, имеющие ≥1 PR с агентом в этом же периоде — чтобы видеть реальную adoption без дублей
- `time_to_first_comment` = `median(first_non_author_comment_date - created_date)` часы
- `agent_comments` = `sum(root comments by --author)` за период
- `feedback_rate` = `comments_with_reactions_or_replies / total_comments × 100`
- `feedback_acceptance_rate` = `yes / (yes + no) × 100` — только комментарии с фидбеком
- `feedback_acceptance_rate_all` = `yes / total_comments × 100` — все комментарии в знаменателе
- `merge_acceptance_rate` = `(YES + 0.5×PARTIAL) / (YES + PARTIAL + NO) × 100` — по итоговому diff файла

Все trend-метрики группируются по `closed_date` в периоды (week: `%G-W%V`, month: `%Y-%m`).

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

# Эффект AI-агента: split по формальному ревьюверу
.venv/bin/python pr_analytics.py plot \
  --projects PROJ1,PROJ2 --since 2025-01-01 --state MERGED \
  --type trend --period month \
  --metrics cycle_time,acceptance_rate,time_to_first_comment \
  --split reviewer:ai-review-bot \
  --layout stack --output output/ai_effect.html

# Эффект AI-агента: split по наличию хотя бы одного комментария
.venv/bin/python pr_analytics.py plot \
  --projects PROJ1,PROJ2 --since 2025-01-01 --state MERGED \
  --type trend --period week \
  --metrics cycle_time,total_prs \
  --split commenter:ai-review-bot \
  --layout stack --output output/ai_effect.html

# Adoption: сколько репов подключили агента vs ещё нет, понедельно
.venv/bin/python pr_analytics.py plot \
  --projects PROJ1,PROJ2 --since 2025-01-01 --state MERGED \
  --type trend --period week \
  --metrics total_repos \
  --split reviewer:ai-review-bot \
  --output output/adoption.html

# Воронка эффективности AI-агента (агрегировано по всем проектам)
.venv/bin/python pr_analytics.py plot \
  --projects PROJ1,PROJ2 --since 2025-01-01 --state MERGED \
  --type trend --period week \
  --metrics agent_comments,feedback_rate,feedback_acceptance_rate,merge_acceptance_rate \
  --author ai-review-bot \
  --split total \
  --layout stack --output output/agent_funnel.html

# Сравнение acceptance по фидбеку vs по diff
.venv/bin/python pr_analytics.py plot \
  --projects PROJ1,PROJ2 --since 2025-01-01 --state MERGED \
  --type trend --period month \
  --metrics feedback_acceptance_rate,merge_acceptance_rate \
  --author ai-review-bot \
  --split total \
  --layout stack --output output/acceptance_comparison.html

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
| `--split` | Режим серий (см. ниже) |
| `--reviewer` | `include:<slug>` или `exclude:<slug>` — фильтр датасета (не разбивает) |
| `--author` | Slug AI-агента — обязателен для `agent_comments`, `feedback_rate`, `feedback_acceptance_rate` |
| `--judge-model` | LLM-модель судьи (default из конфига, см. `judge.model`) |
| `--output` | `.png`, `.svg` или `.html` (интерактивный plotly, для `trend`) |

**Режимы `--split`:**

| Значение | Описание |
|---|---|
| `reviewer:<slug>` | Два когорта: PR с аккаунтом в ревьюверах / без |
| `commenter:<slug>` | Два когорта: PR с хотя бы одним комментарием от аккаунта / без |
| `total[:<label>]` | Все репозитории в одну агрегированную серию |

Без `--split` — одна серия на репозиторий.

Cycle Time и Time to First Comment используют **логарифмическую** ось Y. Все trend-метрики группируются по `closed_date`.

**Добавить новую метрику** (DORA, PDLC и др.): написать функцию `(rows, period, state) -> dict[str, float]` и добавить запись в `METRICS` в `pa/metrics.py`. Рендер-слой менять не нужно.

---

### `find-repos` — репозитории по ревьюверу или комментатору

Возвращает все репозитории, где пользователь был формальным ревьювером или оставил комментарий.

```bash
# По формальному ревьюверу
.venv/bin/python pr_analytics.py find-repos \
  --reviewer ivan.ivanov \
  --state MERGED \
  --since 2026-01-01 \
  --output repos.txt

# По наличию комментариев (например, AI-агент)
.venv/bin/python pr_analytics.py find-repos \
  --commenter ai-review-bot \
  --state MERGED \
  --since 2026-01-01 \
  --output repos.txt
```

| Параметр | Описание |
|---|---|
| `--reviewer` | Slug пользователя в формальном списке ревьюверов |
| `--commenter` | Slug пользователя, оставившего хотя бы один комментарий |

Вывод — `PROJ/repo`, по одному в строке. Файл совместим с `plot --repos-file`.

---

### `find-prs` — список PR по фильтрам

```bash
.venv/bin/python pr_analytics.py find-prs \
  --projects SBLOOM,MCPN --since 2026-01-01 --state MERGED

# PR где бот оставил комментарии
.venv/bin/python pr_analytics.py find-prs \
  --projects MCPN --commenter ai-review-bot --format csv --output prs.csv
```

| Параметр | Описание |
|---|---|
| `--repos` / `--projects` / `--repos-file` | Фильтр по репозиториям |
| `--author` | Автор PR |
| `--reviewer` | Slug в формальном списке ревьюверов |
| `--commenter` | Slug оставившего хотя бы один комментарий |
| `--state` | `MERGED`, `DECLINED`, `OPEN` |
| `--since` / `--until` | Диапазон по дате создания PR |
| `--limit` | Максимум строк (default: 1000, `0` = все) |
| `--format` | `table` (default), `csv`, `json` |

---

### `find-comments` — список комментариев по фильтрам

```bash
# Все комментарии агента с якорем к файлу
.venv/bin/python pr_analytics.py find-comments \
  --author ai-review-bot --file-only --projects MCPN

# Комментарии severity=BLOCKER
.venv/bin/python pr_analytics.py find-comments \
  --projects SBLOOM --severity BLOCKER --format json
```

| Параметр | Описание |
|---|---|
| `--repos` / `--projects` / `--repos-file` | Фильтр по репозиториям |
| `--author` | Автор комментария |
| `--pr-author` | Автор PR |
| `--state` | Фильтр по состоянию PR |
| `--severity` | `NORMAL` или `BLOCKER` |
| `--file-only` | Только inline-комментарии (с `file_path`) |
| `--include-replies` | Включить ответы в тредах (по умолчанию: только root) |
| `--since` / `--until` | Диапазон по дате создания PR |
| `--limit` | Максимум строк (default: 1000, `0` = все) |
| `--format` | `table` (default), `csv`, `json` |

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

### `analyze-feedback` — LLM-оценка замечаний AI-агента

Прогоняет комментарии AI-агента через LLM-судью и сохраняет вердикты в таблицу `comment_analysis`. Питает метрику `feedback_acceptance_rate` в `plot`.

```bash
# Анализировать по 100 комментариев за раз
.venv/bin/python pr_analytics.py analyze-feedback \
  --author ai-review-bot \
  --since 2025-01-01 \
  --batch-size 100

# Посмотреть, что будет обработано — без вызова LLM
.venv/bin/python pr_analytics.py analyze-feedback \
  --author ai-review-bot \
  --dry-run

# Отдельные репозитории, другая модель судьи
.venv/bin/python pr_analytics.py analyze-feedback \
  --author ai-review-bot \
  --repos "PROJ/backend,PROJ/frontend" \
  --judge-model deepseek-reasoner \
  --batch-size 200
```

| Параметр | Описание |
|---|---|
| `--author` | Slug AI-агента (обязательный) |
| `--since` / `--until` | Диапазон по дате создания PR |
| `--repos` / `--projects` / `--repos-file` | Фильтр по репозиториям |
| `--judge-model` | LLM-модель судьи (из конфига по умолчанию) |
| `--batch-size` | Максимум комментариев за один запуск (по умолчанию: 50, `0` = все) |
| `--dry-run` | Показать список без вызова LLM |

Повторный запуск с той же моделью пропускает уже проанализированные комментарии. Смена `--judge-model` заново анализирует все комментарии этой моделью.

**Конфигурация LLM-судьи** (в `config.local.yaml`):

```yaml
judge:
  model: "deepseek-reasoner"
  api_key: "${DEEPSEEK_API_KEY}"
  base_url: "https://api.deepseek.com/v1"   # OpenAI-compatible; убрать для Anthropic
```

Поддерживается любой OpenAI-compatible endpoint (DeepSeek, OpenRouter, local LLM). Для Anthropic — убрать `base_url` и выставить `ANTHROPIC_API_KEY`.

После анализа можно построить тренд:

```bash
.venv/bin/python pr_analytics.py plot \
  --projects PROJ --since 2025-01-01 --state MERGED \
  --type trend --period month \
  --metrics feedback_acceptance_rate,acceptance_rate \
  --author ai-review-bot \
  --layout stack --output output/semantic.html
```

---

### `analyze-merges` — проверка влияния комментариев на код через diff

Для каждого комментария AI-агента с привязкой к файлу в **смержённых** PR скачивает diff, исходный код якоря, список коммитов и спрашивает LLM-судью, было ли замечание учтено в изменениях.

```bash
# Проанализировать комментарии с file anchors
.venv/bin/python pr_analytics.py analyze-merges \
  --author ai-review-bot \
  --since 2026-01-01 \
  --batch-size 100 --budget-tokens 200000

# Verbose: видеть полный промпт и ответ судьи для каждого комментария
.venv/bin/python pr_analytics.py analyze-merges \
  --author ai-review-bot --since 2026-01-01 \
  --verbose --batch-size 5

# Посмотреть что будет обработано (без API-вызовов)
.venv/bin/python pr_analytics.py analyze-merges \
  --author ai-review-bot --dry-run
```

| Параметр | Описание |
|---|---|
| `--author` | Slug AI-агента (обязательный) |
| `--since` / `--until` | Диапазон по дате создания PR |
| `--repos` / `--projects` / `--repos-file` | Фильтр по репозиториям |
| `--judge-model` | LLM-модель судьи |
| `--batch-size` | Макс. комментариев за запуск (default: 50) |
| `--budget-tokens` | Лимит токенов |
| `--max-diff-chars` | Обрезка diff до N символов (default: 4000) |
| `--verbose` | Печатать полный промпт и ответ LLM для каждого комментария |
| `--dry-run` | Показать список без вызова API |

**Только MERGED PR.** Несмержённые PR пропускаются — код не попал в кодовую базу, merge acceptance бессмысленен.

#### Методика

Для каждого корневого комментария AI-агента с привязкой к файлу (`file_path IS NOT NULL`):

1. **Скачать итоговый diff файла** из Bitbucket PR:
   ```
   GET /rest/api/1.0/projects/{proj}/repos/{repo}/pull-requests/{id}/diff/{file_path}?contextLines=5
   ```
   Endpoint работает для любого состояния PR (OPEN, MERGED, DECLINED) — Bitbucket хранит diff между source и target branch на момент последнего обновления PR.

2. **Скачать исходный код вокруг якоря** (±10 строк) из source-ветки PR:
   ```
   GET /rest/api/1.0/projects/{proj}/repos/{repo}/browse/{file_path}?at={toHash}&start={line-10}&limit=21
   ```
   `toHash` — SHA коммита source-ветки из ответа diff endpoint. **Работает для смержённых PR с удалёнными ветками** — коммит доступен через merge commit, т.к. это SHA, а не имя ветки.

3. **Скачать коммиты PR** и для каждого коммита — список изменённых файлов:
   ```
   GET /rest/api/1.0/projects/{proj}/repos/{repo}/pull-requests/{id}/commits
   GET /rest/api/1.0/projects/{proj}/repos/{repo}/commits/{hash}/changes
   ```
   Фильтр: только коммиты после `comment.created_date`. Для каждого показывается hash, сообщение, дата и список файлов. Если файл якоря удалён или переименован — это видно по `changeType` (DELETE, RENAME).

4. **Конвертировать** Bitbucket JSON diff → unified diff формат с номерами строк (`-old +new`).

5. **Отправить LLM-судье** промпт с четырьмя входами:
   - Текст комментария с номером строки якоря
   - Исходный код вокруг якоря (маркер `>>>` на строке) — чтобы судья видел что критикуется
   - Список коммитов после комментария с изменёнными файлами — объективный сигнал были ли изменения
   - Итоговый diff файла с номерами строк — чтобы судья видел что изменилось

   Если файл якоря **не фигурирует** ни в одном коммите после комментария, судья получает явный маркер `⚠ Файл НЕ фигурирует ни в одном коммите после комментария` и должен ответить `NO`.

   Это позволяет корректно определять:
   - "удалите этот код" — судья видит код в якоре и DELETE в коммитах
   - ложные YES — файл не менялся после комментария, значит изменение было до него
   - переименования — судья видит RENAME в коммитах

5. **Сохранить вердикт** в таблицу `merge_analysis` с `analyzer_version`.

**Вердикты:**

| Вердикт | Вес | Описание |
|---|---|---|
| `YES` | 1.0 | В diff видно исправление, явно соответствующее замечанию |
| `PARTIAL` | 0.5 | Изменения частично затрагивают проблему из замечания |
| `NO` | 0.0 | Diff не содержит изменений, связанных с замечанием |

**Метрика `merge_acceptance_rate`** (для `plot --metrics`):
```
merge_acceptance_rate = (count(YES) + 0.5 × count(PARTIAL)) / (count(YES) + count(PARTIAL) + count(NO)) × 100%
```

При построении графика для каждого комментария берётся **только последний** результат по `analyzed_at` — т.е. результат самой свежей версии анализатора.

#### Версионирование анализатора

Каждый результат в `merge_analysis` хранится с `analyzer_version` — sha256 от промпт-шаблона (первые 8 символов). При изменении промпта автоматически создаётся новая версия.

**Поведение:**

| Сценарий | Что происходит |
|---|---|
| Повторный запуск с тем же промптом | Пропускает уже проанализированные (идемпотентно) |
| Изменение промпта | Новая `analyzer_version` → все комментарии анализируются заново |
| Запуск на другом компе с обновлённым промптом | Новые результаты сохраняются рядом со старыми |
| Построение графика | Берётся последняя версия по `analyzed_at` для каждого комментария |

PRIMARY KEY: `(comment_id, judge_model, analyzer_version)` — старые результаты не удаляются, лежат рядом.

```bash
# Посмотреть все версии и их результаты
.venv/bin/python pr_analytics.py sql --query "
  SELECT analyzer_version, COUNT(*) AS n,
         SUM(CASE WHEN verdict='YES' THEN 1 ELSE 0 END) AS yes_count,
         SUM(CASE WHEN verdict='NO' THEN 1 ELSE 0 END) AS no_count,
         datetime(MAX(analyzed_at)/1000, 'unixepoch') AS last_run
  FROM merge_analysis
  GROUP BY analyzer_version
  ORDER BY MAX(analyzed_at) DESC
"
```

**Что анализируется / не анализируется:**
- Только **MERGED** PR — несмержённые пропускаются
- Только комментарии с `file_path` (inline-комментарии к файлу) — general-комментарии пропускаются
- Нет коммитов после комментария → автоматический `NO` без вызова LLM
- Коммиты есть, но файл якоря не затронут → автоматический `NO` без вызова LLM
- Комментарии без diff (файл не менялся в PR) → `SKIP`, не учитываются в метрике
- Новые файлы (`source=null`) корректно обрабатываются

**Итоговая саммари** показывает статистику только по комментариям, попавшим в анализ (inline, MERGED, с file anchor). General-комментарии и комментарии в несмержённых PR в саммари не входят.

**Отличие от `feedback_acceptance_rate`:**
- `feedback_acceptance_rate` — судья оценивает по фидбеку (реакции, ответы в треде): "разработчик согласился?"
- `merge_acceptance_rate` — судья оценивает по итоговому diff + исходному коду якоря: "код реально был изменён в соответствии с замечанием?"

Второй сигнал объективнее — не зависит от того, ответил ли разработчик. Но работает только для inline-комментариев к файлам.

**Стоимость:** Bitbucket API: 1 diff + 1 browse + 1 commits + N changes (per commit) на PR. Всё кешируется в памяти per-(repo, pr, file) — несколько комментариев к одному PR/файлу не генерируют повторных запросов. + 1 LLM-вызов на комментарий.

---

### `acceptance` — метрики по поколению агента (diffgraph)

Показывает acceptance rate для конкретного prompt hash из diffgraph. Связь через тег `` `dg:gen:hash:run` `` в комментариях агента.

```bash
.venv/bin/python pr_analytics.py acceptance --dg-hash f7917d6
.venv/bin/python pr_analytics.py acceptance --dg-hash f7917d6 --format json
```

| Параметр | Описание |
|---|---|
| `--dg-hash` | Prompt hash из diffgraph (первые 7 символов) |
| `--since` | Начало периода (YYYY-MM-DD) |
| `--format` | `text` (по умолчанию) или `json` |

Вывод: acceptance_rate, false_positive_rate, feedback_rate, total/analyzed/accepted/rejected counts.

**Тег извлекается автоматически** при кешировании комментариев. Формат: `` `dg:<generation>:<hash>:<run_id>` ``. Хранится в колонках `dg_gen`, `dg_hash`, `dg_run` таблицы `pr_comments`.

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

### `select-golden` — отбор эталонных PR для бенчмарка

Находит высококачественные PR с разнообразными и глубокими замечаниями — пригодные для тестирования AI-код-ревьюверов.

**Пайплайн (управляется `--steps`):**

```
heuristic → classify → analyze → score → judge
```

| Шаг | Что делает | LLM? |
|---|---|---|
| `heuristic` | SQL-фильтр по времени жизни, ревьюверам, числу комментариев | нет |
| `classify` | Классифицирует каждый комментарий: тип (10 классов) + глубина (1-3) | да |
| `analyze` | Оценивает принятие комментариев (yes/no/unclear) — автоматически для непроанализированных | да |
| `score` | Вычисляет составной скор PR из классификаций + вердиктов | нет |
| `judge` | Финальный вердикт GOLD / SILVER / REJECT на топ-N% | да |

**Типы замечаний (classify):** `СТИЛЬ`, `ПОВЕРХНОСТНАЯ_ЛОГИКА`, `ГЛУБОКАЯ_ЛОГИКА`, `АРХИТЕКТУРА`, `ПРОИЗВОДИТЕЛЬНОСТЬ`, `БЕЗОПАСНОСТЬ`, `ТЕСТЫ`, `БИЗНЕС_ЛОГИКА`, `УСТОЙЧИВОСТЬ`, `ЧИТАЕМОСТЬ`

**Глубокие типы** (учитываются отдельно при ранжировании): `ГЛУБОКАЯ_ЛОГИКА`, `АРХИТЕКТУРА`, `БЕЗОПАСНОСТЬ`, `БИЗНЕС_ЛОГИКА`, `УСТОЙЧИВОСТЬ`

#### Эвристический фильтр (heuristic)

Чистый SQL без LLM-вызовов. Проходят PR, удовлетворяющие всем условиям:

| Параметр | Default | CLI | YAML (`golden:`) | Описание |
|---|---|---|---|---|
| Время жизни | 0.25–120 ч | `--min-lifetime-h` / `--max-lifetime-h` | `min_lifetime_h` / `max_lifetime_h` | `(closed_date - created_date)` в часах |
| Ревьюверы | ≥ 1 | `--min-reviewers` | `min_reviewers` | `json_array_length(reviewers)` |
| Корневые комментарии (не от автора PR) | 2–30 | `--min-comments` / `--max-comments` | `min_comments` / `max_comments` | `COUNT WHERE parent_id IS NULL AND author != pr.author` |
| Ответы | > 0 | — | — | Хотя бы один reply в треде |
| Файлов изменено (если есть `pr_diff_stats`) | 2–20 | — | — | Из кеша diff stats |
| Доля тестов/конфигов | < 40% | — | — | `test_config_ratio` из `pr_diff_stats` |

Пороги настраиваются в `config.yaml` (секция `golden:`), переопределяются CLI-аргументами:

```yaml
golden:
  min_lifetime_h: 0.25    # 15 минут
  max_lifetime_h: 120     # 5 дней
  min_reviewers: 1
  min_comments: 2
  max_comments: 30
  # Исключить комментарии от этих аккаунтов из всех фаз
  exclude_authors:
    - bot-account
    - ci-system
```

`exclude_authors` убирает комментарии указанных аккаунтов из всех фаз: эвристика (не считаются в `root_comment_count`), classify, analyze и score. Задаётся в YAML или через CLI `--exclude-authors slug1,slug2`.

#### Классификация комментариев (classify)

Каждый корневой комментарий (не от автора PR) отправляется в LLM с промптом, который возвращает:
- `type` — один из 10 типов
- `depth` — 1 (поверхностный), 2 (средний), 3 (глубокий)

#### Анализ принятия (analyze)

Для комментариев с фидбеком (реакция или ответ) LLM-судья определяет, было ли замечание принято:
- `yes` — замечание признали обоснованным
- `no` — замечание отклонили
- `unclear` — невозможно определить

Комментарии без фидбека пропускаются. Результат сохраняется в `comment_analysis`.

#### Скор PR (score)

Составной скор `total_score ∈ [0, 1]` — взвешенная сумма пяти компонентов.

**Если есть данные о принятии** (хотя бы один вердикт `yes` или `no` в PR):

```
total_score = diversity × 0.25 + depth × 0.25 + change × 0.30 + noise × 0.10 + size × 0.10
```

**Если данных о принятии нет** (все вердикты `unclear` или у комментариев нет фидбека — ни реакций, ни ответов, т.е. невозможно определить, привели ли замечания к изменениям):

```
total_score = diversity × 0.35 + depth × 0.35 + noise × 0.15 + size × 0.15
```

В этом случае `change` исключается из формулы, а вес перераспределяется на качество комментариев (разноплановость и глубина).

| Компонент | Формула | Диапазон |
|---|---|---|
| `diversity` | `min(unique_types, 3) / 3` | 0..1 |
| `depth` | `(avg_depth - 1) / 2` где `avg_depth = mean(depth по всем комментариям)` | 0..1 |
| `change` | `count(verdict='yes') / count(verdict IN ('yes','no'))` | 0..1 |
| `noise` | `1 - count(type='СТИЛЬ') / total_comments` | 0..1 |
| `size` | `max(0, 1 - abs(lines_changed - 200) / 200)` (пик в 200 строк; 0.5 если нет данных) | 0..1 |

- `unique_types` — количество уникальных типов замечаний в PR
- `avg_depth` — среднее значение depth (1–3) по всем классифицированным комментариям
- `lines_changed` = `lines_added + lines_deleted` из `pr_diff_stats`

#### Финальный вердикт (judge)

Топ N% PR по `total_score` (default: 20%) отправляются на финальную оценку LLM:
- `GOLD` — идеальный эталон для бенчмарка
- `SILVER` — хороший PR, но не идеальный
- `REJECT` — не подходит как эталон

```bash
# Быстрый просмотр кандидатов — только эвристика (без LLM, мгновенно)
.venv/bin/python pr_analytics.py select-golden \
  --projects PROJ1,PROJ2 --since 2025-01-01 \
  --steps heuristic

# Полный пайплайн с бюджетом
.venv/bin/python pr_analytics.py select-golden \
  --projects PROJ1,PROJ2 --since 2025-01-01 \
  --steps heuristic,classify,analyze,score,judge \
  --budget-classify 200000 --budget-analyze 100000 --budget-judge 50000 \
  --output output/golden.html
```

| Параметр | Описание |
|---|---|
| `--steps` | Шаги пайплайна через запятую (default: все) |
| `--classifier-model` | LLM для классификации комментариев (default: из конфига) |
| `--judge-model` | LLM для финального вердикта GOLD/SILVER/REJECT |
| `--change-judge-model` | Judge model для `change_score` (default: classifier-model) |
| `--top-pct` | Топ N% по скору отправляется на финального судью (default: 20) |
| `--budget-tokens` | Общий лимит токенов на запуск |
| `--budget-classify` | Лимит токенов на шаг classify |
| `--budget-analyze` | Лимит токенов на шаг analyze |
| `--budget-judge` | Лимит токенов на шаг judge |
| `--exclude-authors` | Comma-separated slugs — исключить из всех фаз (также `golden.exclude_authors` в YAML) |
| `--max-comment-chars` | Обрезка текста комментария (default: 1500) |
| `--min-lifetime-h` / `--max-lifetime-h` | Время жизни PR в часах (default: 0.25–120) |
| `--min-reviewers` | Минимум ревьюверов (default: 1) |
| `--min-comments` / `--max-comments` | Диапазон числа комментариев (default: 2–30) |
| `--output` | HTML-отчёт (default: `output/golden.html`) |

**HTML-отчёт** содержит: воронку фильтрации, scatter-диаграмму (разнообразие vs глубина), распределение типов комментариев и таблицу PR с вердиктами.

Пайплайн **идемпотентен** — повторный запуск пропускает уже классифицированные и проанализированные комментарии. Промежуточные результаты сохраняются в `comment_classification`, `comment_analysis` и `pr_scores`.

Шаг `analyze` автоматически оценивает комментарии, у которых есть реакции или ответы, но нет записи в `comment_analysis` — не требует отдельного запуска `analyze-feedback`.

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
├── comment_reactions      comment_id, author, emoji
├── comment_analysis       comment_id, judge_model, verdict, confidence,
│                          reasoning, analyzed_at
├── comment_classification comment_id, classifier_model, comment_type,
│                          depth, confidence, classified_at
├── merge_analysis         comment_id, judge_model, analyzer_version,
│                          verdict (YES/PARTIAL/NO), confidence, reasoning,
│                          analyzed_at
├── pr_diff_stats          repo_id, pr_id, lines_added, lines_deleted,
│                          files_changed, test_config_ratio, fetched_at
└── pr_scores              repo_id, pr_id, scorer_model, diversity_score,
                           depth_score, change_score_ratio, style_noise_score,
                           size_score, total_score, verdict, verdict_reasoning,
                           scored_at
```

`reviewers` — JSON-массив slug'ов, доступен через `json_each()` в SQL-запросах.

`comment_analysis.verdict` — `yes` / `no` / `unclear` (PRIMARY KEY: `comment_id + judge_model`).

`merge_analysis.verdict` — `YES` / `PARTIAL` / `NO` (PRIMARY KEY: `comment_id + judge_model + analyzer_version`). `analyzer_version` = sha256 промпт-шаблона (8 символов).

`pr_scores.verdict` — `GOLD` / `SILVER` / `REJECT` (PRIMARY KEY: `repo_id + pr_id + scorer_model`).
