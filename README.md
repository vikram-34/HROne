# Employee Attendance & Analytics API

FastAPI/PyMongo implementation of all endpoints in the supplied assessment's `openapi.yaml` contract, with all application code in `app/main.py`.
Includes IST/overnight attendance, atomic punch/correction writes, audit history, paginated lists, four MongoDB analytics pipelines, and matching execution-plan explanations.

Requires Python 3.11+ and MongoDB 6.0+ (the grader uses 7.0). The database user must be able to read/write both collections, create indexes, and execute explain. No external services are called by the application.

## Run on Windows PowerShell

From the repository root, install once:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Set the connection and start:

```powershell
$env:MONGO_URI = "mongodb://localhost:27017"
$env:MONGO_DB = "attendance_db"
.\.venv\Scripts\uvicorn.exe app.main:app --port 8000
```

Any installed Python 3.11+ can replace `py -3.13`. An optional local `.env` may use `.env.example` as a template; real environment variables take priority.

For other shells, install with `python -m pip install -r requirements.txt`, export `MONGO_URI`/`MONGO_DB`, and run the required command `uvicorn app.main:app --port 8000`. Indexes are created automatically at startup. `/health` returns 200 only after MongoDB responds and indexes are ready; database failures return 503. API documentation is served at `/docs`.

## Data and implementation notes

The original candidate kit supplies an optional `sample_seed.py` and sample data, which are not included in this submission. That loader **deletes and replaces both collections' documents** in the configured database, so only use it against a disposable sample database. The application itself never seeds or clears collections at startup.

- Analytics use stored derived values. Joining dates affect working-day denominators and headcounts; monthly log metrics use the requested calendar month.
- Trend ranges contain at most 92 days, inclusive. Gaps are generated with MongoDB `$range`/`$dateAdd`, and the moving average includes only rows inside the requested range.
- Optional request timestamps may be omitted but cannot be null. Clear punch times by correcting status to `ABSENT` or `LEAVE`.
- Punch-out selects the latest punch-in at or before the supplied time, even if already closed. A time before every punch-in returns 404; equal punch-in/out times return 422 for an open record.
- `REVIEW.md` documents starter defects; `DECISIONS.md` explains indexes, concurrency, ranking, headcount, and scaling.

## Verification status

All required endpoints are implemented. The following manual checks were confirmed from Swagger screenshots and saved explain responses on 2026-10-10. The explain responses identify the local database as `attendance_demo`, running MongoDB 8.3.11.

| Check | Observed result |
|---|---|
| `GET /health` | HTTP 200 with `{"status":"ok"}`; the application reached MongoDB and completed index initialization. |
| `GET /employees?page=1&page_size=20` | HTTP 200, `total: 6`, `page: 1`, `page_size: 20`; visible creation timestamps were integer epoch milliseconds. |
| Explain `attendance_list`, page 1, size 20, no filters | Successful execution, 9 rows returned, 9 keys and 9 documents examined; `IXSCAN` on `attendance_date_employee`, with no `COLLSCAN`. |
| Explain `employee_monthly`, `EMP0001`, `2026-07` | Successful execution producing one summary row; employee lookup examined 1 key/document using `employee_emp_code`; attendance lookup examined 2 keys/documents using `attendance_employee_date`, with `collectionScans: 0`. |

These checks establish local connectivity, the observed employee-list response, and indexed execution for the two sample queries. Explain output does not establish that the calculated monthly summary values are correct. Other endpoint results, validation/error cases, concurrent writes, overnight and other edge cases, the remaining analytics query plans, the 20-second startup target, MongoDB 7.0 compatibility, and performance on 100,000 records are not verified by this evidence. No automated tests or benchmarks were run during implementation or this documentation update.

Submit the required source and documentation in a public repository. Do not commit `.env`, secrets, virtual environments, a Dockerfile, or generated data dumps; `.gitignore` excludes local settings and environments. Review the code and decisions before the assessment's live walkthrough.
