# SISREG Unified Flow — Implementation Plan

> Working document for building the single, parameterized SISREG scraping flow under
> `pipelines/datalake/extract_load/sisreg/`. It enumerates **atomic commits** (not PRs), each
> with a Definition of Ready, a Definition of Done, the exact **commit message**, and the
> **hooks + tests** that must pass first. Tick a `[ ]` box only when the item is truly done.
>
> Design source of truth: the session design plan (the agreed architecture, registry, anti-ban
> rules, write model, and BQ inputs). This file is the *how/when*; that design is the *what/why*.
> **This plan is in English; the code and the README are in Brazilian Portuguese** (see §2.3).

---

## 0. How to use this document

- Work top to bottom. Commits are ordered by dependency; do not reorder without updating deps.
- **One logical change = one commit.** Keep commits small enough to revert cleanly.
- Before every commit, run the **Quality Gates** (§3). A commit is allowed only when all its
  listed gates pass.
- **No Pull Requests.** Commit to the current feature branch and (optionally) push it. Never
  commit to `main`/`master` (the `no-commit-to-branch` hook enforces this).
- Update the checkboxes here in the same commit that completes the work (so the plan and the
  code never drift).

---

## 1. Context & references

- **Goal:** replace six legacy SISREG scrapers (`sisreg_web`, `sisreg_afastamentos`,
  `sisreg_preparos`, `sisreg_solicitacoes`, `sisreg_pendentes_vagas`, `sisreg_afastamentos_web`)
  with **one** parameterized Prefect-v1 flow + per-dataset schedules. `sisreg_api` and
  `sisreg_web_v2` are **out of scope** (untouched).
- **Datasets produced (5):** `escalas`, `afastamentos`, `preparos`, `solicitacoes`, `fila_vagas`
  → all into the single BigQuery dataset **`brutos_sisreg_web`** (one table each; `afastamentos`
  and `fila_vagas` produce two tables).
- **Read-only curated inputs:** afastamentos CPFs from `rj-sms.saude_sisreg.oferta_programada`
  (~30-day-active filter, ~2–3k CPFs); fila_vagas procedures from
  `rj-sms.saude_sisreg.solicitacoes`. **Trust codes, not names.**
- **Write model:** latest-only `overwrite`, **written only on 100% success**; rolling 180-day
  window; partition by a date column.
- **Anti-ban:** one transport (`requests`), conservative jittered delays **8.5–9.5 s**,
  **single-flight per account**, retreat-on-soft-block, never bypass CAPTCHA.

---

## 2. Conventions & style guide

### 2.1 Commits — semantic, English, revertible

Format: **`action: description`** (imperative, lower-case, English, no trailing period).

Allowed `action` values:

| action | use for |
|---|---|
| `feat` | new behavior / module |
| `fix` | bug fix |
| `refactor` | behavior-preserving restructure |
| `docs` | README / comments / this plan |
| `test` | tests only |
| `chore` | scaffolding, scheduling, cutover, dependency wiring |

- One concern per commit; the message describes the *what*, the body (optional) the *why*.
- Co-author/sign-off as configured by the repo; **do not** add new trailers.
- **Never** `--no-verify` (hooks must run).

### 2.2 Checkboxes

Every EPIC, task, and commit in §7 carries a `[ ]`. Tick `[x]` only when its **Definition of
Done** and **Gates** are satisfied.

### 2.3 Language & naming (Brazilian Portuguese)

- **Code identifiers** (functions, variables, classes, modules): Brazilian Portuguese, **ASCII
  only** — no accents/cedilla in identifiers (Python convention + portability). Example:
  `obter_sessao_autenticada`, `extrair_afastamentos_por_cpf`, `lista_procedimentos`.
- **Docstrings and comments**: natural Brazilian Portuguese, accents allowed (`ç`, `ã`, `á`, …
  are standard on Brazilian keyboards). **Avoid typographic symbols not on a Brazilian keyboard**
  (no `–`/`—` em-dashes, no curly quotes `“ ”`, no `…`; use `-`, `"`, `...`).
- **IMPLEMENTATION_PLAN.md** is English (this file). **README.md** is Brazilian Portuguese.

### 2.4 Code style

- **Single Responsibility:** one function does one thing; if a function needs the word "and" to
  describe it, split it. Target < ~30 lines per function.
- **Explicit types everywhere:** every parameter and return value is type-annotated. No bare
  `Any` unless unavoidable (justify with a comment).
- **Docstrings:** every function/class gets a short docstring (1–3 lines) stating its purpose,
  in pt-BR. No need to restate types (they are in the signature).
- **Comments justify decisions:** comment the *why*, not the *what*. Required on the most
  critical/complex sections (anti-ban timing, single-flight, completeness gate, failover
  gating, code-vs-name handling, HTML parsing landmarks).
- **Formatting:** `black` (line length **100**, py3.10) + `isort` (black profile). `flake8`
  ignores `E203, W503`. **Every `.py` starts with** `# -*- coding: utf-8 -*-` (the
  `fix-encoding-pragma` hook enforces it).
- **No secrets in code** — credentials only via Infisical (`get_secret_key`). **No PII in
  logs** — never log raw CPFs (mask or count).
- **Simple, elegant, effective.** Prefer composition + a small registry over inheritance and
  `if/elif` ladders. Resist gold-plating (YAGNI).

### 2.5 Best practices baked into this plan

See §5 for the full staff-SWE / data-engineering / web-scraping checklist that every commit
must respect.

---

## 3. Quality gates (must pass before EVERY commit)

### 3.1 Hooks (already configured in this repo — use as-is, install nothing new)

From `.pre-commit-config.yaml`:
- `black` 24.4.2 (py3.10), `isort` 5.13.2, `flake8` 7.1.0 (`.flake8`: max-line 100, ignore
  E203/W503).
- Hygiene: `check-added-large-files`, `detect-private-key`, `fix-byte-order-marker`,
  `fix-encoding-pragma`, `no-commit-to-branch`, `trailing-whitespace`.

**Command (run before staging):**
```
pre-commit run --files <changed files>     # or: pre-commit run --all-files
task lint                                  # black . && isort . && flake8 .  (mirrors CI)
```
Both must exit clean. CI (`.github/workflows/lint.yaml`) runs `task lint` on push — keep it green.

### 3.2 Tests (stdlib `unittest` — no new dependency)

The repo has **no test framework** (no `pytest`, no `tests/`). Per the "install nothing new"
rule, tests use the **standard-library `unittest`**, no extra dependency.

- Location: `pipelines/datalake/extract_load/sisreg/tests/` with HTML/CSV fixtures under
  `tests/fixtures/`. Tests are **offline** (no live SISREG/BigQuery calls) — parse saved
  fixtures and mock `requests`/BigQuery clients.
- **Command:**
  ```
  python -m unittest discover -s pipelines/datalake/extract_load/sisreg/tests -p "test_*.py"
  ```
- Commits that add pure scaffolding or docs have **no test gate** (lint only); every commit that
  adds *logic* must add/extend `unittest` tests and pass them.

> Flagged decision: `unittest` chosen because the repo forbids new deps and has no `pytest`. If
> you prefer "lint + manual verification only," drop the test rows from each commit below.

### 3.3 Branch policy

- Commit to the current feature branch (`staging/sisreg-refactor-all-matheus`). The
  `no-commit-to-branch` hook blocks `main`/`master`.
- Pushing the branch is allowed; **opening PRs is not**.

---

## 4. Definition of Ready / Done — global templates

**Definition of Ready (DoR)** — before starting a commit:
- The design point it implements is settled in the session plan.
- Its upstream commits are done and green.
- For extractor commits: the relevant **fixture(s)** exist. Fixtures are **synthetic** —
  hand-authored minimal HTML/CSV that match the structure documented in the original/legacy
  flows. No live SISREG access is needed to build or test (live validation is EPIC 5 parity).

**Definition of Done (DoD)** — before ticking the box:
- Code follows §2 (SRP, types, pt-BR docstrings/comments, encoding pragma).
- New logic has `unittest` coverage; **all gates in §3 pass**.
- Checkbox ticked in this file within the same commit.

---

## 5. Style guide & best practices (staff SWE + data engineering + web scraping)

**Staff software engineering**
- [ ] SRP per function; small, named, pure where possible.
- [ ] Explicit parameter/return types; pt-BR docstrings; comments explain *why*.
- [ ] Registry/strategy over `if/elif`; composition over inheritance.
- [ ] Typed error taxonomy (`ErroSisreg` base → auth/blocked/structure/empty/upload/transient)
      so failures self-classify and drive retry/failover/circuit-break.
- [ ] Fail loud on contract violations (missing columns/landmarks); never silently emit empty.
- [ ] No over-engineering: build gated contingencies only when a spike proves them.

**Data engineering**
- [ ] Idempotent writes: `overwrite` latest window; **write only on 100% success** (else keep
      last good table + alert).
- [ ] Standard partition by a date column; rolling **180-day** window; never full backfill.
- [ ] Schema validation against `expected_columns`; row-count/sanity bounds; suspicious-empty
      is an error, not a SKIP.
- [ ] Run-log table (`{dataset, run_id, as_of, items_total, items_ok, rows, status}`) +
      freshness monitor (alarm if no successful run within the dataset's SLA).
- [ ] **Trust codes, not names** (BigQuery and website names differ; join/match on codes).

**Web scraping with Python**
- [ ] One transport: `requests` + a single authenticated `Session` per run (no re-login churn).
- [ ] Conservative, **jittered** delays (8.5–9.5 s); per-request timeouts (30 s login / 180 s
      page); per-session request budget.
- [ ] **Single-flight per account** (SISREG allows one session per account, system-wide);
      `num_workers=1`; never parallel requests on one account.
- [ ] Detect-and-retreat: CAPTCHA/403/429/login-redirect -> circuit-break + alert; **never**
      grind retries; **never** attempt to bypass a CAPTCHA.
- [ ] Validate the response *before* parsing (status, content-type, expected landmark/marker);
      assert DOM/file landmarks; on structure/block errors snapshot raw HTML to GCS for triage.
- [ ] TLS verification **on** (`verify=True`); pin a CA bundle if the chain is incomplete; never
      `verify=False`.
- [ ] Coherent header set + plausible `Referer` chain; secrets via Infisical only.

---

## 6. Module/test layout (target)

```
pipelines/datalake/extract_load/sisreg/
  __init__.py
  flows.py            # the single Flow + DAG + run config + schedule wiring
  tasks.py            # generic tasks (resolve_credentials, plan_work, extract_item, ...)
  registry.py         # SisregDataset dataclass + DATASETS
  constants.py        # dataset/table ids, urls, headers, credential profiles, behavior profiles
  schedules.py        # one clock per dataset (daily/weekly)
  errors.py           # error taxonomy
  common/
    __init__.py
    auth.py           # login/session + gated failover
    http.py           # polite_get + retry + soft-block detection + snapshot
    parsing.py        # html-table -> dataframe, hidden fields, column normalization
  extractors/
    __init__.py
    escalas.py afastamentos.py preparos.py solicitacoes.py fila_vagas.py
  tests/
    __init__.py
    fixtures/         # saved html/csv (offline tests)
    test_*.py
  README.md           # pt-BR (EPIC 5)
  IMPLEMENTATION_PLAN.md
```

---

## 7. Task breakdown (EPICs -> commits)

Legend per commit: **DoR** (ready), **DoD** (done), **Commit** (message), **Gates** (must pass).
Default gates: `hooks` = §3.1 clean; `tests` = §3.2 green. "lint only" = no test gate.

### [x] EPIC 0 — Spike decisions (resolved by trusting originals + conservative defaults)

Per direction, the spikes are resolved **by decision** — no live SISREG probing (we lack creds
and must not risk the account). Conservative choices, locked:
- [x] **A — escalas via `requests`:** trusted. v1/v2 hit the same `cons_escalas...EXPORTAR_ESCALAS`
      export, and the `requests`-based flows prove SISREG login+fetch work over plain HTTP; the
      escalas columns are trusted to exist. Build reads the export via an authenticated `requests`
      GET. (If runtime ever proves JS is required — very unlikely — the registry still allows a
      Selenium-backed extractor for just that one dataset.)
- [x] **B — TLS:** `verify=True` always; if the production cert chain is incomplete, pin the
      `certifi` CA bundle. **Never `verify=False`** (we send government credentials).
- [x] **C — single-flight:** one-session-per-account is confirmed; use a GCS/BQ **soft-lock with
      TTL**, per account (portable — does not depend on the Prefect tag-limit being enabled).
- [x] **D — rate / anti-bot:** adopt the most conservative values already in the flows
      (8.5-9.5 s jitter, single session, `num_workers=1`); do NOT add a TLS-impersonation lib
      unless a real production block is observed (gated).

### [ ] EPIC 1 — Scaffolding & shared core

- [x] **C1 `chore: scaffold unified sisreg package`** — create the tree in §6 (empty
      `__init__.py`, `tests/`, `tests/fixtures/`), each `.py` with the encoding pragma.
      DoR: branch ready. DoD: package imports cleanly; `tests/` discoverable. Gates: hooks (lint only).
- [x] **C2 `feat: add sisreg error taxonomy`** — `errors.py`: `ErroSisreg` base (carries
      `dataset/etapa/item/url/detalhe`) + `ErroAutenticacao`, `ErroBloqueio`, `ErroEstrutura`,
      `ErroVazioSuspeito`, `ErroUpload`, `ErroTransitorio`.
      DoR: C1. DoD: classes + docstrings; `test_errors.py` checks construction/context.
      Gates: hooks + tests.
- [x] **C3 `feat: add sisreg constants and behavior profiles`** — `constants.py`:
      `BRUTOS_SISREG_WEB`, default table names, base URLs, request headers, credential-profile ->
      Infisical-path map, per-dataset behavior profile (delay min/max, timeouts, budget),
      `JANELA_DIAS = 180`, agent label.
      DoR: C1. DoD: constants documented. Gates: hooks (lint only).
- [x] **C4 `feat: add polite http layer with block detection`** — `common/http.py`:
      `requisicao_educada(...)` (jittered sleep 8.5-9.5 s, per-request timeout, bounded
      exponential backoff, `Retry-After`/429 handling), `detectar_bloqueio(resposta)` (CAPTCHA/
      403/429/login-redirect), `salvar_snapshot_html(...)`.
      DoR: C2, C3. DoD: deterministic-with-seed tests for jitter bounds, block detection, retry,
      timeout (mock `requests`). Gates: hooks + tests.
- [x] **C5 `feat: add sisreg auth, session and gated failover`** — `common/auth.py`:
      `sha256_maiusculo`, `extrair_campos_ocultos`, `abrir_sessao_autenticada` (GET -> hidden
      fields -> POST -> success check, `verify=True`, `raise_for_status`), `reautenticar_se_deslogado`,
      `failover_por_categoria` (rotate account only on `ErroAutenticacao`; circuit-break on
      `ErroBloqueio`). DoR: C2-C4. DoD: tests for hash, hidden-field parse, success/failure
      detection, failover gating (mocked). Gates: hooks + tests.
- [x] **C6 `feat: add html table parsing helpers`** — `common/parsing.py`:
      `tabela_listagem_para_dataframe`, `normalizar_nomes_colunas` (ASCII, snake_case),
      landmark assertions raising `ErroEstrutura`. DoR: C2. DoD: tests against saved HTML
      fixtures (incl. a "renamed landmark" negative case). Gates: hooks + tests.
- [x] **C7 `feat: add sisreg dataset registry`** — `registry.py`: `SisregDataset` dataclass
      (`chave, dataset_id, tabelas, perfil_credencial, colunas_esperadas, planejar_trabalho,
      extrair_item`) + `DATASETS` (the 5). DoR: C3. DoD: tests assert registry integrity (unique
      keys, callables set, table names non-empty). Gates: hooks + tests.
- [x] **C8 `feat: add generic flow tasks`** — `tasks.py`: `resolver_credenciais`,
      `planejar_trabalho`, `extrair_item`, `consolidar` (concat, drop failed, schema-validate,
      **100% completeness gate**, suspicious-empty), `normalizar_e_subir` (per-table
      `handle_columns_to_bq` + `upload_df_to_datalake(dump_mode="overwrite")`), `registrar_log_execucao`.
      DoR: C5-C7. DoD: tests for completeness gate (writes only at 100%), suspicious-empty,
      consolidation. Gates: hooks + tests.
- [x] **C9 `feat: add single flow definition and run config`** — `flows.py`: the map-reduce DAG,
      `LocalDaskExecutor(num_workers=1)`, `GCS` storage, `VertexRun` (image, Vertex label,
      machine, Infisical env), `state_handlers=[handle_flow_state_change]`,
      `owners=[constants.MATHEUS_ID.value]`, in-flow fire-time jitter, runtime cap, and the
      **per-account single-flight** guard (GCS/BQ soft-lock with TTL).
      DoR: C8. DoD: flow imports and builds; dry "build" test. Gates: hooks + tests.

### [ ] EPIC 1.5 — Walking skeleton: `escalas` end-to-end

- [x] **C10 `feat: implement escalas extractor`** — `extractors/escalas.py`: `planejar_trabalho`
      = single item; `extrair_item` = authenticated GET of `cons_escalas...EXPORTAR_ESCALAS` ->
      `pd.read_csv(BytesIO, sep=";")` -> `{ "escalas": df }`; `colunas_esperadas`.
      DoR: C9, synthetic escalas CSV fixture. DoD: test parses the CSV fixture into the schema.
      Gates: hooks + tests.
- [ ] **C11 `chore: schedule escalas and register flow`** — `schedules.py` with the `escalas`
      daily clock; register the flow in `pipelines/flows.py` (new name, e.g. `sms_sisreg_web`),
      running in parallel with the legacy flows. DoR: C10. DoD: schedule builds; flow registered;
      no collision with legacy datasets. Gates: hooks + tests.
- [ ] **(manual)** parallel-run escalas vs legacy `escala`; cross-dataset parity
      (counts/schema/sample). Proceed only when green.

### [ ] EPIC 2 — Remaining HTTP extractors

- [ ] **C12 `feat: implement afastamentos extractor`** — `extractors/afastamentos.py`:
      `planejar_trabalho` runs the **30-day-active** CPF query on
      `rj-sms.saude_sisreg.oferta_programada` (~2-3k CPFs; treat vigencia columns as DATE, wrap
      in `PARSE_DATE` if text); `extrair_item` fetches current
      (`mostrar_antigos=1`) + historico (`op=Log`) with **one regulador session** (no collapse)
      -> `{ "afastamentos": df1, "afastamento_historico": df2 }`. DoR: C9, fixtures. DoD: tests
      for both page parsers + CPF query builder; suspicious-empty if CPF source empty.
      Gates: hooks + tests.
- [ ] **C13 `feat: implement preparos extractor`** — `extractors/preparos.py`:
      `planejar_trabalho` = all units (no hardcoded limit); `extrair_item` walks
      unit -> procedures -> `<textarea id=preparo>`. DoR: C9, fixtures. DoD: parser tests.
      Gates: hooks + tests.
- [ ] **C14 `feat: implement solicitacoes extractor`** — `extractors/solicitacoes.py`:
      `planejar_trabalho` = date x situacao roteiro over the rolling 180-day window;
      `extrair_item` = GET `gerenciador_solicitacao` -> table; bounded retry (not 100).
      DoR: C9, fixtures. DoD: parser + roteiro tests. Gates: hooks + tests.

### [ ] EPIC 3 — fila_vagas (BigQuery discovery + autorizador scrape)

- [ ] **C15 `feat: implement procedure source from bigquery`** — in `extractors/fila_vagas.py`:
      `obter_procedimentos` reads `rj-sms.saude_sisreg.solicitacoes` (situacao in P/R/D, PPI
      ends-with exclusion, dedup by code) -> distinct procedures. DoR: C9. DoD: test the query
      builder + dedup-by-code logic (mock BigQuery). Gates: hooks + tests.
- [ ] **C16 `feat: implement fila_vagas extractor with autorizador scrape`** —
      `extrair_item` per procedure: `LISTAR` then `APLICAR`; parse units/slots; **match on
      codes, names are labels** -> `{ "fila_e_vagas": df1, "vagas_detalhadas": df2 }`.
      DoR: C15, fixtures. DoD: parser tests for LISTAR/APLICAR + empty/INEXISTENTES paths.
      Gates: hooks + tests.

### [ ] EPIC 4 — Schedules + freshness monitor

- [ ] **C17 `chore: add per-dataset schedule clocks`** — `schedules.py`: daily for
      escalas/afastamentos/solicitacoes/fila_vagas, weekly for preparos; staggered start times;
      per-account grouping respected. DoR: C10-C16. DoD: schedule builds; clocks carry correct
      `parameter_defaults`. Gates: hooks + tests.
- [ ] **C18 `feat: add freshness monitor and discord alerts`** — freshness check over the
      run-log table (alarm if no successful run within SLA) + `send_message`/`send_discord_embed`
      wiring (`data-ingestion` health, `warning` soft-block; `error` is automatic on failure).
      DoR: C8, C17. DoD: tests for SLA computation + alert formatting (mock the sender).
      Gates: hooks + tests.

### [ ] EPIC 5 — README, parity, cutover (no PRs)

- [ ] **C19 `docs: add sisreg readme with mermaid diagrams`** — `README.md` (Brazilian
      Portuguese). Must contain: initiative overview; **directory map describing each item**;
      **all repository conventions** (commits, language/naming, code style, gates, write model,
      anti-ban rules); **Mermaid diagrams** (the DAG; per-account single-flight; the
      curated-input -> scrape data flow); a **brief contribution guide** (how to add a dataset:
      one extractor + one registry entry + one clock; how to run lint/tests; how to commit);
      and a short **"how to delete this cleanly"** note. Lean, simple language, explicit.
      DoR: C9-C18 (so the map is accurate). DoD: renders; links valid; pt-BR; no uncommon symbols.
      Gates: hooks (lint only).
- [ ] **(manual)** full cross-dataset parity (new `brutos_sisreg_web.*` vs legacy) over a few
      cycles; verify idempotency (same-day re-run replaces, no dupes) and robustness (kill a
      mapped item -> run completes, failure logged + alerted).
- [ ] **C20 `chore: disable legacy sisreg schedules`** — turn off the legacy flows' schedules but
      **keep their code** as an instant rollback path for a grace period (~2-4 weeks).
      DoR: parity green. DoD: legacy schedules disabled; unified flow is primary. Gates: hooks.
- [ ] **(grace period ~2-4 weeks under the freshness monitor)**
- [ ] **C21 `chore: remove legacy sisreg flows and rewire registry`** — delete the 6 legacy
      scraper dirs + the dead `sisreg_web_solicitacoes`; update `pipelines/flows.py` imports
      (drop deleted, keep the unified one). DoR: grace period passed with green metrics.
      DoD: repo builds; CI lint green; no dangling imports. Gates: hooks.

---

## 8. Resolved decisions (locked)

- [x] **Test approach** — stdlib `unittest` (no new dependency).
- [x] **Flow owner** — `owners=[constants.MATHEUS_ID.value]` (C9).
- [x] **Curated input columns** — trusted to exist as specified (user instruction / original
      code); no live verification. CPF predicate: `procedimento_vigencia_inicial_data <= hoje AND
      procedimento_vigencia_final_data >= hoje-30d` (~2-3k). Treat the vigencia columns as `DATE`
      (curated layer); if stored as text, wrap in `PARSE_DATE` during C12 — conservative, never
      errors the run.
- [x] **Spikes A-D** — resolved by trusting the original flows + conservative defaults (EPIC 0).
      No live SISREG access needed to build/test: fixtures are synthetic (from the original
      code's known structure). Live validation happens at parallel-run/parity (EPIC 5).
- [x] **Single-flight** — GCS/BQ **soft-lock with TTL**, per account (portable; independent of
      the Prefect tag-limit). C9.
