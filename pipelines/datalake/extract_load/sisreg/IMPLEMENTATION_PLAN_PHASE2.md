# SISREG Unified Flow — Implementation Plan, Phase 2 (Hardening to Prod)

> Working document for the **second pass** over the unified SISREG flow under
> `pipelines/datalake/extract_load/sisreg/`. Phase 1 (`IMPLEMENTATION_PLAN.md`, commits
> `C1`-`C19`) built the whole package and is green on 116 `unittest` tests. An adversarial
> review then found defects that **silently violate the project's own guarantees** (anti-ban,
> "did it load", 100%-or-nothing) plus a failing lint gate. This plan fixes them.
>
> Same rules as Phase 1: **atomic commits (not PRs)**, each with a Definition of Ready, a
> Definition of Done, the exact **commit message**, and the **hooks + tests** that must pass
> first. Tick a `[ ]` box only when the item is truly done. Commit numbering **continues** the
> Phase 1 sequence (Phase 1 ended at `C21`; Phase 2 starts at `C22`).
>
> Design source of truth: Phase 1's design + the three review decisions locked with the user
> (see §1.3). This file is the *how/when*; the audit findings (§1.2) are the *what/why*.
> **This plan is in English; the code and the README stay in Brazilian Portuguese** (Phase 1 §2.3).

---

## 0. How to use this document

- Work top to bottom. Commits are ordered by dependency; do not reorder without updating deps.
- **One logical change = one commit.** Keep commits small enough to revert cleanly.
- Before every commit, run the **Quality Gates** (§3). A commit is allowed only when all its
  listed gates pass.
- **No Pull Requests.** Commit to the current feature branch (`staging/sisreg-refactor-all-matheus`)
  and optionally push. Never commit to `main`/`master` (the `no-commit-to-branch` hook enforces this).
- Update the checkboxes here in the same commit that completes the work.
- **Scope flags.** Commits are tagged `[IN-SCOPE]` (inside `/sisreg/`, do now) or
  `[OUTSIDE /sisreg/]` (touches files outside the directory — **blocked until the user lifts the
  constraint**; listed for completeness, not to be executed yet).

---

## 1. Context

### 1.1 What Phase 1 delivered

- 16 commits (`C1`-`C19`, plus the plan/readme docs), all inside `/sisreg/`.
- The single map-reduce flow, registry, error taxonomy, polite HTTP layer, auth, parsing,
  five extractors, schedules, a freshness monitor module, README, and 116 passing tests.
- **Still pending from Phase 1 (carried into §EPIC 12 here):** `C20` (disable legacy schedules),
  `C21` (delete legacy + rewire `pipelines/flows.py`) — both outside `/sisreg/`.

### 1.2 Audit findings (the work of this phase)

Severity: 🔴 blocker (breaks a guarantee or a merge gate) · 🟠 anti-ban regression ·
🟡 dead/over-engineered or smell · 🔵 verify-before-prod (needs live access).

| ID | Sev | Finding | Evidence |
|---|---|---|---|
| **B1** | 🔴 | Lint not clean. `E501` and isort failures — CI/pre-commit will reject. | `tests/test_afastamentos.py:74` (105>100); `tests/test_tasks.py` (two `normalizar_e_subir` imports need wrapping) |
| **B2** | 🔴 | Empty partition. `data_extracao` defaults to `""` and the schedule never sets it, so every row partitions on `""`. | `flows.py:81`, `schedules.py:51`, `tasks.py:224` |
| **B3** | 🔴 | Run-log records constant garbage. `registrar_log_execucao` is called with literals `items_total=0, items_ok=0, status="OK"`; it always logs `OK` regardless of reality. The freshness monitor keys on exactly this, so it can never fire. | `flows.py:121-126`, `tasks.py:289-340`, `monitor.py:48` |
| **B4** | 🔴 | Freshness monitor never wired, and modeled to run *inside* the extraction flow — but its purpose is to detect the flow **not** running. Must be its own scheduled flow. | `monitor.py:114` (imported by nothing); `flows.py` (no call) |
| **B5** | 🔴 | Silent-failure footgun. `_importar_extratores` swallows `ImportError`; a real import error in prod → default no-op lambdas → empty no-op upload → B3 logs `OK`. | `registry.py:60-70`, `registry.py:45,49` |
| **A1** | 🟠 | Per-item login churn. One full SISREG login **per work-item**: solicitacoes ≈ 1,267/run, afastamentos ≈ 2-3k/run, fila_vagas hundreds-thousands. Violates "one login, reuse the cookie jar"; re-login churn is a named ban trigger. | `tasks.py:129`; `solicitacoes.py:51` (181×7); `afastamentos.py:80`; `fila_vagas.py:100` |
| **A2** | 🟠 | Politeness layer bypassed. Raw `sessao.get` with no jitter sleep and no 403/429/CAPTCHA detection. | `escalas.py:96`; `afastamentos.py:206,215` |
| **A3** | 🟠 | No run-level circuit-break. A block on one mapped child does not stop the other children from continuing to log in and hammer. | `tasks.py:138-141` + `.map` fan-out in `flows.py:95` |
| **D1** | 🟡 | `failover_por_categoria` is dead (never called) **and** unusable (`resolver_credenciais` fetches only one account — no reserve exists). | `auth.py:212`; `tasks.py:42-63` |
| **D2** | 🟡 | `salvar_snapshot_html` never called. | `http.py:151` |
| **D3** | 🟡 | `JITTER_MAXIMO_S` Parameter defined but never used; the "in-flow start jitter" was never built. | `flows.py:68` |
| **D4** | 🟡 | `reautenticar_se_deslogado` never called by any extractor (Phase 1). **Becomes relevant after coarsening** — see §1.3 decision 2 deviation. | `auth.py:157` |
| **S1** | 🟡 | Schema drift only **warns**; the plan said fail loudly. Layout changes ship silently. | `tasks.py:230-239` |
| **S2** | 🟡 | Completeness gate × huge fan-out: one failed item among ~1,267 cancels the entire overwrite, so the table may rarely update. | `tasks.py:192-206` |
| **S3** | 🟡 | Stale docstrings: `escalas` claims a date window it never applies (`dataInicial/dataFinal=""`, `janela_dias` ignored); `solicitacoes` claims re-auth it never does. | `escalas.py:10,84`; `solicitacoes.py:97-98,147-153` |
| **S4** | 🔵 | `fila_vagas` hardcodes probe values `co_cid="H539"`, `nu_classificacao_risco=1` (legacy carry-over). | `fila_vagas.py:182-184` |
| **V1** | 🔵 | Spikes A-D never run (need live SISREG): requests-only escala parity, `verify=True` connectivity, concurrency, real anti-bot ceiling. | Phase 1 EPIC 0 (resolved "by decision", unproven) |
| **V2** | 🔵 | Infisical: `/sisreg_regulacao` assumed to use the same key names `SISREG_USER`/`SISREG_PASSWORD` as `/sisreg`. | `constants.py:87-91` |
| **V3** | 🔵 | `requests`/`lxml` are required at runtime (`parsing.py:92,172`, `pd.read_html`) and present transitively in `poetry.lock`, but not declared in `pyproject.toml`. | `pyproject.toml` (only `beautifulsoup4`) |

### 1.3 Decisions locked with the user (this phase)

1. **Login churn (A1) → coarsen work-items, reuse one session.** Each extractor's
   `planejar_trabalho` returns **one** work-item (or a few bounded batches); the per-sub-item
   loop (CPFs / date×status / procedures) moves **inside** `extrair_item`, reusing the single
   authenticated `sessao` (the `preparos` pattern, already correct). This honors "one login per
   run" and, as a side effect, fixes A3 (one mapped child per run = a clean circuit-break).
   Trade-off accepted: Prefect's per-item retry isolation is replaced by an **in-extractor**
   bounded retry + a structured per-sub-item result (§EPIC 7 contract).
2. **Dead code → wire what matters, delete the rest.** Make the run-log/monitor **truthful**
   and run the monitor as **its own scheduled flow** (B3/B4). **Delete** `failover_por_categoria`
   (D1), `salvar_snapshot_html` (D2), and the `JITTER_MAXIMO_S` param (D3).
   - **Deviation flagged for sign-off:** decision 1 creates long-lived sessions (afastamentos can
     run ~10-15 h), where a mid-run logout is likely. So `reautenticar_se_deslogado` (D4) becomes
     genuinely useful and is **KEPT and WIRED** in §EPIC 8, *not* deleted. If you prefer strict
     deletion, drop the re-auth step in `C29`/`C30`/`C31` and accept "logout = run failure".
     **Default in this plan: keep + wire.**
3. **This turn: write this plan only.** No code or other file changes until you approve.

### 1.4 Goal of Phase 2

Make the flow **mergeable and prod-correct**: green lint, real partitioning, a run-log that
tells the truth, a monitor that can actually detect a stopped schedule, an anti-ban posture that
matches the design (one login/run, every request polite), no dead code, and a crisp pre-prod
checklist for the things only a human with live access can verify.

---

## 2. Conventions & style guide

**Unchanged from Phase 1 §2** (semantic English commits; pt-BR ASCII identifiers; pt-BR
docstrings/comments with accents but no uncommon typographic symbols; `black` line-length 100 +
`isort` black profile + `flake8` ignoring `E203/W503`; every `.py` starts with
`# -*- coding: utf-8 -*-`; no secrets in code; no raw CPFs in logs; SRP; explicit types; YAGNI).
Re-read Phase 1 §2 before each commit. Commit `action` vocabulary is identical
(`feat|fix|refactor|docs|test|chore`).

---

## 3. Quality gates (must pass before EVERY commit)

Identical to Phase 1 §3.

### 3.1 Hooks
```
pre-commit run --files <changed files>      # or: pre-commit run --all-files
task lint                                   # black . && isort . && flake8 .  (mirrors CI)
```

### 3.2 Tests (stdlib `unittest`, offline, no new deps)
```
python -m unittest discover -s pipelines/datalake/extract_load/sisreg/tests -p "test_*.py"
```
Run from the **repo root** (so `import pipelines` resolves). Commits that add/alter *logic* must
add/extend tests and pass them. Pure-docs/scaffolding commits are "lint only".

### 3.3 Branch policy
Feature branch only; pushing allowed; **no PRs**; `no-commit-to-branch` blocks `main`/`master`;
**never** `--no-verify`.

---

## 4. Definition of Ready / Done — global templates

**DoR:** the finding it fixes is in §1.2; upstream commits are done and green; any new fixture
exists (synthetic, offline).

**DoD:** code follows §2; new logic has `unittest` coverage; **all §3 gates pass**; the checkbox
here is ticked in the same commit; no finding is left half-fixed (if a commit claims to fix `Bx`,
`Bx` is fully resolved or the residue is explicitly noted).

---

## 5. New best-practice checks specific to Phase 2

- [ ] **Truthful telemetry:** the run-log row reflects the actual outcome (`items_total`,
      `items_ok`, `status`, `linhas_por_tabela`). No hardcoded status anywhere.
- [ ] **Monitor independence:** the freshness monitor runs on its **own** schedule, reads the
      run-log, and does not depend on the extraction flow having run.
- [ ] **One login per run per account:** assert (in tests, by counting `abrir_sessao_autenticada`
      calls) that each conjunto performs exactly one login for N sub-items.
- [ ] **Every network call is polite:** no raw `sessao.get`/`requests.get` outside
      `requisicao_educada`; block detection runs on every response.
- [ ] **Fail loud, not silent:** schema drift and import errors raise; suspicious-empty raises;
      incomplete sub-items skip the overwrite (never a partial write).
- [ ] **No dead code:** a symbol that ships is reachable from a flow or a test of a reachable
      path; otherwise delete it.

---

## 6. Module/test layout — delta from Phase 1

```
pipelines/datalake/extract_load/sisreg/
  resultado.py          # NEW (C26): ResultadoConjunto + Consolidado contracts (sub-item metrics)
  flows.py              # EDIT: extraction-date task; drop JITTER param; pass real metrics to log
  monitor_flows.py      # NEW (C33): sisreg_monitor_flow + its own daily schedule
  tasks.py              # EDIT: consolidar/normalizar/registrar use the new contract + real metrics
  registry.py           # EDIT (C25): stop swallowing ImportError
  common/
    auth.py             # EDIT (C34): remove failover_por_categoria (keep reautenticar_se_deslogado)
    http.py             # EDIT (C34): remove salvar_snapshot_html
  extractors/
    afastamentos.py     # EDIT (C29): single item, internal CPF loop, polite http, inline re-auth
    solicitacoes.py     # EDIT (C30): single item, internal roteiro loop, inline re-auth
    fila_vagas.py       # EDIT (C31): single item, internal procedure loop
    escalas.py          # EDIT (C32): route through requisicao_educada; fix docstring
  tests/
    test_resultado.py   # NEW (C26)
    test_monitor_flows.py  # NEW (C33)
    test_*.py           # EDIT across the affected modules
  IMPLEMENTATION_PLAN.md
  IMPLEMENTATION_PLAN_PHASE2.md   # this file
  README.md             # EDIT (C35)
```

---

## 7. Task breakdown (EPICs -> commits)

Legend per commit: **DoR** (ready), **DoD** (done), **Commit** (exact message), **Gates**, and a
**Fixes** field linking §1.2 findings. Default gates: `hooks` clean + `tests` green; "lint only"
= no test gate.

### [x] EPIC 6 — Merge gates & correctness (in-scope, no design change)

- [x] **C22 [IN-SCOPE] `test: fix lint in sisreg tests`** — **Fixes B1.**
      - Wrap the long line at `tests/test_afastamentos.py:74` to ≤ 100 chars; run `isort` over
        `tests/test_tasks.py` (the two `normalizar_e_subir` imports). Do **not** change test logic.
      - **DoR:** none. **DoD:** `task lint` exits clean on the whole `/sisreg/` tree; 116 tests
        still pass unchanged. **Gates:** hooks + tests.

- [x] **C23 [IN-SCOPE] `fix: compute extraction date at runtime for partitioning`** — **Fixes B2.**
      - Add `@task obter_data_extracao() -> str` in `tasks.py` returning
        `datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")` (mirror the sibling
        `sisreg_afastamentos` `get_extraction_date` pattern).
      - In `flows.py`: **delete** `DATA_EXTRACAO = Parameter("data_extracao", ...)`; wire
        `data_extracao = obter_data_extracao()` and pass it to `consolidar`.
      - `registrar_log_execucao` already computes its own SP date — leave it, but reuse the same
        value for the partition column to keep `consolidar` and the log row consistent.
      - **DoR:** none. **DoD:** a unit test asserts the task returns `YYYY-MM-DD` (mock the clock
        or assert the format/regex); `consolidar` test asserts the partition column is non-empty
        and equals the passed date. **Gates:** hooks + tests.

- [x] **C24 [IN-SCOPE] `fix: fail loudly on schema drift in consolidar`** — **Fixes S1.**
      - In `consolidar`, when a table's `colunas_esperadas` is non-empty and required columns are
        missing, raise `ErroEstrutura(... etapa="consolidacao", item=tabela ...)` instead of
        logging a warning. Tables with an empty `frozenset()` (escalas, solicitacoes) skip the
        check, as today.
      - Note: this can fail a run if `colunas_esperadas` is mis-specified vs. live HTML; that is
        the intended "fail loud" behavior and will be validated at parity (EPIC 11). Keep the
        expected sets conservative.
      - **DoR:** C23. **DoD:** test: missing required column ⇒ `ErroEstrutura`; complete schema
        ⇒ passes; empty-expected table ⇒ never raises. **Gates:** hooks + tests.

- [x] **C25 [IN-SCOPE] `fix: stop swallowing extractor import errors in registry`** — **Fixes B5.**
      - In `registry._importar_extratores`, remove the `try/except ImportError: pass`. All five
        extractors now exist, so the import must succeed; a real `ImportError` must propagate and
        crash loudly (caught by the flow state-handler in prod), never degrade to no-op lambdas.
      - Keep the function (it still defers the import to break the cycle) — only drop the swallow.
      - Optionally make the dataclass defaults raise `NotImplementedError` instead of returning
        `[{}]`/`{}` so an unregistered conjunto fails loudly rather than no-ops.
      - **DoR:** none. **DoD:** test asserts all five conjuntos resolve to real (non-default)
        `planejar_trabalho`/`extrair_item` after `obter_conjunto`; test asserts a deliberately
        broken import surfaces (not swallowed). **Gates:** hooks + tests.

### [x] EPIC 7 — Extraction-result contract + truthful run-log

- [x] **C26 [IN-SCOPE] `refactor: introduce ResultadoConjunto extraction contract`** —
      **Enables A1/B3/S2 fixes.**
      - New `resultado.py` with two frozen dataclasses:
        - `ResultadoConjunto`: `tabelas: Dict[str, pd.DataFrame]`, `total: int`, `ok: int`,
          `ids_falhos: List[str]` (sub-item ids, **no PII**). Property `incompleto: bool` =
          `ok < total or bool(ids_falhos)`.
        - `Consolidado`: `tabelas: Optional[Dict[str, pd.DataFrame]]`,
          `metricas: Dict[str, Any]` (`items_total`, `items_ok`, `linhas_por_tabela`,
          `status_parcial`).
      - Update `extrair_item` (task) to return `Optional[ResultadoConjunto]`: still re-raises
        `ErroBloqueio`; other `ErroSisreg` ⇒ `None`. For now, wrap existing single-table extractor
        returns as `ResultadoConjunto(tabelas=<dict>, total=1, ok=1, ids_falhos=[])` so the four
        already-single-item extractors (escalas, preparos) and the soon-to-be-coarsened ones share
        one contract. Extractors will populate real sub-metrics in EPIC 8.
      - **DoR:** C25. **DoD:** `test_resultado.py` covers `incompleto`; existing extractor tests
        updated to the new return type; all green. **Gates:** hooks + tests.

- [x] **C27 [IN-SCOPE] `feat: skip overwrite on incomplete sub-items`** — **Fixes S2 (completeness
      at sub-item granularity).**
      - `consolidar` now consumes `List[Optional[ResultadoConjunto]]` and returns a `Consolidado`:
        - any `None` element ⇒ item-level failure ⇒ `tabelas=None` + alert (`warning`).
        - aggregate `total`/`ok`/`ids_falhos`; if aggregate is incomplete ⇒ `tabelas=None` +
          alert (the 100% gate, now sub-item aware). Last good table stays live.
        - else concat per table, add the partition column, run the C24 schema check, return
          `tabelas` populated.
        - always populate `metricas` (used by the log) regardless of outcome.
      - **DoR:** C26. **DoD:** tests: full success ⇒ tables returned + metrics correct; one
        `ids_falhos` entry ⇒ tables `None`, metrics show the shortfall; one `None` element ⇒
        tables `None`. **Gates:** hooks + tests.

- [x] **C28 [IN-SCOPE] `fix: record real metrics and status in run log`** — **Fixes B3.**
      - `normalizar_e_subir` takes the `Consolidado`, uploads `consolidado.tabelas` (None ⇒ skip),
        returns a `bool subiu`.
      - `registrar_log_execucao` takes `consolidado` + `subiu` and derives the **real** row:
        `items_total/items_ok/linhas_por_tabela` from `consolidado.metricas`; `status` =
        `"OK"` if `subiu` else `"FALHA_PARCIAL"` if `consolidado is not None` else `"FALHA"`.
        Remove the hardcoded `0/0/"OK"` literals from the `flows.py` call site.
      - **DoR:** C27. **DoD:** tests: success ⇒ row `status="OK"` with real counts; gate-skip ⇒
        `status="FALHA_PARCIAL"`; upstream crash (consolidado `None`) ⇒ `status="FALHA"`. The
        freshness monitor's existing tests still pass against these rows. **Gates:** hooks + tests.

### [x] EPIC 8 — Anti-ban: coarsen to one reused session (login-churn fix)

> Pattern for all four: `planejar_trabalho` returns **one** item carrying the full sub-work list
> (CPFs / roteiro / procedures); `extrair_item` opens **one** session and loops internally, every
> request via `requisicao_educada`; per sub-item bounded retry; classify failures —
> `ErroBloqueio`/`ErroAutenticacao` abort the whole run (raise ⇒ circuit-break), recoverable
> per-sub-item errors append to `ids_falhos`; return a `ResultadoConjunto` with real
> `total/ok/ids_falhos`. Assert "exactly one login" in tests.

- [x] **C29 [IN-SCOPE] `refactor: coarsen afastamentos to a single reused session`** —
      **Fixes A1 (worst), A2 (afastamentos), wires D4.**
      - `planejar_trabalho_afastamentos` returns `[{"id": "todos_os_cpfs", "cpfs": [...]}]` (still
        runs the 30-day-active BQ query; raises `ErroVazioSuspeito` if empty). **Never** log raw
        CPFs — index as `cpf_0`, `cpf_1` ….
      - `extrair_item_afastamentos` loops CPFs on one regulador session; replace the two raw
        `sessao.get` (lines ~206/215) with `requisicao_educada`; before each page-parse, call
        `reautenticar_se_deslogado(...)` and re-issue on logout (D4 wiring). Concatenate per CPF
        into the two tables; build `ResultadoConjunto(total=len(cpfs), ok=..., ids_falhos=[cpf_i…])`.
      - **DoR:** C28. **DoD:** test (mock session) asserts: one `abrir_sessao_autenticada` call for
        N CPFs; both pages go through `requisicao_educada`; a single CPF parse failure lands in
        `ids_falhos` (does not abort); an injected `ErroBloqueio` aborts; no raw CPF in any log
        string. **Gates:** hooks + tests.

- [x] **C30 [IN-SCOPE] `refactor: coarsen solicitacoes to a single reused session`** —
      **Fixes A1, S3 (solicitacoes docstring).**
      - `planejar_trabalho_solicitacoes` returns `[{"id": "roteiro", "roteiro": _gerar_roteiro(janela_dias)}]`.
      - `extrair_item_solicitacoes` loops the date×status roteiro on one session (already via
        `requisicao_educada`); add the inline `reautenticar_se_deslogado` on `LOGOUT` the docstring
        already promises (currently it just raises). Build `ResultadoConjunto` with real counts.
      - **DoR:** C28. **DoD:** test: one login for the full roteiro; `LOGOUT` triggers one re-auth
        then continues; bounded retry honored (≤ `MAX_TENTATIVAS_ITEM`); docstring matches behavior.
        **Gates:** hooks + tests.

- [x] **C31 [IN-SCOPE] `refactor: coarsen fila_vagas to a single reused session`** — **Fixes A1.**
      - `planejar_trabalho_fila_vagas` returns `[{"id": "procedimentos", "procedimentos": <df rows>}]`
        (still the BQ discovery; trust codes not names; raises `ErroVazioSuspeito` if empty).
      - `extrair_item_fila_vagas` loops procedures on one session (already via `requisicao_educada`),
        LISTAR→APLICAR per procedure, INEXISTENTES path preserved; build `ResultadoConjunto`.
      - **DoR:** C28. **DoD:** test: one login for N procedures; INEXISTENTES yields the
        null-`qtd_pend` row in `ids_ok` (not a failure); structure error on one procedure ⇒
        `ids_falhos`. **Gates:** hooks + tests.

- [x] **C32 [IN-SCOPE] `fix: route escalas through polite http and correct docstring`** —
      **Fixes A2 (escalas), S3 (escalas docstring).**
      - Replace the raw `sessao.get` (line ~96) with `requisicao_educada` (gains jitter + block
        detection even for the single request). Reading the CSV from `resposta.content` is fine.
      - Correct the docstring: escalas pulls the **full current export**; it does **not** apply a
        date window. Either drop the unused `janela_dias`/`params` references or document why they
        are ignored. Keep `colunas_esperadas` empty (trusted) until parity refines it.
      - **DoR:** C28. **DoD:** test: the GET goes through `requisicao_educada`; a 403/CAPTCHA
        response raises `ErroBloqueio`; docstring no longer claims a date window. **Gates:** hooks + tests.

### [x] EPIC 9 — Observability done right & remove dead code

- [x] **C33 [IN-SCOPE] `feat: add standalone freshness monitor flow`** — **Fixes B4.**
      - New `monitor_flows.py`: a tiny Prefect flow `sisreg_monitor_flow` whose only task is
        `verificar_frescor_conjuntos(dataset_id, environment)` (already in `monitor.py`), with the
        same `VertexRun`/`GCS`/`state_handlers`/`owners` infra as the main flow and its **own**
        daily `Schedule` (e.g. one clock at a fixed morning time), `parameter_defaults`
        `{environment:"prod", dataset_id: BRUTOS_SISREG_WEB}`.
      - This flow runs **independently** of the extraction flow, so it detects "the extraction
        flow stopped firing" — the absence-of-signal failure mode the monitor exists for.
      - **DoR:** C28 (the run-log now carries truthful `status`). **DoD:** `test_monitor_flows.py`
        builds the flow and asserts its schedule/parameters; existing `test_monitor.py` SLA tests
        still pass. **Gates:** hooks + tests.

- [x] **C34 [IN-SCOPE] `chore: remove unused failover, snapshot and jitter scaffolding`** —
      **Fixes D1, D2, D3.**
      - Delete `failover_por_categoria` from `auth.py` (D1 — no reserve account is fetched; dead
        and unusable) and its tests in `test_auth.py`.
      - Delete `salvar_snapshot_html` from `http.py` (D2) and any reference/test.
      - Delete the `JITTER_MAXIMO_S` Parameter from `flows.py` (D3).
      - **Keep** `reautenticar_se_deslogado` (now wired in EPIC 8 — see §1.3 deviation).
      - **DoR:** C29-C32 (so re-auth is wired before the cleanup decides what stays). **DoD:** no
        dangling imports; `flake8` reports no unused symbols; full suite green; README updated in
        C35 to drop the removed pieces. **Gates:** hooks + tests.

- [x] **C35 [IN-SCOPE] `docs: update readme and phase-2 plan for hardened architecture`** —
      - Update `README.md` (pt-BR): the DAG now ends at `registrar_log_execucao`; the freshness
        monitor is a **separate** flow (update the Mermaid `I-->J` to a standalone diagram);
        failover/snapshot removed from the directory map and "anti-ban" section; document
        "one login per run" (coarsened items) and the sub-item completeness gate.
      - Tick the completed Phase 2 checkboxes here.
      - **DoR:** C22-C34 done. **DoD:** README renders; pt-BR; no uncommon symbols; matches code.
        **Gates:** hooks (lint only).

### [ ] EPIC 10 — Out-of-scope wiring (BLOCKED until the constraint is lifted)

- [ ] **C36 [OUTSIDE /sisreg/] `chore: declare requests and lxml in pyproject`** — **Fixes V3.**
      - Add explicit `requests` and `lxml` to `pyproject.toml` (both used directly; present only
        transitively today). No version bumps beyond what `poetry.lock` already pins.
      - **DoR:** user lifts the `/sisreg/`-only constraint. **DoD:** `poetry lock --no-update`
        clean; imports unaffected. **Gates:** hooks.

- [ ] **C37 [OUTSIDE /sisreg/] `chore: register unified and monitor flows`** —
      - In `pipelines/flows.py`, add `from pipelines.datalake.extract_load.sisreg.flows import *`
        and `... .monitor_flows import *`. Without this the flows do not deploy at all.
      - **DoR:** C33; constraint lifted. **DoD:** `pipelines/flows.py` imports clean; both flows
        discoverable; CI lint green. **Gates:** hooks.

### [ ] EPIC 11 — Pre-prod verification (live / human-in-the-loop — cannot be done from here)

- [ ] **(manual) V1 — Spikes A-D on live SISREG (low volume, careful):** confirm the authenticated
      `requests` escala export matches the legacy download; `verify=True` connects (else pin the
      `certifi` CA bundle, never `verify=False`); confirm the single-flight assumption; observe the
      real CAPTCHA/rate ceiling to tune `DELAY_MINIMO_S`/`DELAY_MAXIMO_S`.
- [ ] **(manual) V2 — Infisical:** confirm `/sisreg_regulacao` exposes `SISREG_USER`/`SISREG_PASSWORD`
      (or correct `constants.PERFIS_CREDENCIAL`).
- [ ] **(manual) S4 — fila_vagas probe values:** confirm `co_cid="H539"` / `nu_classificacao_risco=1`
      are still valid against the live autorizador.
- [ ] **(manual) Parity:** parallel-run `brutos_sisreg_web.*` vs legacy `brutos_sisreg_*`
      (counts/schema/sample) for a few cycles per dataset.
- [ ] **(manual) Idempotency:** re-run a dataset same day ⇒ overwrite replaces, no dupes.
- [ ] **(manual) Robustness:** inject a sub-item failure ⇒ run completes, overwrite skipped,
      run-log row `FALHA_PARCIAL`, monitor would alert if SLA breached.

### [ ] EPIC 12 — Cutover (carryover of Phase 1 C20/C21 — OUTSIDE /sisreg/)

> These are Phase 1's `C20`/`C21`, unchanged, repeated here so the end-to-end path is in one place.
> Renumbered to continue the sequence.

- [ ] **(manual)** full cross-dataset parity green over several cycles (EPIC 11).
- [ ] **C38 [OUTSIDE /sisreg/] `chore: disable legacy sisreg schedules`** — turn off the six legacy
      flows' schedules but **keep their code** as an instant rollback for the grace period.
      **DoR:** parity green; constraint lifted. **DoD:** legacy schedules off; unified flow primary.
      **Gates:** hooks.
- [ ] **(grace period ~2-4 weeks under the standalone freshness monitor).**
- [ ] **C39 [OUTSIDE /sisreg/] `chore: remove legacy sisreg flows and rewire registry`** — delete the
      six legacy scraper dirs + the dead `sisreg_web_solicitacoes`; drop their imports from
      `pipelines/flows.py` (keep the unified + monitor flows). **DoR:** grace period green.
      **DoD:** repo builds; CI lint green; no dangling imports. **Gates:** hooks.

---

## 8. Finding -> commit traceability

| Finding | Resolved by |
|---|---|
| B1 lint | C22 |
| B2 empty partition | C23 |
| B3 run-log garbage | C26 + C27 + C28 |
| B4 monitor not wired / wrong shape | C33 |
| B5 swallowed ImportError | C25 |
| A1 login churn | C29 + C30 + C31 (escalas/preparos already single) |
| A2 politeness bypass | C29 (afastamentos) + C32 (escalas) |
| A3 no circuit-break | C29-C31 (one mapped child per run) |
| D1 dead failover | C34 |
| D2 dead snapshot | C34 |
| D3 dead jitter param | C34 |
| D4 re-auth unwired | C29/C30/C31 (kept + wired; see §1.3 deviation) |
| S1 schema warns not fails | C24 |
| S2 gate × fan-out brittleness | C26 + C27 (sub-item completeness) |
| S3 stale docstrings | C30 + C32 |
| S4 fila_vagas probe values | EPIC 11 (manual) |
| V1 spikes / V2 Infisical / V3 deps | EPIC 11 + C36 |

---

## 9. Resolved decisions (locked, Phase 2)

- [x] **Login churn** — coarsen to one reused session per run (§1.3.1).
- [x] **Dead code** — wire run-log/monitor truthfully + monitor as its own flow; delete failover,
      snapshot, jitter param; **keep + wire** re-auth (§1.3.2, deviation flagged for sign-off).
- [x] **Sequencing** — contract first (C26), then truthful log (C27/C28), then coarsening (EPIC 8),
      then cleanup (C34): each step keeps the suite green.
- [x] **Scope** — `C22`-`C35` are inside `/sisreg/` and may proceed now; `C36`-`C39` touch files
      outside `/sisreg/` and stay **blocked** until the user lifts the constraint.
- [ ] **Open — re-auth keep/delete:** default is keep+wire (D4). Flip only on explicit user veto.
```
