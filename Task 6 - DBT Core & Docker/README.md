# Task 6 — DBT Core & Docker (MVP)

Minimal dbt Core project: load one seed into Snowflake `$DB_DEV` via Okta SSO.

**Auth reality check:** `$DBT_TECH_USER` is Okta SSO only (no password). `authenticator: externalbrowser` needs a real browser → **run dbt locally for this MVP**.

All connection values live in `.envrc` — do not hardcode them in docs or commands.

---

## Layout

```
Task 6 - DBT Core & Docker/
├── .envrc                 # all env vars (direnv)
├── README.md
└── dbt/
    ├── Dockerfile         # local only (gitignored); not used for SSO MVP
    ├── profiles.yml       # Snowflake SSO profile (env-driven)
    ├── dbt_project.yml
    └── seeds/
        └── sample_countries.csv
```

---

## 0. One-time: direnv

```bash
# install
curl -sfL https://direnv.net/install.sh | bash

# hook zsh (once)
grep -q 'direnv hook zsh' ~/.zshrc || echo 'eval "$(direnv hook zsh)"' >> ~/.zshrc
source ~/.zshrc

# allow this project (run from this Task 6 folder)
direnv allow
direnv status
```

Confirm vars from `.envrc`:

```bash
echo "$SF_ACCOUNT $DBT_TECH_USER $SF_ROLE $SF_WAREHOUSE $DB_DEV $DBT_SCHEMA"
echo "$DBT_PROFILES_DIR $DBT_PROJECT_DIR"
```

Edit `.envrc` if role/warehouse/account differ from your Snowflake access, then `direnv allow` again.

Example `.envrc` (values masked — real file is gitignored):

```bash
# direnv: auto-loads when you cd into this directory
# Setup once: https://direnv.net/ — then: direnv allow

# --- identity ---
export MY_FIRST_NAME="<first>"
export MY_LAST_NAME="<last>"

# --- Snowflake connection (SSO / Okta) ---
# Account form: org-account (from URL app.snowflake.com/<org>/<account>/)
export SF_ACCOUNT="<account>"
export DBT_TECH_USER="<user>"
export SF_ROLE="<role>"
export SF_WAREHOUSE="<warehouse>"
export DB_DEV="<_YOUR_DEV_DATABASE>"
export DBT_SCHEMA="<schema_for_seeds>"

# --- dbt local paths ---
# profiles.yml lives next to the project; do NOT rely on ~/.dbt for this task
export DBT_PROFILES_DIR="$(pwd)/dbt"
export DBT_PROJECT_DIR="$(pwd)/dbt"

# --- later: Docker / key-pair (unused for SSO MVP) ---
# export SNOWFLAKE_PRIVATE_KEY_PATH=""
# export DBT_PASSWORD=""
```

| Var | Purpose |
|---|---|
| `SF_ACCOUNT` | Snowflake account (`org-account` from URL) |
| `DBT_TECH_USER` | Snowflake user (SSO) |
| `SF_ROLE` | Role to assume |
| `SF_WAREHOUSE` | Warehouse |
| `DB_DEV` | Target development database |
| `DBT_SCHEMA` | Target schema for seeds/models |
| `DBT_PROFILES_DIR` | Directory containing `profiles.yml` |
| `DBT_PROJECT_DIR` | dbt project root |

If SSO fails with opaque auth errors, lock `$SF_ACCOUNT` / `$DBT_TECH_USER` spelling and reuse the same values every run (SSO cache is keyed by `account::user`).

---

## 1. One-time: local dbt (Mac)

Prefer a venv (keeps system Python clean). Run from this Task 6 folder:

```bash
# Python 3.12 only. 3.13/3.14 pull a different dbt-core and break pins.
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install pip==24.3.1
python -m pip install dbt-core==1.9.4 dbt-snowflake==1.9.4

dbt --version
# expect: dbt-core 1.9.4 / dbt-snowflake 1.9.4
```

Notes:
- Exact pins. Do **not** use `>=` / `~=` — Snowflake CLI / newer dbt-core will overwrite `snowflake-connector-python` and `click`.
- Re-activate later with: `source .venv/bin/activate` (and stay in this folder so direnv loads).
- If you already mixed packages in this venv: `deactivate 2>/dev/null`, `rm -rf .venv` and recreate.

`.venv/` is already in `.gitignore`.

---

## 2. Smoke-test Snowflake SSO (optional, before dbt)

**Skip this entire section if you want.** Step 3 `dbt debug` opens the same Okta browser flow. Use this only if you want to prove Snowflake access *without* dbt in the loop.

What this checks: `$SF_ACCOUNT` / `$DBT_TECH_USER` / `$SF_ROLE` / `$SF_WAREHOUSE` / `$DB_DEV` from `.envrc` actually work with `externalbrowser`.

### 2a. Install Snowflake CLI (once) — **separate venv**

Do **not** `pip install` this into `.venv`. Latest `snowflake-cli` wants `snowflake-connector-python==4.x` + `click==8.1.8`; `dbt-snowflake==1.9.4` wants connector `<4`. Same venv = the conflict you just hit.

```bash
python3.12 -m venv .venv-snow
source .venv-snow/bin/activate
python -m pip install pip==24.3.1
python -m pip install snowflake-cli==3.10.0

snow --version
# expect: Snowflake CLI version: 3.10.0
```

`.venv-snow/` is gitignored. Homebrew (`brew install snowflake-cli`) is also fine — isolated binary, not the dbt venv.

### 2b. Run the query (Task 6 root, direnv loaded)

`--temporary-connection` (`-x`) avoids needing `~/.snowflake/config.toml`.

```bash
source .venv-snow/bin/activate   # not .venv

# fail fast if direnv didn't load
: "${SF_ACCOUNT:?}" "${DBT_TECH_USER:?}" "${SF_ROLE:?}" "${SF_WAREHOUSE:?}" "${DB_DEV:?}" "${DBT_SCHEMA:?}"

snow sql --temporary-connection \
  --account "$SF_ACCOUNT" \
  --user "$DBT_TECH_USER" \
  --authenticator externalbrowser \
  --role "$SF_ROLE" \
  --warehouse "$SF_WAREHOUSE" \
  --database "$DB_DEV" \
  --schema "$DBT_SCHEMA" \
  -q "select current_user() as user, current_role() as role, current_database() as db, current_warehouse() as wh, current_schema() as sch"
```

Expected:
1. Browser opens → Okta login for `$DBT_TECH_USER`.
2. One row: user / role / db / warehouse / schema match the `.envrc` values.

If the browser never opens, you are in Docker/SSH/headless — stop; this MVP is host-only.

Then continue to step 3.

---

## 3. MVP: debug → compile → seed

From the Task 6 root. If you ran step 2, `.venv-snow` is still active — switch back:

```bash
deactivate 2>/dev/null
source .venv/bin/activate
which dbt
# expect: .../Task 6 - DBT Core & Docker/.venv/bin/dbt

cd "$DBT_PROJECT_DIR"

dbt debug
# → browser Okta login once; expect "All checks passed!"

dbt compile
# → should succeed with empty models (seeds-only project is fine)

dbt seed --select sample_countries
# → loads dbt/seeds/sample_countries.csv into:
#    $DB_DEV.$DBT_SCHEMA.sample_countries
```

Verify in Snowflake:

```sql
-- after substituting env variables' values locally:
select * from <DB_DEV>.<DBT_SCHEMA>.sample_countries;
```

Schema note: target schema is only from `profiles.yml` (`$DBT_SCHEMA`). Do not also set `+schema` in `dbt_project.yml` unless you override `generate_schema_name` — default dbt would create `<target_schema>_<custom>`.

Useful resets:

```bash
dbt clean
dbt seed --full-refresh --select sample_countries
```

---

## 4. Docker — not in this MVP

Production would run dbt in Docker with a **technical user + password** (non-interactive). We do not have that user for this MVP — only Okta SSO / `externalbrowser`, which cannot run headless.

So Docker is the next step for a production implementation: CI builds the image by `COPY`ing this project folder into the image.

---

## 5. Feedback checklist (for next iteration)

After you run the steps, report back:

1. `direnv status` + `echo $SF_ACCOUNT $DBT_PROFILES_DIR`
2. `dbt --version`
3. Full `dbt debug` output (redact nothing sensitive except tokens)
4. `dbt seed --select sample_countries` result
5. Any role/warehouse/account corrections you had to make in `.envrc`

Then we can add a trivial model on top of the seed.
