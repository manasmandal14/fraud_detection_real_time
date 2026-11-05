# Databricks Environment Setup Guide (Dev -> UAT -> Prod)

A reusable, step-by-step guide for setting up a Databricks project end to end on **Azure**:
secrets in Azure Key Vault, a parameterized notebook, a Declarative Automation Bundle (DAB) with
per-environment targets, service principals with least-privilege permissions, and GitHub Actions
CI/CD that deploys as a service principal without stored secrets.

It was written while building `fraud_detection_real_time` (Kafka -> bronze) and is generalized so it can be
reused for any project and for any higher environment (UAT, prod, ...).

**How to use it:** replace the placeholders below with your values, then work through the phases in order.
Every step ends with a **Check**. Do not continue until the check passes.

---

## 0. Placeholders and naming conventions

| Placeholder | Meaning | Example (this project) |
| --- | --- | --- |
| `<app>` | Bundle / application name (`bundle.name`) | `fraud_detection_real_time` |
| `<app-short>` | Short name used in identities and groups | `fraud-detection` |
| `<env>` | Environment / bundle target name (lowercase) | `dev`, `uat`, `prod` |
| `<workspace-url>` | Workspace URL of that environment | `https://adb-7405608228214157.17.azuredatabricks.net` |
| `<catalog>` / `<schema>` | Unity Catalog names for that environment | `161pmprojectcatalog_uat` / `uat_catlocation` |
| `<storage-account>` | ADLS Gen2 account for managed storage | `manasmandal1408` |
| `<kv-name>` | Azure Key Vault name | `kv-fraud-dev-01` |
| `<scope>` | Databricks secret scope name (same in every workspace) | `kv-fraud-detection` |
| `<sp-app-id>` | Application (client) ID of the environment's service principal | `6437fc33-...` |
| `<tenant-id>` | Microsoft Entra Directory (tenant) ID | |
| `<gh-owner>` / `<gh-repo>` | GitHub owner and repository | `manasmandal14` / `fraud_detection_real_time` |
| `<gh-owner-id>` / `<gh-repo-id>` | Numeric GitHub IDs (see Phase 7) | `64917018` / `1437059802` |
| `<branch>` | Git branch that deploys the environment | `DEVELOP` -> UAT, `Master` -> prod |

**Naming conventions (recommended)**

| Object | Pattern | Example |
| --- | --- | --- |
| Service principal | `sp-<app-short>-<env>-entra` (one per environment) | `sp-fraud-detection-uat-entra` |
| Job identity group | `grp-<app-short>-<env>-jobs` | `grp-fraud-detection-uat-jobs` |
| Developer group | `grp-<app-short>-developers` | `grp-fraud-detection-developers` |
| Deployment folder | `/Workspace/Deployments/<app>/<env>` | `/Workspace/Deployments/fraud_detection_real_time/uat` |
| Resource file | `resources/<name>.<type>.yml` | `resources/kafka_bronze_ingestion.job.yml` |
| Config file | `config/<env>.json` (file name = target name) | `config/uat.json` |

**Golden rules**

1. Never put secrets in code, config files, notebook outputs, or Git. Secrets live only in Key Vault.
2. One service principal per higher environment. People never own or run UAT/prod jobs.
3. Grant to **groups**, never to individual users or service principals.
4. Higher environments are deployed **only** by CI/CD. No manual `bundle deploy` to UAT/prod.
5. Platform objects (workspace, metastore, storage, external locations, catalogs, schemas, secret scopes)
   are set up once by the platform team. The bundle deploys only application objects (jobs, code, permissions).

---

## 1. Target architecture

```
feature branch --PR--> <branch> (e.g. DEVELOP) --push--> GitHub Actions (environment <env>)
                                                          | 1. GitHub issues OIDC token
                                                          | 2. Entra ID exchanges it for an SP token (federated credential)
                                                          | 3. Databricks CLI: bundle validate + deploy --target <env>
                                                          v
                                   <env> workspace
                                   /Workspace/Deployments/<app>/<env>/files   <- code + config
                                   Job owned by and running as sp-<app-short>-<env>-entra
                                   Reads secrets from scope <scope> -> Azure Key Vault
                                   Writes to <catalog>.<schema> (UC grants via grp-<app-short>-<env>-jobs)
```

**Who owns what**

| Layer | Owner | Created by |
| --- | --- | --- |
| Workspace, metastore assignment, storage, external location | Platform team | Azure portal / account console (Phase 3) |
| Catalog, schema, volume | Platform team | SQL script (Phase 3) |
| Key Vault + secrets, secret scope | Security / platform team | Phase 2 |
| Service principals, groups, grants | Identity / platform team | Phase 6 |
| Job, code, config, job permissions | Application team | Bundle via CI/CD (Phases 4, 5, 7) |
| Tables | Application | Created by the pipeline on first write |

---

## 2. Prerequisites

| Requirement | Why |
| --- | --- |
| Azure Databricks workspaces on the **Premium** tier, one per environment, same region | Unity Catalog permissions and secret scope ACLs need Premium. Same region = same metastore. |
| Account admin on the Databricks account | Create service principals and groups, assign workspaces |
| Owner (or User Access Administrator) on the Key Vault / resource group | Assign Key Vault roles |
| Permission to create Entra app registrations | Entra ID-managed service principals |
| A GitHub repository for the project | CI/CD |
| Databricks CLI in the workspace web terminal (installed automatically) | Bundle commands |

---

## Phase 1: Repository and bundle structure

### 1.1 Layout

```
<app>/                          <- Git root = bundle root
├── databricks.yml              <- bundle name, include, variables, sync, targets
├── resources/                  <- one YAML per deployable resource
│   └── <name>.job.yml
├── config/                     <- per-environment names (NO secrets)
│   ├── dev.json
│   ├── uat.json
│   └── prod.json
├── src/
│   ├── ingestion/              <- source -> bronze
│   └── transformation/         <- bronze -> silver (-> gold)
├── setup/                      <- one-time / test notebooks (not deployed)
├── tests/  fixtures/
├── archive/                    <- retired code (not deployed)
├── docs/                       <- this guide
└── .github/workflows/          <- CI/CD
```

### 1.2 Rules

* Git root and bundle root are the same folder.
* Empty folders are not tracked by Git; add `.gitkeep` if a folder must exist.
* Notebooks are stored as `<name>.py.ipynb` in Git. Bundle paths must use the real file name **including `.ipynb`**.
* `.gitignore` must include `.databricks/`, `dist/`, `build/`, `.venv/`, `__pycache__/`.
* Clear notebook outputs before committing (outputs can contain data such as card numbers).

**Check:** `git status` clean; `src/` contains only current code; nothing secret in `config/`.

---

## Phase 2: Secrets in Azure Key Vault

Repeat 2.1-2.3 per Key Vault (recommended: one Key Vault per environment), and 2.4-2.6 **per workspace**.

### 2.1 Create the Key Vault (Azure portal)

| Setting | Value |
| --- | --- |
| Region | Same as the workspace |
| Pricing tier | Standard |
| Soft delete / Purge protection | On / On (enterprise) |
| Permission model | **Azure role-based access control** |
| Networking | Start with all networks; later restrict and keep **Allow trusted Microsoft services** on |

### 2.2 Give yourself access to manage secrets

Key Vault -> Access control (IAM) -> Add role assignment -> **Key Vault Secrets Officer** -> your user.
With RBAC even subscription owners cannot read/write secrets without this data-plane role.

### 2.3 Add secrets

Key Vault -> Objects -> Secrets -> Generate/Import (Manual), one secret per value.

* Names may contain only letters, digits and **dashes** (no underscores): e.g. `kafka-api-key`, `kafka-api-secret`, `kafka-bootstrap-servers`.
* Watch for leading/trailing spaces or line breaks when pasting.
* Delete any plain-text copy afterwards.

**Check:** secrets listed as Enabled; Show Secret Value matches.

### 2.4 Let Databricks read the vault (once per Key Vault)

Key Vault -> Access control (IAM) -> Add role assignment -> **Key Vault Secrets User** ->
member **AzureDatabricks** (application ID `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`), scope = this vault.
This one assignment covers every workspace in the tenant.

### 2.5 Create the Key Vault-backed secret scope (per workspace)

Open `<workspace-url>/#secrets/createScope` (case-sensitive; put it after `?o=<id>` if present).

| Field | Value |
| --- | --- |
| Scope Name | `<scope>` (same name in every workspace so code is identical) |
| Manage Principal | **Creator** |
| DNS Name | Vault URI (Key Vault -> Properties) |
| Resource ID | Key Vault Resource ID |

A scope belongs to **one** workspace: create it again in every workspace. CLI/API creation needs a Microsoft Entra token, not a PAT.

### 2.6 Verify (web terminal of that workspace)

```bash
databricks secrets list-scopes                 # <scope> with backend AZURE_KEYVAULT
databricks secrets list-secrets <scope>        # secret names only
```

In a scratch notebook: read each secret with `dbutils.secrets.get("<scope>", "<key>")` and print **only its length**.
Never print values (redaction only hides exact full strings). Clear outputs afterwards.

---

## Phase 3: Platform setup per higher environment (UAT / prod)

### 3.1 Workspace

Create the workspace (Azure portal): Premium tier, same region as dev.

**Check:** the workspace shows the same metastore as dev (Catalog -> metastore details); serverless is available.

### 3.2 Storage for the environment's catalog

1. Storage account -> Containers -> create a private container per environment (e.g. `uat`, `prod`).
   Enterprise: prefer a separate storage account + access connector per environment.
2. Make sure the access connector behind the storage credential has **Storage Blob Data Contributor** on the account or container.

### 3.3 External location (SQL, run once)

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS `<app-short>-<env>-catalog-location`
  URL 'abfss://<env>@<storage-account>.dfs.core.windows.net/catalog'
  WITH (STORAGE CREDENTIAL `<storage-credential>`)
  COMMENT 'Managed storage root for the <env> catalog';
```

**Check:** `DESCRIBE EXTERNAL LOCATION ...`; Catalog Explorer -> Test connection (read/list/write/delete pass);
the credential and external location are accessible from the environment's workspace (Workspaces tab).

### 3.4 Catalog, schema, volume (SQL, run once in the environment's workspace)

```sql
CREATE CATALOG IF NOT EXISTS `<catalog>`
  MANAGED LOCATION 'abfss://<env>@<storage-account>.dfs.core.windows.net/catalog'
  COMMENT '<app> catalog (<env>)';

CREATE SCHEMA IF NOT EXISTS `<catalog>`.`<schema>`
  COMMENT '<app> schema (<env>)';

CREATE VOLUME IF NOT EXISTS `<catalog>`.`<schema>`.`checkpoints`
  COMMENT 'Structured Streaming checkpoints (<env>)';
```

* Use backticks (names may start with a digit or contain dashes).
* Optional: Catalog -> Workspaces tab -> bind the catalog to this environment's workspace only.

**Check:** `DESCRIBE CATALOG EXTENDED`, `SHOW SCHEMAS IN`, `SHOW VOLUMES IN` show the objects; names match `config/<env>.json`.

### 3.5 Deployment folder (UI, as workspace admin)

Create `/Workspace/Deployments/<app>` (do **not** create `<env>` below it; the bundle creates it).

| Principal | Permission on `/Workspace/Deployments/<app>` |
| --- | --- |
| `grp-<app-short>-<env>-jobs` | Can Manage |
| `grp-<app-short>-developers` | Can Read |
| All workspace users | none |

**Check (web terminal):**

```bash
databricks workspace get-status /Workspace/Deployments/<app> -o json   # note object_id
databricks workspace get-permissions directories <object_id>
```

Why not `/Workspace/Shared`: writable by all users (the CLI warns). Why not the SP home folder: path shows a UUID.

---

## Phase 4: Parameterize the code

### 4.1 Environment parameter (first code cell)

```python
dbutils.widgets.dropdown("env", "dev", ["dev", "uat", "prod"], "Environment")
env = dbutils.widgets.get("env")
print(f"Running in Environment: {env}")
```

The job passes `env` as a job parameter; the dropdown rejects unknown values (fail fast).

### 4.2 Config file per environment

`config/<env>.json` - same keys in every file, names only, never secrets:

```json
{
  "CATALOG": "<catalog>",
  "SCHEMA": "<schema>",
  "KAFKA_TABLE": "kafka_ingestion_bronze",
  "CHECKPOINT_VOLUME": "checkpoints",
  "CHECKPOINT_FOLDER": "cc_fraud_kafka_ingestion",
  "KAFKA_TOPIC": "credit_card_transactions",
  "STARTING_OFFSETS": "earliest",
  "SECRET_SCOPE": "<scope>"
}
```

### 4.3 Load and validate config (relative path, fail fast)

```python
import os, json

config_relative_path = os.path.join("..", "..", "config", f"{env}.json")   # from src/<folder>/
config_path = os.path.realpath(config_relative_path)
if not os.path.isfile(config_path):
    raise FileNotFoundError(f"Config file for env '{env}' not found at {config_path}")
with open(config_path) as f:
    config = json.load(f)

REQUIRED_KEYS = ["CATALOG", "SCHEMA", "KAFKA_TABLE", "CHECKPOINT_VOLUME",
                 "CHECKPOINT_FOLDER", "KAFKA_TOPIC", "STARTING_OFFSETS", "SECRET_SCOPE"]
missing = [k for k in REQUIRED_KEYS if k not in config]
if missing:
    raise KeyError(f"Config '{config_path}' is missing keys: {missing}")
for k in REQUIRED_KEYS:
    print(f"  {k:20s}: {config[k]}")
```

Never use an absolute `/Workspace/Users/...` path - it breaks for other users, service principals and deployments.

### 4.4 Build full names in one place

```python
catalog, schema = config["CATALOG"], config["SCHEMA"]
bronze_table_fqn = f"`{catalog}`.`{schema}`.`{config['KAFKA_TABLE']}`"
checkpoint_path = f"/Volumes/{catalog}/{schema}/{config['CHECKPOINT_VOLUME']}/{config['CHECKPOINT_FOLDER']}"
```

### 4.5 Streaming write rules

* `.trigger(availableNow=True)` for triggered jobs; `streaming_query.awaitTermination()` so the task waits and surfaces errors.
* One checkpoint per stream per environment; never share a checkpoint between jobs.
* Keep the checkpoint path stable - a new path re-reads the source.
* Credentials: build the Kafka JAAS string inline from `dbutils.secrets.get(...)`; never store or print it.

**Check:** run the notebook top to bottom for `env=dev`; printed table/checkpoint are correct; rerun gives no duplicates
(same streaming query ID = same checkpoint).

---

## Phase 5: Bundle configuration

### 5.1 `databricks.yml` template

```yaml
bundle:
  name: <app>
  engine: direct

include:
  - resources/*.yml

variables:
  notification_email:
    description: Email address that receives job failure notifications
    default: <team-email>

# Not copied to the workspace on deploy (.gitignore entries are excluded automatically)
sync:
  exclude:
    - archive/**
    - setup/**
    - tests/**
    - fixtures/**
    - docs/**
    - .vscode/**
    - .github/**
    - AGENTS.md

targets:
  dev:
    default: true
    mode: development
    workspace:
      host: <dev-workspace-url>
      sync_mode: overwrite          # copy files on every deploy (older CLIs: presets.source_linked_deployment: false)

  uat:
    mode: production
    workspace:
      host: <uat-workspace-url>
      root_path: /Workspace/Deployments/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: <uat-sp-app-id>
    permissions:
      - level: CAN_MANAGE
        group_name: grp-<app-short>-uat-jobs
      - level: CAN_RUN
        group_name: grp-<app-short>-developers

  prod:
    mode: production
    workspace:
      host: <prod-workspace-url>
      root_path: /Workspace/Deployments/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: <prod-sp-app-id>
    permissions:
      - level: CAN_MANAGE
        group_name: grp-<app-short>-prod-jobs
      - level: CAN_VIEW
        group_name: grp-<app-short>-developers
```

Notes:

* Target-level `permissions` accept only `CAN_VIEW`, `CAN_RUN`, `CAN_MANAGE` (`CAN_RUN` becomes `CAN_MANAGE_RUN` on jobs).
* Declare every principal that has permissions on the deployment folder (including inherited ones), otherwise
  the CLI warns "workspace folder has permissions not configured in bundle".
* `engine: direct` avoids the Terraform download and is required for some newer resource types (e.g. catalogs).
* Do not set `default: true` on higher environments; every command must say `--target <env>`.

### 5.2 Job resource template (`resources/<name>.job.yml`)

```yaml
resources:
  jobs:
    <job_key>:
      name: <job_name>
      max_concurrent_runs: 1          # never two runs on one checkpoint
      timeout_seconds: 3600           # remove for continuous jobs
      queue:
        enabled: true
      parameters:
        - name: env
          default: ${bundle.target}   # loads config/<target>.json
      email_notifications:
        on_failure:
          - ${var.notification_email}
      tags:
        project: <app-short>
        layer: bronze
        env: ${bundle.target}
      tasks:
        - task_key: <task_key>
          notebook_task:
            notebook_path: ../src/ingestion/<notebook>.py.ipynb   # relative to resources/, real extension
          max_retries: 1
          min_retry_interval_millis: 60000
          # no compute block -> serverless
```

Tip: start from an existing UI job - Jobs -> job -> kebab menu -> **View as code** -> YAML, then make the
notebook path relative, set `env` to `${bundle.target}`, and remove `run_as`, `job_id` and default fields.

### 5.3 Validate and deploy dev (dev workspace web terminal, at the bundle root)

```bash
databricks bundle validate --target dev
databricks bundle summary  --target dev
databricks bundle deploy   --target dev
databricks bundle run <job_key> --target dev
```

**Check:** job `[dev <user>] <job_name>` exists, parameter `env = dev`, notebook path under
`.bundle/<app>/dev/files/...`, run succeeds. Delete any manually created duplicate job (one writer per checkpoint).

Rules: change bundle jobs only via YAML + deploy (UI edits are overwritten); with `sync_mode: overwrite`,
redeploy after every code/config change.

---

## Phase 6: Identities and permissions (per higher environment)

### 6.1 Service principal types

| | Databricks-managed | Microsoft Entra ID-managed |
| --- | --- | --- |
| Lives in | Databricks account only | Entra ID (app registration), then added to Databricks |
| Credentials | Databricks OAuth secret or Databricks federation policy | Entra client secret/certificate or **federated credential** |
| Access to Azure resources | No | Yes (Azure RBAC) |
| Typical use | Pure Databricks automation | Azure enterprises with central identity governance |

Inside Databricks both behave the same (jobs, UC grants, scopes). This guide uses the **Entra ID-managed** SP.

### 6.2 Create the service principal

1. Azure portal -> Microsoft Entra ID -> App registrations -> New registration
   (`sp-<app-short>-<env>-entra`, single tenant, no redirect URI). Note **Application (client) ID** and **Directory (tenant) ID**.
2. Databricks account console -> User management -> Service principals -> Add service principal.
   * With automatic identity management: search the name and select it.
   * Otherwise: Management = Microsoft Entra ID managed, paste the Application ID.
   * Alternative: workspace Settings -> Identity and access -> Service principals -> Add new.

**Check:** the Application ID in Databricks equals the Azure Application (client) ID (not the Object ID).
Roles tab: Account admin / Marketplace admin / Billing admin all **off**.

### 6.3 Job group and workspace assignment

1. Account console -> Groups -> `grp-<app-short>-<env>-jobs`; add the SP (no humans).
2. Account console -> Workspaces -> `<env>` workspace -> Permissions -> add the group with **User** (not Admin).
3. Workspace Settings -> Identity and access -> Service principals: Workspace access on, cluster creation off.

### 6.4 Roles on the service principal (who may use it)

Account console -> Service principals -> SP -> **Permissions** tab -> Grant access -> select the **person or deployer group**
(not the SP itself):

| Role | Allows | Note |
| --- | --- | --- |
| Service principal: **User** | Set the SP as job `run_as` | **Required explicitly** - Manager does NOT include User |
| Service principal: Manager | Manage roles, create secrets | Creator gets it automatically; keep to a small group |

### 6.5 Unity Catalog grants (SQL, environment workspace)

```sql
GRANT USE CATALOG ON CATALOG `<catalog>` TO `grp-<app-short>-<env>-jobs`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA `<catalog>`.`<schema>` TO `grp-<app-short>-<env>-jobs`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME `<catalog>`.`<schema>`.`checkpoints` TO `grp-<app-short>-<env>-jobs`;

-- Read access for developers (UAT; for prod prefer masked silver/gold only)
GRANT USE CATALOG ON CATALOG `<catalog>` TO `grp-<app-short>-developers`;
GRANT USE SCHEMA, SELECT ON SCHEMA `<catalog>`.`<schema>` TO `grp-<app-short>-developers`;
```

Schema-level privileges are inherited by current and future tables. Tables created by the job are owned by the SP;
optionally `ALTER TABLE ... OWNER TO <group>` so ownership does not depend on one identity.

**Check:** `SHOW GRANTS ON CATALOG/SCHEMA/VOLUME ...`; the job group has no grants on other environments' catalogs.

### 6.6 Secret scope ACL (environment workspace web terminal)

```bash
databricks auth describe                                         # confirm the right workspace host
databricks secrets put-acl <scope> grp-<app-short>-<env>-jobs READ
databricks secrets list-acls <scope>                             # you: MANAGE, group: READ
```

"User or Group ... does not exist" means the terminal is in the wrong workspace or the group is not assigned to it.
CLI commands must run in the **web terminal**, not in `%sh` notebook cells.

---

## Phase 7: CI/CD with GitHub Actions (per higher environment)

### 7.1 Remove any manual deployment first

If the environment was ever deployed by a person (or with a different `root_path`), remove it **before** the first
pipeline deploy, otherwise the pipeline creates a duplicate job:

```bash
databricks bundle destroy --target <env>     # run with the OLD root_path still in databricks.yml
```

Read the plan: it must list only the job and the deployment folder. Catalog, tables, checkpoints and grants are not
bundle resources and are not touched.

### 7.2 Federated credential (Azure portal)

App registration -> Certificates & secrets -> Federated credentials -> Add credential -> **Other issuer**:

| Field | Value |
| --- | --- |
| Issuer | `https://token.actions.githubusercontent.com` |
| Subject identifier | `repo:<gh-owner>@<gh-owner-id>/<gh-repo>@<gh-repo-id>:environment:<env>` |
| Audience | `api://AzureADTokenExchange` |
| Name | `github-<app-short>-<env>` |

* GitHub now sends the **ID-based** subject. The numeric IDs are public: open
  `https://api.github.com/repos/<gh-owner>/<gh-repo>` -> `id` (repo) and `owner.id` (owner).
* If sign-in fails with `AADSTS700213` / `AADSTS70021`, the error prints the exact subject GitHub sent - copy it.
* No Azure subscription role and no client secret are needed.

### 7.3 GitHub environment

GitHub repo -> Settings -> Environments -> New environment `<env>` (lowercase, must match the subject):

| Setting | Value |
| --- | --- |
| Required reviewers | Release approvers (needs a public repo or a paid plan for private repos) |
| Deployment branches | Selected branches: `<branch>` only |
| Variables (not secrets) | `DATABRICKS_HOST` = `<workspace-url>` (no trailing slash), `AZURE_CLIENT_ID` = `<sp-app-id>`, `AZURE_TENANT_ID` = `<tenant-id>` |

### 7.4 Workflow (`.github/workflows/deploy-<env>.yml`)

```yaml
name: Deploy bundle to <env>

on:
  push:
    branches:
      - <branch>
  workflow_dispatch: {}

permissions:
  id-token: write
  contents: read

concurrency:
  group: deploy-<env>
  cancel-in-progress: false

jobs:
  deploy:
    name: Validate and deploy to <env>
    runs-on: ubuntu-latest
    environment: <env>
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_AUTH_TYPE: azure-cli
      DATABRICKS_BUNDLE_TARGET: <env>      # REQUIRED: otherwise CLI commands use the default (dev) target's host
    steps:
      - uses: actions/checkout@v4
      - name: Sign in to Microsoft Entra ID (OIDC, no secret)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.AZURE_CLIENT_ID }}
          tenant-id: ${{ vars.AZURE_TENANT_ID }}
          allow-no-subscriptions: true
      - uses: databricks/setup-cli@main
      - name: Show identity
        run: databricks current-user me
      - name: Validate
        run: databricks bundle validate --target <env>
      - name: Deploy
        run: databricks bundle deploy --target <env>
```

### 7.5 Release

1. Commit to a feature branch, open a PR into `<branch>`, merge.
2. GitHub Actions -> the run -> (approve if reviewers are set) -> watch the logs.
3. To rerun after fixing Azure/GitHub settings: open the run summary page -> Re-run jobs.
   Workflow-file changes must be merged to `<branch>` first.

**Check (log):** login successful; identity `userName` = `<sp-app-id>` (may be masked as `***`);
validate shows host `<workspace-url>` and path `/Workspace/Deployments/<app>/<env>`; `Deployment complete!`.

---

## Phase 8: Verify the environment

| Where | Expected |
| --- | --- |
| Jobs & Pipelines | Exactly one `<job_name>` (no `[dev ...]` prefix) |
| Job Run as / owner | The environment's service principal |
| Job permissions | Groups from `databricks.yml` only |
| Tags | `env: <env>` |
| `/Workspace/Deployments/<app>/<env>/files` | `src/`, `config/`, `resources/`; no `archive/`, `setup/`, `.github/` |
| `/Workspace/Deployments/<app>/<env>/state/metadata.json` | Git branch and commit of the release |
| Job run | Succeeds; config path ends with `config/<env>.json`; tables in `<catalog>` |
| Second release (no-op change) | Same job ID (updated, not recreated) |

---

## Phase 9: Production checklist (in addition to Phases 3, 6, 7)

- [ ] Separate prod workspace, catalog, storage container/account, and ideally a separate Key Vault and Kafka credentials
- [ ] `config/prod.json` with prod names
- [ ] `prod` target in `databricks.yml` (mode production, `/Workspace/Deployments`, prod SP `run_as`, no human CAN_MANAGE)
- [ ] `sp-<app-short>-prod-entra`, `grp-<app-short>-prod-jobs`, UC grants, scope READ
- [ ] Federated credential subject `...:environment:prod`; GitHub environment `prod` limited to `Master`, with required reviewers
- [ ] Workflow `deploy-prod.yml` triggered by `Master`
- [ ] Branch protection / rulesets on `DEVELOP` and `Master` (PR required, no force push, no deletion)
- [ ] Developers: `CAN_VIEW` on prod jobs; data access only to masked layers
- [ ] Key Vault network restricted (selected networks / private endpoint, trusted Microsoft services allowed)
- [ ] Schedules or continuous trigger defined in the job YAML (production mode does not pause them)
- [ ] Failure notifications to a team mailbox, not a person
- [ ] Kafka/topic retention long enough for planned outages (a new checkpoint can only read what is still retained)

---

## Phase 10: Hardening (any higher environment)

| Item | How |
| --- | --- |
| No direct pushes to release branches | GitHub rulesets: require PR, block force push and deletion |
| Changes only through Git | Reduce human job permissions to `CAN_RUN` / `CAN_VIEW` |
| Validate before merge | PR workflow running `bundle validate` (needs its own federated credential, ideally a separate low-privilege SP) |
| Secret rotation | Rotate in the source system, update Key Vault, restart jobs; no code change |

---

## Troubleshooting (errors met while building this setup)

| Error | Cause | Fix |
| --- | --- | --- |
| `notebook src/... not found` (validate) | Missing `.ipynb` extension | Use `../src/.../<name>.py.ipynb` |
| `.bundle/.../files` is empty after deploy | Source-linked deployment in dev | `workspace.sync_mode: overwrite` |
| `presets.source_linked_deployment is deprecated` | Newer CLI | Use `workspace.sync_mode: overwrite` |
| `root path ... is writable by all workspace users` | `root_path` under `/Workspace/Shared` | Use `/Workspace/Deployments/...` or a user/SP folder |
| `invalid permission level: CAN_MANAGE_RUN` | Target-level permissions | Use `CAN_RUN` |
| `workspace folder has permissions not configured in bundle` | Inherited folder ACL not declared | Add the principal to `permissions` |
| `Cannot bind the service principal provided in 'run_as' ... servicePrincipal.user` | Deployer lacks SP User role | Grant Service principal: User explicitly |
| `User or Group ... does not exist` (put-acl) | Wrong workspace or group not assigned | `databricks auth describe`; assign the group to the workspace |
| `AADSTS700213` / `AADSTS70021` | Federated credential subject mismatch | Use the subject printed in the error (ID-based) |
| `User not authorized` with the dev host in CI | CLI used the default target | `DATABRICKS_BUNDLE_TARGET: <env>` in the workflow |
| `No subscriptions found` (azure/login) | SP has no Azure role | `allow-no-subscriptions: true` |
| Duplicate job after changing deployer or `root_path` | New deployment state location | `bundle destroy` with the old config first |
| Table has 0 rows in a new environment | Source data expired (e.g. Kafka retention) | Produce new data; keep bronze as the long-term copy |
| `PERMISSION_DENIED` reading a table owned by an SP | No grant for your user/group | Schema-level `SELECT` for the developer group |
| CLI in `%sh` cell prints "only supported ... from the web terminal" | CLI not supported in notebook shells | Use the web terminal |

---

## Appendix: command cheat sheet

```bash
# Identity / workspace
databricks auth describe
databricks current-user me

# Bundle
databricks bundle validate --target <env> [-o json]
databricks bundle summary  --target <env>
databricks bundle deploy   --target <env>          # dev only by hand; higher envs via CI/CD
databricks bundle run <job_key> --target <env>
databricks bundle destroy  --target <env>          # read the plan before confirming

# Secrets
databricks secrets list-scopes
databricks secrets list-secrets <scope>
databricks secrets put-acl <scope> <principal> READ
databricks secrets list-acls <scope>

# Workspace folders
databricks workspace get-status <path> -o json
databricks workspace get-permissions directories <object_id>
```
