CREATE EXTENSION IF NOT EXISTS citext;   -- case-insensitive email column
CREATE EXTENSION IF NOT EXISTS pgcrypto; -- gen_random_uuid()

-- ---------------------------------------------------------------------
-- 2. IMPORT BATCHES (one row per uploaded CSV file)
-- ---------------------------------------------------------------------
CREATE TABLE import_batches (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    filename            TEXT NOT NULL,
    file_hash           TEXT NOT NULL UNIQUE,        -- sha256: block re-uploading same file
    storage_path        TEXT,                        -- where the original file is stored
    original_headers    TEXT[] NOT NULL,             -- needed to rebuild CSV on export
    status              TEXT NOT NULL DEFAULT 'uploaded', -- 'uploaded','mapped','validating','completed','failed'
    total_rows          INT NOT NULL DEFAULT 0,
    valid_rows          INT NOT NULL DEFAULT 0,
    invalid_rows        INT NOT NULL DEFAULT 0,
    duplicate_rows      INT NOT NULL DEFAULT 0,
    error_message       TEXT,
    uploaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ
);
CREATE INDEX idx_batches_status ON import_batches(status);

-- ---------------------------------------------------------------------
-- 3. CONTACTS (valid emails only, unique across all files)
-- ---------------------------------------------------------------------
CREATE TABLE contacts (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email             TEXT NOT NULL UNIQUE,
    email_domain      TEXT GENERATED ALWAYS AS (split_part(email::text, '@', 2)) STORED,
    first_name        TEXT,
    last_name         TEXT,
    full_name         TEXT,
    company           TEXT,
    job_title         TEXT,
    industry          TEXT,
    country           TEXT,
    city              TEXT,
    phone             TEXT,
    source_filename TEXT,   -- file the email was first imported from
    attributes        JSONB NOT NULL DEFAULT '{}'::jsonb,  -- every unmapped CSV column
    status            TEXT NOT NULL DEFAULT 'active', -- 'active','unsubscribed','bounced','complained'
    syntax_valid      BOOLEAN NOT NULL DEFAULT TRUE,
    mx_valid          BOOLEAN,           -- domain has MX record
    is_disposable     BOOLEAN NOT NULL DEFAULT FALSE,
    is_role_account   BOOLEAN NOT NULL DEFAULT FALSE,  -- info@, admin@, sales@
    smtp_verified     BOOLEAN,           -- optional deep check
    validated_at      TIMESTAMPTZ,
    first_batch_id    BIGINT REFERENCES import_batches(id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_contacts_domain   ON contacts(email_domain);
CREATE INDEX idx_contacts_industry ON contacts(industry);
CREATE INDEX idx_contacts_country  ON contacts(country);
CREATE INDEX idx_contacts_status   ON contacts(status);
CREATE INDEX idx_contacts_attrs    ON contacts USING GIN (attributes);

-- ---------------------------------------------------------------------
-- 4. CONTACT SOURCES (same email may appear in many files)
-- ---------------------------------------------------------------------
CREATE TABLE contact_sources (
    contact_id  BIGINT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    batch_id    BIGINT NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
    row_number  INT NOT NULL,                 -- line in the original CSV
    PRIMARY KEY (contact_id, batch_id, row_number)
);
CREATE INDEX idx_contact_sources_batch ON contact_sources(batch_id);

-- ---------------------------------------------------------------------
-- 5. INVALID EMAILS (separate table, downloadable as CSV)
--    raw_data keeps the ORIGINAL row keyed by original CSV headers,
--    so export = original columns + error columns.
-- ---------------------------------------------------------------------
CREATE TABLE invalid_emails (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id       BIGINT NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
    row_number     INT NOT NULL,
    raw_email      TEXT,                        -- exactly what was in the file
    raw_data       JSONB NOT NULL,              -- full original row
    reason_detail  TEXT,
    status         TEXT NOT NULL DEFAULT 'pending', -- 'pending','exported','fixed','discarded'
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, row_number)
);
CREATE INDEX idx_invalid_batch  ON invalid_emails(batch_id, status);


-- Export jobs: user clicks "download invalid" -> job -> CSV file
CREATE TABLE export_jobs (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id       BIGINT REFERENCES import_batches(id) ON DELETE SET NULL, -- NULL = all batches
    filters        JSONB NOT NULL DEFAULT '{}'::jsonb,  -- e.g. {"reason_code":"BAD_SYNTAX"}
    status         TEXT NOT NULL DEFAULT 'queued', -- 'queued','in_progress','completed','failed'
    row_count      INT,
    file_path      TEXT,
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ
);

-- ---------------------------------------------------------------------
-- 6. SUPPRESSION LIST (do-not-email; checked before every campaign)
-- ---------------------------------------------------------------------
CREATE TABLE suppressions (
    email       CITEXT PRIMARY KEY,
    reason      TEXT NOT NULL,       -- 'unsubscribe','hard_bounce','complaint','manual'
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =====================================================================
-- PHASE 2: MARKET FIT
-- =====================================================================

-- 7. PRODUCTS you want to market
CREATE TABLE products (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         TEXT NOT NULL,
    description  TEXT,
    -- Ideal Customer Profile, example:
    -- {"industries":["fintech","saas"],
    --  "countries":["ID","SG"],
    --  "job_titles":["CTO","Head of Engineering"],
    --  "company_keywords":["software","cloud"],
    --  "exclude_domains":["gmail.com"]}
    icp          JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 8. A scoring run = one execution (rules or AI) of product vs. contacts
CREATE TABLE market_fit_runs (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id       BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    method           TEXT NOT NULL,           -- 'rules' | 'llm' | 'hybrid'
    model_version    TEXT,                    -- e.g. rules v1 / model name
    contact_filter   JSONB NOT NULL DEFAULT '{}'::jsonb,  -- which contacts were scored
    status           TEXT NOT NULL DEFAULT 'queued', -- 'queued','in_progress','completed','failed'
    total_contacts   INT NOT NULL DEFAULT 0,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 9. Score of each contact per product per run
CREATE TABLE market_fit_scores (
    run_id       BIGINT NOT NULL REFERENCES market_fit_runs(id) ON DELETE CASCADE,
    contact_id   BIGINT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    product_id   BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    score        NUMERIC(5,2) NOT NULL CHECK (score BETWEEN 0 AND 100),
    is_fit       BOOLEAN NOT NULL,            -- score >= threshold
    reasons      JSONB NOT NULL DEFAULT '{}'::jsonb,  -- why: {"industry":+30,"country":+20}
    scored_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, contact_id)
);
CREATE INDEX idx_fit_product_score ON market_fit_scores(product_id, score DESC);
CREATE INDEX idx_fit_contact       ON market_fit_scores(contact_id);

-- Convenience view: latest score per contact per product
CREATE VIEW v_latest_market_fit AS
SELECT DISTINCT ON (s.product_id, s.contact_id) s.product_id, s.contact_id, s.score, s.is_fit, s.reasons, s.scored_at
FROM market_fit_scores s
ORDER BY s.product_id, s.contact_id, s.scored_at DESC;

-- ---------------------------------------------------------------------
-- updated_at trigger
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_contacts_updated  BEFORE UPDATE ON contacts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
