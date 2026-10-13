CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY,
  filename TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued','running','completed','failed')),
  total_count INTEGER NOT NULL DEFAULT 0,
  processed_count INTEGER NOT NULL DEFAULT 0,
  valid_count INTEGER NOT NULL DEFAULT 0,
  invalid_count INTEGER NOT NULL DEFAULT 0,
  risky_count INTEGER NOT NULL DEFAULT 0,
  unknown_count INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS email_results (
  id BIGSERIAL PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  row_number INTEGER NOT NULL,
  original_email TEXT NOT NULL,
  normalized_email TEXT NOT NULL,
  syntax_valid BOOLEAN NOT NULL,
  domain TEXT,
  domain_status TEXT NOT NULL,
  mx_status TEXT NOT NULL,
  mx_host TEXT,
  disposable BOOLEAN NOT NULL DEFAULT FALSE,
  smtp_status TEXT NOT NULL DEFAULT 'not_checked',
  result_status TEXT NOT NULL,
  reason TEXT NOT NULL,
  UNIQUE(job_id, row_number)
);
CREATE INDEX IF NOT EXISTS idx_email_results_job_status ON email_results(job_id, result_status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);
