rollback;
CREATE SCHEMA IF NOT EXISTS synth;
SET search_path = synth, public;

SET client_min_messages = warning;
SET synchronous_commit = off;

DROP TABLE IF EXISTS synth.fact_inpatient_stay;
DROP TABLE IF EXISTS synth.ip_claim_line_canonical;
DROP TABLE IF EXISTS synth.ip_claim_header_canonical;
DROP TABLE IF EXISTS synth.ip_claim_line_messy;
DROP TABLE IF EXISTS synth.ip_claim_header_messy;
DROP TABLE IF EXISTS synth.true_stays_reference;
DROP TABLE IF EXISTS synth.person_msis_xwalk;

CREATE UNLOGGED TABLE synth.person_msis_xwalk (
  person_id        text NOT NULL,
  msis_id          text NOT NULL,
  submtg_state_cd  text NOT NULL,
  state_cd         text NOT NULL
);

CREATE UNLOGGED TABLE synth.true_stays_reference (
  msis_id           text NOT NULL,
  submtg_state_cd   text NOT NULL,
  state_cd          text NOT NULL,
  true_admit_dt     date NOT NULL,
  true_discharge_dt date NOT NULL,
  true_los          integer NOT NULL,
  true_drg_cd       text,
  true_dx_cd_1      text,
  true_prvdr_npi    text
);

CREATE UNLOGGED TABLE synth.ip_claim_header_messy (
  clm_id          text NOT NULL,
  msis_id         text NOT NULL,
  submtg_state_cd text NOT NULL,
  state_cd        text NOT NULL,
  admit_dt        date,
  disch_dt        date,
  bill_type       text,
  drg_cd          text,
  dx_cd_1         text,
  prvdr_npi       text,
  total_charges   numeric(14,2),
  allowed_amt     numeric(14,2),
  paid_amt        numeric(14,2),
  denied_ind      integer
);

CREATE UNLOGGED TABLE synth.ip_claim_line_messy (
  clm_id           text NOT NULL,
  line_num         integer NOT NULL,
  rev_cntr_cd      text,
  line_srvc_bgn_dt date,
  line_srvc_end_dt date,
  units            integer,
  line_allowed_amt numeric(14,2),
  line_paid_amt    numeric(14,2)
);

CREATE TABLE synth.fact_inpatient_stay (
  stay_id         text PRIMARY KEY,
  msis_id         text NOT NULL,
  submtg_state_cd text NOT NULL,
  state_cd        text NOT NULL,
  admit_dt        date NOT NULL,
  discharge_dt    date NOT NULL,
  length_of_stay  integer NOT NULL,
  icu_days        integer NOT NULL,
  drg_cd          text,
  dx_cd_1         text,
  total_paid      numeric(14,2),
  total_allowed   numeric(14,2),
  any_denied      integer,
  n_claims        integer,
  admit_year      integer,
  admit_quarter   integer,
  qtr_start       date
);

COPY synth.person_msis_xwalk
FROM '/files/person_msis_xwalk.csv'
WITH (FORMAT csv, HEADER true);

COPY synth.true_stays_reference
FROM '/files/true_stays_reference.csv'
WITH (FORMAT csv, HEADER true);

COPY synth.ip_claim_header_messy
FROM '/files/ip_claim_header_messy.csv'
WITH (FORMAT csv, HEADER true);

COPY synth.ip_claim_line_messy
FROM '/files/ip_claim_line_messy.csv'
WITH (FORMAT csv, HEADER true);

COPY synth.fact_inpatient_stay
FROM '/files/fact_inpatient_stay.csv'
WITH (FORMAT csv, HEADER true);

DROP TABLE IF EXISTS synth.ip_claim_header_canonical;
CREATE TABLE synth.ip_claim_header_canonical AS
SELECT DISTINCT ON (clm_id)
  clm_id,
  msis_id,
  submtg_state_cd,
  state_cd,
  admit_dt,
  disch_dt,
  bill_type,
  drg_cd,
  dx_cd_1,
  prvdr_npi,
  total_charges,
  allowed_amt,
  paid_amt,
  denied_ind
FROM synth.ip_claim_header_messy
ORDER BY
  clm_id,
  paid_amt DESC NULLS LAST,
  allowed_amt DESC NULLS LAST;

ALTER TABLE synth.ip_claim_header_canonical
ADD CONSTRAINT ip_claim_header_canonical_pkey PRIMARY KEY (clm_id);

DROP TABLE IF EXISTS synth.ip_claim_line_canonical;
CREATE TABLE synth.ip_claim_line_canonical AS
SELECT DISTINCT ON (clm_id, line_num, rev_cntr_cd, line_srvc_bgn_dt, line_srvc_end_dt)
  clm_id,
  line_num,
  rev_cntr_cd,
  line_srvc_bgn_dt,
  line_srvc_end_dt,
  units,
  line_allowed_amt,
  line_paid_amt
FROM synth.ip_claim_line_messy
ORDER BY
  clm_id,
  line_num,
  rev_cntr_cd,
  line_srvc_bgn_dt,
  line_srvc_end_dt,
  line_paid_amt DESC NULLS LAST,
  line_allowed_amt DESC NULLS LAST;

ALTER TABLE synth.ip_claim_line_canonical
ADD CONSTRAINT ip_claim_line_canonical_pkey
PRIMARY KEY (clm_id, line_num, rev_cntr_cd, line_srvc_bgn_dt, line_srvc_end_dt);

CREATE INDEX IF NOT EXISTS ix_ip_hdr_messy_msis_dates
  ON synth.ip_claim_header_messy (msis_id, admit_dt, disch_dt);

CREATE INDEX IF NOT EXISTS ix_ip_hdr_canon_msis_dates
  ON synth.ip_claim_header_canonical (msis_id, admit_dt, disch_dt);

CREATE INDEX IF NOT EXISTS ix_ip_line_canon_clm
  ON synth.ip_claim_line_canonical (clm_id);

CREATE INDEX IF NOT EXISTS ix_fact_stay_msis_qtr
  ON synth.fact_inpatient_stay (msis_id, admit_year, admit_quarter);

ANALYZE synth.person_msis_xwalk;
ANALYZE synth.true_stays_reference;
ANALYZE synth.ip_claim_header_messy;
ANALYZE synth.ip_claim_line_messy;
ANALYZE synth.ip_claim_header_canonical;
ANALYZE synth.ip_claim_line_canonical;
ANALYZE synth.fact_inpatient_stay;

--rollback ;
