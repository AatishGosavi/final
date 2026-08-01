-- STEP 1: Add the column (skip if you already ran this)
ALTER TABLE pt_machine_logs
ADD COLUMN IF NOT EXISTS machine_number VARCHAR(10);

-- STEP 2: Backfill existing rows before making the column part of the key.
-- All rows inserted so far came from ONE machine (this table had no machine
-- tracking before). Replace '01' below with whatever that machine's number
-- actually is, then run it:
UPDATE pt_machine_logs
SET machine_number = '01'
WHERE machine_number IS NULL;

-- STEP 3: Drop the old single-column primary key.
-- Default constraint name is <table>_pkey unless you renamed it -- check with:
--   \d pt_machine_logs   (in psql)
-- to confirm the constraint name before running this.
ALTER TABLE pt_machine_logs
DROP CONSTRAINT pt_machine_logs_pkey;

-- STEP 4: Make machine_number required, then create the composite primary key.
ALTER TABLE pt_machine_logs
ALTER COLUMN machine_number SET NOT NULL;

ALTER TABLE pt_machine_logs
ADD PRIMARY KEY (spool_code_tu, machine_number);
