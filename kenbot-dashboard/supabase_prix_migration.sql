-- Kenbot Reprise — Colonnes prix et wholesale
-- Execute dans Supabase Dashboard → SQL Editor

ALTER TABLE evaluations ADD COLUMN IF NOT EXISTS prix_reprise NUMERIC;
ALTER TABLE evaluations ADD COLUMN IF NOT EXISTS prix_par TEXT;
ALTER TABLE evaluations ADD COLUMN IF NOT EXISTS prix_date TIMESTAMPTZ;
ALTER TABLE evaluations ADD COLUMN IF NOT EXISTS wholesale_emails JSONB DEFAULT '[]';

-- Ajouter colonne email aux users
ALTER TABLE dashboard_users ADD COLUMN IF NOT EXISTS email TEXT;
