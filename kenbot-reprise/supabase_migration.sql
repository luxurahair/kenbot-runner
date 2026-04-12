-- Kenbot Reprise — Table evaluations
-- Execute this SQL in your Supabase Dashboard → SQL Editor

CREATE TABLE IF NOT EXISTS evaluations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ,
    status TEXT DEFAULT 'NOUVEAU',
    client_name TEXT,
    client_phone TEXT,
    client_email TEXT,
    client_notes TEXT,
    vin TEXT,
    make TEXT,
    model TEXT,
    year TEXT,
    trim TEXT,
    engine TEXT,
    drive_type TEXT,
    fuel_type TEXT,
    km INTEGER,
    paiement_restant NUMERIC,
    etat_general TEXT,
    photos JSONB DEFAULT '[]'::jsonb,
    vin_decoded JSONB DEFAULT '{}'::jsonb,
    form_data JSONB DEFAULT '{}'::jsonb
);

-- Enable RLS (Row Level Security)
ALTER TABLE evaluations ENABLE ROW LEVEL SECURITY;

-- Policy: service role can do everything
CREATE POLICY "Service role full access" ON evaluations
    FOR ALL USING (true) WITH CHECK (true);

-- Index on status for filtering
CREATE INDEX IF NOT EXISTS idx_evaluations_status ON evaluations(status);
CREATE INDEX IF NOT EXISTS idx_evaluations_created_at ON evaluations(created_at DESC);
