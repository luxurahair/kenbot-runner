-- Kenbot — Table wholesale_contacts
-- Execute dans Supabase Dashboard → SQL Editor

CREATE TABLE IF NOT EXISTS wholesale_contacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT now(),
    name TEXT,
    email TEXT,
    phone TEXT,
    active BOOLEAN DEFAULT true
);

ALTER TABLE wholesale_contacts ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON wholesale_contacts FOR ALL USING (true) WITH CHECK (true);
