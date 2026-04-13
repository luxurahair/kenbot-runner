-- Kenbot Dashboard — Table dashboard_users
-- Execute dans Supabase Dashboard → SQL Editor

CREATE TABLE IF NOT EXISTS dashboard_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT now(),
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'conseiller',
    active BOOLEAN DEFAULT true
);

ALTER TABLE dashboard_users ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON dashboard_users FOR ALL USING (true) WITH CHECK (true);

-- Insert default users
INSERT INTO dashboard_users (username, password, name, role) VALUES
  ('admin', 'Daniel7$', 'Daniel Giroux', 'admin'),
  ('directeur', 'Ventes2025!', 'Directeur des ventes', 'directeur')
ON CONFLICT (username) DO NOTHING;
