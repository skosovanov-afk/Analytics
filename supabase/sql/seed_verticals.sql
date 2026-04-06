-- =============================================================================
-- Seed: Verticals + Subverticals для INXY ICP Library
-- Применить: вставить в Supabase SQL Editor и запустить
-- =============================================================================

-- 1. Verticals (top-level categories)
INSERT INTO public.sales_verticals (name, sort_order, is_active) VALUES
  ('Finance & Payments',         1, true),
  ('Affiliate & Advertising',    2, true),
  ('HR & Payroll',               3, true),
  ('Gaming & Entertainment',     4, true),
  ('E-commerce & Marketplaces',  5, true),
  ('SaaS & Tech',                6, true),
  ('Telecom & Infrastructure',   7, true),
  ('Travel',                     8, true),
  ('Non-profit & Crowdfunding',  9, true)
ON CONFLICT DO NOTHING;

-- 2. Subverticals
-- Получаем vertical_id через подзапрос

-- Finance & Payments
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('FinTech',                          1),
  ('Digital Banking Infrastructure',   2),
  ('Cross-Border Corporate Payments',  3),
  ('PS and FX',                        4),
  ('Trading',                          5),
  ('Investment',                       6),
  ('Tokenization',                     7),
  ('Companies accepting crypto',       8)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'Finance & Payments'
ON CONFLICT DO NOTHING;

-- Affiliate & Advertising
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('Affiliate & Ad Network',    1),
  ('Monetization Platform',     2)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'Affiliate & Advertising'
ON CONFLICT DO NOTHING;

-- HR & Payroll
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('Payroll Services',                1),
  ('EOR/PEO & Freelance Platform',    2),
  ('Creator Economy',                 3)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'HR & Payroll'
ON CONFLICT DO NOTHING;

-- Gaming & Entertainment
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('Gaming',                      1),
  ('iGaming Service Provider',    2),
  ('Skins & Cases',               3),
  ('Tournament Platforms',        4),
  ('Music',                       5),
  ('Ticketing',                   6)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'Gaming & Entertainment'
ON CONFLICT DO NOTHING;

-- E-commerce & Marketplaces
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('E-commerce',     1),
  ('Marketplaces',   2),
  ('Merchants',      3),
  ('Luxury',         4)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'E-commerce & Marketplaces'
ON CONFLICT DO NOTHING;

-- SaaS & Tech
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('SaaS',                        1),
  ('Sales Engagement Platform',   2),
  ('AI Agents',                   3),
  ('EdTech',                      4)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'SaaS & Tech'
ON CONFLICT DO NOTHING;

-- Telecom & Infrastructure
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('Telecom',          1),
  ('VAS & CPaaS',      2),
  ('eSIM',             3),
  ('Cloud / Hosting',  4),
  ('VPN',              5)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'Telecom & Infrastructure'
ON CONFLICT DO NOTHING;

-- Travel
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('Travel Tech',  1)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'Travel'
ON CONFLICT DO NOTHING;

-- Non-profit & Crowdfunding
INSERT INTO public.sales_subverticals (vertical_id, name, sort_order, is_active)
SELECT v.id, sub.name, sub.sort_order, true
FROM (VALUES
  ('NGO & Foundations',          1),
  ('Crowdfunding Platforms',     2)
) AS sub(name, sort_order)
CROSS JOIN public.sales_verticals v
WHERE v.name = 'Non-profit & Crowdfunding'
ON CONFLICT DO NOTHING;

-- Проверка
SELECT v.name AS vertical, s.name AS subvertical, s.sort_order
FROM public.sales_verticals v
JOIN public.sales_subverticals s ON s.vertical_id = v.id
ORDER BY v.sort_order, s.sort_order;
