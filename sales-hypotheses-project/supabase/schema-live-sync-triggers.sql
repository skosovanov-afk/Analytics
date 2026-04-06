-- ============================================================
-- Live Sync Triggers
-- Cascades rename/edit operations to all denormalized copies.
-- Safe to run multiple times (CREATE OR REPLACE + IF NOT EXISTS).
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- 1. Role name → sales_hypothesis_rows.role_label
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION sync_role_name_cascade()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.name IS DISTINCT FROM NEW.name THEN
    UPDATE sales_hypothesis_rows
       SET role_label = NEW.name
     WHERE role_id = NEW.id
       AND role_label IS DISTINCT FROM NEW.name;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_role_name ON sales_icp_roles;
CREATE TRIGGER trg_sync_role_name
  AFTER UPDATE ON sales_icp_roles
  FOR EACH ROW EXECUTE FUNCTION sync_role_name_cascade();

-- ────────────────────────────────────────────────────────────
-- 2. Vertical name → hypothesis rows + company profiles
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION sync_vertical_name_cascade()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.name IS DISTINCT FROM NEW.name THEN
    -- Rows linked by FK
    UPDATE sales_hypothesis_rows
       SET vertical_name = NEW.name
     WHERE vertical_id = NEW.id
       AND vertical_name IS DISTINCT FROM NEW.name;

    -- Company profiles (no FK, match by text)
    UPDATE sales_icp_company_profiles
       SET vertical_name = NEW.name
     WHERE lower(trim(vertical_name)) = lower(trim(OLD.name))
       AND vertical_name IS DISTINCT FROM NEW.name;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_vertical_name ON sales_verticals;
CREATE TRIGGER trg_sync_vertical_name
  AFTER UPDATE ON sales_verticals
  FOR EACH ROW EXECUTE FUNCTION sync_vertical_name_cascade();

-- ────────────────────────────────────────────────────────────
-- 3. Subvertical name → hypothesis rows + company profiles
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION sync_subvertical_name_cascade()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.name IS DISTINCT FROM NEW.name THEN
    UPDATE sales_hypothesis_rows
       SET sub_vertical = NEW.name
     WHERE subvertical_id = NEW.id
       AND sub_vertical IS DISTINCT FROM NEW.name;

    UPDATE sales_icp_company_profiles
       SET sub_vertical = NEW.name
     WHERE lower(trim(sub_vertical)) = lower(trim(OLD.name))
       AND sub_vertical IS DISTINCT FROM NEW.name;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_subvertical_name ON sales_subverticals;
CREATE TRIGGER trg_sync_subvertical_name
  AFTER UPDATE ON sales_subverticals
  FOR EACH ROW EXECUTE FUNCTION sync_subvertical_name_cascade();

-- ────────────────────────────────────────────────────────────
-- 4. Company scale name → hypothesis rows + company profiles
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION sync_company_scale_name_cascade()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.name IS DISTINCT FROM NEW.name THEN
    UPDATE sales_hypothesis_rows
       SET company_scale = NEW.name
     WHERE company_scale_id = NEW.id
       AND company_scale IS DISTINCT FROM NEW.name;

    -- Company profiles use size_bucket column
    UPDATE sales_icp_company_profiles
       SET size_bucket = NEW.name
     WHERE lower(trim(size_bucket)) = lower(trim(OLD.name))
       AND size_bucket IS DISTINCT FROM NEW.name;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_company_scale_name ON sales_company_scales;
CREATE TRIGGER trg_sync_company_scale_name
  AFTER UPDATE ON sales_company_scales
  FOR EACH ROW EXECUTE FUNCTION sync_company_scale_name_cascade();

-- ────────────────────────────────────────────────────────────
-- 5. Company profile fields → hypothesis rows
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION sync_company_profile_to_rows()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  -- Only cascade to rows that don't have their own explicit taxonomy FK
  IF OLD.vertical_name IS DISTINCT FROM NEW.vertical_name THEN
    UPDATE sales_hypothesis_rows
       SET vertical_name = NEW.vertical_name
     WHERE company_profile_id = NEW.id
       AND vertical_id IS NULL
       AND vertical_name IS DISTINCT FROM NEW.vertical_name;
  END IF;

  IF OLD.sub_vertical IS DISTINCT FROM NEW.sub_vertical THEN
    UPDATE sales_hypothesis_rows
       SET sub_vertical = NEW.sub_vertical
     WHERE company_profile_id = NEW.id
       AND subvertical_id IS NULL
       AND sub_vertical IS DISTINCT FROM NEW.sub_vertical;
  END IF;

  IF OLD.size_bucket IS DISTINCT FROM NEW.size_bucket THEN
    UPDATE sales_hypothesis_rows
       SET company_scale = NEW.size_bucket
     WHERE company_profile_id = NEW.id
       AND company_scale_id IS NULL
       AND company_scale IS DISTINCT FROM NEW.size_bucket;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_company_profile ON sales_icp_company_profiles;
CREATE TRIGGER trg_sync_company_profile
  AFTER UPDATE ON sales_icp_company_profiles
  FOR EACH ROW EXECUTE FUNCTION sync_company_profile_to_rows();

-- ────────────────────────────────────────────────────────────
-- 6. Library row (vp_matrix) → Hypothesis rows (manual)
--    Cascades: vp_point, decision_context, pain,
--              job_to_be_done, outcome_metric (inside notes JSON)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION sync_library_to_hypothesis_rows()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  src_job      text;
  src_outcome  text;
  dest         record;
  dest_notes   jsonb;
BEGIN
  -- Only fire for Library rows
  IF NEW.source IS DISTINCT FROM 'vp_matrix' THEN
    RETURN NEW;
  END IF;

  -- Check if any messaging field actually changed
  IF NOT (
    OLD.vp_point IS DISTINCT FROM NEW.vp_point OR
    OLD.decision_context IS DISTINCT FROM NEW.decision_context OR
    OLD.pain IS DISTINCT FROM NEW.pain OR
    OLD.notes IS DISTINCT FROM NEW.notes
  ) THEN
    RETURN NEW;
  END IF;

  -- Extract job_to_be_done and outcome_metric from Library notes JSON
  BEGIN
    src_job     := trim(COALESCE((NEW.notes::jsonb)->>'job_to_be_done', ''));
    src_outcome := trim(COALESCE((NEW.notes::jsonb)->>'outcome_metric', ''));
  EXCEPTION WHEN OTHERS THEN
    src_job     := '';
    src_outcome := '';
  END;

  -- Update all hypothesis rows that reference this library row
  FOR dest IN
    SELECT id, notes
      FROM sales_hypothesis_rows
     WHERE source = 'manual'
       AND notes IS NOT NULL
       AND notes::jsonb->>'position_source_row_id' = NEW.id::text
  LOOP
    -- Merge into existing hypothesis notes, preserving user_notes/tal_id/position_source_row_id
    BEGIN
      dest_notes := COALESCE(dest.notes::jsonb, '{}'::jsonb);
    EXCEPTION WHEN OTHERS THEN
      dest_notes := '{}'::jsonb;
    END;

    IF src_job <> '' THEN
      dest_notes := jsonb_set(dest_notes, '{job_to_be_done}', to_jsonb(src_job));
    END IF;
    IF src_outcome <> '' THEN
      dest_notes := jsonb_set(dest_notes, '{outcome_metric}', to_jsonb(src_outcome));
    END IF;

    UPDATE sales_hypothesis_rows
       SET vp_point          = COALESCE(NULLIF(trim(NEW.vp_point), ''), vp_point),
           decision_context  = NEW.decision_context,
           pain              = NEW.pain,
           notes             = dest_notes::text
     WHERE id = dest.id;
  END LOOP;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_library_to_hypotheses ON sales_hypothesis_rows;
CREATE TRIGGER trg_sync_library_to_hypotheses
  AFTER UPDATE ON sales_hypothesis_rows
  FOR EACH ROW
  WHEN (NEW.source = 'vp_matrix')
  EXECUTE FUNCTION sync_library_to_hypothesis_rows();


-- ============================================================
-- BACKFILL: fix existing stale denormalized values
-- ============================================================

-- Role labels
UPDATE sales_hypothesis_rows r
   SET role_label = ir.name
  FROM sales_icp_roles ir
 WHERE r.role_id = ir.id
   AND r.role_label IS DISTINCT FROM ir.name;

-- Vertical names (via FK)
UPDATE sales_hypothesis_rows r
   SET vertical_name = v.name
  FROM sales_verticals v
 WHERE r.vertical_id = v.id
   AND r.vertical_name IS DISTINCT FROM v.name;

-- Sub-vertical names (via FK)
UPDATE sales_hypothesis_rows r
   SET sub_vertical = sv.name
  FROM sales_subverticals sv
 WHERE r.subvertical_id = sv.id
   AND r.sub_vertical IS DISTINCT FROM sv.name;

-- Company scale names (via FK)
UPDATE sales_hypothesis_rows r
   SET company_scale = cs.name
  FROM sales_company_scales cs
 WHERE r.company_scale_id = cs.id
   AND r.company_scale IS DISTINCT FROM cs.name;

-- Library → Hypothesis messaging backfill
-- Updates hypothesis rows whose position_source_row_id points to a library row
UPDATE sales_hypothesis_rows dest
   SET vp_point         = COALESCE(NULLIF(trim(src.vp_point), ''), dest.vp_point),
       decision_context = src.decision_context,
       pain             = src.pain
  FROM sales_hypothesis_rows src
 WHERE src.source = 'vp_matrix'
   AND dest.source = 'manual'
   AND dest.notes IS NOT NULL
   AND (dest.notes::jsonb->>'position_source_row_id') = src.id::text
   AND (
     dest.vp_point IS DISTINCT FROM src.vp_point OR
     dest.decision_context IS DISTINCT FROM src.decision_context OR
     dest.pain IS DISTINCT FROM src.pain
   );
