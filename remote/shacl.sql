-- @use_case Development/UseCase/UC_Citus
-- @facet Store
--
-- SHACL enforced at COMMIT, as GraphDB (RDF4J's ShaclSail) does: a transaction whose datums violate the shapes is
-- refused, and nothing of it lands. pgRDF (forked: laquereric/pgRDF) has SHACL only as a stub until its 0.4, so the
-- shapes are compiled to rows (shacl.py) and checked in SQL. Applied after schema.sql, as postgres; idempotent.
--
--   iq.shape_target / iq.shape_property / iq.shape_closed   reference tables: the compiled shapes, on every node
--   iq.quad.dt               a literal's datatype IRI (sh:datatype); NULL for IRIs and blank nodes (`_:` ...)
--   iq.put_datum(...)        THE governed write: the datum replaced whole, and queued for checking at COMMIT
--   iq.shacl_pending         coordinator-local, one row per datum written in the open transaction
--   shacl_at_commit          a deferred constraint trigger on it: at COMMIT -- on the coordinator, before Citus
--                            sends PREPARE to any worker -- each written datum is held to the shapes; a violation
--                            raises check_violation with the report as DETAIL, and the transaction rolls back
--   iq.shacl_check(datum)    the report on demand (as Stardog's integrity check can be run), the same code
--   iq_agent                 a role that may write ONLY through put_datum: for it the shapes cannot be gone around
--                            (the functions run as iq; an agent reads through them too, in a transaction it writes)
--
-- SHACL Core supported: sh:targetClass (and its subclasses, from iq.vocab), sh:property with an IRI sh:path and
-- sh:minCount, sh:maxCount, sh:datatype, sh:nodeKind, sh:class (the value's type read across every datum, the
-- transaction's own uncommitted ones included), sh:in, sh:pattern, sh:minInclusive, sh:maxInclusive; sh:closed with
-- sh:ignoredProperties. Anything else is refused when the shapes are loaded, never ignored.
-- A focus node is held to the triples of its own datum (one datum per subject is the plane's rule).

ALTER TABLE iq.quad ADD COLUMN IF NOT EXISTS dt text;

CREATE TABLE IF NOT EXISTS iq.shape_target (
    graph text NOT NULL, shape text NOT NULL, target text NOT NULL,
    PRIMARY KEY (graph, shape, target));
CREATE TABLE IF NOT EXISTS iq.shape_property (
    graph text NOT NULL, shape text NOT NULL, ord int NOT NULL, path text NOT NULL,
    min_count int, max_count int, datatype text, node_kind text, class text, in_values text[],
    pattern text, min_incl numeric, max_incl numeric,
    PRIMARY KEY (graph, shape, ord, path));
CREATE TABLE IF NOT EXISTS iq.shape_closed (
    graph text NOT NULL, shape text NOT NULL, ignored text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (graph, shape));
CREATE UNLOGGED TABLE IF NOT EXISTS iq.shacl_pending (g text NOT NULL);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'iq.shape_target'::regclass) THEN
        PERFORM create_reference_table('iq.shape_target');
        PERFORM create_reference_table('iq.shape_property');
        PERFORM create_reference_table('iq.shape_closed');
    END IF;
END $$;

-- a term as sh:in lists it: I:<iri> or L:<datatype>|<lexical form>
CREATE OR REPLACE FUNCTION iq.term(o text, lit boolean, dt text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN lit THEN 'L:' || coalesce(dt, 'http://www.w3.org/2001/XMLSchema#string') || '|' || o
                ELSE 'I:' || o END
$$;

CREATE OR REPLACE FUNCTION iq.shacl_check(datum text)
RETURNS TABLE (focus text, shape text, path text, constraint_name text, detail text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $fn$
DECLARE
    a   CONSTANT text := 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
    sco CONSTANT text := 'http://www.w3.org/2000/01/rdf-schema#subClassOf';
    xs  CONSTANT text := 'http://www.w3.org/2001/XMLSchema#string';
BEGIN
    -- the datum's own triples, and every class the shapes name with its subclasses (all local: the vocabulary and
    -- the shapes are reference tables)
    DROP TABLE IF EXISTS pg_temp.sh_own, pg_temp.sh_sub, pg_temp.sh_focus, pg_temp.sh_vals, pg_temp.sh_typ;
    CREATE TEMP TABLE sh_own ON COMMIT DROP AS
        SELECT q.s, q.p, q.o, q.lit, q.dt FROM iq.quad q WHERE q.g = datum;
    CREATE TEMP TABLE sh_sub ON COMMIT DROP AS
        WITH RECURSIVE sub(c, top) AS (
            SELECT DISTINCT x.c, x.c FROM (SELECT t.target AS c FROM iq.shape_target t
                                           UNION SELECT p.class FROM iq.shape_property p WHERE p.class IS NOT NULL) x
            UNION
            SELECT v.s, sub.top FROM iq.vocab v JOIN sub ON v.o = sub.c AND v.p = sco)
        SELECT * FROM sub;
    CREATE TEMP TABLE sh_focus ON COMMIT DROP AS
        SELECT DISTINCT o.s AS f, t.graph, t.shape FROM sh_own o
          JOIN sh_sub sb ON sb.c = o.o JOIN iq.shape_target t ON t.target = sb.top
         WHERE o.p = a AND NOT o.lit;
    CREATE TEMP TABLE sh_vals ON COMMIT DROP AS
        SELECT fo.f, sp.graph, sp.shape, sp.ord, sp.path, sp.min_count, sp.max_count, sp.datatype, sp.node_kind,
               sp.class, sp.in_values, sp.pattern, sp.min_incl, sp.max_incl, o.o, o.lit, o.dt
          FROM sh_focus fo JOIN iq.shape_property sp ON sp.graph = fo.graph AND sp.shape = fo.shape
          LEFT JOIN sh_own o ON o.s = fo.f AND o.p = sp.path;
    -- the types of every IRI value a sh:class names, from whichever datum states them (this transaction's included)
    CREATE TEMP TABLE sh_typ ON COMMIT DROP AS
        SELECT DISTINCT q.s, q.o FROM iq.quad q
         WHERE q.p = a AND NOT q.lit
           AND q.s = ANY (ARRAY(SELECT DISTINCT v.o FROM sh_vals v WHERE v.class IS NOT NULL AND v.o IS NOT NULL AND NOT v.lit));

    RETURN QUERY
    SELECT v.f, v.shape, v.path, 'minCount', count(v.o) || ' < ' || v.min_count
      FROM sh_vals v WHERE v.min_count IS NOT NULL
     GROUP BY v.f, v.shape, v.path, v.min_count HAVING count(v.o) < v.min_count
    UNION ALL
    SELECT v.f, v.shape, v.path, 'maxCount', count(v.o) || ' > ' || v.max_count
      FROM sh_vals v WHERE v.max_count IS NOT NULL
     GROUP BY v.f, v.shape, v.path, v.max_count HAVING count(v.o) > v.max_count
    UNION ALL
    SELECT v.f, v.shape, v.path, 'datatype', v.o FROM sh_vals v
     WHERE v.o IS NOT NULL AND v.datatype IS NOT NULL AND NOT (v.lit AND coalesce(v.dt, xs) = v.datatype)
    UNION ALL
    SELECT v.f, v.shape, v.path, 'class', v.o FROM sh_vals v
     WHERE v.o IS NOT NULL AND v.class IS NOT NULL
       AND (v.lit OR NOT EXISTS (SELECT 1 FROM sh_typ t JOIN sh_sub sb ON sb.c = t.o AND sb.top = v.class WHERE t.s = v.o))
    UNION ALL
    SELECT v.f, v.shape, v.path, 'in', v.o FROM sh_vals v
     WHERE v.o IS NOT NULL AND v.in_values IS NOT NULL AND NOT (iq.term(v.o, v.lit, v.dt) = ANY (v.in_values))
    UNION ALL
    SELECT v.f, v.shape, v.path, 'nodeKind', v.o FROM sh_vals v
     WHERE v.o IS NOT NULL AND v.node_kind IS NOT NULL AND NOT CASE v.node_kind
        WHEN 'IRI' THEN NOT v.lit AND v.o NOT LIKE '\_:%'
        WHEN 'BlankNode' THEN NOT v.lit AND v.o LIKE '\_:%'
        WHEN 'Literal' THEN v.lit
        WHEN 'BlankNodeOrIRI' THEN NOT v.lit
        WHEN 'IRIOrLiteral' THEN v.lit OR v.o NOT LIKE '\_:%'
        WHEN 'BlankNodeOrLiteral' THEN v.lit OR v.o LIKE '\_:%'
        ELSE true END
    UNION ALL
    SELECT v.f, v.shape, v.path, 'pattern', v.o FROM sh_vals v
     WHERE v.o IS NOT NULL AND v.pattern IS NOT NULL AND v.o !~ v.pattern
    UNION ALL
    SELECT v.f, v.shape, v.path, 'minInclusive', v.o FROM sh_vals v
     WHERE v.o IS NOT NULL AND v.min_incl IS NOT NULL
       AND (NOT v.lit OR v.o !~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$' OR v.o::numeric < v.min_incl)
    UNION ALL
    SELECT v.f, v.shape, v.path, 'maxInclusive', v.o FROM sh_vals v
     WHERE v.o IS NOT NULL AND v.max_incl IS NOT NULL
       AND (NOT v.lit OR v.o !~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$' OR v.o::numeric > v.max_incl)
    UNION ALL
    SELECT DISTINCT fo.f, fo.shape, o.p, 'closed', '' FROM sh_focus fo
      JOIN iq.shape_closed c ON c.graph = fo.graph AND c.shape = fo.shape
      JOIN sh_own o ON o.s = fo.f
     WHERE NOT (o.p = ANY (c.ignored))
       AND NOT EXISTS (SELECT 1 FROM iq.shape_property sp WHERE sp.graph = fo.graph AND sp.shape = fo.shape AND sp.path = o.p);
END
$fn$;

CREATE OR REPLACE FUNCTION iq.shacl_at_commit() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $fn$
DECLARE n bigint; report jsonb; flag text := 'iq.shacl_' || md5(NEW.g);
BEGIN
    IF current_setting(flag, true) = '1' THEN RETURN NULL; END IF;     -- once per datum per transaction
    PERFORM set_config(flag, '1', true);
    SELECT count(*), jsonb_agg(to_jsonb(c) ORDER BY c.focus, c.shape, c.path, c.constraint_name)
      INTO n, report FROM iq.shacl_check(NEW.g) c;
    DELETE FROM iq.shacl_pending WHERE g = NEW.g;
    IF n > 0 THEN
        RAISE EXCEPTION 'shacl: % violates % constraint(s)', NEW.g, n
            USING ERRCODE = 'check_violation', DETAIL = report::text;
    END IF;
    RETURN NULL;
END
$fn$;

DROP TRIGGER IF EXISTS shacl_at_commit ON iq.shacl_pending;
CREATE CONSTRAINT TRIGGER shacl_at_commit AFTER INSERT ON iq.shacl_pending DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION iq.shacl_at_commit();

CREATE OR REPLACE FUNCTION iq.put_datum(datum text, tier text, body jsonb, embedding vector, quads jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $fn$
BEGIN
    -- refused here, with its own code: from a worker, through INSERT ... SELECT, Citus returns the error as XX000
    IF EXISTS (SELECT 1 FROM jsonb_to_recordset(quads) AS x(s text, p text, o text)
                WHERE x.s IS NULL OR x.p IS NULL OR x.o IS NULL) THEN
        RAISE EXCEPTION 'a triple of % has no subject, predicate or object', datum USING ERRCODE = 'not_null_violation';
    END IF;
    DELETE FROM iq.datum d WHERE d.iri = datum;
    INSERT INTO iq.datum (iri, tier, body, embedding) VALUES (datum, tier, body, embedding);
    INSERT INTO iq.quad (g, s, p, o, lit, dt)
        SELECT datum, x.s, x.p, x.o, coalesce(x.lit, false), x.dt
          FROM jsonb_to_recordset(quads) AS x(s text, p text, o text, lit boolean, dt text);
    INSERT INTO iq.shacl_pending (g) VALUES (datum);
END
$fn$;

CREATE OR REPLACE FUNCTION iq.drop_datum(datum text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $fn$
BEGIN
    DELETE FROM iq.datum d WHERE d.iri = datum;
    INSERT INTO iq.shacl_pending (g) VALUES (datum);
END
$fn$;

-- The write path runs as iq, whoever calls it: Citus refuses a transaction in which two users touch the same shards
-- ("cannot perform query on placements that were modified in this transaction by a different user"), so the
-- functions are iq's, not postgres's. iq_agent reaches the record only through them.
ALTER FUNCTION iq.shacl_check(text) OWNER TO iq;
ALTER FUNCTION iq.shacl_at_commit() OWNER TO iq;
ALTER FUNCTION iq.put_datum(text, text, jsonb, vector, jsonb) OWNER TO iq;
ALTER FUNCTION iq.drop_datum(text) OWNER TO iq;

DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iq_agent') THEN CREATE ROLE iq_agent NOLOGIN; END IF; END $$;
GRANT iq_agent TO iq;
GRANT USAGE ON SCHEMA iq TO iq_agent;
GRANT SELECT ON iq.datum, iq.quad, iq.vocab, iq.shape_target, iq.shape_property, iq.shape_closed TO iq_agent;
GRANT SELECT, INSERT, UPDATE, DELETE ON iq.shape_target, iq.shape_property, iq.shape_closed TO iq;
GRANT SELECT, INSERT, DELETE ON iq.shacl_pending TO iq, iq_agent;
GRANT EXECUTE ON FUNCTION iq.put_datum(text, text, jsonb, vector, jsonb), iq.drop_datum(text),
    iq.shacl_check(text), iq.term(text, boolean, text) TO iq, iq_agent;
