-- @use_case Development/UseCase/UC_Citus
-- @facet Store
--
-- The governed record and graph at scale, as one Citus schema. Applied by the scenarios as postgres, over
-- ssh (Citus propagates a table owner's role to the workers, which takes a superuser); iq gets rights on the data
-- only. Idempotent. What MarkLogic commits in one transaction -- a document, its triples, its text, its
-- vector -- is here one row and its quads, CO-LOCATED by the datum's IRI, so a datum is always a single-shard
-- transaction on one worker. The vocabulary is a reference table (a copy on every node, changed by two-phase
-- commit), so inference joins it locally. Inference itself is query-time, as MarkLogic's is: iq.entail() computes
-- the RDFS-Plus closure inside the caller's transaction, so it sees that transaction's own uncommitted writes,
-- never another's, and is never stale.
--
--   iq.datum  (iri)          distributed by iri: the document (jsonb), its text (tsvector), its vector (pgvector)
--   iq.quad   (g, s, p, o)   distributed by g = the datum's iri, co-located with iq.datum; no quad without its
--                            datum (a foreign key checked at COMMIT, so across nodes it is checked by PREPARE)
--   iq.vocab  (s, p, o)      reference table: the T-Box (subClassOf, domain, ..., owl:*Property types)
--   iq.entail()              the closure under iq-rdfs-plus.rules (UC_DataPlatform/inference), stated and inferred

CREATE SCHEMA IF NOT EXISTS iq;

CREATE TABLE IF NOT EXISTS iq.datum (
    iri         text PRIMARY KEY,
    tier        text NOT NULL CHECK (tier IN ('Bronze', 'Silver', 'Gold')),
    body        jsonb NOT NULL,
    words       tsvector GENERATED ALWAYS AS (jsonb_to_tsvector('english', body, '["string"]')) STORED,
    embedding   vector(3),
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS iq.quad (
    g   text NOT NULL,
    s   text NOT NULL,
    p   text NOT NULL,
    o   text NOT NULL,
    lit boolean NOT NULL DEFAULT false,
    PRIMARY KEY (g, s, p, o, lit)
);
CREATE TABLE IF NOT EXISTS iq.vocab (
    s text NOT NULL,
    p text NOT NULL,
    o text NOT NULL,
    PRIMARY KEY (s, p, o)
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'iq.datum'::regclass) THEN
        SET LOCAL citus.shard_count = 8;
        PERFORM create_distributed_table('iq.datum', 'iri');
        PERFORM create_distributed_table('iq.quad', 'g', colocate_with => 'iq.datum');
        ALTER TABLE iq.quad ADD CONSTRAINT quad_of_datum FOREIGN KEY (g) REFERENCES iq.datum (iri)
            ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
        PERFORM create_reference_table('iq.vocab');
    END IF;
END $$;

-- the data plane's datum (slice 3, GherkinIq/Facet/Store/sparql_citus.py): the exact body as given, its media type,
-- what it was derived from, when it was recorded; and each quad's named graph ('' = the default graph) and language
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS raw text;
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS media_type text;
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS derived_from text[] NOT NULL DEFAULT '{}';
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS recorded text;
ALTER TABLE iq.quad ADD COLUMN IF NOT EXISTS ng text NOT NULL DEFAULT '';
ALTER TABLE iq.quad ADD COLUMN IF NOT EXISTS lang text;
CREATE INDEX IF NOT EXISTS quad_ng ON iq.quad (ng);

CREATE INDEX IF NOT EXISTS datum_words ON iq.datum USING gin (words);
CREATE INDEX IF NOT EXISTS datum_embedding ON iq.datum USING hnsw (embedding vector_l2_ops);
CREATE INDEX IF NOT EXISTS quad_po ON iq.quad (p, o);

-- RDFS-Plus to fixpoint, every rule of iq-rdfs-plus.rules as one INSERT ... SELECT over the working closure.
CREATE OR REPLACE FUNCTION iq.entail()
RETURNS TABLE (s text, p text, o text, lit boolean, inferred boolean)
LANGUAGE plpgsql AS $fn$
DECLARE
    a    CONSTANT text := 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
    sco  CONSTANT text := 'http://www.w3.org/2000/01/rdf-schema#subClassOf';
    spo  CONSTANT text := 'http://www.w3.org/2000/01/rdf-schema#subPropertyOf';
    dom  CONSTANT text := 'http://www.w3.org/2000/01/rdf-schema#domain';
    rng  CONSTANT text := 'http://www.w3.org/2000/01/rdf-schema#range';
    same CONSTANT text := 'http://www.w3.org/2002/07/owl#sameAs';
    inv  CONSTANT text := 'http://www.w3.org/2002/07/owl#inverseOf';
    eqc  CONSTANT text := 'http://www.w3.org/2002/07/owl#equivalentClass';
    eqp  CONSTANT text := 'http://www.w3.org/2002/07/owl#equivalentProperty';
    fp   CONSTANT text := 'http://www.w3.org/2002/07/owl#FunctionalProperty';
    ifp  CONSTANT text := 'http://www.w3.org/2002/07/owl#InverseFunctionalProperty';
    sym  CONSTANT text := 'http://www.w3.org/2002/07/owl#SymmetricProperty';
    tra  CONSTANT text := 'http://www.w3.org/2002/07/owl#TransitiveProperty';
    n bigint; added bigint;
BEGIN
    DROP TABLE IF EXISTS pg_temp.iq_closure;
    CREATE TEMP TABLE iq_closure (s text, p text, o text, lit boolean, inferred boolean,
                                  PRIMARY KEY (s, p, o, lit)) ON COMMIT DROP;
    INSERT INTO iq_closure SELECT q.s, q.p, q.o, q.lit, false FROM iq.quad q ON CONFLICT DO NOTHING;
    INSERT INTO iq_closure SELECT v.s, v.p, v.o, false, false FROM iq.vocab v ON CONFLICT DO NOTHING;
    LOOP
        added := 0;
        -- rdfs2 domain
        INSERT INTO iq_closure SELECT x.s, a, d.o, false, true FROM iq_closure x JOIN iq_closure d ON d.s = x.p AND d.p = dom ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfs3 range
        INSERT INTO iq_closure SELECT x.o, a, r.o, false, true FROM iq_closure x JOIN iq_closure r ON r.s = x.p AND r.p = rng WHERE NOT x.lit ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfs5, rdfs11, rdfp7, rdfp12c, rdfp13c: subPropertyOf, subClassOf, sameAs, equivalentClass, equivalentProperty transitive
        INSERT INTO iq_closure SELECT x.s, x.p, y.o, false, true FROM iq_closure x JOIN iq_closure y ON y.s = x.o AND y.p = x.p
            WHERE x.p IN (spo, sco, same, eqc, eqp) AND NOT x.lit AND NOT y.lit AND x.s <> x.o AND y.s <> y.o AND x.s <> y.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfs7 subPropertyOf, rdfp13a equivalentProperty
        INSERT INTO iq_closure SELECT x.s, e.o, x.o, x.lit, true FROM iq_closure e JOIN iq_closure x ON x.p = e.s
            WHERE e.p IN (spo, eqp) AND e.s <> e.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp13b equivalentProperty, the other way
        INSERT INTO iq_closure SELECT x.s, e.s, x.o, x.lit, true FROM iq_closure e JOIN iq_closure x ON x.p = e.o
            WHERE e.p = eqp AND e.s <> e.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfs9 subClassOf, rdfp12a equivalentClass
        INSERT INTO iq_closure SELECT x.s, a, c.o, false, true FROM iq_closure x JOIN iq_closure c ON c.s = x.o
            WHERE x.p = a AND c.p IN (sco, eqc) AND c.s <> c.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp12b equivalentClass, the other way
        INSERT INTO iq_closure SELECT x.s, a, c.s, false, true FROM iq_closure x JOIN iq_closure c ON c.o = x.o
            WHERE x.p = a AND c.p = eqc AND c.s <> c.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp6 sameAs symmetric
        INSERT INTO iq_closure SELECT x.o, same, x.s, false, true FROM iq_closure x WHERE x.p = same AND NOT x.lit AND x.s <> x.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp11a, b, c: sameAs in object, subject, predicate
        INSERT INTO iq_closure SELECT u.s, u.p, m.o, false, true FROM iq_closure u JOIN iq_closure m ON m.s = u.o AND m.p = same
            WHERE NOT u.lit AND m.s <> m.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        INSERT INTO iq_closure SELECT m.o, u.p, u.o, u.lit, true FROM iq_closure u JOIN iq_closure m ON m.s = u.s AND m.p = same
            WHERE m.s <> m.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        INSERT INTO iq_closure SELECT u.s, m.o, u.o, u.lit, true FROM iq_closure u JOIN iq_closure m ON m.s = u.p AND m.p = same
            WHERE m.s <> m.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp1 FunctionalProperty
        INSERT INTO iq_closure SELECT x.o, same, y.o, false, true FROM iq_closure f JOIN iq_closure x ON x.p = f.s
            JOIN iq_closure y ON y.s = x.s AND y.p = x.p
            WHERE f.p = a AND f.o = fp AND x.o <> y.o AND NOT x.lit AND NOT y.lit ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp2 InverseFunctionalProperty
        INSERT INTO iq_closure SELECT x.s, same, y.s, false, true FROM iq_closure f JOIN iq_closure x ON x.p = f.s
            JOIN iq_closure y ON y.p = x.p AND y.o = x.o AND y.lit = x.lit
            WHERE f.p = a AND f.o = ifp AND x.s <> y.s ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp3 SymmetricProperty
        INSERT INTO iq_closure SELECT x.o, x.p, x.s, false, true FROM iq_closure f JOIN iq_closure x ON x.p = f.s
            WHERE f.p = a AND f.o = sym AND NOT x.lit AND x.s <> x.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp4 TransitiveProperty
        INSERT INTO iq_closure SELECT x.s, x.p, y.o, y.lit, true FROM iq_closure f JOIN iq_closure x ON x.p = f.s
            JOIN iq_closure y ON y.s = x.o AND y.p = x.p
            WHERE f.p = a AND f.o = tra AND NOT x.lit AND x.s <> x.o AND y.s <> y.o AND x.s <> y.o ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- rdfp8a, b inverseOf
        INSERT INTO iq_closure SELECT v.o, i.s, v.s, false, true FROM iq_closure i JOIN iq_closure v ON v.p = i.o
            WHERE i.p = inv AND NOT v.lit ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        INSERT INTO iq_closure SELECT v.o, i.o, v.s, false, true FROM iq_closure i JOIN iq_closure v ON v.p = i.s
            WHERE i.p = inv AND NOT v.lit ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        EXIT WHEN added = 0;
    END LOOP;
    RETURN QUERY SELECT c.s, c.p, c.o, c.lit, c.inferred FROM iq_closure c;
END
$fn$;

-- iq reads and writes; the schema is postgres's (applied over ssh), so no DDL is in the client's reach
GRANT USAGE ON SCHEMA iq TO iq;
GRANT SELECT, INSERT, UPDATE, DELETE ON iq.datum, iq.quad, iq.vocab TO iq;
GRANT EXECUTE ON FUNCTION iq.entail() TO iq;
