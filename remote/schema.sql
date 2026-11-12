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
--   iq.entail(seeds)         the same, for what is connected to the seeds only (slice 4)
--   iq.rule   (name)         reference table: rules held as data, Horn rules over triple patterns (iq.put_rule), applied
--                            in the closure with the vocabulary's own; owl:propertyChainAxiom of two or three properties
--                            (iq.put_chain) is applied there too (docs/architecture/ONTOLOGY_ENGINEERING.md slice 7)

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
-- A rule as data: body and head each a list of triple patterns [s, p, o], a term a ?variable, an IRI, or (as an
-- object only) a "quoted literal". Put by iq.put_rule, which refuses one that is not safe or not connected.
CREATE TABLE IF NOT EXISTS iq.rule (
    name text PRIMARY KEY,
    body jsonb NOT NULL,
    head jsonb NOT NULL
);

-- The store's shape (deploy-citus.sh shape): on a single server (`server`, no Citus) the tables stay local and only
-- the foreign key is added; on a Citus cluster (`citus`) they are distributed first. A server made a cluster later
-- (MIGRATE-to-Citus.md) runs this again: the server's rows are distributed, and its foreign key is kept as it is.
DO $$
BEGIN
    IF to_regclass('pg_catalog.pg_dist_partition') IS NOT NULL THEN
        IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'iq.datum'::regclass) THEN
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'quad_of_datum') THEN
                ALTER TABLE iq.quad DROP CONSTRAINT quad_of_datum;   -- Citus distributes a table without it
            END IF;
            SET LOCAL citus.shard_count = 8;
            PERFORM create_distributed_table('iq.datum', 'iri');
            PERFORM create_distributed_table('iq.quad', 'g', colocate_with => 'iq.datum');
            ALTER TABLE iq.quad ADD CONSTRAINT quad_of_datum FOREIGN KEY (g) REFERENCES iq.datum (iri)
                ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
            PERFORM create_reference_table('iq.vocab');
        END IF;
    ELSIF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'quad_of_datum') THEN
        ALTER TABLE iq.quad ADD CONSTRAINT quad_of_datum FOREIGN KEY (g) REFERENCES iq.datum (iri)
            ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    END IF;
    IF to_regclass('pg_catalog.pg_dist_partition') IS NOT NULL THEN       -- nested: a server has no pg_dist_partition
        IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'iq.rule'::regclass) THEN
            PERFORM create_reference_table('iq.rule');      -- a copy on every node, as the vocabulary is
        END IF;
    END IF;
END $$;

-- A rule's checks, as iq.put_rule makes them: every atom three terms, a literal only as an object; SAFE (every variable
-- of the head bound by the body); CONNECTED (the body's atoms joined through shared variables, so the scoped closure,
-- which follows nodes, sees all a rule can use). Raises 'bad_rule', 'unsafe_rule', 'disconnected_rule'.
CREATE OR REPLACE FUNCTION iq.check_rule(body jsonb, head jsonb) RETURNS void
LANGUAGE plpgsql IMMUTABLE AS $fn$
DECLARE
    atom jsonb; t text; j int; i int; k int; vars text[] := '{}'; mine text[]; groups text[][] := '{}';
    seen int[] := '{}'; changed boolean;
BEGIN
    IF jsonb_typeof(body) <> 'array' OR jsonb_typeof(head) <> 'array' OR jsonb_array_length(body) = 0
       OR jsonb_array_length(head) = 0 THEN
        RAISE EXCEPTION 'bad_rule: a body and a head, each a list of [s, p, o]';
    END IF;
    FOR atom IN SELECT * FROM jsonb_array_elements(body || head) LOOP
        IF jsonb_typeof(atom) <> 'array' OR jsonb_array_length(atom) <> 3 THEN
            RAISE EXCEPTION 'bad_rule: % is not [s, p, o]', atom;
        END IF;
        FOR j IN 0 .. 2 LOOP
            t := atom ->> j;
            IF t IS NULL OR t = '' OR (left(t, 1) = '"' AND (j < 2 OR length(t) < 2 OR right(t, 1) <> '"')) THEN
                RAISE EXCEPTION 'bad_rule: % (a literal is an object, "quoted")', atom;
            END IF;
        END LOOP;
    END LOOP;
    FOR atom IN SELECT * FROM jsonb_array_elements(body) LOOP
        vars := vars || ARRAY(SELECT x FROM jsonb_array_elements_text(atom) x WHERE left(x, 1) = '?');
    END LOOP;
    FOR atom IN SELECT * FROM jsonb_array_elements(head) LOOP
        FOR t IN SELECT x FROM jsonb_array_elements_text(atom) x WHERE left(x, 1) = '?' LOOP
            IF NOT t = ANY (vars) THEN
                RAISE EXCEPTION 'unsafe_rule: % is in the head and bound by no atom of the body', t;
            END IF;
        END LOOP;
    END LOOP;
    -- connected: grow from the first atom through shared variables
    seen := ARRAY[0];
    mine := ARRAY(SELECT x FROM jsonb_array_elements_text(body -> 0) x WHERE left(x, 1) = '?');
    LOOP
        changed := false;
        FOR i IN 1 .. jsonb_array_length(body) - 1 LOOP
            CONTINUE WHEN i = ANY (seen);
            IF EXISTS (SELECT 1 FROM jsonb_array_elements_text(body -> i) x WHERE x = ANY (mine)) THEN
                seen := seen || i;
                mine := mine || ARRAY(SELECT x FROM jsonb_array_elements_text(body -> i) x WHERE left(x, 1) = '?');
                changed := true;
            END IF;
        END LOOP;
        EXIT WHEN NOT changed;
    END LOOP;
    IF cardinality(seen) < jsonb_array_length(body) THEN
        RAISE EXCEPTION 'disconnected_rule: the body''s atoms share no variable with the rest';
    END IF;
END
$fn$;

CREATE OR REPLACE FUNCTION iq.put_rule(name text, body jsonb, head jsonb) RETURNS void
LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM iq.check_rule(body, head);
    INSERT INTO iq.rule VALUES (name, body, head) ON CONFLICT ON CONSTRAINT rule_pkey
        DO UPDATE SET body = EXCLUDED.body, head = EXCLUDED.head;
END
$fn$;

-- owl:propertyChainAxiom as OWL writes it (`p owl:propertyChainAxiom (p1 p2 ...)`), into the vocabulary; a chain of
-- two or three properties (the closure applies those). Raises 'chain_length'.
CREATE OR REPLACE FUNCTION iq.put_chain(p text, props text[]) RETURNS void
LANGUAGE plpgsql AS $fn$
DECLARE
    first CONSTANT text := 'http://www.w3.org/1999/02/22-rdf-syntax-ns#first';
    rest  CONSTANT text := 'http://www.w3.org/1999/02/22-rdf-syntax-ns#rest';
    nil   CONSTANT text := 'http://www.w3.org/1999/02/22-rdf-syntax-ns#nil';
    id text := '_:chain-' || md5(p || '|' || array_to_string(props, '|'));
    i int;
BEGIN
    IF cardinality(props) NOT IN (2, 3) THEN
        RAISE EXCEPTION 'chain_length: a chain of two or three properties, not %', cardinality(props);
    END IF;
    INSERT INTO iq.vocab VALUES (p, 'http://www.w3.org/2002/07/owl#propertyChainAxiom', id || '-1') ON CONFLICT DO NOTHING;
    FOR i IN 1 .. cardinality(props) LOOP
        INSERT INTO iq.vocab VALUES (id || '-' || i, first, props[i]) ON CONFLICT DO NOTHING;
        INSERT INTO iq.vocab VALUES (id || '-' || i, rest, CASE WHEN i = cardinality(props) THEN nil
                                                                ELSE id || '-' || (i + 1) END) ON CONFLICT DO NOTHING;
    END LOOP;
END
$fn$;

-- A rule as the statements that apply it once over the working closure (iq_closure): one INSERT ... SELECT per atom of
-- its head, the body's atoms joined on their shared variables.
CREATE OR REPLACE FUNCTION iq.rule_sql(body jsonb, head jsonb) RETURNS text[]
LANGUAGE plpgsql IMMUTABLE AS $fn$
DECLARE
    cols CONSTANT text[] := ARRAY['s', 'p', 'o'];
    i int; j int; t text; col text; bound jsonb := '{}'; lit jsonb := '{}'; conds text[] := '{}';
    froms text[] := '{}'; h jsonb; terms text[]; hlit text; out text[] := '{}';
BEGIN
    FOR i IN 0 .. jsonb_array_length(body) - 1 LOOP
        froms := froms || format('iq_closure b%s', i);
        FOR j IN 0 .. 2 LOOP
            t := body -> i ->> j;
            col := format('b%s.%s', i, cols[j + 1]);
            IF left(t, 1) = '?' THEN
                IF bound ? t THEN
                    conds := conds || format('%s = %s', col, bound ->> t);
                    IF lit ? t AND j = 2 THEN conds := conds || format('b%s.lit = %s', i, lit ->> t);
                    ELSIF lit ? t THEN conds := conds || format('NOT %s', lit ->> t);
                    ELSIF j = 2 THEN conds := conds || format('NOT b%s.lit', i);
                    END IF;
                ELSE
                    bound := bound || jsonb_build_object(t, col);
                    IF j = 2 THEN lit := lit || jsonb_build_object(t, format('b%s.lit', i)); END IF;
                END IF;
            ELSIF left(t, 1) = '"' THEN
                conds := conds || format('%s = %L AND b%s.lit', col, substr(t, 2, length(t) - 2), i);
            ELSE
                conds := conds || format('%s = %L', col, t);
                IF j = 2 THEN conds := conds || format('NOT b%s.lit', i); END IF;
            END IF;
        END LOOP;
    END LOOP;
    FOR h IN SELECT * FROM jsonb_array_elements(head) LOOP
        terms := '{}';
        FOR j IN 0 .. 2 LOOP
            t := h ->> j;
            terms := terms || CASE WHEN left(t, 1) = '?' THEN bound ->> t
                                   WHEN left(t, 1) = '"' THEN quote_literal(substr(t, 2, length(t) - 2))
                                   ELSE quote_literal(t) END;
        END LOOP;
        t := h ->> 2;
        hlit := CASE WHEN left(t, 1) = '"' THEN 'true' WHEN left(t, 1) = '?' THEN coalesce(lit ->> t, 'false')
                     ELSE 'false' END;
        out := out || format('INSERT INTO iq_closure SELECT DISTINCT %s, %s, %s, %s, true FROM %s WHERE %s ON CONFLICT DO NOTHING',
                             terms[1], terms[2], terms[3], hlit, array_to_string(froms, ', '),
                             coalesce(nullif(array_to_string(conds, ' AND '), ''), 'true'));
    END LOOP;
    RETURN out;
END
$fn$;

-- the data plane's datum (slice 3, GherkinIq/Facet/Store/sparql_citus.py): the exact body as given, its media type,
-- what it was derived from, when it was recorded; and each quad's named graph ('' = the default graph) and language
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS raw text;
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS media_type text;
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS derived_from text[] NOT NULL DEFAULT '{}';
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS recorded text;
ALTER TABLE iq.datum ADD COLUMN IF NOT EXISTS operation text;   -- the gate's operation that admitted the write (slice 5)
ALTER TABLE iq.quad ADD COLUMN IF NOT EXISTS ng text NOT NULL DEFAULT '';
ALTER TABLE iq.quad ADD COLUMN IF NOT EXISTS lang text;
CREATE INDEX IF NOT EXISTS quad_ng ON iq.quad (ng);

CREATE INDEX IF NOT EXISTS datum_words ON iq.datum USING gin (words);
CREATE INDEX IF NOT EXISTS datum_embedding ON iq.datum USING hnsw (embedding vector_l2_ops);
CREATE INDEX IF NOT EXISTS quad_po ON iq.quad (p, o);
CREATE INDEX IF NOT EXISTS quad_s ON iq.quad (s);
CREATE INDEX IF NOT EXISTS quad_o ON iq.quad (o);

-- RDFS-Plus to fixpoint, every rule of iq-rdfs-plus.rules as one INSERT ... SELECT over the working closure: the
-- temp table iq_closure, which the caller has loaded (iq.entail: everything; iq.entail(seeds): a scope).
CREATE OR REPLACE FUNCTION iq.entail_fixpoint() RETURNS int
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
    chn  CONSTANT text := 'http://www.w3.org/2002/07/owl#propertyChainAxiom';
    fst  CONSTANT text := 'http://www.w3.org/1999/02/22-rdf-syntax-ns#first';
    rst  CONSTANT text := 'http://www.w3.org/1999/02/22-rdf-syntax-ns#rest';
    n bigint; added bigint; rounds int := 0; rules text[]; stmt text;
BEGIN
    CREATE INDEX ON iq_closure (p, o);
    CREATE INDEX ON iq_closure (o);
    ANALYZE iq_closure;
    -- the chains the vocabulary holds (a chain of two or three properties; a longer one is not applied)
    DROP TABLE IF EXISTS pg_temp.iq_chain;
    CREATE TEMP TABLE iq_chain ON COMMIT DROP AS
    WITH RECURSIVE lst(chain, p, node, pos, prop) AS (
        SELECT c.o, c.s, c.o, 1, f.o FROM iq_closure c JOIN iq_closure f ON f.s = c.o AND f.p = fst WHERE c.p = chn
        UNION ALL
        SELECT l.chain, l.p, r.o, l.pos + 1, f.o FROM lst l JOIN iq_closure r ON r.s = l.node AND r.p = rst
          JOIN iq_closure f ON f.s = r.o AND f.p = fst WHERE l.pos < 4)
    SELECT p, max(prop) FILTER (WHERE pos = 1) AS p1, max(prop) FILTER (WHERE pos = 2) AS p2,
           max(prop) FILTER (WHERE pos = 3) AS p3, count(*) AS n FROM lst GROUP BY chain, p;
    -- the rules held as data, each as the statements that apply it once
    rules := ARRAY(SELECT unnest(iq.rule_sql(r.body, r.head)) FROM iq.rule r ORDER BY r.name);
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
        -- prp-spo2 owl:propertyChainAxiom, of two properties and of three
        INSERT INTO iq_closure SELECT x.s, ch.p, y.o, y.lit, true FROM iq_chain ch JOIN iq_closure x ON x.p = ch.p1
            JOIN iq_closure y ON y.s = x.o AND y.p = ch.p2 WHERE ch.n = 2 AND NOT x.lit ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        INSERT INTO iq_closure SELECT x.s, ch.p, z.o, z.lit, true FROM iq_chain ch JOIN iq_closure x ON x.p = ch.p1
            JOIN iq_closure y ON y.s = x.o AND y.p = ch.p2 JOIN iq_closure z ON z.s = y.o AND z.p = ch.p3
            WHERE ch.n = 3 AND NOT x.lit AND NOT y.lit ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        -- the rules held as data
        FOREACH stmt IN ARRAY rules LOOP
            EXECUTE stmt;
            GET DIAGNOSTICS n = ROW_COUNT; added := added + n;
        END LOOP;
        rounds := rounds + 1;
        EXIT WHEN added = 0;
    END LOOP;
    RETURN rounds;
END
$fn$;

CREATE OR REPLACE FUNCTION iq.closure_table() RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
    DROP TABLE IF EXISTS pg_temp.iq_closure;
    CREATE TEMP TABLE iq_closure (s text, p text, o text, lit boolean, inferred boolean,
                                  PRIMARY KEY (s, p, o, lit)) ON COMMIT DROP;
END
$fn$;

-- the closure of everything: every quad and the vocabulary
CREATE OR REPLACE FUNCTION iq.entail()
RETURNS TABLE (s text, p text, o text, lit boolean, inferred boolean)
LANGUAGE plpgsql AS $fn$
#variable_conflict use_column
BEGIN
    PERFORM iq.closure_table();
    INSERT INTO iq_closure SELECT q.s, q.p, q.o, q.lit, false FROM iq.quad q ON CONFLICT DO NOTHING;
    INSERT INTO iq_closure SELECT v.s, v.p, v.o, false, false FROM iq.vocab v ON CONFLICT DO NOTHING;
    PERFORM iq.entail_fixpoint();
    RETURN QUERY SELECT c.s, c.p, c.o, c.lit, c.inferred FROM iq_closure c;
END
$fn$;

-- The closure of a SCOPE (slice 4): what is connected to the seeds. Every rule's body is joined through shared nodes
-- or through the vocabulary, so what can be inferred about a node depends only on the triples of its component --
-- the nodes reachable from it along any triple, either way, except into a class through rdf:type (the class is
-- vocabulary: every instance of it would otherwise be one component). So: the vocabulary (iq.vocab and any T-Box
-- triples among the quads) in full; a seed that is a class brings its instances -- typed by it or a subclass or an
-- equivalent, or the subject (object) of a property whose domain (range) is one of them; then the component of
-- the seeds, breadth first; then the same fixpoint. Exact for every triple about a node of the scope (checked
-- against iq.entail() in features/inference.feature), and as costly as the scope, not the graph.
CREATE OR REPLACE FUNCTION iq.entail(seeds text[])
RETURNS TABLE (s text, p text, o text, lit boolean, inferred boolean)
LANGUAGE plpgsql AS $fn$
#variable_conflict use_column
DECLARE
    a    CONSTANT text := 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
    tbox CONSTANT text[] := ARRAY['http://www.w3.org/2000/01/rdf-schema#subClassOf',
        'http://www.w3.org/2000/01/rdf-schema#subPropertyOf', 'http://www.w3.org/2000/01/rdf-schema#domain',
        'http://www.w3.org/2000/01/rdf-schema#range', 'http://www.w3.org/2002/07/owl#inverseOf',
        'http://www.w3.org/2002/07/owl#equivalentClass', 'http://www.w3.org/2002/07/owl#equivalentProperty'];
    owl  CONSTANT text := 'http://www.w3.org/2002/07/owl#';
    classes text[]; props text[]; frontier text[]; n int; hops int := 0;
BEGIN
    PERFORM iq.closure_table();
    DROP TABLE IF EXISTS pg_temp.iq_scope;
    CREATE TEMP TABLE iq_scope (node text PRIMARY KEY) ON COMMIT DROP;
    -- the vocabulary, in full
    INSERT INTO iq_closure SELECT v.s, v.p, v.o, false, false FROM iq.vocab v ON CONFLICT DO NOTHING;
    INSERT INTO iq_closure SELECT q.s, q.p, q.o, q.lit, false FROM iq.quad q
     WHERE q.p = ANY (tbox) OR (q.p = a AND q.o LIKE owl || '%') ON CONFLICT DO NOTHING;
    -- classes among the seeds, closed under subClassOf (downwards) and equivalentClass; the properties that type
    WITH RECURSIVE down(c) AS (
        SELECT unnest(seeds)
        UNION
        SELECT CASE WHEN x.o = d.c THEN x.s ELSE x.o END FROM iq_closure x JOIN down d ON (x.o = d.c OR x.s = d.c)
         WHERE (x.p = tbox[1] AND x.o = d.c) OR x.p = tbox[6])
    SELECT array_agg(c) INTO classes FROM down;
    -- the properties that type into them (domain / range), with every sub-, equivalent and inverse property of those
    WITH RECURSIVE typing(p) AS (
        SELECT DISTINCT x.s FROM iq_closure x WHERE x.p IN (tbox[3], tbox[4]) AND x.o = ANY (classes)
        UNION
        SELECT CASE WHEN x.o = t.p THEN x.s ELSE x.o END FROM iq_closure x JOIN typing t ON (x.o = t.p OR x.s = t.p)
         WHERE (x.p = tbox[2] AND x.o = t.p) OR x.p IN (tbox[5], tbox[7]))
    SELECT array_agg(p) INTO props FROM typing;
    -- the first frontier: the seeds, the instances of seed classes, the subjects / objects of typing properties
    INSERT INTO iq_scope SELECT DISTINCT unnest(seeds) ON CONFLICT DO NOTHING;
    INSERT INTO iq_scope SELECT DISTINCT q.s FROM iq.quad q WHERE q.p = a AND q.o = ANY (classes) ON CONFLICT DO NOTHING;
    IF props IS NOT NULL THEN
        INSERT INTO iq_scope SELECT DISTINCT q.s FROM iq.quad q WHERE q.p = ANY (props) ON CONFLICT DO NOTHING;
        INSERT INTO iq_scope SELECT DISTINCT q.o FROM iq.quad q WHERE q.p = ANY (props) AND NOT q.lit ON CONFLICT DO NOTHING;
    END IF;
    frontier := ARRAY(SELECT node FROM iq_scope);
    -- the component, breadth first (each hop one query across the shards)
    WHILE cardinality(frontier) > 0 LOOP
        hops := hops + 1;
        INSERT INTO iq_closure SELECT q.s, q.p, q.o, q.lit, false FROM iq.quad q
         WHERE (q.s = ANY (frontier) OR (q.o = ANY (frontier) AND NOT q.lit)) AND q.p <> ALL (tbox) ON CONFLICT DO NOTHING;
        WITH reached AS (
            SELECT x.s AS node FROM iq_closure x WHERE x.o = ANY (frontier) AND NOT x.lit AND NOT x.inferred
               AND x.p <> ALL (tbox)
            UNION SELECT x.o FROM iq_closure x WHERE x.s = ANY (frontier) AND NOT x.lit AND x.p <> a AND NOT x.inferred
               AND x.p <> ALL (tbox)),
        fresh AS (INSERT INTO iq_scope SELECT r.node FROM reached r ON CONFLICT DO NOTHING RETURNING node)
        SELECT array_agg(node) INTO frontier FROM fresh;
        frontier := coalesce(frontier, '{}');
    END LOOP;
    PERFORM set_config('iq.scope_hops', hops::text, true);
    PERFORM iq.entail_fixpoint();
    RETURN QUERY SELECT c.s, c.p, c.o, c.lit, c.inferred FROM iq_closure c;
END
$fn$;

-- iq reads and writes; the schema is postgres's (applied over ssh), so no DDL is in the client's reach
-- the cluster's record (UC_Group: a domain's own slot onto the cluster, cdc apply.py domain): one row per
-- (owner, iri), the owner computed from the IRI (DataPlatform rule 11) and the table DISTRIBUTED BY IT (rule 3), so a
-- domain's rows sit on one shard together, a domain's read is a single-shard (router) query, and restoring a domain
-- is one select by owner. Written through dataplane.ClusterAdapter only; not co-located with iq.datum (keyed by iri).
CREATE TABLE IF NOT EXISTS iq.cluster_datum (
    owner        text NOT NULL,
    iri          text NOT NULL,
    tier         text NOT NULL,
    media_type   text NOT NULL,
    body         text NOT NULL,
    derived_from text[] NOT NULL,
    recorded_at  text NOT NULL,
    PRIMARY KEY (owner, iri)
);
DO $$
BEGIN
    IF to_regclass('pg_catalog.pg_dist_partition') IS NOT NULL THEN   -- a Citus cluster; a single server keeps the table local
        IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'iq.cluster_datum'::regclass) THEN
            PERFORM create_distributed_table('iq.cluster_datum', 'owner');
        END IF;
    END IF;
END $$;

GRANT USAGE ON SCHEMA iq TO iq;
GRANT SELECT, INSERT, UPDATE, DELETE ON iq.datum, iq.quad, iq.vocab, iq.rule TO iq;
GRANT SELECT, INSERT ON iq.cluster_datum TO iq;
GRANT EXECUTE ON FUNCTION iq.entail(), iq.entail(text[]), iq.entail_fixpoint(), iq.closure_table() TO iq;
GRANT EXECUTE ON FUNCTION iq.put_rule(text, jsonb, jsonb), iq.put_chain(text, text[]), iq.check_rule(jsonb, jsonb),
    iq.rule_sql(jsonb, jsonb) TO iq;
