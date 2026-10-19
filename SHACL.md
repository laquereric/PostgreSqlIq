@use_case Development/UseCase/UC_PostgreSqliq
@facet Agent

database with built-in SHACL engines:

Ontotext GraphDB. You enable SHACL validation in the repository configuration, and it checks each transaction at commit, rejecting writes that violate the shapes. It is built on RDF4J’s SHACL engine. GraphDB is one of the commercial vendors listed as supporting SHACL.
Stardog. It validates SHACL constraints through its integrity-constraint feature, either on demand or enforced at write time.
Eclipse RDF4J. This is an open source Java framework with SHACL validation (the ShaclSail), and it underpins several other stores.
AllegroGraph. Franz added a SHACL validation engine in version 6.6. You can run it with agtool shacl-validate or from SPARQL. A SPARQL magic property returns a SHACL validation report for a given data graph and shapes graph.
OpenLink Virtuoso. Version 8.3 added SHACL support. It compiles SHACL shapes into stored procedures and runs them against RDF data. A demo form page at /shacl-default lets you validate from a UI.
RDFox. Version 5 added SHACL constraint validation.
Apache Jena / Fuseki. Jena has a full SHACL implementation, and Fuseki can expose a SHACL validation service on a dataset.

see https://github.com/laquereric/pgRDF
SHACL validation isn’t implemented yet?

pgRDF is a Rust-native PostgreSQL extension for RDF, SPARQL, SHACL and OWL reasoning. It installs like any other extension (CREATE EXTENSION pgrdf;) and exposes everything as SQL functions. The current release (0.3.0) is alpha:

Turtle loading, SPARQL querying, and OWL 2 RL materialization work.
SHACL validation is only a stub. The real implementation is blocked by an upstream dependency conflict and is targeted for v0.4.
It supports PostgreSQL 14 through 17.

So it’s worth watching, but you can’t use it to validate SHACL today.

Practical options for Postgres right now:

Virtual knowledge graph plus external validator. Use Ontop to expose your Postgres tables as RDF through a SPARQL endpoint. Then validate against it with pySHACL, Jena, or TopBraid SHACL.
Export and validate. Dump the relevant data as RDF and validate it offline, for example in CI.
Run a triple store alongside Postgres. GraphDB, Stardog, or Jena Fuseki with SHACL enforced, if the RDF data can live outside Postgres.
Translate shapes to SQL constraints. Many SHACL Core constraints (cardinality, datatype, pattern, value ranges) map onto NOT NULL, CHECK, foreign keys, and triggers. If your data is relational to begin with, writing these directly is often simpler.

Sources:

pgRDF README
pgRDF introduction
pgRDF architecture