-- Generated test fixture from native-8 src/apply/metadata.rs.
-- Source SHA-256: ebef9685b217811281233b8f116a69a000acf1ff86982d1c155461007ee53aa2
CREATE TABLE @SCHEMA@.copy_done (
    magic pg_catalog.text COLLATE pg_catalog."C" NOT NULL,
    version pg_catalog.int4 NOT NULL,
    run_id pg_catalog.bytea NOT NULL,
    nonce pg_catalog.bytea NOT NULL,
    source_table pg_catalog.oid NOT NULL,
    target_table pg_catalog.oid NOT NULL,
    copy_generation pg_catalog.numeric(20,0) NOT NULL,
    rows pg_catalog.numeric(20,0) NOT NULL,
    bytes pg_catalog.numeric(20,0) NOT NULL,
    target_namespace pg_catalog.oid NOT NULL,
    target_owner pg_catalog.oid NOT NULL,
    PRIMARY KEY (copy_generation, source_table)
) USING heap;
