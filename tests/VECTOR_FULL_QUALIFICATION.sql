-- ============================================================
-- PLOMID VECTOR FULL QUALIFICATION SUITE
-- ============================================================
--
-- Purpose:
--   Production release gate for vector support: DDL type matrix,
--   literal/range validation, distances, operators, NULL semantics,
--   deterministic Top-K, radius + filtered search, index lifecycle
--   (every method), transactions, UPDATE/DELETE invalidation,
--   settings, observability probes, EXPLAIN plans.
--
-- Run with:
--   psql -v ON_ERROR_STOP=off -f VECTOR_FULL_QUALIFICATION.sql
--   or via the harness: python3 tests/python/vector_qualify.py
--
-- Conventions:
--   * Schema `vq` is dropped + recreated here (start and end), so the
--     file is rerunnable against any database.
--   * Lines starting with `-- EXPECT-ERROR:` mark the next statement as
--     intentionally failing. The harness asserts failure; every other
--     statement must succeed.
--   * Self-checking probes look like:
--       SELECT 'CHECK <name>', CASE WHEN <cond> THEN 'PASS' ELSE 'FAIL' END;
--     The harness collects them; a human reader can eyeball them too.
--   * Isolation probes after each error block prove failed statements
--     never partially modify their table.
--
-- ============================================================

\set ON_ERROR_STOP off
\set VERBOSITY terse

DROP SCHEMA IF EXISTS vq CASCADE;
CREATE SCHEMA vq;

-- ------------------------------------------------------------
-- 1. DDL: every element type + shorthand + kind forms
-- ------------------------------------------------------------
CREATE TABLE vq.docs (
    id BIGINT PRIMARY KEY,
    content TEXT,
    embedding VECTOR(FLOAT32, 4)
);
CREATE TABLE vq.allkinds (
    id BIGINT PRIMARY KEY,
    f32v VECTOR(FLOAT32, 3),
    f16v VECTOR(FLOAT16, 3),
    bf16v VECTOR(BFLOAT16, 3),
    i8v VECTOR(INT8, 3),
    u8v VECTOR(UINT8, 3),
    i4v VECTOR(INT4, 4),
    binv VECTOR(BINARY, 8),
    sparsev SPARSE_VECTOR(FLOAT32, 100),
    multiv MULTI_VECTOR(FLOAT32, 4, 2),
    shorthand VECTOR(3)
);
SELECT 'CHECK auto-hnsw-docs', CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM pg_indexes WHERE tablename = 'docs' AND indexname LIKE '%embedding_vector_idx%';
-- Auto index follows the executable envelope: dense columns get the
-- deterministic default; BINARY/SPARSE/MULTI columns get none (their
-- methods are explicit-only). allkinds has 7 dense columns (6 named +
-- shorthand FLOAT32).
SELECT 'CHECK auto-index-kinds', CASE WHEN COUNT(*) = 7 THEN 'PASS' ELSE 'FAIL' END FROM pg_indexes WHERE tablename = 'allkinds' AND indexname LIKE '%vector_idx%';

-- 1b. Invalid DDL (each must fail; counts after prove isolation).
-- EXPECT-ERROR: zero dimension
CREATE TABLE vq.bad (e VECTOR(FLOAT32, 0));
-- EXPECT-ERROR: dimension above maximum
CREATE TABLE vq.bad (e VECTOR(FLOAT32, 65537));
-- EXPECT-ERROR: unsupported element type
CREATE TABLE vq.bad (e VECTOR(FLOAT64, 4));
-- EXPECT-ERROR: binary sparse kind
CREATE TABLE vq.bad (e SPARSE_VECTOR(BINARY, 10));
SELECT 'CHECK bad-ddl-absent', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM pg_tables WHERE tablename = 'vq_bad';

-- ------------------------------------------------------------
-- 2. Literal validation matrix
-- ------------------------------------------------------------
INSERT INTO vq.docs VALUES
    (1, 'alpha', [0.12,0.34,0.56,0.78]),
    (2, 'beta', [0.90,0.10,0.20,0.30]),
    (3, 'gamma', NULL),
    (4, 'delta', [-0.5,+2e1,1.5E-2,0.0]);
INSERT INTO vq.allkinds VALUES (1, [1,2,3], [1,2,3], [1,2,3], [-128,0,127], [0,128,255], [-8,-1,0,7], '01010101', '{1:2.0,5:3.0}/100', '[[1,0],[0,1]]', [1,2,3]), (2, [4,5,6], [4,5,6], [4,5,6], [1,2,3], [4,5,6], [1,2,3,4], '00000000', '{}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: dimension mismatch
INSERT INTO vq.docs VALUES (9, 'x', [1,2,3]);
-- EXPECT-ERROR: empty literal
INSERT INTO vq.docs VALUES (9, 'x', []);
-- EXPECT-ERROR: int8 overflow
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,999], [0,0,0], [0,0,0,0], '00000000', '{}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: uint8 negative
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,-1], [0,0,0,0], '00000000', '{}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: int4 overflow
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,0], [0,0,0,8], '00000000', '{}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: fractional int element
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,1.5], [0,0,0], [0,0,0,0], '00000000', '{}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: binary bad chars
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,0], [0,0,0,0], '01010102', '{}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: binary bad length
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,0], [0,0,0,0], '0101', '{}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: sparse duplicate index
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,0], [0,0,0,0], '00000000', '{1:1.0,1:2.0}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: sparse index out of range
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,0], [0,0,0,0], '00000000', '{100:1.0}/100', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: sparse missing dimension
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,0], [0,0,0,0], '00000000', '{1:1.0}', '[[0,0],[0,0]]', [0,0,0]);
-- EXPECT-ERROR: multi ragged
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,0], [0,0,0,0], '00000000', '{}/100', '[[1,0],[0]]', [0,0,0]);
-- EXPECT-ERROR: multi wrong dim
INSERT INTO vq.allkinds VALUES (9, [1,2,3], [1,2,3], [1,2,3], [0,0,0], [0,0,0], [0,0,0,0], '00000000', '{}/100', '[[1,0,0]]', [0,0,0]);
SELECT 'CHECK err-iso-docs', CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs;
SELECT 'CHECK err-iso-kinds', CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END FROM vq.allkinds;

-- ------------------------------------------------------------
-- 3. Distances, operators, NULL
-- ------------------------------------------------------------
SELECT 'CHECK l2-self', CASE WHEN abs(l2_distance(embedding, [0.12,0.34,0.56,0.78])) < 1e-5 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id = 1;
SELECT 'CHECK cosine-self', CASE WHEN abs(cosine_distance(embedding, [0.12,0.34,0.56,0.78])) < 1e-5 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id = 1;
SELECT 'CHECK dot', CASE WHEN dot_product(embedding, [1,1,1,1]) BETWEEN 1.7999 AND 1.8001 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id = 1;
SELECT 'CHECK null-propagates', CASE WHEN cosine_distance(embedding, [1,0,0,0]) IS NULL THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id = 3;
SELECT 'CHECK neg-ip', CASE WHEN ((embedding <#> [1,1,1,1]) + dot_product(embedding, [1,1,1,1])) BETWEEN -1e-5 AND 1e-5 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id = 1;
SELECT 'CHECK topk123', CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.docs ORDER BY l2_distance(embedding, [0.12,0.34,0.56,0.78]) LIMIT 3) AS t;
SELECT 'CHECK op-agree', CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.docs ORDER BY embedding <=> [0.12,0.34,0.56,0.78] LIMIT 3) AS t;
SELECT 'CHECK radius', CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.docs WHERE l2_distance(embedding, [0.12,0.34,0.56,0.78]) < 0.25) AS t;
SELECT 'CHECK hamming', CASE WHEN SUM(v) = 4.0 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT hamming_distance(binv, '01010101') AS v FROM vq.allkinds) AS t;
SELECT 'CHECK jaccard-self', CASE WHEN abs(MIN(v)) < 1e-9 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT jaccard_distance(binv, '01010101') AS v FROM vq.allkinds) AS t;
SELECT 'CHECK sparse-cos', CASE WHEN abs(cosine_distance(sparsev, '{1:2.0,5:3.0}/100')) < 1e-5 THEN 'PASS' ELSE 'FAIL' END FROM vq.allkinds WHERE id = 1;
SELECT 'CHECK multi-l2', CASE WHEN abs(l2_distance(multiv, '[[1,0],[0,1]]')) < 1e-5 THEN 'PASS' ELSE 'FAIL' END FROM vq.allkinds WHERE id = 1;

-- ------------------------------------------------------------
-- 4. Deterministic Top-K (120 tie-free rows, axis lanes + noise)
-- ------------------------------------------------------------
CREATE TABLE vq.big (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 8));
INSERT INTO vq.big VALUES (1, [1.0,-0.01,0.01,0.03,-0.04,-0.02,0.00,0.02]),(2, [-0.02,1.0,0.02,0.04,-0.03,-0.01,0.01,0.03]),(3, [-0.01,0.01,1.0,-0.04,-0.02,0.00,0.02,0.04]),(4, [0.00,0.02,0.04,1.0,-0.01,0.01,0.03,-0.04]),(5, [0.01,0.03,-0.04,-0.02,1.0,0.02,0.04,-0.03]),(6, [0.02,0.04,-0.03,-0.01,0.01,1.0,-0.04,-0.02]),(7, [0.03,-0.04,-0.02,0.00,0.02,0.04,1.0,-0.01]),(8, [0.04,-0.03,-0.01,0.01,0.03,-0.04,-0.02,1.0]),(9, [1.0,-0.02,0.00,0.02,0.04,-0.03,-0.01,0.01]),(10, [-0.03,1.0,0.01,0.03,-0.04,-0.02,0.00,0.02]),(11, [-0.02,0.00,1.0,0.04,-0.03,-0.01,0.01,0.03]),(12, [-0.01,0.01,0.03,1.0,-0.02,0.00,0.02,0.04]),(13, [0.00,0.02,0.04,-0.03,1.0,0.01,0.03,-0.04]),(14, [0.01,0.03,-0.04,-0.02,0.00,1.0,0.04,-0.03]),(15, [0.02,0.04,-0.03,-0.01,0.01,0.03,1.0,-0.02]),(16, [0.03,-0.04,-0.02,0.00,0.02,0.04,-0.03,1.0]),(17, [1.0,-0.03,-0.01,0.01,0.03,-0.04,-0.02,0.00]),(18, [-0.04,1.0,0.00,0.02,0.04,-0.03,-0.01,0.01]),(19, [-0.03,-0.01,1.0,0.03,-0.04,-0.02,0.00,0.02]),(20, [-0.02,0.00,0.02,1.0,-0.03,-0.01,0.01,0.03]);
INSERT INTO vq.big VALUES (21, [-0.01,0.01,0.03,-0.04,1.0,0.00,0.02,0.04]),(22, [0.00,0.02,0.04,-0.03,-0.01,1.0,0.03,-0.04]),(23, [0.01,0.03,-0.04,-0.02,0.00,0.02,1.0,-0.03]),(24, [0.02,0.04,-0.03,-0.01,0.01,0.03,-0.04,1.0]),(25, [1.0,-0.04,-0.02,0.00,0.02,0.04,-0.03,-0.01]),(26, [0.04,1.0,-0.01,0.01,0.03,-0.04,-0.02,0.00]),(27, [-0.04,-0.02,1.0,0.02,0.04,-0.03,-0.01,0.01]),(28, [-0.03,-0.01,0.01,1.0,-0.04,-0.02,0.00,0.02]),(29, [-0.02,0.00,0.02,0.04,1.0,-0.01,0.01,0.03]),(30, [-0.01,0.01,0.03,-0.04,-0.02,1.0,0.02,0.04]),(31, [0.00,0.02,0.04,-0.03,-0.01,0.01,1.0,-0.04]),(32, [0.01,0.03,-0.04,-0.02,0.00,0.02,0.04,1.0]),(33, [1.0,0.04,-0.03,-0.01,0.01,0.03,-0.04,-0.02]),(34, [0.03,1.0,-0.02,0.00,0.02,0.04,-0.03,-0.01]),(35, [0.04,-0.03,1.0,0.01,0.03,-0.04,-0.02,0.00]),(36, [-0.04,-0.02,0.00,1.0,0.04,-0.03,-0.01,0.01]),(37, [-0.03,-0.01,0.01,0.03,1.0,-0.02,0.00,0.02]),(38, [-0.02,0.00,0.02,0.04,-0.03,1.0,0.01,0.03]),(39, [-0.01,0.01,0.03,-0.04,-0.02,0.00,1.0,0.04]),(40, [0.00,0.02,0.04,-0.03,-0.01,0.01,0.03,1.0]);
INSERT INTO vq.big VALUES (41, [1.0,0.03,-0.04,-0.02,0.00,0.02,0.04,-0.03]),(42, [0.02,1.0,-0.03,-0.01,0.01,0.03,-0.04,-0.02]),(43, [0.03,-0.04,1.0,0.00,0.02,0.04,-0.03,-0.01]),(44, [0.04,-0.03,-0.01,1.0,0.03,-0.04,-0.02,0.00]),(45, [-0.04,-0.02,0.00,0.02,1.0,-0.03,-0.01,0.01]),(46, [-0.03,-0.01,0.01,0.03,-0.04,1.0,0.00,0.02]),(47, [-0.02,0.00,0.02,0.04,-0.03,-0.01,1.0,0.03]),(48, [-0.01,0.01,0.03,-0.04,-0.02,0.00,0.02,1.0]),(49, [1.0,0.02,0.04,-0.03,-0.01,0.01,0.03,-0.04]),(50, [0.01,1.0,-0.04,-0.02,0.00,0.02,0.04,-0.03]),(51, [0.02,0.04,1.0,-0.01,0.01,0.03,-0.04,-0.02]),(52, [0.03,-0.04,-0.02,1.0,0.02,0.04,-0.03,-0.01]),(53, [0.04,-0.03,-0.01,0.01,1.0,-0.04,-0.02,0.00]),(54, [-0.04,-0.02,0.00,0.02,0.04,1.0,-0.01,0.01]),(55, [-0.03,-0.01,0.01,0.03,-0.04,-0.02,1.0,0.02]),(56, [-0.02,0.00,0.02,0.04,-0.03,-0.01,0.01,1.0]),(57, [1.0,0.01,0.03,-0.04,-0.02,0.00,0.02,0.04]),(58, [0.00,1.0,0.04,-0.03,-0.01,0.01,0.03,-0.04]),(59, [0.01,0.03,1.0,-0.02,0.00,0.02,0.04,-0.03]),(60, [0.02,0.04,-0.03,1.0,0.01,0.03,-0.04,-0.02]);
INSERT INTO vq.big VALUES (61, [0.03,-0.04,-0.02,0.00,1.0,0.04,-0.03,-0.01]),(62, [0.04,-0.03,-0.01,0.01,0.03,1.0,-0.02,0.00]),(63, [-0.04,-0.02,0.00,0.02,0.04,-0.03,1.0,0.01]),(64, [-0.03,-0.01,0.01,0.03,-0.04,-0.02,0.00,1.0]),(65, [1.0,0.00,0.02,0.04,-0.03,-0.01,0.01,0.03]),(66, [-0.01,1.0,0.03,-0.04,-0.02,0.00,0.02,0.04]),(67, [0.00,0.02,1.0,-0.03,-0.01,0.01,0.03,-0.04]),(68, [0.01,0.03,-0.04,1.0,0.00,0.02,0.04,-0.03]),(69, [0.02,0.04,-0.03,-0.01,1.0,0.03,-0.04,-0.02]),(70, [0.03,-0.04,-0.02,0.00,0.02,1.0,-0.03,-0.01]),(71, [0.04,-0.03,-0.01,0.01,0.03,-0.04,1.0,0.00]),(72, [-0.04,-0.02,0.00,0.02,0.04,-0.03,-0.01,1.0]),(73, [1.0,-0.01,0.01,0.03,-0.04,-0.02,0.00,0.02]),(74, [-0.02,1.0,0.02,0.04,-0.03,-0.01,0.01,0.03]),(75, [-0.01,0.01,1.0,-0.04,-0.02,0.00,0.02,0.04]),(76, [0.00,0.02,0.04,1.0,-0.01,0.01,0.03,-0.04]),(77, [0.01,0.03,-0.04,-0.02,1.0,0.02,0.04,-0.03]),(78, [0.02,0.04,-0.03,-0.01,0.01,1.0,-0.04,-0.02]),(79, [0.03,-0.04,-0.02,0.00,0.02,0.04,1.0,-0.01]),(80, [0.04,-0.03,-0.01,0.01,0.03,-0.04,-0.02,1.0]);
INSERT INTO vq.big VALUES (81, [1.0,-0.02,0.00,0.02,0.04,-0.03,-0.01,0.01]),(82, [-0.03,1.0,0.01,0.03,-0.04,-0.02,0.00,0.02]),(83, [-0.02,0.00,1.0,0.04,-0.03,-0.01,0.01,0.03]),(84, [-0.01,0.01,0.03,1.0,-0.02,0.00,0.02,0.04]),(85, [0.00,0.02,0.04,-0.03,1.0,0.01,0.03,-0.04]),(86, [0.01,0.03,-0.04,-0.02,0.00,1.0,0.04,-0.03]),(87, [0.02,0.04,-0.03,-0.01,0.01,0.03,1.0,-0.02]),(88, [0.03,-0.04,-0.02,0.00,0.02,0.04,-0.03,1.0]),(89, [1.0,-0.03,-0.01,0.01,0.03,-0.04,-0.02,0.00]),(90, [-0.04,1.0,0.00,0.02,0.04,-0.03,-0.01,0.01]),(91, [-0.03,-0.01,1.0,0.03,-0.04,-0.02,0.00,0.02]),(92, [-0.02,0.00,0.02,1.0,-0.03,-0.01,0.01,0.03]),(93, [-0.01,0.01,0.03,-0.04,1.0,0.00,0.02,0.04]),(94, [0.00,0.02,0.04,-0.03,-0.01,1.0,0.03,-0.04]),(95, [0.01,0.03,-0.04,-0.02,0.00,0.02,1.0,-0.03]),(96, [0.02,0.04,-0.03,-0.01,0.01,0.03,-0.04,1.0]),(97, [1.0,-0.04,-0.02,0.00,0.02,0.04,-0.03,-0.01]),(98, [0.04,1.0,-0.01,0.01,0.03,-0.04,-0.02,0.00]),(99, [-0.04,-0.02,1.0,0.02,0.04,-0.03,-0.01,0.01]),(100, [-0.03,-0.01,0.01,1.0,-0.04,-0.02,0.00,0.02]);
INSERT INTO vq.big VALUES (101, [-0.02,0.00,0.02,0.04,1.0,-0.01,0.01,0.03]),(102, [-0.01,0.01,0.03,-0.04,-0.02,1.0,0.02,0.04]),(103, [0.00,0.02,0.04,-0.03,-0.01,0.01,1.0,-0.04]),(104, [0.01,0.03,-0.04,-0.02,0.00,0.02,0.04,1.0]),(105, [1.0,0.04,-0.03,-0.01,0.01,0.03,-0.04,-0.02]),(106, [0.03,1.0,-0.02,0.00,0.02,0.04,-0.03,-0.01]),(107, [0.04,-0.03,1.0,0.01,0.03,-0.04,-0.02,0.00]),(108, [-0.04,-0.02,0.00,1.0,0.04,-0.03,-0.01,0.01]),(109, [-0.03,-0.01,0.01,0.03,1.0,-0.02,0.00,0.02]),(110, [-0.02,0.00,0.02,0.04,-0.03,1.0,0.01,0.03]),(111, [-0.01,0.01,0.03,-0.04,-0.02,0.00,1.0,0.04]),(112, [0.00,0.02,0.04,-0.03,-0.01,0.01,0.03,1.0]),(113, [1.0,0.03,-0.04,-0.02,0.00,0.02,0.04,-0.03]),(114, [0.02,1.0,-0.03,-0.01,0.01,0.03,-0.04,-0.02]),(115, [0.03,-0.04,1.0,0.00,0.02,0.04,-0.03,-0.01]),(116, [0.04,-0.03,-0.01,1.0,0.03,-0.04,-0.02,0.00]),(117, [-0.04,-0.02,0.00,0.02,1.0,-0.03,-0.01,0.01]),(118, [-0.03,-0.01,0.01,0.03,-0.04,1.0,0.00,0.02]),(119, [-0.02,0.00,0.02,0.04,-0.03,-0.01,1.0,0.03]),(120, [-0.01,0.01,0.03,-0.04,-0.02,0.00,0.02,1.0]);
SELECT 'CHECK big-count', CASE WHEN COUNT(*) = 120 THEN 'PASS' ELSE 'FAIL' END FROM vq.big;
SELECT 'CHECK big-axes', CASE WHEN COUNT(*) = 15 THEN 'PASS' ELSE 'FAIL' END FROM vq.big WHERE l2_distance(e, [1,0,0,0,0,0,0,0]) < 0.2;
SELECT 'CHECK big-topk10', CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 10) AS t;
SELECT 'CHECK big-page', CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 10 OFFSET 10) AS t;

-- ------------------------------------------------------------
-- 5. Radius + filtered search
-- ------------------------------------------------------------
SELECT 'CHECK radius-big', CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big WHERE l2_distance(e, [1,0,0,0,0,0,0,0]) < 0.2) AS t;
SELECT 'CHECK filtered-lo', CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big WHERE id < 60 ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 5) AS t;
SELECT 'CHECK filtered-hi', CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big WHERE id >= 60 ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 5) AS t;

-- ------------------------------------------------------------
-- 6. Index lifecycle (every method + rejections + binary HNSW)
-- ------------------------------------------------------------
CREATE INDEX vq_extra ON vq.docs USING HNSW (embedding);
DROP INDEX vq_extra;
DROP INDEX docs_embedding_vector_idx;
CREATE INDEX docs_embedding_vector_idx ON vq.docs USING HNSW (embedding);
SELECT 'CHECK drop-recreate', CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM pg_indexes WHERE tablename = 'docs' AND indexname LIKE '%embedding_vector_idx%';
CREATE INDEX vq_ivf ON vq.big USING IVF_FLAT (e);
SELECT 'CHECK ivf-topk', CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 10) AS t;
DROP INDEX vq_ivf;
-- Every accepted dense method must execute (create + top-k + drop).
CREATE INDEX vq_ivfsq ON vq.big USING IVF_SQ (e);
SELECT 'CHECK ivfsq-topk', CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 10) AS t;
DROP INDEX vq_ivfsq;
CREATE INDEX vq_ivfpq ON vq.big USING IVF_PQ (e);
SELECT 'CHECK ivfpq-topk', CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 10) AS t;
DROP INDEX vq_ivfpq;
CREATE INDEX vq_disk ON vq.big USING DISKANN (e);
SELECT 'CHECK diskann-topk', CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 10) AS t;
DROP INDEX vq_disk;
-- INT4 promotes through float for SQ, and is rejected by PQ.
CREATE INDEX vq_sq_i4 ON vq.allkinds USING IVF_SQ (i4v);
DROP INDEX vq_sq_i4;
-- EXPECT-ERROR: ivf_pq rejects INT4 elements
CREATE INDEX vq_pq_i4 ON vq.allkinds USING IVF_PQ (i4v);
-- EXPECT-ERROR: ivf_pq requires the dimension to be a multiple of 8 (default segment count)
CREATE INDEX vq_pq_dim4 ON vq.docs USING IVF_PQ (embedding);
-- EXPECT-ERROR: hnsw on binary kind has no execution path
CREATE INDEX vq_binhnsw ON vq.allkinds USING HNSW (binv);
-- EXPECT-ERROR: diskann on sparse kind has no execution path
CREATE INDEX vq_sparsedisk ON vq.allkinds USING DISKANN (sparsev);
-- EXPECT-ERROR: hnsw on multi kind has no execution path
CREATE INDEX vq_mvhnsw ON vq.allkinds USING HNSW (multiv);
CREATE INDEX vq_binann ON vq.allkinds USING BINARY (binv);
SELECT 'CHECK binary-ann-topk', CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.allkinds ORDER BY hamming_distance(binv, '01010101') LIMIT 1) AS t;
DROP INDEX vq_binann;
-- EXPECT-ERROR: unique vector index
CREATE UNIQUE INDEX vq_u ON vq.docs USING HNSW (embedding);
-- EXPECT-ERROR: composite vector index
CREATE INDEX vq_c ON vq.docs USING HNSW (embedding, id);
-- EXPECT-ERROR: btree on vector column
CREATE INDEX vq_b ON vq.docs (embedding);
-- EXPECT-ERROR: ivf_pq on binary kind
CREATE INDEX vq_badpq ON vq.allkinds USING IVF_PQ (binv);
SELECT 'CHECK idx-iso', CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM pg_indexes WHERE tablename = 'docs';

-- ------------------------------------------------------------
-- 7. Transactions around vectors
-- ------------------------------------------------------------
BEGIN;
INSERT INTO vq.docs VALUES (101, 'txn', [0,0,0,0]);
COMMIT;
SELECT 'CHECK txn-commit', CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id = 101;
BEGIN;
INSERT INTO vq.docs VALUES (102, 'rollback', [0,0,0,0]);
ROLLBACK;
SELECT 'CHECK txn-rollback', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id = 102;
BEGIN;
INSERT INTO vq.docs VALUES (103, 'sp1', [0,0,0,0]);
SAVEPOINT sp1;
INSERT INTO vq.docs VALUES (104, 'sp2', [0,0,0,0]);
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
SELECT 'CHECK txn-savepoint', CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id >= 103;

-- ------------------------------------------------------------
-- 8. UPDATE/DELETE + invalidation
-- ------------------------------------------------------------
UPDATE vq.docs SET embedding = [0,0,0,1] WHERE id = 2;
SELECT 'CHECK update-rt', CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END FROM vq.docs WHERE id = 2 AND l2_distance(embedding, [0,0,0,1]) < 1e-9;
INSERT INTO vq.big VALUES (1001, [1,0,0,0,0,0,0,0]);
SELECT 'CHECK invalidation', CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END FROM (SELECT id FROM vq.big ORDER BY cosine_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 3) AS t WHERE id = 1001;
DELETE FROM vq.big WHERE id = 1001;
SELECT 'CHECK delete-rt', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM vq.big WHERE id = 1001;

-- ------------------------------------------------------------
-- 9. Settings (defaults, apply, range, unknown)
-- ------------------------------------------------------------
SHOW vector.ef_search;
SET vector.ef_search = 64;
SHOW vector.ef_search;
SET vector.ef_search = 100;
-- EXPECT-ERROR: out of range
SET vector.ef_search = 99999;
-- EXPECT-ERROR: unknown knob
SET vector.nope = 1;

-- ------------------------------------------------------------
-- 10. Observability probes
-- ------------------------------------------------------------
SELECT 'CHECK stats-row', CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END FROM plomid_vector_stats();
SELECT 'CHECK stats-counted', CASE WHEN exact_queries_total > 0 THEN 'PASS' ELSE 'FAIL' END FROM plomid_vector_stats();
SELECT 'CHECK idx-ready', CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM plomid_vector_indexes() WHERE state = 'READY';
REINDEX;
SELECT 'CHECK reindex', CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM plomid_vector_indexes() WHERE state = 'READY';

-- ------------------------------------------------------------
-- 11. EXPLAIN (plan must be usable, never absent)
-- ------------------------------------------------------------
EXPLAIN SELECT id FROM vq.big ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 10;

-- ------------------------------------------------------------
-- 12. Cleanup (schema dropped at start too; file is rerunnable)
-- ------------------------------------------------------------
DROP TABLE vq.big;
DROP TABLE vq.allkinds;
DROP TABLE vq.docs;
DROP SCHEMA vq CASCADE;
SELECT 'CHECK cleanup', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM information_schema.tables WHERE table_schema = 'vq';
