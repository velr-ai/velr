#![allow(dead_code)]

pub const CURRENT_SCHEMA_VERSION: i32 = 9;
pub const MIN_SUPPORTED_SCHEMA_VERSION: i32 = 3;

pub const PG_SCHEMA: &str = r#"
-- Nodes
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;
PRAGMA wal_autocheckpoint = 1000;
PRAGMA page_size = 4096;
PRAGMA temp_store = MEMORY;
PRAGMA case_sensitive_like = ON;

CREATE TABLE IF NOT EXISTS key_dict(
  id   INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE
);

-- Label type (optional index)
CREATE TABLE IF NOT EXISTS label_type (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL UNIQUE
);

-- Immutable label groups. Nodes point at one group instead of storing one row per label.
CREATE TABLE IF NOT EXISTS label_group (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    signature   TEXT NOT NULL UNIQUE,
    labels_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS label_group_member (
    group_id INTEGER NOT NULL,
    type_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (group_id, type_id),
    UNIQUE (group_id, ordinal),
    FOREIGN KEY (group_id) REFERENCES label_group(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (type_id) REFERENCES label_type(id) ON DELETE RESTRICT ON UPDATE RESTRICT
) WITHOUT ROWID;

INSERT OR IGNORE INTO label_group(id, signature, labels_json)
VALUES (1, '', '[]');

CREATE TABLE IF NOT EXISTS node (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label_group_id INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (label_group_id) REFERENCES label_group(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- Node Properties (value is weakly typed; keep as-is)
CREATE TABLE IF NOT EXISTS node_property (
    node_id INTEGER NOT NULL,
    key_id  INTEGER NOT NULL,
    value,
    PRIMARY KEY (node_id, key_id),
    FOREIGN KEY (node_id) REFERENCES node(id) ON DELETE CASCADE,
    FOREIGN KEY (key_id)  REFERENCES key_dict(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- Edge Types
CREATE TABLE IF NOT EXISTS edge_type (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL UNIQUE
);

-- Edges
CREATE TABLE IF NOT EXISTS edge (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    from_node INTEGER NOT NULL,
    to_node   INTEGER NOT NULL,
    type_id   INTEGER NOT NULL,
    FOREIGN KEY (from_node) REFERENCES node(id) ON DELETE CASCADE,
    FOREIGN KEY (to_node)   REFERENCES node(id) ON DELETE CASCADE,
    FOREIGN KEY (type_id)   REFERENCES edge_type(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- Edge Properties
CREATE TABLE IF NOT EXISTS edge_property (
    edge_id INTEGER NOT NULL,
    key_id  INTEGER NOT NULL,
    value,
    PRIMARY KEY (edge_id, key_id),
    FOREIGN KEY (edge_id) REFERENCES edge(id) ON DELETE CASCADE,
    FOREIGN KEY (key_id)  REFERENCES key_dict(id) ON DELETE RESTRICT ON UPDATE RESTRICT
) WITHOUT ROWID;

-- === Indexes ===

-- node_property lookup by key_id
CREATE INDEX IF NOT EXISTS idx_np_keyid_node
  ON node_property(key_id, node_id);

CREATE INDEX IF NOT EXISTS idx_node_label_group
  ON node(label_group_id, id);

CREATE INDEX IF NOT EXISTS idx_lgm_type_group
  ON label_group_member(type_id, group_id);

-- edge traversal helpers
CREATE INDEX IF NOT EXISTS idx_edge_from_type_to_id ON edge(from_node, type_id, to_node, id);
CREATE INDEX IF NOT EXISTS idx_edge_to_type_from_id ON edge(to_node, type_id, from_node, id);

-- covering read for traversal by (type_id, from_node)
CREATE INDEX IF NOT EXISTS idx_edge_type_from_to_id
  ON edge(type_id, from_node, to_node, id);

CREATE INDEX IF NOT EXISTS idx_edge_type_to_from_id
  ON edge(type_id, to_node, from_node, id);

-- node_property numeric equality/range helper
CREATE INDEX IF NOT EXISTS idx_np_key_num
ON node_property (
  key_id,
  value,
  node_id
)
WHERE typeof(value) IN ('integer', 'real');

-- node_property text prefix helper
CREATE INDEX IF NOT EXISTS idx_np_key_txt12
ON node_property (
  key_id,
  substr(value, 1, 12),
  node_id
)
WHERE typeof(value) = 'text';

CREATE TABLE IF NOT EXISTS velr_index_meta(
  kind TEXT NOT NULL,
  key_id INTEGER NOT NULL,
  index_name TEXT NOT NULL,
  PRIMARY KEY(kind, key_id)
);

CREATE TRIGGER IF NOT EXISTS key_dict_no_delete
BEFORE DELETE ON key_dict
BEGIN
  SELECT RAISE(ABORT, 'key_dict is append-only');
END;

CREATE TRIGGER IF NOT EXISTS key_dict_no_update
BEFORE UPDATE ON key_dict
BEGIN
  SELECT RAISE(ABORT, 'key_dict is immutable');
END;

CREATE TRIGGER IF NOT EXISTS label_type_no_delete
BEFORE DELETE ON label_type
BEGIN
  SELECT RAISE(ABORT, 'label_type is append-only');
END;

CREATE TRIGGER IF NOT EXISTS label_type_no_update
BEFORE UPDATE ON label_type
BEGIN
  SELECT RAISE(ABORT, 'label_type is immutable');
END;

CREATE TRIGGER IF NOT EXISTS label_group_no_delete
BEFORE DELETE ON label_group
BEGIN
  SELECT RAISE(ABORT, 'label_group is append-only');
END;

CREATE TRIGGER IF NOT EXISTS label_group_no_update
BEFORE UPDATE ON label_group
BEGIN
  SELECT RAISE(ABORT, 'label_group is immutable');
END;

CREATE TRIGGER IF NOT EXISTS label_group_member_no_delete
BEFORE DELETE ON label_group_member
BEGIN
  SELECT RAISE(ABORT, 'label_group_member is append-only');
END;

CREATE TRIGGER IF NOT EXISTS label_group_member_no_update
BEFORE UPDATE ON label_group_member
BEGIN
  SELECT RAISE(ABORT, 'label_group_member is immutable');
END;

CREATE TRIGGER IF NOT EXISTS edge_type_no_delete
BEFORE DELETE ON edge_type
BEGIN
  SELECT RAISE(ABORT, 'edge_type is append-only');
END;

CREATE TRIGGER IF NOT EXISTS edge_type_no_update
BEFORE UPDATE ON edge_type
BEGIN
  SELECT RAISE(ABORT, 'edge_type is immutable');
END;

CREATE TABLE IF NOT EXISTS graph_shape_node_scope (
  label_id INTEGER NOT NULL,
  owner_count INTEGER NOT NULL CHECK(owner_count >= 0),
  PRIMARY KEY(label_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS graph_shape_node_property (
  label_id INTEGER NOT NULL,
  key_id INTEGER NOT NULL,
  storage_class TEXT NOT NULL,
  tag TEXT NOT NULL,
  present_count INTEGER NOT NULL CHECK(present_count >= 0),
  PRIMARY KEY(label_id, key_id, storage_class, tag)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS graph_shape_edge_scope (
  type_id INTEGER NOT NULL,
  source_label_id INTEGER NOT NULL,
  target_label_id INTEGER NOT NULL,
  owner_count INTEGER NOT NULL CHECK(owner_count >= 0),
  PRIMARY KEY(type_id, source_label_id, target_label_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS graph_shape_edge_property (
  type_id INTEGER NOT NULL,
  source_label_id INTEGER NOT NULL,
  target_label_id INTEGER NOT NULL,
  key_id INTEGER NOT NULL,
  storage_class TEXT NOT NULL,
  tag TEXT NOT NULL,
  present_count INTEGER NOT NULL CHECK(present_count >= 0),
  PRIMARY KEY(type_id, source_label_id, target_label_id, key_id, storage_class, tag)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS graph_shape_maintenance_control (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  triggers_enabled INTEGER NOT NULL CHECK(triggers_enabled IN (0, 1))
);

INSERT OR IGNORE INTO graph_shape_maintenance_control(id, triggers_enabled)
VALUES (1, 1);

CREATE TRIGGER IF NOT EXISTS graph_shape_node_insert
AFTER INSERT ON node
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  INSERT INTO graph_shape_node_scope(label_id, owner_count)
  SELECT COALESCE(lgm.type_id, 0), 1
  FROM (SELECT NEW.label_group_id AS group_id) AS scope
  LEFT JOIN label_group_member AS lgm ON lgm.group_id = scope.group_id
  WHERE 1
  ON CONFLICT(label_id) DO UPDATE
    SET owner_count = owner_count + excluded.owner_count;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_node_delete
BEFORE DELETE ON node
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  UPDATE graph_shape_node_scope
  SET owner_count = owner_count - 1
  WHERE label_id IN (
    SELECT COALESCE(lgm.type_id, 0)
    FROM (SELECT OLD.label_group_id AS group_id) AS scope
    LEFT JOIN label_group_member AS lgm ON lgm.group_id = scope.group_id
  );

  DELETE FROM graph_shape_node_scope WHERE owner_count <= 0;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_node_label_update
AFTER UPDATE OF label_group_id ON node
WHEN OLD.label_group_id <> NEW.label_group_id
  AND (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  UPDATE graph_shape_node_scope
  SET owner_count = owner_count - 1
  WHERE label_id IN (
    SELECT COALESCE(lgm.type_id, 0)
    FROM (SELECT OLD.label_group_id AS group_id) AS scope
    LEFT JOIN label_group_member AS lgm ON lgm.group_id = scope.group_id
  );

  DELETE FROM graph_shape_node_scope WHERE owner_count <= 0;

  INSERT INTO graph_shape_node_scope(label_id, owner_count)
  SELECT COALESCE(lgm.type_id, 0), 1
  FROM (SELECT NEW.label_group_id AS group_id) AS scope
  LEFT JOIN label_group_member AS lgm ON lgm.group_id = scope.group_id
  WHERE 1
  ON CONFLICT(label_id) DO UPDATE
    SET owner_count = owner_count + excluded.owner_count;

  UPDATE graph_shape_node_property
  SET present_count = present_count - 1
  WHERE EXISTS (
    SELECT 1
    FROM node_property AS np
    LEFT JOIN label_group_member AS lgm ON lgm.group_id = OLD.label_group_id
    WHERE np.node_id = OLD.id
      AND graph_shape_node_property.label_id = COALESCE(lgm.type_id, 0)
      AND graph_shape_node_property.key_id = np.key_id
      AND graph_shape_node_property.storage_class = typeof(np.value)
      AND graph_shape_node_property.tag = CASE
        WHEN typeof(np.value) = 'blob' THEN hex(substr(np.value, 1, 1))
        ELSE ''
      END
  );

  DELETE FROM graph_shape_node_property WHERE present_count <= 0;

  INSERT INTO graph_shape_node_property(label_id, key_id, storage_class, tag, present_count)
  SELECT
    COALESCE(lgm.type_id, 0),
    np.key_id,
    typeof(np.value),
    CASE WHEN typeof(np.value) = 'blob' THEN hex(substr(np.value, 1, 1)) ELSE '' END,
    1
  FROM node_property AS np
  LEFT JOIN label_group_member AS lgm ON lgm.group_id = NEW.label_group_id
  WHERE np.node_id = NEW.id
  ON CONFLICT(label_id, key_id, storage_class, tag) DO UPDATE
    SET present_count = present_count + excluded.present_count;

  UPDATE graph_shape_edge_scope
  SET owner_count = owner_count - 1
  WHERE EXISTS (
    SELECT 1
    FROM edge AS e
    JOIN node AS src ON src.id = e.from_node
    JOIN node AS dst ON dst.id = e.to_node
    LEFT JOIN label_group_member AS src_lgm
      ON src_lgm.group_id = CASE WHEN e.from_node = OLD.id THEN OLD.label_group_id ELSE src.label_group_id END
    LEFT JOIN label_group_member AS dst_lgm
      ON dst_lgm.group_id = CASE WHEN e.to_node = OLD.id THEN OLD.label_group_id ELSE dst.label_group_id END
    WHERE (e.from_node = OLD.id OR e.to_node = OLD.id)
      AND graph_shape_edge_scope.type_id = e.type_id
      AND graph_shape_edge_scope.source_label_id = COALESCE(src_lgm.type_id, 0)
      AND graph_shape_edge_scope.target_label_id = COALESCE(dst_lgm.type_id, 0)
  );

  DELETE FROM graph_shape_edge_scope WHERE owner_count <= 0;

  INSERT INTO graph_shape_edge_scope(type_id, source_label_id, target_label_id, owner_count)
  SELECT
    e.type_id,
    COALESCE(src_lgm.type_id, 0),
    COALESCE(dst_lgm.type_id, 0),
    1
  FROM edge AS e
  JOIN node AS src ON src.id = e.from_node
  JOIN node AS dst ON dst.id = e.to_node
  LEFT JOIN label_group_member AS src_lgm
    ON src_lgm.group_id = CASE WHEN e.from_node = NEW.id THEN NEW.label_group_id ELSE src.label_group_id END
  LEFT JOIN label_group_member AS dst_lgm
    ON dst_lgm.group_id = CASE WHEN e.to_node = NEW.id THEN NEW.label_group_id ELSE dst.label_group_id END
  WHERE e.from_node = NEW.id OR e.to_node = NEW.id
  ON CONFLICT(type_id, source_label_id, target_label_id) DO UPDATE
    SET owner_count = owner_count + excluded.owner_count;

  UPDATE graph_shape_edge_property
  SET present_count = present_count - 1
  WHERE EXISTS (
    SELECT 1
    FROM edge AS e
    JOIN edge_property AS ep ON ep.edge_id = e.id
    JOIN node AS src ON src.id = e.from_node
    JOIN node AS dst ON dst.id = e.to_node
    LEFT JOIN label_group_member AS src_lgm
      ON src_lgm.group_id = CASE WHEN e.from_node = OLD.id THEN OLD.label_group_id ELSE src.label_group_id END
    LEFT JOIN label_group_member AS dst_lgm
      ON dst_lgm.group_id = CASE WHEN e.to_node = OLD.id THEN OLD.label_group_id ELSE dst.label_group_id END
    WHERE (e.from_node = OLD.id OR e.to_node = OLD.id)
      AND graph_shape_edge_property.type_id = e.type_id
      AND graph_shape_edge_property.source_label_id = COALESCE(src_lgm.type_id, 0)
      AND graph_shape_edge_property.target_label_id = COALESCE(dst_lgm.type_id, 0)
      AND graph_shape_edge_property.key_id = ep.key_id
      AND graph_shape_edge_property.storage_class = typeof(ep.value)
      AND graph_shape_edge_property.tag = CASE
        WHEN typeof(ep.value) = 'blob' THEN hex(substr(ep.value, 1, 1))
        ELSE ''
      END
  );

  DELETE FROM graph_shape_edge_property WHERE present_count <= 0;

  INSERT INTO graph_shape_edge_property(
    type_id, source_label_id, target_label_id, key_id, storage_class, tag, present_count
  )
  SELECT
    e.type_id,
    COALESCE(src_lgm.type_id, 0),
    COALESCE(dst_lgm.type_id, 0),
    ep.key_id,
    typeof(ep.value),
    CASE WHEN typeof(ep.value) = 'blob' THEN hex(substr(ep.value, 1, 1)) ELSE '' END,
    1
  FROM edge AS e
  JOIN edge_property AS ep ON ep.edge_id = e.id
  JOIN node AS src ON src.id = e.from_node
  JOIN node AS dst ON dst.id = e.to_node
  LEFT JOIN label_group_member AS src_lgm
    ON src_lgm.group_id = CASE WHEN e.from_node = NEW.id THEN NEW.label_group_id ELSE src.label_group_id END
  LEFT JOIN label_group_member AS dst_lgm
    ON dst_lgm.group_id = CASE WHEN e.to_node = NEW.id THEN NEW.label_group_id ELSE dst.label_group_id END
  WHERE e.from_node = NEW.id OR e.to_node = NEW.id
  ON CONFLICT(type_id, source_label_id, target_label_id, key_id, storage_class, tag) DO UPDATE
    SET present_count = present_count + excluded.present_count;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_node_property_insert
AFTER INSERT ON node_property
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  INSERT INTO graph_shape_node_property(label_id, key_id, storage_class, tag, present_count)
  SELECT
    COALESCE(lgm.type_id, 0),
    NEW.key_id,
    typeof(NEW.value),
    CASE WHEN typeof(NEW.value) = 'blob' THEN hex(substr(NEW.value, 1, 1)) ELSE '' END,
    1
  FROM node AS n
  LEFT JOIN label_group_member AS lgm ON lgm.group_id = n.label_group_id
  WHERE n.id = NEW.node_id
  ON CONFLICT(label_id, key_id, storage_class, tag) DO UPDATE
    SET present_count = present_count + excluded.present_count;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_node_property_delete
BEFORE DELETE ON node_property
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  UPDATE graph_shape_node_property
  SET present_count = present_count - 1
  WHERE key_id = OLD.key_id
    AND storage_class = typeof(OLD.value)
    AND tag = CASE WHEN typeof(OLD.value) = 'blob' THEN hex(substr(OLD.value, 1, 1)) ELSE '' END
    AND label_id IN (
      SELECT COALESCE(lgm.type_id, 0)
      FROM node AS n
      LEFT JOIN label_group_member AS lgm ON lgm.group_id = n.label_group_id
      WHERE n.id = OLD.node_id
    );

  DELETE FROM graph_shape_node_property WHERE present_count <= 0;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_node_property_update
AFTER UPDATE OF node_id, key_id, value ON node_property
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  UPDATE graph_shape_node_property
  SET present_count = present_count - 1
  WHERE key_id = OLD.key_id
    AND storage_class = typeof(OLD.value)
    AND tag = CASE WHEN typeof(OLD.value) = 'blob' THEN hex(substr(OLD.value, 1, 1)) ELSE '' END
    AND label_id IN (
      SELECT COALESCE(lgm.type_id, 0)
      FROM node AS n
      LEFT JOIN label_group_member AS lgm ON lgm.group_id = n.label_group_id
      WHERE n.id = OLD.node_id
    );

  DELETE FROM graph_shape_node_property WHERE present_count <= 0;

  INSERT INTO graph_shape_node_property(label_id, key_id, storage_class, tag, present_count)
  SELECT
    COALESCE(lgm.type_id, 0),
    NEW.key_id,
    typeof(NEW.value),
    CASE WHEN typeof(NEW.value) = 'blob' THEN hex(substr(NEW.value, 1, 1)) ELSE '' END,
    1
  FROM node AS n
  LEFT JOIN label_group_member AS lgm ON lgm.group_id = n.label_group_id
  WHERE n.id = NEW.node_id
  ON CONFLICT(label_id, key_id, storage_class, tag) DO UPDATE
    SET present_count = present_count + excluded.present_count;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_edge_insert
AFTER INSERT ON edge
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  INSERT INTO graph_shape_edge_scope(type_id, source_label_id, target_label_id, owner_count)
  SELECT
    NEW.type_id,
    COALESCE(src_lgm.type_id, 0),
    COALESCE(dst_lgm.type_id, 0),
    1
  FROM node AS src
  JOIN node AS dst ON dst.id = NEW.to_node
  LEFT JOIN label_group_member AS src_lgm ON src_lgm.group_id = src.label_group_id
  LEFT JOIN label_group_member AS dst_lgm ON dst_lgm.group_id = dst.label_group_id
  WHERE src.id = NEW.from_node
  ON CONFLICT(type_id, source_label_id, target_label_id) DO UPDATE
    SET owner_count = owner_count + excluded.owner_count;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_edge_delete
BEFORE DELETE ON edge
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  UPDATE graph_shape_edge_scope
  SET owner_count = owner_count - 1
  WHERE type_id = OLD.type_id
    AND EXISTS (
      SELECT 1
      FROM node AS src
      JOIN node AS dst ON dst.id = OLD.to_node
      LEFT JOIN label_group_member AS src_lgm ON src_lgm.group_id = src.label_group_id
      LEFT JOIN label_group_member AS dst_lgm ON dst_lgm.group_id = dst.label_group_id
      WHERE src.id = OLD.from_node
        AND graph_shape_edge_scope.source_label_id = COALESCE(src_lgm.type_id, 0)
        AND graph_shape_edge_scope.target_label_id = COALESCE(dst_lgm.type_id, 0)
    );

  DELETE FROM graph_shape_edge_scope WHERE owner_count <= 0;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_edge_property_insert
AFTER INSERT ON edge_property
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  INSERT INTO graph_shape_edge_property(
    type_id, source_label_id, target_label_id, key_id, storage_class, tag, present_count
  )
  SELECT
    e.type_id,
    COALESCE(src_lgm.type_id, 0),
    COALESCE(dst_lgm.type_id, 0),
    NEW.key_id,
    typeof(NEW.value),
    CASE WHEN typeof(NEW.value) = 'blob' THEN hex(substr(NEW.value, 1, 1)) ELSE '' END,
    1
  FROM edge AS e
  JOIN node AS src ON src.id = e.from_node
  JOIN node AS dst ON dst.id = e.to_node
  LEFT JOIN label_group_member AS src_lgm ON src_lgm.group_id = src.label_group_id
  LEFT JOIN label_group_member AS dst_lgm ON dst_lgm.group_id = dst.label_group_id
  WHERE e.id = NEW.edge_id
  ON CONFLICT(type_id, source_label_id, target_label_id, key_id, storage_class, tag) DO UPDATE
    SET present_count = present_count + excluded.present_count;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_edge_property_delete
BEFORE DELETE ON edge_property
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  UPDATE graph_shape_edge_property
  SET present_count = present_count - 1
  WHERE key_id = OLD.key_id
    AND storage_class = typeof(OLD.value)
    AND tag = CASE WHEN typeof(OLD.value) = 'blob' THEN hex(substr(OLD.value, 1, 1)) ELSE '' END
    AND EXISTS (
      SELECT 1
      FROM edge AS e
      JOIN node AS src ON src.id = e.from_node
      JOIN node AS dst ON dst.id = e.to_node
      LEFT JOIN label_group_member AS src_lgm ON src_lgm.group_id = src.label_group_id
      LEFT JOIN label_group_member AS dst_lgm ON dst_lgm.group_id = dst.label_group_id
      WHERE e.id = OLD.edge_id
        AND graph_shape_edge_property.type_id = e.type_id
        AND graph_shape_edge_property.source_label_id = COALESCE(src_lgm.type_id, 0)
        AND graph_shape_edge_property.target_label_id = COALESCE(dst_lgm.type_id, 0)
    );

  DELETE FROM graph_shape_edge_property WHERE present_count <= 0;
END;

CREATE TRIGGER IF NOT EXISTS graph_shape_edge_property_update
AFTER UPDATE OF edge_id, key_id, value ON edge_property
WHEN (SELECT triggers_enabled FROM graph_shape_maintenance_control WHERE id = 1) = 1
BEGIN
  UPDATE graph_shape_edge_property
  SET present_count = present_count - 1
  WHERE key_id = OLD.key_id
    AND storage_class = typeof(OLD.value)
    AND tag = CASE WHEN typeof(OLD.value) = 'blob' THEN hex(substr(OLD.value, 1, 1)) ELSE '' END
    AND EXISTS (
      SELECT 1
      FROM edge AS e
      JOIN node AS src ON src.id = e.from_node
      JOIN node AS dst ON dst.id = e.to_node
      LEFT JOIN label_group_member AS src_lgm ON src_lgm.group_id = src.label_group_id
      LEFT JOIN label_group_member AS dst_lgm ON dst_lgm.group_id = dst.label_group_id
      WHERE e.id = OLD.edge_id
        AND graph_shape_edge_property.type_id = e.type_id
        AND graph_shape_edge_property.source_label_id = COALESCE(src_lgm.type_id, 0)
        AND graph_shape_edge_property.target_label_id = COALESCE(dst_lgm.type_id, 0)
    );

  DELETE FROM graph_shape_edge_property WHERE present_count <= 0;

  INSERT INTO graph_shape_edge_property(
    type_id, source_label_id, target_label_id, key_id, storage_class, tag, present_count
  )
  SELECT
    e.type_id,
    COALESCE(src_lgm.type_id, 0),
    COALESCE(dst_lgm.type_id, 0),
    NEW.key_id,
    typeof(NEW.value),
    CASE WHEN typeof(NEW.value) = 'blob' THEN hex(substr(NEW.value, 1, 1)) ELSE '' END,
    1
  FROM edge AS e
  JOIN node AS src ON src.id = e.from_node
  JOIN node AS dst ON dst.id = e.to_node
  LEFT JOIN label_group_member AS src_lgm ON src_lgm.group_id = src.label_group_id
  LEFT JOIN label_group_member AS dst_lgm ON dst_lgm.group_id = dst.label_group_id
  WHERE e.id = NEW.edge_id
  ON CONFLICT(type_id, source_label_id, target_label_id, key_id, storage_class, tag) DO UPDATE
    SET present_count = present_count + excluded.present_count;
END;

CREATE TABLE IF NOT EXISTS velr_fulltext_index_catalog (
  index_name TEXT NOT NULL PRIMARY KEY,
  entity_kind TEXT NOT NULL CHECK(entity_kind IN ('NODE', 'RELATIONSHIP')),
  labels_json TEXT NOT NULL,
  properties_json TEXT NOT NULL,
  analyzer TEXT NOT NULL,
  options_json TEXT NOT NULL DEFAULT '{}',
  state TEXT NOT NULL CHECK(state IN ('ONLINE', 'POPULATING', 'FAILED')),
  generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
  failure_message TEXT
);

CREATE TABLE IF NOT EXISTS velr_fulltext_dirty (
  entity_kind TEXT NOT NULL CHECK(entity_kind IN ('NODE', 'RELATIONSHIP')),
  entity_id INTEGER NOT NULL,
  PRIMARY KEY (entity_kind, entity_id)
) WITHOUT ROWID;

-- End fulltext schema.

CREATE TABLE IF NOT EXISTS velr_vector_index_catalog (
  index_name TEXT NOT NULL PRIMARY KEY,
  entity_kind TEXT NOT NULL CHECK(entity_kind IN ('NODE', 'RELATIONSHIP')),
  labels_json TEXT NOT NULL,
  properties_json TEXT NOT NULL,
  embedder TEXT,
  embedder_fingerprint TEXT,
  dimensions INTEGER NOT NULL CHECK(dimensions > 0),
  metric TEXT NOT NULL CHECK(metric IN ('cosine', 'l2', 'dot')),
  scalar_kind TEXT NOT NULL DEFAULT 'f32',
  sidecar_engine TEXT NOT NULL DEFAULT 'usearch',
  sidecar_options_json TEXT NOT NULL DEFAULT '{}',
  cache_policy TEXT NOT NULL DEFAULT 'required' CHECK(cache_policy IN ('none', 'best_effort', 'required')),
  cache_engine TEXT NOT NULL DEFAULT 'row_blob_v1',
  cache_options_json TEXT NOT NULL DEFAULT '{}',
  cache_schema_version INTEGER NOT NULL DEFAULT 1 CHECK(cache_schema_version > 0),
  state TEXT NOT NULL CHECK(state IN ('ONLINE', 'POPULATING', 'FAILED', 'NEEDS_EMBEDDER')),
  generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
  sidecar_token TEXT,
  failure_message TEXT
);

CREATE TABLE IF NOT EXISTS velr_vector_dirty (
  index_name TEXT NOT NULL,
  entity_kind TEXT NOT NULL CHECK(entity_kind IN ('NODE', 'RELATIONSHIP')),
  entity_id INTEGER NOT NULL,
  PRIMARY KEY (index_name, entity_kind, entity_id),
  FOREIGN KEY (index_name) REFERENCES velr_vector_index_catalog(index_name) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS velr_vector_embedding_row_cache (
  index_name TEXT NOT NULL,
  entity_kind TEXT NOT NULL CHECK(entity_kind IN ('NODE', 'RELATIONSHIP')),
  entity_id INTEGER NOT NULL,
  source_hash TEXT NOT NULL,
  vector_blob BLOB NOT NULL,
  encoding TEXT NOT NULL,
  compression TEXT NOT NULL DEFAULT 'none',
  PRIMARY KEY (index_name, entity_kind, entity_id),
  FOREIGN KEY (index_name) REFERENCES velr_vector_index_catalog(index_name) ON DELETE CASCADE
) WITHOUT ROWID;

-- End vector schema.
PRAGMA user_version = 9;

"#;

pub const GRAPH_SHAPE_TRIGGER_DROP_SQL: &str = r#"
DROP TRIGGER IF EXISTS graph_shape_edge_property_update;
DROP TRIGGER IF EXISTS graph_shape_edge_property_delete;
DROP TRIGGER IF EXISTS graph_shape_edge_property_insert;
DROP TRIGGER IF EXISTS graph_shape_edge_delete;
DROP TRIGGER IF EXISTS graph_shape_edge_insert;
DROP TRIGGER IF EXISTS graph_shape_node_property_update;
DROP TRIGGER IF EXISTS graph_shape_node_property_delete;
DROP TRIGGER IF EXISTS graph_shape_node_property_insert;
DROP TRIGGER IF EXISTS graph_shape_node_label_update;
DROP TRIGGER IF EXISTS graph_shape_node_delete;
DROP TRIGGER IF EXISTS graph_shape_node_insert;
DROP TABLE IF EXISTS graph_shape_maintenance_control;
"#;

pub fn graph_shape_schema_sql() -> &'static str {
    const START: &str = "CREATE TABLE IF NOT EXISTS graph_shape_node_scope";
    const END: &str = "CREATE TABLE IF NOT EXISTS velr_fulltext_index_catalog";

    let start = PG_SCHEMA
        .find(START)
        .expect("PG_SCHEMA must contain graph-shape schema start marker");
    let end = PG_SCHEMA
        .find(END)
        .expect("PG_SCHEMA must contain fulltext schema marker after graph-shape schema");
    &PG_SCHEMA[start..end]
}

pub fn fulltext_schema_sql() -> &'static str {
    const START: &str = "CREATE TABLE IF NOT EXISTS velr_fulltext_index_catalog";
    const END: &str = "-- End fulltext schema.";

    let start = PG_SCHEMA
        .find(START)
        .expect("PG_SCHEMA must contain fulltext schema start marker");
    let end = PG_SCHEMA
        .find(END)
        .expect("PG_SCHEMA must contain fulltext schema end marker");
    &PG_SCHEMA[start..end]
}

pub fn vector_schema_sql() -> &'static str {
    const START: &str = "CREATE TABLE IF NOT EXISTS velr_vector_index_catalog";
    const END: &str = "-- End vector schema.";

    let start = PG_SCHEMA
        .find(START)
        .expect("PG_SCHEMA must contain vector schema start marker");
    let end = PG_SCHEMA
        .find(END)
        .expect("PG_SCHEMA must contain vector schema end marker");
    &PG_SCHEMA[start..end]
}

pub const GRAPH_SHAPE_BACKFILL: &str = r#"
DELETE FROM graph_shape_edge_property;
DELETE FROM graph_shape_edge_scope;
DELETE FROM graph_shape_node_property;
DELETE FROM graph_shape_node_scope;

INSERT INTO graph_shape_node_scope(label_id, owner_count)
SELECT label_id, COUNT(*) AS owner_count
FROM (
  SELECT COALESCE(lgm.type_id, 0) AS label_id
  FROM node AS n
  LEFT JOIN label_group_member AS lgm ON lgm.group_id = n.label_group_id
)
GROUP BY label_id;

INSERT INTO graph_shape_node_property(label_id, key_id, storage_class, tag, present_count)
SELECT
  label_id,
  key_id,
  storage_class,
  tag,
  COUNT(*) AS present_count
FROM (
  SELECT
    COALESCE(lgm.type_id, 0) AS label_id,
    np.key_id,
    typeof(np.value) AS storage_class,
    CASE WHEN typeof(np.value) = 'blob' THEN hex(substr(np.value, 1, 1)) ELSE '' END AS tag
  FROM node AS n
  LEFT JOIN label_group_member AS lgm ON lgm.group_id = n.label_group_id
  JOIN node_property AS np ON np.node_id = n.id
)
GROUP BY label_id, key_id, storage_class, tag;

INSERT INTO graph_shape_edge_scope(type_id, source_label_id, target_label_id, owner_count)
SELECT
  type_id,
  source_label_id,
  target_label_id,
  COUNT(*) AS owner_count
FROM (
  SELECT
    e.type_id,
    COALESCE(src_lgm.type_id, 0) AS source_label_id,
    COALESCE(dst_lgm.type_id, 0) AS target_label_id
  FROM edge AS e
  JOIN node AS src ON src.id = e.from_node
  JOIN node AS dst ON dst.id = e.to_node
  LEFT JOIN label_group_member AS src_lgm ON src_lgm.group_id = src.label_group_id
  LEFT JOIN label_group_member AS dst_lgm ON dst_lgm.group_id = dst.label_group_id
)
GROUP BY type_id, source_label_id, target_label_id;

INSERT INTO graph_shape_edge_property(
  type_id, source_label_id, target_label_id, key_id, storage_class, tag, present_count
)
SELECT
  type_id,
  source_label_id,
  target_label_id,
  key_id,
  storage_class,
  tag,
  COUNT(*) AS present_count
FROM (
  SELECT
    e.type_id,
    COALESCE(src_lgm.type_id, 0) AS source_label_id,
    COALESCE(dst_lgm.type_id, 0) AS target_label_id,
    ep.key_id,
    typeof(ep.value) AS storage_class,
    CASE WHEN typeof(ep.value) = 'blob' THEN hex(substr(ep.value, 1, 1)) ELSE '' END AS tag
  FROM edge AS e
  JOIN edge_property AS ep ON ep.edge_id = e.id
  JOIN node AS src ON src.id = e.from_node
  JOIN node AS dst ON dst.id = e.to_node
  LEFT JOIN label_group_member AS src_lgm ON src_lgm.group_id = src.label_group_id
  LEFT JOIN label_group_member AS dst_lgm ON dst_lgm.group_id = dst.label_group_id
)
GROUP BY type_id, source_label_id, target_label_id, key_id, storage_class, tag;
"#;

pub const MOVIE_TESTDATA: &str = r#"
INSERT INTO node (id) VALUES (1), (2), (3), (4); -- Sven, Bob, Matrix, Inception

-- Label types (unique labels, one row per type)
INSERT INTO label_type (id, type) VALUES
    (1, 'Person'),
    (2, 'Movie');

INSERT INTO label_group (id, signature, labels_json) VALUES
    (2, '1', '["Person"]'),
    (3, '2', '["Movie"]');

INSERT INTO label_group_member (group_id, type_id, ordinal) VALUES
    (2, 1, 0),
    (3, 2, 0);

UPDATE node SET label_group_id = 2 WHERE id IN (1, 2);
UPDATE node SET label_group_id = 3 WHERE id IN (3, 4);




-- Node Properties
INSERT INTO node_property VALUES (1, 'name', json('"SVEN"'));
INSERT INTO node_property VALUES (2, 'name', json('"Bob"'));
INSERT INTO node_property VALUES (3, 'title', json('"The Matrix"'));
INSERT INTO node_property VALUES (4, 'title', json('"Inception"'));

-- Edge Types (only unique types, one row per type)
INSERT INTO edge_type (id, type) VALUES
    (1, 'ACTED_IN'),
    (2, 'DIRECTED');

-- Edges (use type_id from edge_type)
INSERT INTO edge (id, from_node, to_node, type_id) VALUES
    (1, 1, 3, 1),  -- SVEN ACTED_IN Matrix
    (2, 1, 4, 1),  -- SVEN ACTED_IN Inception
    (3, 1, 3, 2),  -- SVEN DIRECTED Matrix
    (4, 2, 4, 2);
"#;

pub const TEST_QUERY_CTE: &str = r#"
WITH person_nodes AS (
    SELECT n.id AS node_id
    FROM node n
    JOIN label_group_member lgm ON lgm.group_id = n.label_group_id
    JOIN label_type lt ON lt.id = lgm.type_id
    WHERE lt.type IN ('Person')
    GROUP BY n.id
    HAVING COUNT(DISTINCT lt.type) = 1
),
movie_nodes AS (
    SELECT n.id AS node_id
    FROM node n
    JOIN label_group_member lgm ON lgm.group_id = n.label_group_id
    JOIN label_type lt ON lt.id = lgm.type_id
    WHERE lt.type = 'Movie'
)
SELECT
    p.node_id AS person_id,
    m.node_id AS movie_id,
    p_name.value AS person_name
FROM person_nodes p
JOIN node_property p_name ON p_name.node_id = p.node_id AND p_name.key = 'name'
JOIN edge e ON e.from_node = p.node_id
JOIN edge_type et ON et.id = e.type_id AND et.type IN ('ACTED_IN', 'EXTRA')
JOIN movie_nodes m ON m.node_id = e.to_node;
"#;

pub const TEST_QUERY: &str = r#"
SELECT
    p.id AS person_id,
    p_name.value AS person_name,
    m.id AS movie_id,
    m_title.value AS movie_title
FROM
    node p
JOIN label_group_member pl ON pl.group_id = p.label_group_id
JOIN label_type plt ON plt.id = pl.type_id AND plt.type = 'Person'
JOIN node_property p_name ON p_name.node_id = p.id AND p_name.key = 'name'

JOIN edge e ON e.from_node = p.id
JOIN edge_type et ON et.id = e.type_id AND et.type = 'ACTED_IN'

JOIN node m ON m.id = e.to_node
JOIN label_group_member ml ON ml.group_id = m.label_group_id
JOIN label_type mlt ON mlt.id = ml.type_id AND mlt.type = 'Movie'
JOIN node_property m_title ON m_title.node_id = m.id AND m_title.key = 'title'
"#;

#[cfg(test)]
mod tests {
    use super::{fulltext_schema_sql, vector_schema_sql, PG_SCHEMA};
    use rusqlite::Connection;

    fn table_info(conn: &Connection, table: &str) -> rusqlite::Result<Vec<(String, String)>> {
        let pragma = format!("PRAGMA table_info({table})");
        let mut stmt = conn.prepare(&pragma)?;
        let rows = stmt.query_map([], |row| Ok((row.get(1)?, row.get(2)?)))?;
        rows.collect()
    }

    #[test]
    fn property_tables_use_single_weakly_typed_value_column() -> rusqlite::Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(PG_SCHEMA)?;

        let node_cols = table_info(&conn, "node_property")?;
        assert_eq!(
            node_cols,
            vec![
                ("node_id".to_string(), "INTEGER".to_string()),
                ("key_id".to_string(), "INTEGER".to_string()),
                ("value".to_string(), "".to_string()),
            ]
        );

        let edge_cols = table_info(&conn, "edge_property")?;
        assert_eq!(
            edge_cols,
            vec![
                ("edge_id".to_string(), "INTEGER".to_string()),
                ("key_id".to_string(), "INTEGER".to_string()),
                ("value".to_string(), "".to_string()),
            ]
        );

        let node_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='node_property'",
            [],
            |row| row.get(0),
        )?;
        assert!(!node_sql.contains("value_num"));
        assert!(!node_sql.contains("value_text"));
        assert!(!node_sql.contains("value_json"));

        let edge_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='edge_property'",
            [],
            |row| row.get(0),
        )?;
        assert!(!edge_sql.contains("value_num"));
        assert!(!edge_sql.contains("value_text"));
        assert!(!edge_sql.contains("value_json"));

        Ok(())
    }

    #[test]
    fn property_indexes_target_single_value_storage() -> rusqlite::Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(PG_SCHEMA)?;

        let idx_num_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_np_key_num'",
            [],
            |row| row.get(0),
        )?;
        assert!(idx_num_sql.contains("node_property"));
        assert!(idx_num_sql.contains("key_id"));
        assert!(idx_num_sql.contains("value"));
        assert!(idx_num_sql.contains("typeof(value) IN ('integer', 'real')"));

        let idx_txt_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_np_key_txt12'",
            [],
            |row| row.get(0),
        )?;
        assert!(idx_txt_sql.contains("substr(value, 1, 12)"));
        assert!(idx_txt_sql.contains("typeof(value) = 'text'"));

        Ok(())
    }

    #[test]
    fn current_schema_installs_fulltext_vector_without_default_relationship_property_indexes(
    ) -> rusqlite::Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(PG_SCHEMA)?;

        let user_version: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        assert_eq!(user_version, 9);

        let relationship_property_index_count: i64 = conn.query_row(
            "SELECT COUNT(*)
               FROM sqlite_master
              WHERE type='index'
                AND name IN ('idx_ep_keyid_edge', 'idx_ep_key_num', 'idx_ep_key_txt12')",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(relationship_property_index_count, 0);

        let catalog_cols = table_info(&conn, "velr_fulltext_index_catalog")?;
        assert_eq!(
            catalog_cols,
            vec![
                ("index_name".to_string(), "TEXT".to_string()),
                ("entity_kind".to_string(), "TEXT".to_string()),
                ("labels_json".to_string(), "TEXT".to_string()),
                ("properties_json".to_string(), "TEXT".to_string()),
                ("analyzer".to_string(), "TEXT".to_string()),
                ("options_json".to_string(), "TEXT".to_string()),
                ("state".to_string(), "TEXT".to_string()),
                ("generation".to_string(), "INTEGER".to_string()),
                ("failure_message".to_string(), "TEXT".to_string()),
            ]
        );
        let fulltext_catalog_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='velr_fulltext_index_catalog'",
            [],
            |row| row.get(0),
        )?;
        assert!(fulltext_catalog_sql.contains("index_name TEXT NOT NULL PRIMARY KEY"));

        let dirty_cols = table_info(&conn, "velr_fulltext_dirty")?;
        assert_eq!(
            dirty_cols,
            vec![
                ("entity_kind".to_string(), "TEXT".to_string()),
                ("entity_id".to_string(), "INTEGER".to_string()),
            ]
        );

        assert!(!fulltext_schema_sql().contains("PRAGMA user_version"));

        let vector_catalog_cols = table_info(&conn, "velr_vector_index_catalog")?;
        assert_eq!(
            vector_catalog_cols,
            vec![
                ("index_name".to_string(), "TEXT".to_string()),
                ("entity_kind".to_string(), "TEXT".to_string()),
                ("labels_json".to_string(), "TEXT".to_string()),
                ("properties_json".to_string(), "TEXT".to_string()),
                ("embedder".to_string(), "TEXT".to_string()),
                ("embedder_fingerprint".to_string(), "TEXT".to_string()),
                ("dimensions".to_string(), "INTEGER".to_string()),
                ("metric".to_string(), "TEXT".to_string()),
                ("scalar_kind".to_string(), "TEXT".to_string()),
                ("sidecar_engine".to_string(), "TEXT".to_string()),
                ("sidecar_options_json".to_string(), "TEXT".to_string()),
                ("cache_policy".to_string(), "TEXT".to_string()),
                ("cache_engine".to_string(), "TEXT".to_string()),
                ("cache_options_json".to_string(), "TEXT".to_string()),
                ("cache_schema_version".to_string(), "INTEGER".to_string()),
                ("state".to_string(), "TEXT".to_string()),
                ("generation".to_string(), "INTEGER".to_string()),
                ("sidecar_token".to_string(), "TEXT".to_string()),
                ("failure_message".to_string(), "TEXT".to_string()),
            ]
        );
        let vector_catalog_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='velr_vector_index_catalog'",
            [],
            |row| row.get(0),
        )?;
        assert!(vector_catalog_sql.contains("index_name TEXT NOT NULL PRIMARY KEY"));

        let vector_dirty_cols = table_info(&conn, "velr_vector_dirty")?;
        assert_eq!(
            vector_dirty_cols,
            vec![
                ("index_name".to_string(), "TEXT".to_string()),
                ("entity_kind".to_string(), "TEXT".to_string()),
                ("entity_id".to_string(), "INTEGER".to_string()),
            ]
        );

        let vector_row_cache_cols = table_info(&conn, "velr_vector_embedding_row_cache")?;
        assert_eq!(
            vector_row_cache_cols,
            vec![
                ("index_name".to_string(), "TEXT".to_string()),
                ("entity_kind".to_string(), "TEXT".to_string()),
                ("entity_id".to_string(), "INTEGER".to_string()),
                ("source_hash".to_string(), "TEXT".to_string()),
                ("vector_blob".to_string(), "BLOB".to_string()),
                ("encoding".to_string(), "TEXT".to_string()),
                ("compression".to_string(), "TEXT".to_string()),
            ]
        );

        assert!(!vector_schema_sql().contains("PRAGMA user_version"));

        Ok(())
    }

    #[test]
    fn fulltext_schema_has_no_permanent_dirty_triggers() -> rusqlite::Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(PG_SCHEMA)?;

        let trigger_count: i64 = conn.query_row(
            "SELECT count(*)
               FROM sqlite_master
              WHERE type = 'trigger'
                AND name LIKE 'velr_fulltext_%'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(trigger_count, 0);

        conn.execute("INSERT INTO node DEFAULT VALUES", [])?;
        let dirty_count: i64 =
            conn.query_row("SELECT count(*) FROM velr_fulltext_dirty", [], |row| {
                row.get(0)
            })?;
        assert_eq!(dirty_count, 0);

        conn.execute(
            "INSERT INTO velr_fulltext_index_catalog(
               index_name, entity_kind, labels_json, properties_json,
               analyzer, options_json, state
             )
             VALUES ('paperSearch', 'NODE', '[\"Paper\"]', '[\"title\"]',
                     'standard', '{}', 'ONLINE')",
            [],
        )?;

        conn.execute("INSERT INTO node DEFAULT VALUES", [])?;
        let second_node = conn.last_insert_rowid();
        conn.execute("INSERT OR IGNORE INTO key_dict(name) VALUES ('title')", [])?;
        let key_id: i64 =
            conn.query_row("SELECT id FROM key_dict WHERE name = 'title'", [], |row| {
                row.get(0)
            })?;
        conn.execute(
            "INSERT INTO node_property(node_id, key_id, value) VALUES (?1, ?2, 'Ada')",
            (second_node, key_id),
        )?;
        conn.execute(
            "UPDATE node_property SET value = 'Grace' WHERE node_id = ?1 AND key_id = ?2",
            (second_node, key_id),
        )?;
        conn.execute(
            "DELETE FROM node_property WHERE node_id = ?1 AND key_id = ?2",
            (second_node, key_id),
        )?;
        conn.execute("DELETE FROM node WHERE id = ?1", [second_node])?;
        let dirty_count: i64 =
            conn.query_row("SELECT count(*) FROM velr_fulltext_dirty", [], |row| {
                row.get(0)
            })?;
        assert_eq!(dirty_count, 0);
        Ok(())
    }
}
