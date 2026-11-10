-- goose 1.55 store (schema v17), reduced to the tables ingestion reads.
-- The rows are synthetic: they mirror the shapes goose actually writes,
-- including a rewritten-in-place conversation, a sub-agent session, a hidden
-- CLI session, a session with no messages, and a ledger whose input tokens
-- include the cache subsets.
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    user_set_name BOOLEAN DEFAULT FALSE,
    session_type TEXT NOT NULL DEFAULT 'user',
    working_dir TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extension_data TEXT DEFAULT '{}',
    total_tokens INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_write_tokens INTEGER,
    accumulated_total_tokens INTEGER,
    accumulated_input_tokens INTEGER,
    accumulated_output_tokens INTEGER,
    accumulated_cache_read_tokens INTEGER,
    accumulated_cache_write_tokens INTEGER,
    accumulated_cost REAL,
    schedule_id TEXT,
    recipe_json TEXT,
    user_recipe_values_json TEXT,
    provider_name TEXT,
    model_config_json TEXT,
    goose_mode TEXT NOT NULL DEFAULT 'auto',
    archived_at TIMESTAMP,
    project_id TEXT,
    parent_session_id TEXT,
    system_prompt_override TEXT,
    system_prompt_extras_json TEXT,
    container_id TEXT
);

CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    role TEXT NOT NULL,
    content_json TEXT NOT NULL,
    created_timestamp INTEGER NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tokens INTEGER,
    metadata_json TEXT
);

CREATE TABLE usage_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    created_timestamp INTEGER NOT NULL,
    model TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    total_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_write_tokens INTEGER,
    cost REAL,
    cost_source TEXT,
    is_compaction INTEGER DEFAULT 0
);

INSERT INTO sessions (id, name, session_type, working_dir, created_at, updated_at, provider_name, model_config_json, parent_session_id)
VALUES
    ('sess_main', 'List the files', 'user', '/work/repo',
     '2026-10-19 20:00:00', '2026-10-19 20:05:00', 'anthropic',
     '{"model_name":"gpt-4o","reasoning":true}', NULL),
    ('sess_child', 'Count them', 'sub_agent', '/work/repo',
     '2026-10-19 20:06:00', '2026-10-19 20:07:00', 'anthropic',
     '{"model_name":"gpt-4o-mini"}', 'sess_main'),
    ('sess_hidden', '', 'hidden', '/work/repo',
     '2026-10-19 20:08:00', '2026-10-19 20:08:10', 'anthropic', NULL, NULL),
    ('sess_empty', 'Abandoned', 'user', '',
     '2026-10-19 20:09:00', '2026-10-19 20:09:00', NULL, NULL, NULL);

INSERT INTO messages (message_id, session_id, role, content_json, created_timestamp, metadata_json)
VALUES
    ('msg_1', 'sess_main', 'user',
     '[{"type":"text","text":"List the files"}]', 1699999990,
     '{"userVisible":true,"agentVisible":true}'),
    -- Per-turn scaffolding goose appends to every request; the user never saw it.
    ('msg_2', 'sess_main', 'user',
     '[{"type":"text","text":"<turn-context>\n<current-time>2026-10-19 20:00:00 +02:00</current-time>\n<working-directory>/work/repo</working-directory>\n</turn-context>"}]',
     1699999995, '{"userVisible":false,"agentVisible":true,"turnContext":true}'),
    ('msg_3', 'sess_main', 'assistant',
     '[{"type":"toolRequest","id":"call_1","toolCall":{"status":"success","value":{"name":"shell","arguments":{"command":"ls"}}},"_meta":{"goose.toolCall.providerIndex":0,"goose_extension":"developer"}}]',
     1700000000,
     '{"userVisible":true,"agentVisible":true,"inference":{"provider":"anthropic","requestedModel":"gpt-4o"}}'),
    ('msg_4', 'sess_main', 'user',
     '[{"type":"toolResponse","id":"call_1","toolResult":{"status":"success","value":{"resultType":"complete","content":[{"type":"text","text":"a.txt","annotations":{"priority":0.0}}]}}}]',
     1700000001, '{"userVisible":true,"agentVisible":true}'),
    ('msg_5', 'sess_main', 'assistant',
     '[{"type":"thinking","thinking":"thinking about it","signature":""}]',
     1700000002, '{"userVisible":true,"agentVisible":true}'),
    ('msg_6', 'sess_main', 'assistant',
     '[{"type":"text","text":"One file"}]', 1700000003,
     '{"userVisible":true,"agentVisible":true}'),
    -- A sub-agent turn: no metadata_json at all, an image, and a failed tool.
    ('msg_7', 'sess_child', 'user',
     '[{"type":"text","text":"Count the entries"},{"type":"image","mimeType":"image/png","data":"iVBORw0KGgo="}]',
     1700000100, NULL),
    ('msg_8', 'sess_child', 'assistant',
     '[{"type":"toolRequest","id":"call_2","toolCall":{"status":"error","error":"invalid arguments"}}]',
     1700000101, NULL),
    ('msg_9', 'sess_hidden', 'user',
     '[{"type":"text","text":"internal plumbing"}]', 1700000200, NULL);

INSERT INTO usage_ledger (session_id, created_timestamp, model, input_tokens, output_tokens, total_tokens, cache_read_tokens, cache_write_tokens, cost, cost_source, is_compaction)
VALUES
    ('sess_main', 1700000000, 'gpt-4o', 1000, 50, 1050, 600, 0, 0.25, 'provider_reported', 0),
    ('sess_child', 1700000100, 'gpt-4o-mini', 200, 20, 220, 0, 0, NULL, 'estimated', 0);
