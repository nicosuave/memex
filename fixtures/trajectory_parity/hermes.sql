PRAGMA journal_mode = WAL;

CREATE TABLE schema_version (
    version INTEGER NOT NULL
);
INSERT INTO schema_version(version) VALUES (21);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    model TEXT,
    parent_session_id TEXT,
    started_at REAL NOT NULL,
    ended_at REAL,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cache_read_tokens INTEGER DEFAULT 0,
    cache_write_tokens INTEGER DEFAULT 0,
    reasoning_tokens INTEGER DEFAULT 0,
    cwd TEXT,
    billing_provider TEXT,
    estimated_cost_usd REAL,
    actual_cost_usd REAL,
    rewind_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT,
    tool_call_id TEXT,
    tool_calls TEXT,
    tool_name TEXT,
    timestamp REAL NOT NULL,
    reasoning TEXT,
    reasoning_content TEXT,
    active INTEGER NOT NULL DEFAULT 1
);

INSERT INTO sessions(
    id, source, model, started_at, ended_at, input_tokens, output_tokens,
    cache_read_tokens, cache_write_tokens, reasoning_tokens, cwd,
    billing_provider, estimated_cost_usd, actual_cost_usd
) VALUES (
    'hermes-session', 'cli', 'gpt-5.2', 1783000000.0, 1783000009.0,
    100, 50, 20, 5, 10, '/workspace/demo', 'openai', 0.04, 0.03
);

INSERT INTO messages(id, session_id, role, content, timestamp, active)
VALUES (101, 'hermes-session', 'user', 'Check the current directory.', 1783000001.25, 1);

INSERT INTO messages(
    id, session_id, role, content, tool_calls, timestamp, reasoning,
    reasoning_content, active
) VALUES (
    102, 'hermes-session', 'assistant', NULL,
    '[{"id":"call-hermes-1","function":{"name":"terminal","arguments":"{\"command\":\"pwd\"}"}}]',
    1783000004.5, 'fallback reasoning', 'Check the working directory.', 1
);

INSERT INTO messages(
    id, session_id, role, content, tool_call_id, tool_name, timestamp, active
) VALUES (
    103, 'hermes-session', 'tool', '/workspace/demo', 'call-hermes-1',
    'terminal', 1783000006.0, 1
);

INSERT INTO messages(id, session_id, role, content, timestamp, active)
VALUES (104, 'hermes-session', 'assistant', 'You are in /workspace/demo.', 1783000008.75, 1);

INSERT INTO messages(id, session_id, role, content, timestamp, active)
VALUES (105, 'hermes-session', 'user', 'rewound prompt', 1783000009.0, 0);

INSERT INTO messages(id, session_id, role, content, timestamp, active)
VALUES (
    106, 'hermes-session', 'user',
    char(0) || 'json:[{"type":"text","text":"Visible multimodal text"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]',
    1783000009.5, 1
);

INSERT INTO messages(id, session_id, role, content, timestamp, reasoning_content, active)
VALUES (
    107, 'hermes-session', 'assistant', 'Visible encrypted-reasoning answer',
    1783000010.0,
    '{"type":"encrypted_reasoning","encrypted_content":"ciphertext-must-never-be-indexed"}',
    1
);
