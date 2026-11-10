-- memex for Rex: a session desk for your agent history.
--
-- memex indexes the transcripts every coding agent on this machine leaves behind (Claude Code,
-- Codex, Cursor, OpenCode, Pi, and more). Inside Rex it becomes a pane: browse and search past
-- sessions, then resume one into a new Rex tab, or jump to the pane already running it.
--
-- `memex rex install` writes this file beside Rex's init.lua. Then, in init.lua:
--
--   require("memex").setup{}
--   rex.bind("cmd+shift+m", "memex_palette")
--
-- and run `rex config reload`.
--
-- Actions (in the command palette under "memex", and `rex do NAME` from a shell):
--   memex_palette      The memex TUI zoomed over the focused pane. Enter resumes a session into a
--                      new tab, or focuses the pane already running it; q closes the TUI and the
--                      layout comes back as it was.
--   memex_here         The palette filtered to the project of the focused terminal's repo.
--   memex_desk         The TUI in its own "memex" tab, reused when one is open.
--   memex_sidebar      The TUI in a split beside the focused pane, or close it if this tab has one.
--   memex_resume_last  Resume the newest session for the focused terminal's directory without
--                      opening the TUI.
--   memex_index        Run an incremental `memex index` in the background.
--   memex_web          Open the memex web UI, starting `memex web` first if nothing answers.
--
-- The palette is a zoomed split rather than a floating layer: Rex draws floating layers without
-- a backdrop, and the memex TUI keeps the terminal's default background, so a floating palette
-- would show the pane underneath through it.

local M = {}

M.options = {
  -- The memex binary. A bare name is looked up on the PATH your login shell sets.
  bin = "memex",
  -- An alternate memex data directory (memex --root), or nil for memex's own default.
  root = nil,
  -- Whether the sidebar takes focus when it opens. Off by default: it sits beside your work.
  sidebar_focus = false,
  -- Where `memex web` listens.
  web_listen = "127.0.0.1:6363",
}

local TERMINAL = "com.superlogical.terminal"
local SHELL = TERMINAL .. ".shell"
-- Every pane this module opens carries this label; the desk and the sidebar toggle find their
-- panes by it.
local LABEL = "memex"

local unpack = table.unpack or unpack

local function fail(message)
  error("memex: " .. tostring(message), 0)
end

local function trim(s)
  return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function first_line(s)
  for line in (s or ""):gmatch("[^\n]+") do
    line = trim(line):gsub("^Error: ", "")
    if line ~= "" then return line end
  end
  return nil
end

local function last_line(s)
  local last
  for line in (s or ""):gmatch("[^\n]+") do
    if trim(line) ~= "" then last = trim(line) end
  end
  return last
end

local function basename(path)
  return (path:gsub("/+$", ""):match("([^/]+)$"))
end

-- memex's argv: the binary, the given words, then --root when one is set.
local function memex(...)
  local argv = { M.options.bin, ... }
  if M.options.root then
    argv[#argv + 1] = "--root"
    argv[#argv + 1] = M.options.root
  end
  return argv
end

-- The working directory of a terminal block: its foreground process's, else the one its program
-- started in. nil when it can't be told; never a guess, since resuming another project's
-- session is worse than refusing.
local function block_cwd(block_id)
  if not block_id then return nil end
  local p = rex.block.call(TERMINAL, "process", { block_id = block_id })
  if not p then return nil end
  local fg, child = p.foreground or {}, p.child or {}
  local cwd = fg.cwd or child.cwd
  if cwd and cwd ~= "" then return cwd end
  return nil
end

local function session_view()
  local view, err = rex.session.view{}
  if view == nil then fail(err) end
  return view
end

local function each_block(window, fn)
  for _, layer in ipairs(window.layers or {}) do
    for _, block in ipairs(layer.blocks or {}) do
      fn(block)
    end
  end
end

-- The window holding block_id, else the session's active window.
local function window_for(view, block_id)
  local active
  for _, window in ipairs(view.windows or {}) do
    local found = false
    each_block(window, function(block)
      if block.block_id == block_id then found = true end
    end)
    if found then return window end
    if window.window_id == view.active_window_id then active = window end
  end
  return active
end

local function memex_blocks(window)
  local ids = {}
  each_block(window, function(block)
    if block.label == LABEL then ids[#ids + 1] = block.block_id end
  end)
  return ids
end

local function terminal(command, cwd)
  return {
    block = { flavor = SHELL, label = LABEL, options = { command = command, cwd = cwd } },
  }
end

-- Where background output goes.
local function state_dir()
  local base = os.getenv("XDG_STATE_HOME")
  if base == nil or base == "" then base = os.getenv("HOME") .. "/.local/state" end
  local dir = base .. "/memex-rex"
  rex.exec{ "mkdir", "-p", dir }
  return dir
end

-- Start argv in the background, outliving the action, with its output appended to log.
local function spawn(argv, log)
  local handle, err = rex.exec.start(
    { "sh", "-c", 'exec "$@" >>"$0" 2>&1', log, unpack(argv) },
    { detach = true })
  if handle == nil then fail(err) end
end

-- The TUI zoomed over the focused pane. Quitting it closes the pane, which ends the zoom.
local function open_palette(ctx, extra)
  local split, err = rex.session.new_split{
    anchor_id = ctx.block_id,
    direction = "horizontal",
    side      = "after",
    focus     = true,
    layout    = terminal(memex("tui", unpack(extra or {})), block_cwd(ctx.block_id)),
  }
  if split == nil then fail(err) end
  local block_id = split.block_ids and split.block_ids[1]
  local _, zerr = rex.session.zoom_block{ block_id = block_id, zoom = true }
  if zerr ~= nil then fail(zerr) end
  return { block_id = block_id }
end

function M.palette(ctx)
  return open_palette(ctx)
end

-- memex projects are directory names, so scope to the repo's top-level directory when there is
-- one, else the focused directory's own name.
function M.here(ctx)
  local cwd = block_cwd(ctx.block_id)
  if cwd == nil then fail("can't tell the working directory of the focused terminal") end
  local repo = cwd
  local r = rex.exec{ "git", "-C", cwd, "rev-parse", "--show-toplevel" }
  if r ~= nil and r.ok and trim(r.stdout) ~= "" then repo = trim(r.stdout) end
  local project = basename(repo)
  if project == nil or project == "" then fail("can't name a project for " .. repo) end
  local result = open_palette(ctx, { "--project", project })
  result.project = project
  return result
end

function M.desk(ctx)
  for _, window in ipairs(session_view().windows or {}) do
    if window.label == LABEL and #memex_blocks(window) > 0 then
      local _, err = rex.session.focus_window{ window_id = window.window_id }
      if err ~= nil then fail(err) end
      return { window_id = window.window_id, reused = true }
    end
  end
  local created, err = rex.session.new_window{
    window_label = LABEL,
    focus        = true,
    layout       = terminal(memex("tui"), block_cwd(ctx.block_id)),
  }
  if created == nil then fail(err) end
  return { window_id = created.window_id }
end

function M.sidebar(ctx)
  local window = window_for(session_view(), ctx.block_id)
  local open = window and memex_blocks(window) or {}
  if #open > 0 then
    for _, block_id in ipairs(open) do
      local _, err = rex.call("block.close", { session_id = ctx.session_id, block_id = block_id })
      if err ~= nil then fail(err) end
    end
    return { closed = open }
  end
  local split, err = rex.session.new_split{
    anchor_id = ctx.block_id,
    direction = "horizontal",
    side      = "after",
    focus     = M.options.sidebar_focus,
    layout    = terminal(memex("tui"), block_cwd(ctx.block_id)),
  }
  if split == nil then fail(err) end
  return { block_id = split.block_ids and split.block_ids[1] }
end

-- memex picks the session and opens its tab itself, or focuses the pane already running it.
function M.resume_last(ctx)
  local cwd = block_cwd(ctx.block_id)
  if cwd == nil then fail("can't tell the working directory of the focused terminal") end
  local argv = memex("rex", "resume-last", "--strict-cwd", "--cwd", cwd)
  for _, pair in ipairs{ { "--rex-session", ctx.session_id }, { "--rex-block", ctx.block_id } } do
    if pair[2] then
      argv[#argv + 1] = pair[1]
      argv[#argv + 1] = pair[2]
    end
  end
  local r, err = rex.exec(argv, { timeout = 25 })
  if r == nil then fail(err) end
  if not r.ok then fail(first_line(r.stderr) or ("memex exited with " .. tostring(r.code))) end
  return { message = last_line(r.stdout) or "resumed the last session" }
end

function M.index(ctx)
  local log = state_dir() .. "/index.log"
  spawn(memex("index"), log)
  return { log = log }
end

function M.web(ctx)
  local listen = M.options.web_listen
  local url = "http://" .. listen
  local log = state_dir() .. "/web.log"
  -- Polls url for up to tries * 0.2s. Actions have no rex.sleep, so the shell waits.
  local function answers(tries)
    local r = rex.exec({ "sh", "-c", [[
i=0
until curl -fsS -m 1 "$0/healthz" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -gt "$1" ] && exit 1
  sleep 0.2
done]], url, tostring(tries) }, { timeout = 15 })
    return r ~= nil and r.ok
  end
  -- Reuse a server that is already up; the memex daemon may well be serving it.
  if not answers(0) then
    spawn(memex("web", "--listen", listen), log)
    if not answers(15) then fail("memex web did not come up on " .. listen .. ", see " .. log) end
  end
  local r, err = rex.exec(memex("index-service", "open", "--listen", listen), { timeout = 15 })
  if r == nil then fail(err) end
  if not r.ok then fail("could not open " .. url .. ": " .. (first_line(r.stderr) or "")) end
  return { url = url }
end

local ACTIONS = {
  { "memex_palette", "memex: session palette", M.palette,
    "Recent agent sessions zoomed over the focused pane. Enter resumes one into a new tab." },
  { "memex_here", "memex: recent sessions here", M.here,
    "The session palette filtered to the focused terminal's repo." },
  { "memex_desk", "memex: session desk", M.desk,
    "Browse and search agent sessions in a memex tab." },
  { "memex_sidebar", "memex: toggle sidebar", M.sidebar,
    "Open memex in a split beside the focused pane, or close it." },
  { "memex_resume_last", "memex: resume last session", M.resume_last,
    "Resume the newest agent session for the focused terminal's directory in a new tab." },
  { "memex_index", "memex: refresh index", M.index,
    "Run an incremental memex index in the background." },
  { "memex_web", "memex: open web UI", M.web,
    "Open the memex web UI, starting memex web if needed." },
}

function M.setup(opts)
  local known = { root = true }
  for key in pairs(M.options) do known[key] = true end
  for key, value in pairs(opts or {}) do
    if known[key] then
      M.options[key] = value
    else
      rex.log("warn", "memex: ignoring unknown option " .. tostring(key))
    end
  end

  for _, action in ipairs(ACTIONS) do
    local name, title, run, description = unpack(action)
    rex.action{
      name        = name,
      title       = title,
      description = description,
      category    = "memex",
      keywords    = "memex agent session history resume claude codex search",
      run         = function(ctx) return run(ctx) end,
    }
  end
  return M
end

return M
