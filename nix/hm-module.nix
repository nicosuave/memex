{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.programs.memex;
  tomlFormat = pkgs.formats.toml {};
  configSource = tomlFormat.generate "memex-config" cfg.settings;
  continuous =
    (cfg.settings.index_service_mode or (
      if cfg.settings.index_service_continuous or false
      then "continuous"
      else "interval"
    ))
    == "continuous"
    || (cfg.settings.index_service_web_ui or false)
    || (cfg.settings.index_service_mcp or false);
  interval = cfg.settings.index_service_interval or 3600;
  label =
    cfg.settings.index_service_label or (
      if pkgs.stdenv.hostPlatform.isDarwin
      then "com.memex.index"
      else "memex-index"
    );
  command =
    ["${cfg.package}/bin/memex"]
    ++ (
      if continuous
      then ["daemon" "run"]
      else ["index"]
    );
in {
  options.programs.memex = {
    enable = lib.mkEnableOption "memex";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.memex;
      description = "The memex package to install.";
    };

    daemon.enable = lib.mkEnableOption "the Home Manager managed memex daemon";

    settings = lib.mkOption {
      type = tomlFormat.type;
      default = {};
      description = ''
        Configuration written to ~/.memex/config.toml.

        Supported keys:
        - embeddings (bool or string): true or "local", "remote", false
        - auto_index_on_search (bool)
        - include_reasoning (bool): opt in to plaintext reasoning records
        - token_usage (bool): opt in to local token and cost tracking
        - model (string): local alias "minilm", "bge", "nomic", "gemma", "potion",
          or a fastembed model name such as "BGELargeENV15"; with remote embeddings,
          the model name sent to the API
        - embedding_batch_size (int): texts per batch or request, 1 to 2048 (default 64)
        - embedding_dimensions (int): remote only; requested output size
        - embedding_base_url (string): remote only; OpenAI-compatible API base URL;
          plain http to a non-loopback host is rejected when an API key is configured
        - embedding_api_key_env (string): remote only; environment variable holding the API key;
          preferred, set the variable on the daemon service
        - embedding_api_key (string): remote only; literal API key; it ends up in the
          world-readable Nix store, so use embedding_api_key_env instead
        - embedding_timeout_secs (int): remote only; request timeout, 1 to 600 (default 60)
        - embedding_max_retries (int): remote only; retry limit, 0 to 10 (default 3)
        - execution_provider (string): "auto", "cpu", "coreml", "cuda"
        - cuda_device_id (int): GPU index when using the CUDA execution provider
        - cuda_library_paths (list of strings): optional CUDA library directories
        - cudnn_library_paths (list of strings): optional cuDNN library directories
        - rerank (bool or string): false, or true or "local" for local reranking
        - rerank_model (string): "jina-turbo", "bge-base", "jina-v2", "bge-v2-m3"
        - rerank_candidates (int): top results reranked, 5 to 100 (default 30)
        - rerank_doc_chars (int): characters per result, 200 to 8000 (default 1500)
        - scan_cache_ttl (int): seconds
        - index_service_mode (string): "interval" or "continuous"
        - index_service_interval (int): seconds
        - index_service_poll_interval (int): seconds
        - index_service_web_ui (bool): serve the local Web UI in continuous mode
        - index_service_web_listen (string): Web UI address and port
        - index_service_label (string): service name for systemd/launchd
        - index_service_systemd_dir (string): systemd user directory (Linux)
        - index_service_plist (string): launchd plist path (macOS)
        - index_service_stdout (string): stdout log path (macOS)
        - index_service_stderr (string): stderr log path (macOS)
        - claude_resume_cmd (string)
        - codex_resume_cmd (string)
        - cursor_resume_cmd (string)
      '';
      example = {
        embeddings = true;
        model = "minilm";
        execution_provider = "auto";
        cuda_device_id = 0;
        auto_index_on_search = true;
        include_reasoning = false;
        token_usage = false;
      };
    };
  };

  config = lib.mkIf cfg.enable (lib.mkMerge [
    {
      assertions = lib.optionals cfg.daemon.enable [
        {
          assertion = builtins.elem (cfg.settings.index_service_mode or "interval") ["interval" "continuous"];
          message = "programs.memex.settings.index_service_mode must be interval or continuous.";
        }
        {
          assertion = builtins.isInt interval && interval > 0;
          message = "programs.memex.settings.index_service_interval must be a positive number of seconds.";
        }
      ];
      home.packages = [cfg.package];

      home.file.".memex/config.toml" = lib.mkIf (cfg.settings != {}) {
        source = configSource;
      };
    }
    (lib.mkIf (cfg.daemon.enable && pkgs.stdenv.hostPlatform.isLinux) {
      systemd.user.services.${label} = {
        Unit = {
          Description = "Memex Index Service";
          X-Restart-Triggers = lib.optional (cfg.settings != {}) configSource;
        };
        Service = {
          ExecStart = lib.escapeShellArgs command;
          Environment = ["MEMEX_SERVICE_MANAGER=nix"];
          Restart =
            if continuous
            then "on-failure"
            else "no";
          RestartSec = 10;
        };
        Install.WantedBy = lib.optional continuous "default.target";
      };
      systemd.user.timers.${label} = lib.mkIf (!continuous) {
        Unit.Description = "Memex Index Timer";
        Timer = {
          OnBootSec = "5m";
          OnUnitActiveSec = "${toString interval}s";
        };
        Install.WantedBy = ["timers.target"];
      };
    })
    (lib.mkIf (cfg.daemon.enable && pkgs.stdenv.hostPlatform.isDarwin) {
      launchd.agents.memex = {
        enable = true;
        config =
          {
            Label = label;
            ProgramArguments = command;
            EnvironmentVariables = {
              MEMEX_SERVICE_MANAGER = "nix";
              # Include the settings generation so Home Manager reloads the agent
              # when settings change even if its command remains `daemon run`.
              MEMEX_CONFIG_GENERATION = builtins.hashString "sha256" (builtins.toJSON cfg.settings);
            };
            RunAtLoad = true;
            KeepAlive = continuous;
          }
          // lib.optionalAttrs (!continuous) {
            StartInterval = interval;
          }
          // lib.optionalAttrs (cfg.settings ? index_service_stdout) {
            StandardOutPath = cfg.settings.index_service_stdout;
          }
          // lib.optionalAttrs (cfg.settings ? index_service_stderr) {
            StandardErrorPath = cfg.settings.index_service_stderr;
          };
      };
    })
  ]);
}
