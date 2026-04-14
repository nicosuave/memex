{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.programs.memex;
  tomlFormat = pkgs.formats.toml {};
in {
  options.programs.memex = {
    enable = lib.mkEnableOption "memex";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.memex;
      description = "The memex package to install.";
    };

    settings = lib.mkOption {
      type = tomlFormat.type;
      default = {};
      description = ''
        Configuration written to ~/.memex/config.toml.

        Supported keys:
        - embeddings (bool)
        - auto_index_on_search (bool)
        - model (string): "minilm", "bge", "nomic", "gemma", "potion"
        - execution_provider (string): "auto", "cpu", "coreml", "cuda"
        - cuda_device_id (int): GPU index when using the CUDA execution provider
        - cuda_library_paths (list of strings): optional CUDA library directories
        - cudnn_library_paths (list of strings): optional cuDNN library directories
        - scan_cache_ttl (int): seconds
        - index_service_mode (string): "interval" or "continuous"
        - index_service_interval (int): seconds
        - index_service_poll_interval (int): seconds
        - index_service_label (string): service name for systemd/launchd
        - index_service_systemd_dir (string): systemd user directory (Linux)
        - index_service_plist (string): launchd plist path (macOS)
        - index_service_stdout (string): stdout log path (macOS)
        - index_service_stderr (string): stderr log path (macOS)
        - claude_resume_cmd (string)
        - codex_resume_cmd (string)
      '';
      example = {
        embeddings = true;
        model = "minilm";
        execution_provider = "auto";
        cuda_device_id = 0;
        auto_index_on_search = true;
      };
    };
  };

  config = lib.mkIf cfg.enable {
    home.packages = [cfg.package];

    home.file.".memex/config.toml" = lib.mkIf (cfg.settings != {}) {
      source = tomlFormat.generate "memex-config" cfg.settings;
    };
  };
}
