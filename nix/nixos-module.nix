{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.memex;
in {
  options.services.memex = {
    enable = lib.mkEnableOption "memex index service";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.memex;
      description = "The memex package to use.";
    };

    continuous = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Run in continuous mode (watch) instead of interval mode.";
    };

    interval = lib.mkOption {
      type = lib.types.str;
      default = "3600";
      description = "Interval for indexing in seconds or systemd time span (if not continuous).";
    };

    watchInterval = lib.mkOption {
      type = lib.types.int;
      default = 30;
      description = "Polling interval in seconds (if continuous).";
    };

    webUI = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Serve the local Web UI and run in continuous mode.";
    };

    webListen = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:6363";
      description = "Address and port for the local Web UI.";
    };
  };

  config = lib.mkMerge [
    {
      # NixOS reloads user managers on switch but does not restart changed user
      # services. Keep this activation hook when disabled to stop obsolete units.
      system.userActivationScripts.memex = ''
        ${pkgs.bash}/bin/bash ${./activate-user-service.sh} ${config.systemd.package}/bin/systemctl ${
          if !cfg.enable
          then "disabled"
          else if cfg.continuous || cfg.webUI
          then "continuous"
          else "interval"
        }
      '';
    }
    (lib.mkIf cfg.enable {
      systemd.user.services.memex-index = {
        description = "Memex Index Service";
        wantedBy = ["default.target"];
        path = [cfg.package];
        serviceConfig = {
          Environment = "MEMEX_SERVICE_MANAGER=nix";
          ExecStart =
            if cfg.continuous || cfg.webUI
            then "${cfg.package}/bin/memex index --watch --watch-interval ${toString cfg.watchInterval}${lib.optionalString cfg.webUI " --web-ui --web-listen ${lib.escapeShellArg cfg.webListen}"}"
            else "${cfg.package}/bin/memex index";

          Restart =
            if cfg.continuous || cfg.webUI
            then "always"
            else "no";
          RestartSec = 10;
        };
      };

      systemd.user.timers.memex-index = lib.mkIf (!cfg.continuous && !cfg.webUI) {
        description = "Memex Index Timer";
        wantedBy = ["timers.target"];
        timerConfig = {
          OnBootSec = "5m";
          OnUnitActiveSec = cfg.interval;
        };
      };
    })
  ];
}
