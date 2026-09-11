# Evaluate with nix-instantiate --eval --strict nix/tests.nix --arg homeManager /path/to/home-manager
{
  nixpkgs ? <nixpkgs>,
  homeManager,
}: let
  linux = import nixpkgs {system = "x86_64-linux";};
  darwin = import nixpkgs {system = "aarch64-darwin";};
  home = pkgs: settings: enabled:
    (import (homeManager + "/modules") {
      inherit pkgs;
      configuration = {
        imports = [./hm-module.nix];
        home.username = "memex-test";
        home.homeDirectory =
          if pkgs.stdenv.hostPlatform.isDarwin
          then "/Users/memex-test"
          else "/home/memex-test";
        home.stateVersion = "25.11";
        programs.memex = {
          enable = true;
          package = pkgs.hello;
          daemon.enable = enabled;
          inherit settings;
        };
      };
    }).config;
  linuxInterval = home linux {} true;
  linuxContinuous = home linux {index_service_mode = "continuous";} true;
  linuxMcp = home linux {index_service_mcp = true;} true;
  darwinInterval = home darwin {} true;
  darwinContinuous = home darwin {index_service_web_ui = true;} true;
  system = continuous:
    (import (nixpkgs + "/nixos/lib/eval-config.nix") {
      system = "x86_64-linux";
      modules = [
        ./nixos-module.nix
        {
          services.memex = {
            enable = true;
            package = linux.hello;
            inherit continuous;
          };
          system.stateVersion = "25.11";
        }
      ];
    }).config;
in
  assert !(builtins.hasAttr "memex-index" (home linux {} false).systemd.user.services);
  assert !(builtins.hasAttr "memex" (home darwin {} false).launchd.agents);
  assert linuxInterval.systemd.user.timers.memex-index.Timer.OnUnitActiveSec == "3600s";
  assert linuxInterval.systemd.user.services.memex-index.Install.WantedBy == [];
  assert linuxContinuous.systemd.user.services.memex-index.Service.ExecStart == ["${linux.hello}/bin/memex daemon run"];
  assert linuxContinuous.systemd.user.services.memex-index.Unit.X-Restart-Triggers != [];
  assert !(builtins.hasAttr "memex-index" linuxContinuous.systemd.user.timers);
  assert !(builtins.hasAttr "memex-index" linuxMcp.systemd.user.timers);
  assert darwinInterval.launchd.agents.memex.config.StartInterval == 3600;
  assert darwinContinuous.launchd.agents.memex.config.KeepAlive;
  assert darwinContinuous.launchd.agents.memex.config.ProgramArguments == ["${darwin.hello}/bin/memex" "daemon" "run"];
  assert darwinInterval.launchd.agents.memex.config.EnvironmentVariables.MEMEX_CONFIG_GENERATION != darwinContinuous.launchd.agents.memex.config.EnvironmentVariables.MEMEX_CONFIG_GENERATION;
  assert (system true).systemd.user.services.memex-index.serviceConfig.Environment == "MEMEX_SERVICE_MANAGER=nix";
  assert (system false).systemd.user.timers.memex-index.timerConfig.OnUnitActiveSec == "3600"; "Nix module checks passed"
