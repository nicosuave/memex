{
  description = "memex - High-performance local search engine for LLM history";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    flake-utils,
    ...
  }: let
    overlays = [
      (import rust-overlay)
      (final: prev: {
        memex = final.callPackage ./nix/package.nix {
          rustPlatform = final.makeRustPlatform {
            cargo = final.rust-bin.stable.latest.default;
            rustc = final.rust-bin.stable.latest.default;
          };
        };
      })
    ];
  in
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = ["rust-src" "rust-analyzer" "clippy"];
        };
      in {
        packages.default = pkgs.memex;
        packages.memex = pkgs.memex;

        apps.default = flake-utils.lib.mkApp {
          drv = pkgs.memex;
        };

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs;
            [
              pkg-config
              openssl
              rustToolchain
            ];

          nativeBuildInputs = with pkgs; [pkg-config];

          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [pkgs.openssl];
        };
      }
    )
    // {
      nixosModules.default = import ./nix/nixos-module.nix;
      homeManagerModules.default = import ./nix/hm-module.nix;
      overlays.default = final: prev: {
        memex = final.callPackage ./nix/package.nix {
          rustPlatform = final.makeRustPlatform {
            cargo = final.rust-bin.stable.latest.default;
            rustc = final.rust-bin.stable.latest.default;
          };
        };
      };
    };
}
