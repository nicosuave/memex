{
  lib,
  rustPlatform,
  pkg-config,
  openssl,
  stdenv,
  darwin,
}:
rustPlatform.buildRustPackage {
  pname = "memex";
  version = (lib.importTOML ../Cargo.toml).package.version;

  src = lib.cleanSource ../.;

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  nativeBuildInputs = [
    pkg-config
  ];

  buildInputs =
    [
      openssl
    ]
    ++ lib.optionals stdenv.isDarwin [
      darwin.apple_sdk.frameworks.Security
      darwin.apple_sdk.frameworks.CoreFoundation
      darwin.apple_sdk.frameworks.SystemConfiguration
    ];

  # Tests require network access to download embedding models
  doCheck = false;

  meta = {
    description = "Fast local history search for local agent logs";
    homepage = "https://github.com/nicosuave/memex";
    license = lib.licenses.mit;
    mainProgram = "memex";
    maintainers = [];
  };
}
