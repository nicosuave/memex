{
  lib,
  rustPlatform,
  pkg-config,
  openssl,
  onnxruntime,
  stdenv,
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
      onnxruntime
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
