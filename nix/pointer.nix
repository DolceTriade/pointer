{
  binaryen,
  cargo-leptos,
  lib,
  makeWrapper,
  rustPlatform,
  rustc,
  tailwindcss,
  wasm-bindgen-cli_0_2_104,
  openssl,
  pkg-config,
}:
rustPlatform.buildRustPackage rec {
  pname = "pointer";
  version = "0.1.0";
  outputs = ["out" "web" "backend" "indexer"];

  src = lib.cleanSource ../.;

  cargoHash = lib.fakeHash;

  cargoLock = {
    lockFile = ../Cargo.lock;
    allowBuiltinFetchGit = true;
  };

  nativeBuildInputs = [
    wasm-bindgen-cli_0_2_104
    binaryen
    cargo-leptos
    rustc.llvmPackages.lld
    makeWrapper
    pkg-config
  ];

  buildInputs = [openssl.dev];

  buildPhase = ''
    runHook preBuild
    export SQLX_OFFLINE=true
    export SQLX_OFFLINE_DIR=$PWD/.sqlx
    cargo leptos build --release
    export SQLX_OFFLINE_DIR=$PWD/backend/.sqlx
    cargo build --release --package pointer-backend --bin pointer-backend
    cargo build --release --package pointer-indexer --bin pointer-indexer

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin $web/bin $web/share/target $backend/bin $indexer/bin
    cp target/release/pointer-indexer $indexer/bin/pointer-indexer
    cp target/release/pointer-backend $backend/bin/pointer-backend
    cp target/release/pointer $web/bin/pointer
    cp Cargo.toml $web/share
    cp -r target/site $web/share/target/site
    wrapProgram $web/bin/pointer --chdir $web/share

    ln -sf $indexer/bin/pointer-indexer $out/bin/pointer-indexer
    ln -sf $backend/bin/pointer-backend $out/bin/pointer-backend
    ln -sf $web/bin/pointer $out/bin/pointer

    runHook postInstall
  '';

  meta = {
    description = "Code search tool with a Leptos web UI";
    license = lib.licenses.unlicense;
    mainProgram = "pointer";
  };
}
