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
    cargo leptos build --release

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin $out/share
    cp target/release/pointer $out/bin
    cp -r target/site $out/share
    wrapProgram $out/bin/pointer --set LEPTOS_SITE_ROOT $out/share/site

    runHook postInstall
  '';

  meta = {
    description = "Code search tool with a Leptos web UI";
    license = lib.licenses.unlicense;
    mainProgram = "pointer";
  };
}
