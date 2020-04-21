let
  name =
    "postgrest";

  compiler =
    "ghc883";

  # PostgREST source files, filtered based on the rules in the .gitignore files
  # and file extensions. We want to include as litte as possible, as the files
  # added here will increase the space used in the Nix store and trigger the
  # build of new Nix derivations when changed.
  src =
    pkgs.lib.sourceFilesBySuffices
      (pkgs.gitignoreSource ./.)
      [ ".cabal" ".hs" ".lhs" "LICENSE" ];

  # Commit of the Nixpkgs repository that we want to use.
  nixpkgsVersion =
    import nix/nixpkgs-version.nix;

  # Nix files that describe the Nixpkgs repository. We evaluate the expression
  # using `import` below.
  nixpkgs =
    builtins.fetchTarball {
      url = "https://github.com/nixos/nixpkgs/archive/${nixpkgsVersion.rev}.tar.gz";
      sha256 = nixpkgsVersion.tarballHash;
    };

  overlays =
    [
      (import nix/overlays/postgresql-legacy.nix)
      (import nix/overlays/gitignore.nix)
      (import nix/overlays/haskell-packages)
    ];

  # Evaluated expression of the Nixpkgs repository.
  pkgs =
    import nixpkgs { inherit overlays; };

  postgresqlVersions =
    [
      pkgs.postgresql_12
      pkgs.postgresql_11
      pkgs.postgresql_10
      pkgs.postgresql_9_6
      pkgs.postgresql_9_5
      pkgs.postgresql_9_4
    ];

  patches =
    pkgs.callPackage nix/patches {};

  # Base dynamic derivation for the PostgREST package.
  drv =
    pkgs.haskellPackages.callCabal2nix name src {};

  # Static derivation for the PostgREST executable.
  drvStatic =
    import nix/static { inherit nixpkgs name src compiler patches; };

  lib =
    pkgs.haskell.lib;
in
rec {
  inherit nixpkgs pkgs;

  # Derivation for the PostgREST Haskell package, including the executable,
  # libraries and documentation. We disable running the test suite on Nix
  # builds, as they require a database to be set up.
  postgrestWithLib =
    lib.dontCheck drv;

  # Derivation for just the PostgREST binary, where we strip all dynamic
  # libraries and documentation, leaving only the executable. Note that the
  # executable is static with regards to Haskell libraries, but not system
  # libraries like glibc and libpq.
  postgrest =
    lib.justStaticExecutables postgrestWithLib;

  # Static executable.
  postgrestStatic =
    lib.justStaticExecutables (lib.dontCheck drvStatic);

  dockerImage =
    pkgs.dockerTools.buildImage {
      inherit name;
      tag = "latest";
      contents = postgrest;
      runAsRoot = ''
        #!${pkgs.stdenv.shell}
        ${pkgs.dockerTools.shadowSetup}
        groupadd --system postgrest
        useradd --system --gid postgrest postgrest
      '';
      config = {
        Cmd = [ "/bin/postgrest" docker/postgrest.conf ];
        Env = [
          "PGRST_DB_URI="
          "PGRST_DB_URI="
          "PGRST_DB_SCHEMA=public"
          "PGRST_DB_ANON_ROLE="
          "PGRST_DB_POOL=100"
          "PGRST_DB_EXTRA_SEARCH_PATH=public"
          "PGRST_SERVER_HOST=*4"
          "PGRST_SERVER_PORT=3000"
          "PGRST_OPENAPI_SERVER_PROXY_URI="
          "PGRST_JWT_SECRET="
          "PGRST_SECRET_IS_BASE64=false"
          "PGRST_JWT_AUD="
          "PGRST_MAX_ROWS="
          "PGRST_PRE_REQUEST="
          "PGRST_ROLE_CLAIM_KEY=.role"
          "PGRST_ROOT_SPEC="
          "PGRST_RAW_MEDIA_TYPES="
        ];
        ExposedPorts = {
          "3000/tcp" = {};
        };
        User = "postgrest";
      };
    };

  loadDocker =
    pkgs.writeShellScriptBin "postgrest-docker-load"
      ''
        set -euo pipefail

        docker load -i ${dockerImage}
      '';

  # Environment in which PostgREST can be built with cabal, useful e.g. for
  # defining a shell for nix-shell.
  env =
    drv.env;

  # Utility for updating the pinned version of Nixpkgs.
  nixpkgsUpgrade =
    pkgs.callPackage nix/nixpkgs-upgrade.nix {};

  tests =
    pkgs.callPackage nix/tests.nix
      {
        inherit postgresqlVersions;
        postgrestBuildEnv = env;
      };

  style =
    pkgs.callPackage nix/style.nix {};

  lint =
    pkgs.callPackage nix/lint.nix {};
}
