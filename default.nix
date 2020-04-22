let
  name =
    "postgrest";

  # PostgREST source files, filtered based on the rules in the .gitignore files
  # and file extensions. We want to include as litte as possible, as the files
  # added here will increase the space used in the Nix store and trigger the
  # build of new Nix derivations when changed.
  src =
    pkgs.lib.sourceFilesBySuffices
      (pkgs.gitignoreSource ./.)
      [ ".cabal" ".hs" ".lhs" "LICENSE" ];

  nixpkgsVersion =
    import nix/nixpkgs-version.nix;

  pinnedPkgs =
    builtins.fetchTarball {
      url = "https://github.com/nixos/nixpkgs/archive/${nixpkgsVersion.rev}.tar.gz";
      sha256 = nixpkgsVersion.tarballHash;
    };

  overlays =
    [
      (import nix/overlays/gitignore.nix)
    ];

  pkgs =
    import pinnedPkgs { inherit overlays; };

  # Base dynamic derivation for the PostgREST package.
  drv =
    pkgs.haskellPackages.callCabal2nix name src {};

  # Static derivation for the PostgREST executable.
  drvStatic =
    pkgs.callPackage nix/static.nix {
      inherit pkgs name src;
      # Currently only works with GHC 8.6.5.
      compiler = "ghc865";
    };

  lib =
    pkgs.haskell.lib;
in
rec {
  inherit pkgs pinnedPkgs;

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

  # Environment in which PostgREST can be built with cabal, useful e.g. for
  # defining a shell for nix-shell.
  env =
    drv.env;

  # Utility for updating the pinned version of Nixpkgs.
  nixpkgsUpgrade =
    pkgs.callPackage nix/nixpkgs-upgrade.nix {};

  # Static executable.
  postgrestStatic =
    lib.justStaticExecutables (lib.dontCheck drvStatic);
}
