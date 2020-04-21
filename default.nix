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

  # Revision and hash of the tarball that contains the version of Nixpkgs that
  # we want to use.
  nixpkgsVersion =
    import nix/nixpkgs-version.nix;

  pinnedPkgs =
    builtins.fetchTarball {
      url = "https://github.com/nixos/nixpkgs/archive/${nixpkgsVersion.rev}.tar.gz";
      sha256 = nixpkgsVersion.tarballHash;
    };

  # With overlays, we can add and override derivations in our version of `pkgs`.
  overlays =
    [
      (import nix/overlays/gitignore.nix)
    ];

  pkgs =
    import pinnedPkgs { inherit overlays; };

  ghcVersion =
    "ghc883";

  # Use integer-simple instead of GMP. GMP can be faster, but is licensed under
  # the LGPL and we want to avoid the risk of PostgREST becoming a 'derivative
  # work' when linking a static executable. See also:
  # https://github.com/NixOS/nixpkgs/blob/master/doc/languages-frameworks/haskell.section.md#building-ghc-with-integer-simple
  haskellPackages =
    pkgs.haskell.packages.integer-simple."${ghcVersion}".override
      {
        overrides =
          self: super:
            {
              cryptonite = super.cryptonite.overrideAttrs
                (
                  oldAttrs:
                    {
                      configureFlags =
                        oldAttrs.configureFlags ++ [ "-f-integer-gmp" ];
                    }
                );
            };
      };

  # Base PostgREST derivation, generated from the `postgrest.cabal` file.
  drv =
    haskellPackages.callCabal2nix name src {};
in
rec {
  inherit pkgs pinnedPkgs;

  # Derivation for the PostgREST Haskell package, including the executable,
  # libraries and documentation. We disable running the test suite on Nix
  # builds, as they require a database to be set up.
  postgrestPackage =
    pkgs.haskell.lib.dontCheck drv;

  # Derivation for just the PostgREST binary, where we strip all dynamic
  # libraries and documentation, leaving only the executable.
  postgrest =
    pkgs.haskell.lib.justStaticExecutables postgrestPackage;

  # Environment in which PostgREST can be built with cabal, useful e.g. for
  # defining a shell for nix-shell.
  env =
    drv.env;

  # Utility for updating the pinned version of Nixpkgs.
  nixpkgsUpgrade =
    pkgs.callPackage nix/nixpkgs-upgrade.nix {};
}
