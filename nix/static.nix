# Turn a Haskell source package (given its name and source) into a derivation
# for a fully static executable.
{ pkgs, compiler, name, src }:
let
  # The nh2/static-haskell-nix project does all the hard work for us for
  # building static Haskell executables. We are using a fork here until a patch
  # that is needed for PostgREST is merged. See:
  # https://github.com/nh2/static-haskell-nix/pull/91
  # statix-haskell-nix builds everything based on Musl instead of glibc, so
  # there will be a _lot_ to rebuild if you don't use a binary cache.
  static-haskell-nix =
    let
      rev = "bb4c1e27e391eff01591fe60830ff68a9ada41ef";
    in
      builtins.fetchTarball {
        url = "https://github.com/monacoremo/static-haskell-nix/archive/${rev}.tar.gz";
        sha256 = "15zyaii6c5pangyzz69qksg6sc6d5qzbcqxxwz0bm6gb5igpwhym";
      };

  # This overlay adds our source package and applies adjustments to the
  # derivation of other packages that it depends on. The overlay applies to
  # Haskell packages for the given compiler, which we will later use
  # with the static-haskell-nix survey.
  overlay =
    self: super:
      let
        overrides =
          final: prev:
            {
              # Add our source package.
              "${name}" =
                #prev.callPackage ./postgrest.nix { inherit name src; };
                prev.callCabal2nix name src {};

              Cabal = prev.Cabal_3_2_0_0;

              # The tests for the packages below took a long time on static
              # builds, so we disable them for now - to be investigated.
              happy = self.haskell.lib.dontCheck prev.happy;
              text-short = self.haskell.lib.dontCheck prev.text-short;
              jose = self.haskell.lib.dontCheck prev.jose;
              tls = self.haskell.lib.dontCheck prev.tls;
            };
      in
        # Override the set of Haskell packages at
        # pkgs.haskell.packages."${compiler}".
        {
          haskell = super.haskell // {
            packages = super.haskell.packages // {
              "${compiler}" =
                super.haskell.packages."${compiler}".override
                  { inherit overrides; };
            };
          };
        };

  # Apply our overlay to the given pkgs.
  normalPkgs =
    pkgs.appendOverlays [ overlay ];

  # Let the static-haskell-nix project do the hard work of deriving a set of
  # fully static Haskell executables, including one for the our source package
  # that we added through the overlay.
  survey =
    import "${static-haskell-nix}/survey" { inherit normalPkgs compiler; };
in
  # Return the fully static derivation of our source package.
survey.haskellPackages."${name}"
