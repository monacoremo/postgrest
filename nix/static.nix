{ pkgs, compiler, name, src }:
let
  static-haskell-nix =
    let
      rev = "bb4c1e27e391eff01591fe60830ff68a9ada41ef";
    in
      builtins.fetchTarball {
        url = "https://github.com/monacoremo/static-haskell-nix/archive/${rev}.tar.gz";
        sha256 = "15zyaii6c5pangyzz69qksg6sc6d5qzbcqxxwz0bm6gb5igpwhym";
      };

  overlay =
    self: super:
      let
        lib = self.haskell.lib;
        unbreak = drv: lib.overrideCabal drv (drv: { broken = false; });

        overrides =
          final: prev:
            {
              postgrest = prev.callCabal2nix name src {};

              # The tests for the packages below took a long time on static
              # builds, so we disable them for now - to be investigated.
              happy = lib.dontCheck prev.happy;
              text-short = lib.dontCheck prev.text-short;
              jose = lib.dontCheck prev.jose;
            };
      in
        {
          haskell = super.haskell // {
            packages = super.haskell.packages // {
              integer-simple = super.haskell.packages.integer-simple // {
                "${compiler}" = super.haskell.packages.integer-simple."${compiler}".override
                  { inherit overrides; };
              };
            };
          };
        };

  normalPkgs =
    pkgs.appendOverlays [ overlay ];

  survey =
    import "${static-haskell-nix}/survey" {
      inherit normalPkgs compiler;
      integer-simple = true;
    };
in
survey.haskellPackages.postgrest
