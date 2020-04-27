# Derviations for cross-compiling a Haskell source package.

{ name, src, pkgs, compiler }:

let
  overlay =
    self: super:
      let
        lib = self.haskell.lib;

        addExternalInterpreter =
          drv:
            lib.appendConfigureFlags
              drv
              [ "--ghc-options=-fexternal-interpreter" ];

        overrides =
          final: prev: {
            "${name}" = prev.callCabal2nix name src {};

            # The following packages fail to build with Haddock, apparently
            # due to it panicing when trying to run coverage.
            binary-parser = lib.dontHaddock prev.binary-parser;
            cmdargs = lib.dontHaddock prev.cmdargs;
            vector-builder = lib.dontHaddock prev.vector-builder;
            QuickCheck = lib.dontHaddock prev.QuickCheck;
            bytestring-strict-builder = lib.dontHaddock prev.bytestring-strict-builder;
            bytestring-tree-builder = lib.dontHaddock prev.bytestring-tree-builder;
            loch-th = lib.dontHaddock prev.loch-th;

            data-dword = addExternalInterpreter prev.data-dword;
            generics-sop = addExternalInterpreter prev.generics-sop;
            th-orphans = addExternalInterpreter prev.th-orphans;
            math-functions = addExternalInterpreter prev.th-orphans;
            jose = addExternalInterpreter prev.jose;
          };
      in
        {
          haskell = super.haskell // {
            packages = super.haskell.packages // {
              "${compiler}" =
                super.haskell.packages."${compiler}".override
                  { inherit overrides; };
            };
          };
        };

  aarch64Pkgs =
    # there is also '...-multiplatform-musl'
    pkgs.pkgsCross.aarch64-multiplatform.appendOverlays [ overlay ];
in
{
  aarch64 =
    aarch64Pkgs.haskell.packages."${compiler}"."${name}";
}
