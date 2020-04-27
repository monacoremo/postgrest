# Derviations for cross-compiling a Haskell source package.

{ name, src, pkgs, compiler }:

let
  overlay =
    self: super:
      let
        overrides =
          final: prev: {
            "${name}" = prev.callCabal2nix name src {};
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
