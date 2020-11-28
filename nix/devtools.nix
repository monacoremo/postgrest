{ buildEnv
, cabal-install
, checkedShellScript
, entr
, git
, hlint
, nixpkgs-fmt
, silver-searcher
, stylish-haskell
, tests
}:
let
  style =
    checkedShellScript "postgrest-style"
      ''
        rootdir="$(${git}/bin/git rev-parse --show-toplevel)"

        # Format Nix files
        ${nixpkgs-fmt}/bin/nixpkgs-fmt "$rootdir" > /dev/null 2> /dev/null

        # Format Haskell files
        ${silver-searcher}/bin/ag -l -g '\.l?hs$' . "$rootdir" \
          | xargs ${stylish-haskell}/bin/stylish-haskell -i
      '';

  # Script to check whether any uncommited changes result from postgrest-style
  styleCheck =
    checkedShellScript "postgrest-style-check"
      ''
        ${style}

        ${git}/bin/git diff-index --exit-code HEAD -- '*.hs' '*.lhs' '*.nix'
      '';

  lint =
    checkedShellScript "postgrest-lint"
      ''
        rootdir="$(${git}/bin/git rev-parse --show-toplevel)"

        # Lint Haskell files
        ${silver-searcher}/bin/ag -l -g '\.l?hs$' "$rootdir" \
          | xargs ${hlint}/bin/hlint -X QuasiQuotes -X NoPatternSynonyms
      '';

  watch =
    checkedShellScript "postgrest-watch"
      ''
        rootdir="$(${git}/bin/git rev-parse --show-toplevel)"

        while true; do
          ${silver-searcher}/bin/ag -l . "$rootdir" | ${entr}/bin/entr -rd "$@"
        done
      '';

  pushCachix =
    checkedShellScript "postgrest-push-cachix"
      ''
        nix-store -qR --include-outputs "$(nix-instantiate)" \
          | cachix push postgrest
      '';

  build =
    checkedShellScript "postgrest-build"
      ''exec ${cabal-install}/bin/cabal v2-build "$@"'';

  run =
    checkedShellScript "postgrest-run"
      ''exec ${cabal-install}/bin/cabal v2-run postgrest -- "$@"'';

  clean =
    checkedShellScript "postgrest-clean"
      ''
        ${cabal-install}/bin/cabal v2-clean
      '';

  check =
    checkedShellScript "postgrest-check"
      ''
        ${style}
        ${tests}/bin/postgrest-test-spec-all
      '';
in
buildEnv {
  name = "postgrest-devtools";
  paths = [
    style.bin
    styleCheck.bin
    lint.bin
    watch.bin
    pushCachix.bin
    build.bin
    run.bin
    clean.bin
    check.bin
  ];
}
