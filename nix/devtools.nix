{ buildEnv
, checkedShellScript
, entr
, git
, hlint
, nixpkgs-fmt
, silver-searcher
, stylish-haskell
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
        ${style}/bin/${style.name}

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
in
buildEnv {
  name = "postgrest-devtools";
  paths = [ style.bin styleCheck.bin lint.bin watch.bin ];
}
