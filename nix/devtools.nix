{ writeShellScriptBin
, buildEnv
, entr
, git
, hlint
, nixpkgs-fmt
, silver-searcher
, stylish-haskell
}:
let
  style =
    writeShellScriptBin "postgrest-style"
      ''
        set -euo pipefail

        rootdir="$(${git}/bin/git rev-parse --show-toplevel)"

        # Format Nix files
        ${nixpkgs-fmt}/bin/nixpkgs-fmt "$rootdir" > /dev/null 2> /dev/null

        # Format Haskell files
        ${silver-searcher}/bin/ag -l -g '\.l?hs$' . "$rootdir" \
          | xargs ${stylish-haskell}/bin/stylish-haskell -i
      '';

  # Script to check whether any uncommited changes result from postgrest-style
  styleCheck =
    writeShellScriptBin "postgrest-style-check"
      ''
        set -euo pipefail

        ${style}/bin/${style.name}

        ${git}/bin/git diff-index --exit-code HEAD -- '*.hs' '*.lhs' '*.nix'
      '';

  lint =
    writeShellScriptBin "postgrest-lint"
      ''
        set -euo pipefail

        rootdir="$(${git}/bin/git rev-parse --show-toplevel)"

        # Lint Haskell files
        ${silver-searcher}/bin/ag -l -g '\.l?hs$' "$rootdir" \
          | xargs ${hlint}/bin/hlint -X QuasiQuotes -X NoPatternSynonyms
      '';

  watch =
    writeShellScriptBin "postgrest-watch"
      ''
        set -euo pipefail

        rootdir="$(${git}/bin/git rev-parse --show-toplevel)"

        while true; do
          ${silver-searcher}/bin/ag -l . "$rootdir" | ${entr}/bin/entr -rd "$@"
        done
      '';
in
buildEnv {
  name = "postgrest-devtools";
  paths = [ style styleCheck lint watch ];
}
