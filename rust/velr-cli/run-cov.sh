#!/bin/sh
set -eu

# File with glob patterns to exclude (one per line, # = comment)
COVIGNORE_FILE="${COVIGNORE_FILE:-.covignore}"

# Make a temp for cleaned patterns (comments removed)
patterns_tmp="$(mktemp)"
cleanup() { rm -f "$patterns_tmp"; }
trap cleanup EXIT

if [ -f "$COVIGNORE_FILE" ]; then
  # Strip blank lines and comments
  awk '!/^[[:space:]]*($|#)/ {print}' "$COVIGNORE_FILE" > "$patterns_tmp"
else
  # Reasonable defaults
  cat > "$patterns_tmp" <<'EOF'
build.rs
target/**
src/legacy/**
src/ffi/bindings.rs
generated/**
examples/**
benches/**
tests/**
EOF
fi

# Convert globs to a single regex alternation using awk (portable on macOS/BSD/Linux)
IGNORE_RE="$(awk '
function esc_re(s,   i,c,out) {
  out="";
  for (i=1;i<=length(s);i++) {
    c=substr(s,i,1);
    # Escape regex metachars: . [ ] { } ( ) + * ? ^ $ | \
    if (c ~ /[.[\]{}()+*?^$|\\]/) out = out "\\" c;
    else out = out c;
  }
  return out;
}
{
  pat = $0;
  # Temporarily mark globs
  gsub(/\*/, "\001STAR\001", pat);
  gsub(/\?/, "\001QMARK\001", pat);

  pat = esc_re(pat);

  # Restore globs as regex
  gsub(/\001STAR\001/, ".*", pat);
  gsub(/\001QMARK\001/, ".",  pat);

  pats[++n] = pat;
}
END {
  if (n == 0) { print ""; exit }
  printf("(");
  for (i=1;i<=n;i++) {
    if (i>1) printf("|");
    printf("%s", pats[i]);
  }
  printf(")");
}
' "$patterns_tmp")"

echo "Running coverage with ignore regex:"
[ -n "$IGNORE_RE" ] && echo "  $IGNORE_RE" || echo "  <none>"
echo

# Pass through any extra args, e.g. --open, --lcov, etc.
if [ -n "$IGNORE_RE" ]; then
  exec cargo llvm-cov --workspace --ignore-filename-regex "$IGNORE_RE" "$@"
else
  exec cargo llvm-cov --workspace "$@"
fi