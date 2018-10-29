#!/usr/bin/env bash

SLIDES_HTML=slides.html
SLIDES_SOURCE=slides.md

LESSC=$(type -p lessc)
PANDOC=$(type -p pandoc)
GIT=$(type -p git)

die() {
  echo "$@" >&2
  exit 1
}

if [ -z "$PANDOC" ]
then
  die "Can't find pandoc executable in path."
fi

if [ -z "$GIT" ]
then
  die "Can't find git executable in path."
fi

if [ -z "$LESSC" ]
then
  die "Can't find lessc executable in path."
fi

if [ ! -d "reveal.js" ]
then
  echo "Cloning reveal.js Git repo..."
  $GIT clone https://github.com/hakimel/reveal.js.git
fi

echo "Creating CSS..."
lessc style.less style.css || die "lessc failed."

echo "Building slides..."
$PANDOC \
    --self-contained \
    --css style.css \
    -i \
    -t revealjs \
    -s \
    -o $SLIDES_HTML \
    $SLIDES_SOURCE || \
die "pandoc failed."

echo "Done. Standalone reveal.js slide presentation is in $SLIDES_HTML."
