# Source to my "Introduction to Apache Spark using Frameless" talk

This repo contains the sources for both the slides and the Databricks
notebooks for my _Introduction to Apache Spark using Frameless_ talk,
given at [ScalaIO](https://scala.io/talks.html#/#WUW-9677) and at
[Scale by the Bay](http://scale.bythebay.io/) in 2018.

## The slides

The slides are written in Markdown and must be translated to HTML+Reveal.JS
using [Pandoc](https://pandoc.org). The following executables must be present
in your shell's PATH to build the slides:

- `pandoc` (version 2.3.1 or better)
- `lessc` (version 3.0.4 or better), for LESS stylesheet translation
- `git`, to check out the Reveal.js repository.

To build the slides, just run `./build.sh`. It'll build a standalone
`slides.html` file in the top-level directory.

## The Databricks notebooks

The `notebooks` folder contains the notebooks used during the presentation.
You can import these notebooks into Databricks, including Databricks Community
Edition, by following the instructions at 
<https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#import-a-notebook>

## The data

TBD
