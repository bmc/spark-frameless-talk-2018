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

The `notebooks` folder contains the individual notebooks used during the
presentation. You'll need all three. If you want, you can import them
individually. Or, you can simply download and import the `notebooks.dbc`
file in this directory; it contains all three notebooks.

For information on how to import notebooks into Databricks, including
Databricks Community Edition, see
<https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#import-a-notebook>

There are three notebooks:

- `Defs.scala`: definitions shared across the other two notebooks (each of
  which invokes `Defs`)
- `00-Create-Data-Files.scala`, which downloads a data file of tweets from
  early 2018 and also parses a Kafka stream of current tweets, producing
  the new data files needed by the presentation. Follow the instructions
  in this notebook to create local copies of the data.
- `01-Presentation.scala` is the hands-on notebook part of the presentation.

