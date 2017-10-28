word2vec patch for Mac OS X
===========================


## Purpose

[word2vec](https://code.google.com/p/word2vec/) was written in plain C, but some gotcha forbidded it to be compiled on Mac OS X:

- Non-standard header `<malloc.h>` instead of `<stdlib.h>`. 

- Non portable `gzip` handling in demo scripts.

This repository tries to fix them. See "[Issue 1: Build for Mac?](https://code.google.com/p/word2vec/issues/detail?id=1)" for more info.


## Status

Updated to [r37](https://code.google.com/p/word2vec/source/detail?r=37) (revision date: Dec 18, 2013).

I'll try to keep track of original [word2vec](https://code.google.com/p/word2vec/) project.


## Quick start

- Download the code: `git clone https://github.com/William-Yeh/word2vec-mac.git`

- Run `make` to compile word2vec tool

- Run the demo scripts: `./demo-word.sh` and `./demo-phrases.sh`


## For more information

Of course, official [word2vec](https://code.google.com/p/word2vec/) page hosted at Google Code.


## License

[Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0), same as original project.