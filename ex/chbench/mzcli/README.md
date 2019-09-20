mzcli build
===========

Run `_build.sh` to build the docker image, and `_push.sh` to push it to the docker hub,
after you've verified it.

Building mzcli is a little touchy because the mzcli command itself is currently closed
source. Running the _build.sh script will open a web browser to download a zipfile and
move things around.
