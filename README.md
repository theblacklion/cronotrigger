Fast backup tool for Ubuntu 12.04+ based distros (like ElementaryOS, Linux Mint etc.) using Python 2.7/3.3.

ATTENTION: This is work in progress. Backup and restore work now but many things have to get into proper shape and might change. But in all cases I try to keep it compatible as much as I can.

Goals are:
* Scan as fast as possible (using harmless tricks).
* Backup as much as possible.
* Backup as fast as possible (using multi threading).
* (Rebuild same files as hard links (like time machine etc.).)
* Keep the footprint as low as possible (don't hog cpu/ram, let the harddrives sweat).
* Stay out of the way but still provide meaningful logging.
* Provide a nice simple GUI.

Let's see if this works out! :)


Requirements:
* Python (one of / 3.3 is best)
  * 2.7 with the gi module (probably built in)
  * 3.0 with the gi module (had to install python3-gi)
  * 3.3 with the gi module (had to install python3-gi)
* Python scandir module for speedup (pip install thirdparty/scandir)

You want to setup a virtualenv for installing the scandir module. Later on I'm
planning to provide everything as a ppa.


[![Join the chat at https://gitter.im/theblacklion/cronotrigger](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/theblacklion/cronotrigger?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)