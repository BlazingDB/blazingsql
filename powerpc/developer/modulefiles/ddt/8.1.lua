prereq("cmake/3.18.2")

help([[For detailed instructions, go to:
   https://...

]])
whatis("Version: 8.1")
whatis("Keywords: System, Utility")
whatis("URL: http://content.allinea.com/downloads/userguide.pdf")
whatis("Description: Parallel, graphical, symbolic debugger")
setenv("DDTPATH","/opt/apps/ddt/8.1/bin")
prepend_path("PATH","/opt/apps/ddt/8.1/bin")
prepend_path("LD_LIBRARY_PATH","/opt/apps/ddt/8.1/lib")
