help([[
For detailed instructions, go to:
   https://github.com/blazingdb/blazingsql
]])
whatis("Version: 0.16")
whatis("Keywords: GPU, SQL, Engine")
whatis("URL: https://blazingsql.com/")
whatis("Description: SQL Engine")

setenv("JAVA_HOME", "/usr/lib/jvm/jre")
setenv("CONDA_PREFIX", "/opt/blazingsql-powerpc-prefix")

prepend_path("PATH", "/opt/blazingsql-powerpc-prefix/bin")

prepend_path("LD_LIBRARY_PATH", "/usr/local/lib")
prepend_path("LD_LIBRARY_PATH", "/usr/local/lib64")
prepend_path("LD_LIBRARY_PATH", "/opt/blazingsql-powerpc-prefix/lib")

