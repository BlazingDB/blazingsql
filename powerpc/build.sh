build.sh
 -> Asume que existe en el system ciertas dependencias base
 -> blazingsql.tar.gz: pyenv, rmm, cudf, daskcudf, deps blazing (zeromq, spdlog), blazingsql
    /
      bin/
      lib/
      lib[ARCH?64?]/
      include/
      blazingsql.info
         commit hash
         version + repos/usls for each dependency (python, cuda, cudf, ...)

 -> blazingsql.tar.gz -> descomprimido puede ser usado como pyenv y alli se puede correr blazingsql

 -> Issues
    As soon it fails we need to have commands and ask to fill an issue

 -> pyenv: tmp
   - no va a estar versionada
   - antes de empezar el build borrar el tmp (cada vez)
   - el user nunca tiene que entrar o nodeberia tener necesidad de entrar al folder tmp
   - al finalzar de generar el paquete se debe borrar

 -> Implementaiton details
   - Dependencias basicas de (module list) van al systema (DockerFile)
   - El docker container ejcutara el build.
   - El docker ya ejecuto install-dependencies.sh y ya tiene las dependencia ejecutara build.sh
   - La imagen de Docker para este proyecto es solo un env para ejecutar el build.sh: 
     - Esa imagen esta basada en centos7 sera creada con el script de install-dependencies.sh

 -> Ambos de los 2 siguientes scripts debete tner el mismo comportamiento:
   ./build-with-docker.sh (este se baja el container la imagen y entro ejecuta el build.sh)
   ./build.sh (aki no hay nada de docker: asumes que tienes todas las dependencias)

  -> Uses cases:
     User:
       instalar ciertas dependencias en tu system
       ./build.sh ... blazingsql.tar.gz :)
     Power user/Dev:
       ./build-with-docker.sh

  -> Folder strcuture:
blazingsql/powerpc/
                   _old_
                   build.sh -> ultima version del god script -> basarnos
                   install-dependencies.sh
                   DockerFile
                     RUN install-dependencies.sh
                   docker-build.sh -> construye la imagen basada en Dockerfile con nombre/label fijo/standard
                   version.properties (python 3.7, cuda10.1, rapids/blazingsql 0.15...)
                   build-with-docker.sh
                   patches/
                   # siguiente sprint
                   blazingsql.tar.gz -> package.sh -> spack.package
