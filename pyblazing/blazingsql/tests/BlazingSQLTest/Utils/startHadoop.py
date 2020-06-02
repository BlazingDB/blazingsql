import os
import subprocess
import sys

from Configuration import Settings as Settings

def start_hdfs():

    os.environ['JAVA_HOME'] = os.getenv('CONDA_PREFIX')
    os.environ['HADOOP_HOME'] = Settings.data['TestSettings']['hadoopDirectory']

    if(os.getenv('LD_LIBRARY_PATH') is None):
        os.environ['LD_LIBRARY_PATH'] = os.getenv('HADOOP_HOME') + "/lib/native/"
    else:
        os.environ['LD_LIBRARY_PATH'] = os.getenv('LD_LIBRARY_PATH') + ":" + os.getenv('HADOOP_HOME') + "/lib/native/"

    os.environ['PATH'] = os.getenv('PATH') + ":" + os.getenv('HADOOP_HOME') + "/bin"

    import subprocess
    callGlob = subprocess.run([os.getenv('HADOOP_HOME')+"/bin/hdfs", 'classpath', '--glob'], stdout=subprocess.PIPE)

    os.environ['CLASSPATH'] = callGlob.stdout.decode('utf-8').rstrip()
    os.environ['ARROW_LIBHDFS_DIR'] = os.getenv('PATH') + ":" + os.getenv('HADOOP_HOME') + "/lib/native/"

    print("JAVA_HOME: " + os.getenv('CONDA_PREFIX'))
    print("HADOOP_HOME: " + os.getenv('HADOOP_HOME'))
    print("PATH: " + os.getenv('PATH'))
    print("LD_LIBRARY_PATH: " + os.getenv('LD_LIBRARY_PATH'))
    print("CLASSPATH: " + os.getenv('CLASSPATH'))
    print("ARROW_LIBHDFS_DIR: " + os.getenv('ARROW_LIBHDFS_DIR'))

    getEnvVars = subprocess.run(['printenv'], stdout=subprocess.PIPE)

    env = dict()
    print("Starting docker-compose ...")

    command = ['../KrbHDFS/start_hdfs.sh', Settings.data['TestSettings']['hadoopDirectory'], Settings.data['TestSettings']['dataDirectory']]
    environ = dict(os.environ)
    proc = subprocess.Popen(' '.join(command), env=environ, shell=True)
    proc.wait()


COMPOSE_FILE = "../KrbHDFS/docker-compose.yml"

def stop_hdfs():
    env = dict()
    command = ['docker-compose', '-f', COMPOSE_FILE, 'down']
    print("Shutting down docker-compose ...")
    proc = subprocess.Popen(' '.join(command), env=env, shell=True)
    proc.wait()

    command = ['sudo', 'cp', '/etc/hosts.old', '/etc/hosts']
    print("Restoring /etc/hosts file ...")
    proc = subprocess.Popen(' '.join(command), env=env, shell=True)
    proc.wait()
