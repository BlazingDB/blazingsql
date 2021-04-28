import docker
from time import sleep

def start_backend(backendType: str, **args):
    try:
        client = docker.from_env()
        container = client.containers.run(**args)
        sleep(20)
        return container
    except docker.errors.DockerException as e:
        raise Exception(e)
        return None

def stop_backend(container):
    try:
        if container:
            container.kill()
    except docker.errors.DockerException as e:
        raise Exception(e)
        return None
