import docker
from time import sleep

def start_backend(backendType: str, **args):
    try:
        print(f"Starting backend for {backendType}")
        client = docker.from_env()
        container = client.containers.run(**args, detach=True, auto_remove=True)
        sleep(20)
        print(f"{backendType} Backend status:" + str(container.status))
        return container
    except docker.errors.DockerException as e:
        raise Exception(e)
        return None

def stop_backend(container):
    try:
        if container:
            print("Stopping backend...")
            container.kill()
    except docker.errors.DockerException as e:
        raise Exception(e)
        return None
