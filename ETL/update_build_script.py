# Stops, Removes, Builds and Runs an updated etl-service container, removes dangling images

import subprocess
import time

def execute_docker_command(command):
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        exit(1)


# Check if container exists
container_exists = subprocess.run("docker ps -aq --filter name=etl-service", capture_output=True, check=False).stdout.decode().strip()

if container_exists:
  # Stop and remove container if it exists
  execute_docker_command("docker stop etl-service")
  execute_docker_command("docker rm etl-service")

# Build and run the container
execute_docker_command("docker build -t etl-service .")
execute_docker_command("docker run -d --network=kafka-compose_kafka-network --name etl-service etl-service")

# Prune dangling images
execute_docker_command('docker image prune -f --filter "dangling=true"')


time.sleep(2)

print("etl-service container started successfully!")