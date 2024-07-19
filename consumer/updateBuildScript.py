# Stops, Removes, Builds and Runs an updated Kafka consumer container, remove dangling images

import subprocess
import time

def execute_docker_command(command):
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        exit(1)


# Check if container exists
container_exists = subprocess.run("docker ps -aq --filter name=kafka-consumer", capture_output=True, check=False).stdout.decode().strip()

if container_exists:
  # Stop and remove container if it exists
  execute_docker_command("docker stop kafka-consumer")
  execute_docker_command("docker rm kafka-consumer")

# Build and run the container
execute_docker_command("docker build -t kafka-consumer .")
execute_docker_command("docker run -d --network=kafka-compose_kafka-network --name kafka-consumer kafka-consumer")

# Prune dangling images
execute_docker_command('docker image prune -f --filter "dangling=true"')


time.sleep(2)

print("Kafka consumer container started successfully!")