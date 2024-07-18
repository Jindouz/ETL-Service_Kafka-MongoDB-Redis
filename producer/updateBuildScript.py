import subprocess
import time

def execute_docker_command(command):
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        exit(1)

# Stop, Remove, Build and Run an updated Kafka producer container
execute_docker_command("docker stop kafka-producer")
execute_docker_command("docker rm kafka-producer")

execute_docker_command("docker build -t kafka-producer .")
execute_docker_command("docker run -d --network=kafka-compose_kafka-network --name kafka-producer kafka-producer")

time.sleep(3)

print("Kafka producer container started successfully!")