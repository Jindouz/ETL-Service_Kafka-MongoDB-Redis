FROM bitnami/zookeeper:latest

# Expose the default Zookeeper client port
EXPOSE 2181

# Choose between authentication methods (comment out the unwanted option)

# Option 1: Allow Anonymous Login (Not Recommended for Production)
ENV ALLOW_ANONYMOUS_LOGIN=yes

# Option 2: Configure Authentication (Recommended - Requires Additional Setup)
# VOLUME /opt/bitnami/zookeeper/conf  # Mount your custom configuration file here

# Optional: Set environment variables for Zookeeper configuration
# Replace these with your desired settings (if using custom config)
ENV ZOOKEEPER_SERVER_ID=1
# ENV ZOOKEEPER_NODE_COUNT=1

# Run Zookeeper server in foreground
CMD ["zkServer.sh", "start-foreground"]
