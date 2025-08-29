#!/bin/bash

# Event Processing Pipeline Setup Script
# This script sets up the development environment and prepares the system

set -e

echo "🚀 Event Processing Pipeline Setup"
echo "=================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Check if Docker is installed and running
check_docker() {
    print_step "Checking Docker installation..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        echo "Visit: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    print_status "Docker is installed and running"
}

# Check if Docker Compose is available
check_docker_compose() {
    print_step "Checking Docker Compose..."
    
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker compose"
        print_status "Using Docker Compose V2"
    elif command -v docker-compose &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker-compose"
        print_status "Using Docker Compose V1"
    else
        print_error "Docker Compose is not installed."
        exit 1
    fi
}

# Check system resources
check_system_resources() {
    print_step "Checking system resources..."
    
    # Check available memory (minimum 8GB recommended)
    available_memory=$(free -g | awk '/^Mem:/{print $2}')
    if [ "$available_memory" -lt 8 ]; then
        print_warning "Less than 8GB RAM available. Pipeline may run slowly."
    else
        print_status "Sufficient memory available (${available_memory}GB)"
    fi
    
    # Check available disk space (minimum 10GB recommended)
    available_disk=$(df -BG . | awk 'NR==2 {print $4}' | sed 's/G//')
    if [ "$available_disk" -lt 10 ]; then
        print_warning "Less than 10GB disk space available."
    else
        print_status "Sufficient disk space available (${available_disk}GB)"
    fi
}

# Create necessary directories
create_directories() {
    print_step "Creating necessary directories..."
    
    directories=(
        "logs"
        "backups"
        "data/clickhouse"
        "data/mongodb"
        "data/kafka"
        "configs/grafana/dashboards"
    )
    
    for dir in "${directories[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            print_status "Created directory: $dir"
        fi
    done
}

# Set proper permissions
set_permissions() {
    print_step "Setting proper permissions..."
    
    # Make scripts executable
    chmod +x scripts/*.py scripts/*.sh 2>/dev/null || true
    
    # Set permissions for data directories
    chmod -R 755 data/ logs/ backups/ 2>/dev/null || true
    
    print_status "Permissions set correctly"
}

# Validate configuration files
validate_configs() {
    print_step "Validating configuration files..."
    
    required_configs=(
        "docker-compose.yaml"
        "requirements.txt"
        "configs/event-categories.yml"
        "configs/clickhouse/init-cluster.sql"
        "configs/mongodb/init-replica.js"
    )
    
    for config in "${required_configs[@]}"; do
        if [ ! -f "$config" ]; then
            print_error "Missing configuration file: $config"
            exit 1
        fi
    done
    
    print_status "Configuration files validated"
}

# Pull Docker images
pull_docker_images() {
    print_step "Pulling Docker images (this may take a while)..."
    
    $DOCKER_COMPOSE_CMD pull
    
    print_status "Docker images pulled successfully"
}

# Build custom images
build_custom_images() {
    print_step "Building custom Docker images..."
    
    $DOCKER_COMPOSE_CMD build --no-cache
    
    print_status "Custom images built successfully"
}

# Initialize Python virtual environment (optional)
setup_python_env() {
    if command -v python3 &> /dev/null; then
        print_step "Setting up Python virtual environment..."
        
        if [ ! -d "venv" ]; then
            python3 -m venv venv
            print_status "Virtual environment created"
        fi
        
        if [ -f "venv/bin/activate" ]; then
            source venv/bin/activate
            pip install -r requirements.txt
            print_status "Python dependencies installed"
        fi
    else
        print_warning "Python3 not found. Skipping virtual environment setup."
    fi
}

# Create sample configuration files
create_sample_configs() {
    print_step "Creating sample configuration files..."
    
    # Create .env file if it doesn't exist
    if [ ! -f ".env" ]; then
        cat > .env << EOF
# Event Processing Pipeline Configuration
ENVIRONMENT=development
DEBUG=true

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094
KAFKA_TOPIC=events_raw
KAFKA_GROUP=event-processing-group

# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=analytics
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# MongoDB Configuration
MONGODB_URL=mongodb://admin:password@localhost:27017,localhost:27018,localhost:27019/events?replicaSet=rs0&authSource=admin

# Processing Configuration
BATCH_SIZE=1000
MAX_WORKERS=4
HEALTH_PORT=8080

# Producer Configuration
EVENTS_PER_SECOND=1000
TOTAL_EVENTS=1000000
EOF
        print_status "Created .env configuration file"
    fi
}

# Test Docker Compose configuration
test_docker_compose() {
    print_step "Testing Docker Compose configuration..."
    
    $DOCKER_COMPOSE_CMD config > /dev/null
    
    print_status "Docker Compose configuration is valid"
}

# Display next steps
show_next_steps() {
    echo ""
    echo "🎉 Setup completed successfully!"
    echo ""
    echo "Next steps:"
    echo "==========="
    echo "1. Start the pipeline:"
    echo "   make up"
    echo ""
    echo "2. Initialize databases (wait 2-3 minutes after step 1):"
    echo "   make init-db"
    echo ""
    echo "3. Start event processing:"
    echo "   make start-pipeline"
    echo ""
    echo "4. Monitor the system:"
    echo "   make logs"
    echo "   make health"
    echo ""
    echo "5. Run benchmarks:"
    echo "   make benchmark"
    echo ""
    echo "Useful commands:"
    echo "==============="
    echo "• make help           - Show all available commands"
    echo "• make status         - Check service status"
    echo "• make logs           - View logs"
    echo "• make clean          - Clean up everything"
    echo ""
    echo "Web interfaces:"
    echo "==============="
    echo "• ClickHouse:   http://localhost:8123"
    echo "• Health Check: http://localhost:8080/health"
    echo "• Grafana:      http://localhost:3000 (admin/admin)"
    echo "• Prometheus:   http://localhost:9090"
    echo ""
}

# Main setup function
main() {
    echo ""
    print_status "Starting setup process..."
    
    check_docker
    check_docker_compose
    check_system_resources
    create_directories
    set_permissions
    validate_configs
    create_sample_configs
    test_docker_compose
    
    # Ask user if they want to pull images
    echo ""
    read -p "Do you want to pull Docker images now? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        pull_docker_images
        build_custom_images
    else
        print_status "Skipping Docker image pull. Run 'make up' when ready."
    fi
    
    # Ask user if they want to set up Python environment
    echo ""
    read -p "Do you want to set up Python virtual environment? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        setup_python_env
    else
        print_status "Skipping Python environment setup."
    fi
    
    show_next_steps
}

# Handle script interruption
trap 'print_error "Setup interrupted by user"; exit 1' INT

# Check if script is run from correct directory
if [ ! -f "docker-compose.yaml" ] && [ ! -f "docker-compose.yml" ]; then
    print_error "Please run this script from the project root directory (where docker-compose.yaml is located)"
    exit 1
fi

# Run main setup
main "$@"