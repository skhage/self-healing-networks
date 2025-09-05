# Self-Healing Networks

A comprehensive AI-powered system for autonomous telecommunications network management, built on Databricks. This system automatically detects, diagnoses, and resolves network device issues while optimizing field technician routing and resource allocation.

## 🚀 Overview

The Self-Healing Networks project demonstrates how AI can transform telecommunications operations by:

- **Automated Triage**: AI-powered analysis of device errors to determine if issues can be resolved automatically or require human intervention
- **Intelligent Routing**: Geographic optimization of field technician assignments based on device locations and technician availability
- **Predictive Maintenance**: Proactive identification of potential device failures before they cause outages
- **Real-time Monitoring**: Continuous monitoring of network health with automated response capabilities

## 🏗️ Architecture

### Core Components

1. **Data Simulation Engine** (`SHN Synthetic Data Gen.ipynb`)
   - Generates realistic evaluation data for AI agent testing
   - Creates synthetic scenarios for field technician training
   - Simulates mobile tower locations and device configurations

2. **Outage Triage System** (`NetworkOptimization/outage_triage.ipynb`)
   - Analyzes device errors using AI
   - Determines automated vs. human intervention requirements
   - Integrates with field technician routing system

3. **Field Technician Management** (`NetworkOptimization/Field Technician Routes.ipynb`)
   - Generates realistic daily routes for technicians
   - Optimizes assignments based on geographic proximity
   - Manages technician availability and scheduling

4. **Database Functions** (`NetworkOptimization/Create Tools.ipynb`)
   - Provides reusable database functions
   - Handles scenario selection and location lookup
   - Manages field technician routing queries

### Workflow Components

- **Incremental Auto Triage** (`Workflows/incremental_auto_triage.ipynb`)
  - Processes real-time device status changes
  - Generates AI-powered telemetry error predictions
  - Updates triage tables with new issues

- **New Routing** (`Workflows/get_new_routing.ipynb`)
  - Calculates optimal field technician assignments
  - Uses Haversine distance calculations
  - Updates device status with technician assignments

- **Outage Scenario Generation** (`Workflows/outage_scenario.ipynb`)
  - Creates random outage scenarios for testing
  - Simulates device failures and status changes
  - Feeds into the triage and routing system

- **Error Prediction** (`Workflows/outage_incremental_predict_errors.ipynb`)
  - Predicts telemetry errors using AI
  - Processes incremental device status changes
  - Generates realistic error messages

### Demo Components

- **FieldTech Scheduler Demo** (`DAIS Demo FieldTech Scheduler.ipynb`)
  - Demonstrates field technician scheduling capabilities
  - Shows route generation and GPS tracking
  - Integrates with the main triage system

- **Preventative Maintenance Demo** (`DAIS Demo Preventative Maintenance.ipynb`)
  - Shows predictive maintenance capabilities
  - Demonstrates failure prediction and prevention
  - Uses machine learning for proactive maintenance

- **Data Simulation Demo** (`DAIS Telco SHN Data Simulation.ipynb`)
  - Comprehensive data simulation for the entire system
  - Creates realistic mobile tower and device data
  - Simulates multiple years of operational data

## 🛠️ Technology Stack

- **Platform**: Databricks
- **AI/ML**: MLflow, LangChain, Databricks Agents
- **Data Processing**: Apache Spark, Delta Lake
- **Vector Search**: Databricks Vector Search
- **Deployment**: Databricks Model Serving
- **Languages**: Python, SQL

## 📊 Data Sources

The system works with several types of data:

- **Mobile Tower Locations**: OpenCellID data for LTE towers in San Francisco
- **Device Inventory**: Various mobile tower equipment (Nokia, Ericsson, Delta, Cisco, etc.)
- **Field Technicians**: Realistic technician data with GPS coordinates and availability
- **Device Manuals**: Technical documentation for maintenance and repair procedures
- **Outage History**: Historical data for predictive maintenance and pattern analysis

## 🔧 Setup and Configuration

### Prerequisites

- Databricks workspace with appropriate permissions
- Access to Databricks Model Serving
- Unity Catalog for data management
- Vector Search endpoint

### Configuration

The system uses a `params.yml` file for configuration:

```yaml
data_params:
  catalog: "your_catalog"
  schema: "your_schema"
  towers_volume: "your_volume_path"

simulation_params:
  n_fieldtechs: 50
```

### Installation

1. Clone the repository
2. Install required dependencies (see individual notebooks)
3. Configure `params.yml` with your Databricks settings
4. Run the data simulation notebooks to generate test data
5. Deploy the AI agents using the provided notebooks

## 🚀 Getting Started

### 1. Data Generation

Start by running the data simulation notebooks to generate test data:

```bash
# Generate comprehensive test data
jupyter notebook "DAIS Telco SHN Data Simulation.ipynb"

# Generate field technician routes
jupyter notebook "Field Technician Routes.ipynb"
```

### 2. Agent Setup

Set up the AI agents for device maintenance:

```bash
# Create database functions
jupyter notebook "Create Tools.ipynb"

# Generate synthetic evaluation data
jupyter notebook "SHN Synthetic Data Gen.ipynb"
```

### 3. System Testing

Test the complete system with generated scenarios:

```bash
# Generate outage scenarios
jupyter notebook "Workflows/outage_scenario.ipynb"

# Run triage system
jupyter notebook "outage_triage.ipynb"
```

## 📈 Key Features

### Automated Triage
- AI-powered analysis of device errors
- Automatic determination of fix requirements
- Integration with field technician routing

### Geographic Optimization
- Haversine distance calculations for technician assignment
- Real-time routing optimization
- GPS-based location tracking

### Predictive Maintenance
- Machine learning models for failure prediction
- Proactive maintenance scheduling
- Historical pattern analysis

### Real-time Processing
- Delta Lake change data feed integration
- Incremental processing of device status changes
- Real-time triage and routing updates

## 🔍 Monitoring and Evaluation

The system includes comprehensive monitoring and evaluation capabilities:

- **Agent Performance**: MLflow-based evaluation of AI agent responses
- **Guideline Adherence**: Automated checking of response quality
- **Geographic Accuracy**: Validation of technician routing decisions
- **System Health**: Real-time monitoring of device status and triage performance

## 📚 Documentation

Each notebook includes detailed markdown documentation explaining:

- Purpose and functionality
- Step-by-step processes
- Input/output specifications
- Integration points
- Configuration requirements

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add documentation for new features
5. Submit a pull request

## 📄 License

This project is part of the Databricks Demo series and is intended for educational and demonstration purposes.

## 🆘 Support

For questions or issues:

1. Check the notebook documentation
2. Review the configuration settings
3. Verify Databricks workspace permissions
4. Check the MLflow experiment logs

## 🔮 Future Enhancements

- Integration with more device types and manufacturers
- Enhanced predictive maintenance algorithms
- Real-time dashboard for network operations
- Integration with external ticketing systems
- Advanced optimization algorithms for technician routing
