# Materialize Platform: Zero-Downtime Upgrades

## How to read this document

This document describes the design and implementation of zero-downtime upgrades
in Materialize Platform. The architecture described here is the current
implementation for production zero-downtime deployments.

Phrases in ALL CAPS are terms of art specific to the zero-downtime upgrade
system. They are written in ALL CAPS to indicate their specific meaning within
this context and should not be conflated with how similar terms are used
elsewhere.

## Overview

Zero-downtime upgrades in Materialize enable seamless software updates without
service interruption. The system maintains two key properties during upgrades:

- **RESPONSIVENESS**: Storage objects (sources, tables, sinks) remain available
  and responsive to queries
- **FRESHNESS**: Compute objects (indexes, materialized views) continue to
  process updates and remain fresh

The architecture achieves zero-downtime through a carefully orchestrated
sequence of READ-ONLY MODE initialization, CAUGHT-UP DETECTION, PROMOTION
SIGNALING, and FENCING mechanisms.

## Core Components

### DEPLOYMENT GENERATION

A monotonically increasing number that represents the version of a deployment.
Higher generations can fence out lower generations, providing the primary
coordination mechanism for zero-downtime upgrades.

### DEPLOYMENT STATE MACHINE

The zero-downtime upgrade process follows a strict state machine:

1. **INITIALIZING**: Environment starts up and determines its deployment mode
2. **CATCHING_UP**: Environment hydrates workloads in read-only mode
3. **READY_TO_PROMOTE**: Environment is fully caught up and ready for promotion
4. **PROMOTING**: External orchestrator has triggered promotion
5. **IS_LEADER**: Environment is the active leader serving queries

### READ-ONLY MODE

A special operating mode where controllers and replicas can read from external
systems but cannot write to them. This enables pre-hydration of workloads
without affecting persistent state.

## Zero-Downtime Upgrade Flow

### Phase 1: Environment Startup

When a new `environmentd` starts, it follows this sequence:

1. **Generation Comparison**: The new deployment compares its
   `deploy_generation` with the catalog generation
2. **Mode Determination**: If the new generation is higher, it starts in
   READ-ONLY MODE
3. **Controller Initialization**: Storage and compute controllers initialize
   with `read_only: true`
4. **Catalog Opening**: Catalog opens in savepoint mode, preventing leadership
   assertion

```rust
// From environmentd/src/deployment/preflight.rs:168-169
if catalog_generation < deploy_generation {
    info!("this deployment is a new generation; booting in read only mode");
    return Ok(PreflightOutput {
        read_only: true,
        caught_up_trigger: Some(caught_up_trigger),
    });
}
```

### Phase 2: Workload Hydration

During the CATCHING_UP phase:

1. **Cluster Reconnection**: Compute replicas reconnect to existing cluster
   instances
2. **Dataflow Hydration**: Indexes and materialized views hydrate using leased
   read handles
3. **Source Preparation**: Storage sources prepare for ingestion without
   actually writing
4. **DDL Monitoring**: System monitors for schema changes that would require
   restart

### Phase 3: Caught-Up Detection

The system implements CAUGHT-UP DETECTION:

#### New Frontier-Based Algorithm
- **Frontier Comparison**: Compares collection frontiers against live
  deployment frontiers
- **Lag Tolerance**: Allows configurable lag via
  `WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG`
- **Cutoff Mechanism**: Ignores collections too far behind via
  `WITH_0DT_CAUGHT_UP_CHECK_CUTOFF`
- **Hydration Verification**: Confirms both frontier progress and actual
  hydration

#### Legacy Hydration Check
- **Simple Boolean**: Checks if all compute clusters have hydrated
- **Compatibility**: Fallback for environments with
  `ENABLE_0DT_CAUGHT_UP_CHECK: false`

### Phase 4: Promotion Signaling

When caught up, the environment signals readiness:

1. **State Transition**: Deployment state becomes `READY_TO_PROMOTE`
2. **API Exposure**: `/api/leader/status` endpoint returns `ReadyToPromote`
3. **External Coordination**: Orchestrator monitors status and determines
   promotion timing
4. **Promotion Trigger**: Orchestrator calls `/api/leader/promote` when ready

### Phase 5: Fencing and Takeover

The promotion process executes an atomic takeover:

1. **Catalog Fencing**: New deployment writes higher generation fence token
2. **Atomic Operation**: Compare-and-append ensures atomic transition
3. **Old Deployment Fencing**: Previous generation detects fence and shuts down
4. **Leadership Assertion**: New deployment becomes IS_LEADER

## Implementation Details

### Fencing Mechanism

The fencing system operates on two levels:

#### Deployment Generation Fencing

```rust
// Higher generation always fences lower generation
if current_deploy_generation < fence_token.deploy_generation {
    return Err(FenceError::DeployGeneration {
        current_generation: current_deploy_generation,
        fence_generation: fence_token.deploy_generation,
    });
}
```

#### Epoch-Based Fencing

Within the same generation, epochs provide fencing for process restarts:
```rust
if current_epoch < fence_token.epoch {
    return Err(FenceError::Epoch {
        current_epoch,
        fence_epoch: fence_token.epoch,
    });
}
```

### Read-Only Mode Implementation

#### Controller Read-Only Mode

```rust
// Controllers start with read_only flag
pub struct Controller {
    read_only: bool,
    storage: Box<dyn StorageController>,
    compute: ComputeController,
}

// Read-only controllers prevent writes
if self.read_only() {
    return; // Skip write operations
}
```

#### Storage Read-Only Mode

- **Leased Read Handles**: Uses time-limited read capabilities instead of
  critical since handles
- **No Source Execution**: Prevents source command execution that would advance
  state
- **Persist Integration**: Read-only mode table worker restricts persist
  operations

#### Compute Read-Only Mode

- **Hydration Only**: Allows dataflow hydration but prevents new sink creation
- **Sequential Hydration**: Controlled hydration prevents resource thrashing
- **Replica Continuity**: Existing replicas continue running without restart

### Orchestration Integration

The system integrates with external orchestrators through well-defined APIs:

#### Status API

```
GET /api/leader/status
Response: {"status": "ReadyToPromote"}
```

#### Promotion API
```
POST /api/leader/promote
Response: {"result": "Success"}
```

#### Skip Catchup API
```
POST /api/leader/skip-catchup
Response: {"result": "Success"}
```

## Configuration

The zero-downtime upgrade system is controlled by several configuration parameters:

### Core Configuration

- `ENABLE_0DT_DEPLOYMENT`: Enable zero-downtime deployment mode
- `WITH_0DT_DEPLOYMENT_MAX_WAIT`: Maximum wait time for caught-up detection
- `WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL`: Frequency of DDL change monitoring

### Caught-Up Detection

- `ENABLE_0DT_CAUGHT_UP_CHECK`: Use enhanced frontier-based caught-up detection
- `WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG`: Maximum allowed lag for caught-up
  status
- `WITH_0DT_CAUGHT_UP_CHECK_CUTOFF`: Ignore collections beyond this lag

### Operational Control

- `ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT`: Panic vs. continue on timeout
- `WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL`: Caught-up check frequency

## Architectural Properties

### Consistency

The system maintains strict consistency through:

- **Atomic Fencing**: All fence operations are atomic compare-and-append
- **Transactional Catalog**: All catalog changes are transactional with fencing
  validation
- **Deployment Generation Ordering**: Strict ordering prevents split-brain
  scenarios

### Availability

Zero-downtime is achieved through:

- **Workload Continuity**: Dataflows continue running during upgrades
- **Pre-Hydration**: New deployments hydrate before taking over
- **Graceful Fencing**: Old deployments shut down gracefully when fenced

### Fault Tolerance

The system handles failures through:

- **Infinite Retry**: Replica connections retry indefinitely with exponential
  backoff
- **Timeout Handling**: Configurable timeouts with graceful degradation
- **DDL Change Detection**: Automatic restart if schema changes detected
- **Rollback Safety**: Failed promotions don't affect running deployments

## Future Considerations

### Performance Optimizations
- **Hydration Concurrency**: Configurable parallel hydration for faster startup
- **Incremental Hydration**: Hydrate only changed components
- **Smart Reconnection**: Optimize replica reconnection patterns

### Operational Improvements
- **Automated Rollback**: Automatic rollback on promotion failures
- **Health Monitoring**: Enhanced monitoring of upgrade progress
- **Metric Collection**: Comprehensive metrics for upgrade performance

### Scaling Considerations
- **Multi-Region Support**: Coordinate upgrades across regions
- **Staging Rollouts**: Gradual rollouts with canary deployments
- **Load Balancing**: Intelligent load balancing during upgrades

## Conclusion

The zero-downtime upgrade system in Materialize provides a robust foundation
for seamless software updates. Through careful orchestration of read-only mode,
caught-up detection, promotion signaling, and fencing mechanisms, the system
maintains both responsiveness and freshness during upgrades while providing
strong consistency guarantees.

The architecture's layered approach - with clear separation between deployment
state management, workload hydration, and fencing coordination - enables
flexible deployment strategies while maintaining operational safety and data
consistency.
