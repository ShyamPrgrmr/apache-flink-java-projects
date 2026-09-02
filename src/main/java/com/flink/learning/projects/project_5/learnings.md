# Flink Execution Model Notes

## JobGraph

* Java code is converted into a JobGraph.

* A JobGraph is the logical blueprint of the data pipeline.

* It represents the operators and their connections, but nothing is running yet.

Flow

```
Java Code
    ↓
 JobGraph (Blueprint)
```

## ExecutionGraph

* Flink takes the JobGraph and applies parallelism.

* Each operator gets multiple copies (subtasks) based on the configured parallelism.

* These subtasks become the execution tasks that will run in the cluster.

Example:

```
Filter (parallelism = 3)

Filter-1
Filter-2
Filter-3
```

## JobManager vs TaskManager

This distinction is extremely important.

### JobManager

The JobManager is the coordinator.

Responsibilities:

1. Coordinates the job.

2. Builds the ExecutionGraph.

3. Schedules tasks on available slots.

4. Coordinates checkpoints.

5. Restarts failed tasks during recovery.

### TaskManager

The TaskManager is the worker node.

Responsibilities:

1. Executes the assigned tasks.

2. Processes incoming records.

3. Maintains operator and keyed state (including window state).

4. Takes state snapshots during checkpoints.

5. Sends processed results downstream.

## TaskManager

### Task Slots

A Task Slot is a resource container inside a TaskManager.

Key points:

1. Tasks run inside task slots.

2. A chained task can contain multiple operators (for example, Source → Parse → Filter).

3. A TaskManager can have multiple task slots.

4. The JobManager schedules execution tasks onto available slots.

### Understanding "6 tasks need 6 slots"

> If your job needs six tasks, you'll need six slots across the cluster.

This means Flink only requires six available slots in total.

Those slots can be:

* all on one TaskManager, or

* distributed across multiple TaskManagers.

Example:

```
TaskManager-1 (3 slots)
TaskManager-2 (3 slots)

Total = 6 slots
```

### Operator Chaining

Flink automatically chains compatible operators together to reduce overhead.

Instead of:

```
Source
 ↓
Map
 ↓
Filter
```

Flink can execute them as one chained task:

```
Source → Map → Filter
```

Benefits:

* fewer threads

* less network communication

* better performance

## Data Movement Between Operators

Flink moves data between operators in two main ways.

### Forward Partitioning

* Records go directly to the corresponding downstream subtask.

* Used when parallelism matches.

### Shuffle (`keyBy()`)

* Records are redistributed based on the key.

* All events for the same key (for example, `user1`) always reach the same execution subtask.

* That subtask maintains the keyed/window state for that user.

## State Management

* Live window state is stored inside the TaskManager that runs the subtask.

* The JobManager does not store the state.

* During checkpointing, TaskManagers snapshot their state to external storage (S3, HDFS, filesystem, etc.).

Example:

```
user1
  ↓
Window Task 2
  ↓
State stored in TaskManager
```

## What Happens During a Failure?

1. The JobManager initiates a checkpoint.

2. Each TaskManager snapshots its local state.

3. The checkpoint is stored in external storage.

4. If a TaskManager fails, the JobManager starts the affected tasks on available slots.

5. The new task restores its state from the latest successful checkpoint.

6. Processing resumes without losing the saved state.

Recovery Flow

```
Checkpoint
    ↓
TaskManager Failure
    ↓
JobManager Reschedules Task
    ↓
Restore State from Checkpoint
    ↓
Continue Processing
```

## End-to-End Execution Flow

This summarizes the complete execution model:

```
Java Code
    ↓
JobGraph (Blueprint)
    ↓
ExecutionGraph (Tasks created using parallelism)
    ↓
JobManager schedules tasks
    ↓
TaskManager Slots execute tasks
    ↓
Operators process records
    ↓
State stored in TaskManager
    ↓
Checkpoint to external storage
    ↓
Recovery on failure
```

This flow is a useful mental model to remember the relationship between JobGraph → ExecutionGraph → JobManager → TaskManager → Task Slots → State → Checkpoints.
