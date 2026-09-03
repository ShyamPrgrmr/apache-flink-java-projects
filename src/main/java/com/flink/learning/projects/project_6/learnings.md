# Flink Notes – `keyBy()` + Parallelism

## Core Idea

`keyBy()` ensures that all events with the same key are always processed by the same subtask.

Example:

```
user1 → Subtask 2
user1 → Subtask 2
user1 → Subtask 2
```

This is what makes keyed state (`ValueState`, `MapState`, etc.) possible.

> Mental model: `hash(key) % parallelism → Subtask`

> Production reality: `Key → Hash → Key Group → Subtask`

The key-group layer allows Flink to redistribute state when parallelism changes.

# Flink Execution Hierarchy

```
Job
 ├── Operator Chain(s)
 │     ├── Task (parallel instance)
 │     │     └── Slot
 │     │           └── TaskManager
```

## Factory Analogy

* Job -> Whole factory
* Operator Chain > Assembly line
* Task -> One parallel assembly-line worker
* Slot -> Workstation with reserved CPU & Memory
* TaskManager -> Factory building (worker JVM)

# Operator Chaining

Flink combines compatible operators into a single runtime task.

Example:

```
Kafka → Map → Filter
```

becomes one operator chain.

### Benefits

* Same thread execution

* No network communication

* Lower latency

* Better CPU utilization

# When Does Flink Break the Chain?

Transition              Same Chain?
Map → Filter            ✅ Yes
Filter → FlatMap        ✅ Yes
Map → keyBy             ❌ No
Different Parallelism   ❌ No
rebalance()             ❌ No


`keyBy()` introduces a network shuffle.

```
Map
 ↓
Network Shuffle
 ↓
Process
```

Records move between subtasks based on the key.

# Task vs Slot

## Task

* Runtime execution of an operator chain.

* Created from parallelism.

* Processes records.

* Owns keyed state.

## Slot

* Reserved CPU and memory inside a TaskManager.

* Hosts one or more tasks through slot sharing.

* Does not own state.

Example:

```
Slot 0
├── Map-0
├── Filter-0
└── Sink-0
```

Multiple chained operators can share the same slot.

# Task Scheduling

The JobManager schedules tasks onto available slots.

Example:

Task            TaskManager     Slot        
Process-0       TM-1            Slot 0
Process-1       TM-2            Slot 1


The key does not directly map to a TaskManager.

Instead:

```
Key
 ↓
Hash
 ↓
Subtask
 ↓
Task
 ↓
Slot
 ↓
TaskManager
```

# Subtasks and Slots

A task stays in the same slot during normal execution.

It moves only when Flink reschedules it because of:

* TaskManager failure

* Job restart

* Parallelism change

* Savepoint/Checkpoint restoration

The state moves with the subtask because Flink restores it from checkpoints.

# Dependencies Between Operators

Flink guarantees dependencies in two ways.

## Inside an Operator Chain

Same thread execution.

```
Kafka
 ↓
Map
 ↓
Filter
```

Equivalent to:

Java

```
record = kafka.read();
record = map(record);
record = filter(record);
```

## Between Operator Chains

Flink inserts managed network buffers.

```
Task A
   ↓
Network Buffer
   ↓
Task B
```

If downstream becomes slow, buffers fill up and backpressure slows upstream automatically.

# What Happens if No Slots Are Available?

If a required slot is unavailable:

* JobManager keeps the task in CREATED/SCHEDULED state.

* The task starts when a slot becomes available.

* Existing running tasks continue unaffected.

Example:

Cluster Slots   Required Tasks
4               6


Result:

* 4 tasks run

* 2 tasks wait

# Key Takeaways

* `keyBy()` guarantees one key has one subtask owner.

* State belongs to the subtask, not the slot.

* A Job can contain multiple operator chains.

* `keyBy()` is the most common reason Flink performs a network shuffle.

* The JobManager schedules tasks to TaskManagers based on available slots.

* Backpressure propagates upstream when downstream operators cannot keep up.

## One-liner to remember

> `keyBy()` decides who processes a key; the JobManager decides where that subtask runs.
