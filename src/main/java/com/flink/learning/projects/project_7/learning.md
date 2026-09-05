# Keyed State

>Keyed State is data that Flink stores separately for each key created by keyBy().


## Where does the data will be stored ?

1. Each subtask stores state only for the keys it owns.
2. In case of failure of the taskManager the processing start from the same checkpoint from where the it was inturrupted. 
3. In case of no checkpoint the state will not be persisted. 


## Types of Keyed State

1. ValuedState
2. ListState
3. MapState
4. ReducedState
5. AggregatedState


## State TTL



## Example:

ValueState        -> Last transaction amount
ListState         -> Last 5 transactions
MapState          -> fromEntity payments total amount
MapState          -> upiHandle payments total amount
ReducingState     -> Total amount spent
AggregatingState  -> Average transaction amount

