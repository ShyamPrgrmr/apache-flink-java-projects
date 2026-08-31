## Watermark 

Flink's estimate that "events older than this time are unlikely to arrive anymore and when to close the window.


### Mental model

* Think of the watermark as a moving deadline. It doesn't guarantee no older events exist—it says, "We've waited long enough; it's time to close this window."


The flow is almost right, but `keyBy()` happens before the window, and Flink doesn't literally have a "selectSubprocess" step—it hashes the key and routes the event to the correct operator subtask.

The execution flow for CPU example is:

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22120%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20120%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Crect%20x%3D%2220%22%20y%3D%2230%22%20width%3D%22100%22%20height%3D%2240%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%2270%22%20y%3D%2255%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EEvent%3C%2Ftext%3E%3Cpath%20d%3D%22M120%2050%20L180%2050%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22180%22%20y%3D%2230%22%20width%3D%22140%22%20height%3D%2240%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22250%22%20y%3D%2255%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EAssign%20Event%20Time%3C%2Ftext%3E%3Cpath%20d%3D%22M320%2050%20L400%2050%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22400%22%20y%3D%2230%22%20width%3D%22120%22%20height%3D%2240%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22460%22%20y%3D%2255%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EWatermark%3C%2Ftext%3E%3Cpath%20d%3D%22M520%2050%20L600%2050%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22600%22%20y%3D%2230%22%20width%3D%22140%22%20height%3D%2240%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22670%22%20y%3D%2255%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EkeyBy\(\)%3C%2Ftext%3E%3C%2Fsvg%3E)

Then:

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22140%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20140%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Crect%20x%3D%2240%22%20y%3D%2230%22%20width%3D%22180%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22130%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3ERoute%20to%20Subtask%3C%2Ftext%3E%3Cpath%20d%3D%22M220%2055%20L330%2055%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22330%22%20y%3D%2230%22%20width%3D%22180%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22420%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3E5s%20Window%3C%2Ftext%3E%3Cpath%20d%3D%22M510%2055%20L620%2055%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22620%22%20y%3D%2230%22%20width%3D%22100%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22670%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EAggregate%3C%2Ftext%3E%3C%2Fsvg%3E)

Finally:

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22100%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20520%20100%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Crect%20x%3D%2230%22%20y%3D%2225%22%20width%3D%22180%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22120%22%20y%3D%2253%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EWindow%20closes%3C%2Ftext%3E%3Cpath%20d%3D%22M210%2050%20L320%2050%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22320%22%20y%3D%2225%22%20width%3D%22170%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22405%22%20y%3D%2253%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EEmit%20to%20Kafka%3C%2Ftext%3E%3C%2Fsvg%3E)


### Stepwise execution

1. Event is created with its timestamp (`System.currentTimeMillis()`).

2. `assignTimestampsAndWatermarks()` tells Flink to use that timestamp as event time.

3. Watermark advances based on the highest event time seen (for example, `maxEventTime - 1s`).

4. `keyBy(CPU::getName)` hashes `EC2-CPU-1`, `EC2-CPU-2`, and `EC2-CPU-3` so each CPU always goes to the same subtask.

5. Each subtask keeps a separate 5-second window for each CPU.

6. When the watermark passes the window end, Flink runs your `AggregateFunction`, emits `"EC2-CPU-1: 63.2%"`, and clears that window's state.

## One subtle but important point

Many people think the watermark decides which subtask an event goes to. It doesn't.

* `keyBy()` decides where the event is processed (which subtask).

* Watermark decides when a window is allowed to close.

These are independent concepts that work together: `keyBy()` partitions the stream, while watermarks control time-based completeness for each partition's windows.


### Important undestanding

The 5-second window and the 1-second watermark are measuring different things, so they don't need to match.

* 5 seconds (Window) = How long a bucket of events lasts.
* 1 second (Watermark) = How much out-of-order delay Flink is willing to tolerate before closing that bucket.


---

## Production scenarios

### What happens when event comes with delay of 1 hour ?


This is one of the most important production questions in Flink. The answer is: it depends on your watermark and allowed lateness configuration. By default, a 1-hour-late event will be dropped if the watermark has already passed that window.

Let's walk through it.

## Scenario

Configuration:

Java

```
WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(3))

.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
```

Suppose an event was created at 10:00:02.

JSON

```
{
  "cpu": "EC2-CPU-1",
  "usage": 85,
  "timestamp": "10:00:02"
}
```

But it arrives at 11:00:02 — one hour later.

## What happens?

By the time it's 11:00:

* The latest event time seen is around 11:00:02.

* Watermark is around 10:59:59 (3 seconds behind).

The original window (10:00:00–10:00:05) was closed almost an hour ago.

So when the late event arrives:

> Flink sees that its event time (`10:00:02`) is far behind the watermark (`10:59:59`).

Result: The event is considered late and is dropped by default.

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22180%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20180%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Cline%20x1%3D%2240%22%20y1%3D%2280%22%20x2%3D%22720%22%20y2%3D%2280%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Cline%20x1%3D%2240%22%20y1%3D%2265%22%20x2%3D%2240%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22200%22%20y1%3D%2265%22%20x2%3D%22200%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22720%22%20y1%3D%2265%22%20x2%3D%22720%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%2240%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A00%3C%2Ftext%3E%3Ctext%20x%3D%22200%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A00%3A05%3C%2Ftext%3E%3Ctext%20x%3D%22720%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E11%3A00%3C%2Ftext%3E%3Crect%20x%3D%2240%22%20y%3D%2230%22%20width%3D%22160%22%20height%3D%2225%22%20rx%3D%226%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22120%22%20y%3D%2247%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3EWindow%2010%3A00%E2%80%9310%3A00%3A05%3C%2Ftext%3E%3Ccircle%20cx%3D%22108%22%20cy%3D%2280%22%20r%3D%225%22%20fill%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22108%22%20y%3D%22145%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3EEvent%20time%2010%3A00%3A02%3C%2Ftext%3E%3Cpath%20d%3D%22M700%2030%20L700%2095%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%20stroke-dasharray%3D%226%204%22%2F%3E%3Ctext%20x%3D%22700%22%20y%3D%2220%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3EWatermark%20%E2%89%8810%3A59%3A59%3C%2Ftext%3E%3Cpath%20d%3D%22M108%2080%20C220%20140%2C%20520%20140%2C%20700%2080%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%20fill%3D%22none%22%20stroke-dasharray%3D%224%204%22%2F%3E%3Ctext%20x%3D%22420%22%20y%3D%22160%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3EArrived%20one%20hour%20late%20%E2%86%92%20too%20late%3C%2Ftext%3E%3C%2Fsvg%3E)

## What if I want to accept late events?

Flink provides allowed lateness.

Java

```
window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
.allowedLateness(Duration.ofMinutes(2))
```

Now the timeline looks like this:

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22160%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20160%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Cline%20x1%3D%2240%22%20y1%3D%2280%22%20x2%3D%22720%22%20y2%3D%2280%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Cline%20x1%3D%2240%22%20y1%3D%2265%22%20x2%3D%2240%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22200%22%20y1%3D%2265%22%20x2%3D%22200%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22360%22%20y1%3D%2265%22%20x2%3D%22360%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%2240%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A00%3C%2Ftext%3E%3Ctext%20x%3D%22200%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A00%3A05%3C%2Ftext%3E%3Ctext%20x%3D%22360%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A02%3A05%3C%2Ftext%3E%3Crect%20x%3D%2240%22%20y%3D%2230%22%20width%3D%22160%22%20height%3D%2225%22%20rx%3D%226%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22120%22%20y%3D%2247%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3EWindow%3C%2Ftext%3E%3Crect%20x%3D%22200%22%20y%3D%2230%22%20width%3D%22160%22%20height%3D%2225%22%20rx%3D%226%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22280%22%20y%3D%2247%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E2%20min%20lateness%3C%2Ftext%3E%3C%2Fsvg%3E)

* Window closes at 10:00:05.

* Flink keeps the window state for another 2 minutes.

* If an event with timestamp `10:00:02` arrives at `10:01:30`, Flink reopens that window, updates the aggregation, and emits an updated result.

* If it arrives at `11:00:02`, it's still dropped.

## What if I never want to lose late events?

Instead of dropping them, you can send them to a side output.

Java

```
.sideOutputLateData(lateOutputTag)
```

Then your architecture becomes:

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22220%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20220%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Crect%20x%3D%2240%22%20y%3D%2230%22%20width%3D%22160%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22120%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EKafka%20Events%3C%2Ftext%3E%3Cpath%20d%3D%22M200%2055%20L320%2055%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22320%22%20y%3D%2230%22%20width%3D%22180%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22410%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EWindow%20%2B%20Watermark%3C%2Ftext%3E%3Cpath%20d%3D%22M500%2055%20L620%2055%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22620%22%20y%3D%2230%22%20width%3D%22100%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22670%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EOutput%3C%2Ftext%3E%3Cpath%20d%3D%22M410%2080%20L410%20160%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Cpath%20d%3D%22M410%20160%20L620%20160%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22620%22%20y%3D%22135%22%20width%3D%22100%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22670%22%20y%3D%22163%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3ELate%20Events%3C%2Ftext%3E%3C%2Fsvg%3E)

This is common in production. The main pipeline keeps low latency, while very late events are stored separately for auditing, reprocessing, or offline correction.

### How companies usually choose the watermark

* CPU metrics (Prometheus-like): 2–10 seconds

* Application logs: 30 seconds to a few minutes

* IoT devices: 1–5 minutes

* Mobile analytics: Several minutes or even hours, depending on network conditions

The key tradeoff is: a larger watermark catches more late events but delays when windows can be finalized.

---

### So, if there is backlogs in upstream system due to some issue then flink can create window using lateOutputTag, but how it will consider end time of window in this case ?


This is actually a production-level question. The key insight is that `lateOutputTag` does not create a new window. The original window's start and end time are always determined by the event's timestamp, not by when the event arrives.

Let's walk through a backlog scenario.

## Scenario: Upstream outage

* Window size: 5 seconds

* Watermark: 3 seconds

* Allowed lateness: 2 minutes

Imagine Kafka has an outage and events are stuck for an hour.

|
Event Time

|

Arrival Time

|
| --- | --- |
|

10:00:01

|

11:00:01

|
|

10:00:03

|

11:00:03

|
|

10:00:04

|

11:00:04

|

These events belong to the window `10:00:00–10:00:05` because Flink computes the window from the event timestamp.

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22200%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20200%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Cline%20x1%3D%2240%22%20y1%3D%2280%22%20x2%3D%22720%22%20y2%3D%2280%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Cline%20x1%3D%2240%22%20y1%3D%2265%22%20x2%3D%2240%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22220%22%20y1%3D%2265%22%20x2%3D%22220%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22720%22%20y1%3D%2265%22%20x2%3D%22720%22%20y2%3D%2295%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%2240%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A00%3A00%3C%2Ftext%3E%3Ctext%20x%3D%22220%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A00%3A05%3C%2Ftext%3E%3Ctext%20x%3D%22720%22%20y%3D%22115%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E11%3A00%3C%2Ftext%3E%3Crect%20x%3D%2240%22%20y%3D%2230%22%20width%3D%22180%22%20height%3D%2225%22%20rx%3D%226%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22130%22%20y%3D%2247%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3EWindow%2010%3A00%E2%80%9310%3A00%3A05%3C%2Ftext%3E%3Ccircle%20cx%3D%2276%22%20cy%3D%2280%22%20r%3D%225%22%20fill%3D%22currentColor%22%2F%3E%3Ccircle%20cx%3D%22148%22%20cy%3D%2280%22%20r%3D%225%22%20fill%3D%22currentColor%22%2F%3E%3Ccircle%20cx%3D%22184%22%20cy%3D%2280%22%20r%3D%225%22%20fill%3D%22currentColor%22%2F%3E%3Cpath%20d%3D%22M184%2080%20C340%20150%2C%20560%20150%2C%20700%2080%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%20fill%3D%22none%22%20stroke-dasharray%3D%224%204%22%2F%3E%3Ctext%20x%3D%22520%22%20y%3D%22170%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3EArrived%20one%20hour%20later%20%E2%86%92%20still%20belongs%20to%20the%20old%20window%3C%2Ftext%3E%3C%2Fsvg%3E)

The window end is still 10:00:05.

## What Flink does

### Case 1: Within allowed lateness

If the event arrives before:

```
windowEnd + allowedLateness
```

Example:

* Window ends: 10:00:05

* Allowed lateness: 2 minutes

Flink keeps the window state until 10:02:05.

A late event arriving at 10:01:30 will:

1. Reopen that existing window.

2. Update the aggregation.

3. Emit an updated result.

The window end does not change.

### Case 2: One-hour backlog

At 11:00, that window has been cleaned up long ago.

Now:

* Flink cannot reopen the old window.

* The event is sent to `lateOutputTag` (or dropped if no side output is configured).

## So what is `lateOutputTag` for?

Think of it as a separate emergency lane.

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22220%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20220%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Crect%20x%3D%2240%22%20y%3D%2230%22%20width%3D%22160%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22120%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EKafka%20Events%3C%2Ftext%3E%3Cpath%20d%3D%22M200%2055%20L320%2055%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22320%22%20y%3D%2230%22%20width%3D%22180%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22410%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EWindow%20%2B%20Watermark%3C%2Ftext%3E%3Cpath%20d%3D%22M500%2055%20L620%2055%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22620%22%20y%3D%2230%22%20width%3D%22100%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22670%22%20y%3D%2258%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3EOutput%3C%2Ftext%3E%3Cpath%20d%3D%22M410%2080%20L410%20160%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Cpath%20d%3D%22M410%20160%20L620%20160%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Crect%20x%3D%22620%22%20y%3D%22135%22%20width%3D%22100%22%20height%3D%2250%22%20rx%3D%228%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22670%22%20y%3D%22163%22%20text-anchor%3D%22middle%22%20font-size%3D%2213%22%3ELate%20Events%3C%2Ftext%3E%3C%2Fsvg%3E)

The late event still carries:

JSON

```
{
  "cpu": "EC2-CPU-1",
  "timestamp": "10:00:03"
}
```

Notice that the timestamp is preserved. Flink does not assign it to an `11:00` window.

## How companies handle this

This is common in systems like Kafka, Prometheus, or IoT platforms.

* Main pipeline: Uses a small watermark (2–10 seconds) for low-latency dashboards.

* Late events (`lateOutputTag`): Sent to another Kafka topic, such as `cpu-late-events`.

* Reprocessing job: Later reads `cpu-late-events` and recomputes historical windows in batch or another Flink job.

So the window boundaries always come from the event timestamp, while `lateOutputTag` simply preserves events that arrived too late for the original window instead of silently losing them.

---

### One more question, lets say our kafka is having the event but not consumed by the flink as flink was down for sometime and when we made it up it was already 1 hour passed. In this case can you explain me how it will consume in both window function without watermark and with watermarks.

This is actually one of the most important production scenarios. The interesting answer is:

> If Flink was down and Kafka retained the events, then after Flink restarts it will usually process those events correctly—even if they're an hour old. The watermark does not immediately discard them.

The key is that watermarks are based on the event times of the records Flink is currently reading, not on the wall clock after the restart.

Let's compare both cases.

# Scenario

* Kafka already contains events from 10:00 to 10:05.

* Flink crashes at 10:00.

* Flink restarts at 11:00.

* Kafka still has those events (offsets weren't lost).

Timeline:

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22120%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20120%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Cline%20x1%3D%2240%22%20y1%3D%2260%22%20x2%3D%22720%22%20y2%3D%2260%22%20stroke%3D%22currentColor%22%20stroke-width%3D%222%22%2F%3E%3Cline%20x1%3D%2240%22%20y1%3D%2245%22%20x2%3D%2240%22%20y2%3D%2275%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22210%22%20y1%3D%2245%22%20x2%3D%22210%22%20y2%3D%2275%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22550%22%20y1%3D%2245%22%20x2%3D%22550%22%20y2%3D%2275%22%20stroke%3D%22currentColor%22%2F%3E%3Cline%20x1%3D%22720%22%20y1%3D%2245%22%20x2%3D%22720%22%20y2%3D%2275%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%2240%22%20y%3D%2295%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A00%3C%2Ftext%3E%3Ctext%20x%3D%22210%22%20y%3D%2295%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E10%3A05%3C%2Ftext%3E%3Ctext%20x%3D%22550%22%20y%3D%2295%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3E11%3A00%20Restart%3C%2Ftext%3E%3Ctext%20x%3D%22720%22%20y%3D%2295%22%20text-anchor%3D%22middle%22%20font-size%3D%2212%22%3EConsume%20backlog%3C%2Ftext%3E%3C%2Fsvg%3E)

# Case 1: No Watermark (Processing Time Window)

Java

```
.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
```

Here Flink completely ignores the event timestamp.

At 11:00, when it starts consuming the backlog:

* Event created at 10:00:01

* Event created at 10:00:02

* Event created at 10:00:04

Flink sees them arriving now.

It creates windows like:

|
Arrival Time

|

Window

|
| --- | --- |
|

11:00:01

|

11:00:00–11:00:05

|
|

11:00:02

|

11:00:00–11:00:05

|
|

11:00:04

|

11:00:00–11:00:05

|

Output:

```
11:00 window:
EC2-CPU-1: 63%
```

The historical timing is lost.

This is why processing time is unsuitable for replaying old data.

# Case 2: Event Time + Watermark

Java

```
.assignTimestampsAndWatermarks(...)
.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
```

Now Flink reads the backlog.

First event:

|
Arrival

|

Event Time

|
| --- | --- |
|

11:00:00

|

10:00:01

|

Watermark becomes approximately:

```
09:59:58
```

Notice something important.

The watermark is not 10:59.

It's based on:

```
maximum event time seen - 3 seconds
```

So Flink starts rebuilding history.

### As backlog is consumed

|
Event Time

|

Watermark

|
| --- | --- |
|

10:00:01

|

09:59:58

|
|

10:00:02

|

09:59:59

|
|

10:00:04

|

10:00:01

|

The window 10:00:00–10:00:05 remains open while those records are being replayed.

Eventually Flink reads events from:

```
10:00:08
```

Now:

```
watermark = 10:00:05
```

Only now does Flink close the first window.

Output:

```
10:00:00–10:00:05
EC2-CPU-1: 63%
```

Notice:

* The job restarted at 11:00.

* The output still represents the 10:00 window.

That's exactly what event time is designed for.

# Visual comparison

![](data\:image/svg+xml;charset=utf-8,%3Csvg%20font-family%3D%22-apple-system-body%2C%20ui-sans-serif%2C%20-apple-system%2C%20system-ui%2C%20%26quot%3BSegoe%20UI%26quot%3B%2C%20Helvetica%2C%20%26quot%3BApple%20Color%20Emoji%26quot%3B%2C%20Arial%2C%20sans-serif%2C%20%26quot%3BSegoe%20UI%20Emoji%26quot%3B%2C%20%26quot%3BSegoe%20UI%20Symbol%26quot%3B%22%20font-weight%3D%22400%22%20data-d-component%3D%22svg%22%20fill%3D%22currentColor%22%20height%3D%22260%22%20style%3D%22color%3Argb\(255%2C%20255%2C%20255\)%22%20viewBox%3D%220%200%20760%20260%22%20width%3D%22100%25%22%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Crect%20x%3D%2230%22%20y%3D%2220%22%20width%3D%22320%22%20height%3D%22200%22%20rx%3D%2210%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22190%22%20y%3D%2245%22%20text-anchor%3D%22middle%22%20font-size%3D%2216%22%3EProcessing%20Time%3C%2Ftext%3E%3Ctext%20x%3D%2250%22%20y%3D%2280%22%20font-size%3D%2213%22%3ERestart%3A%2011%3A00%3C%2Ftext%3E%3Ctext%20x%3D%2250%22%20y%3D%22105%22%20font-size%3D%2213%22%3EEvents%20arrive%20now%3C%2Ftext%3E%3Ctext%20x%3D%2250%22%20y%3D%22130%22%20font-size%3D%2213%22%3EWindow%3A%2011%3A00%E2%80%9311%3A00%3A05%3C%2Ftext%3E%3Ctext%20x%3D%2250%22%20y%3D%22155%22%20font-size%3D%2213%22%3EHistorical%20timing%20lost%3C%2Ftext%3E%3Crect%20x%3D%22410%22%20y%3D%2220%22%20width%3D%22320%22%20height%3D%22200%22%20rx%3D%2210%22%20fill%3D%22none%22%20stroke%3D%22currentColor%22%2F%3E%3Ctext%20x%3D%22570%22%20y%3D%2245%22%20text-anchor%3D%22middle%22%20font-size%3D%2216%22%3EEvent%20Time%20%2B%20Watermark%3C%2Ftext%3E%3Ctext%20x%3D%22430%22%20y%3D%2280%22%20font-size%3D%2213%22%3ERestart%3A%2011%3A00%3C%2Ftext%3E%3Ctext%20x%3D%22430%22%20y%3D%22105%22%20font-size%3D%2213%22%3EWindow%3A%2010%3A00%E2%80%9310%3A00%3A05%3C%2Ftext%3E%3Ctext%20x%3D%22430%22%20y%3D%22130%22%20font-size%3D%2213%22%3EWatermark%20follows%20replayed%20events%3C%2Ftext%3E%3Ctext%20x%3D%22430%22%20y%3D%22155%22%20font-size%3D%2213%22%3EHistory%20reconstructed%20correctly%3C%2Ftext%3E%3C%2Fsvg%3E)

# But what about a huge Kafka backlog?

Suppose Kafka contains 6 hours of events.

Flink will replay them in event-time order.

The watermark will move like this:

```
10:00
10:05
10:10
...
11:00
...
16:00
```

It advances as newer event timestamps are read.

This is why Flink can rebuild historical windows correctly after outages.

# The real production benefit

This is where Kafka offsets, Flink checkpoints, and watermarks work together:

* Kafka offsets remember where to resume reading.

* Checkpoints restore Flink's operator state.

* Watermarks reconstruct when each historical window should close based on the replayed event timestamps.

That's why an hour-long Flink outage usually doesn't corrupt historical aggregations—as long as Kafka retained the data and Flink resumes from the correct offsets.
