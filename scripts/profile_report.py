#!/usr/bin/env python3
"""Summarize MEMEX_PROFILE spans, or emit wall-time folded stacks for Inferno."""

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import sys


def report(document):
    if document.get("schema_version") != 1:
        raise ValueError("unsupported profiling schema")
    threads = defaultdict(list)
    for event in document["traceEvents"]:
        if event["ph"] == "X":
            threads[event["tid"]].append(event)
    phases = defaultdict(lambda: {"calls": 0, "inclusive_us": 0, "self_us": 0})
    folded = Counter()
    for tid, events in threads.items():
        stack = []
        frames = []
        for event in sorted(events, key=lambda e: (e["ts"], -e["dur"])):
            start, duration = event["ts"], event["dur"]
            end = start + duration
            while stack and start >= stack[-1]["end"]:
                stack.pop()
            if stack and end > stack[-1]["end"]:
                raise ValueError(f"overlapping non-nested spans on thread {tid}")
            path = [frame["name"] for frame in stack] + [event["name"]]
            frame = {"name": event["name"], "end": end,
                     "self_us": duration, "path": path}
            if stack:
                stack[-1]["self_us"] -= duration
            frames.append(frame)
            stack.append(frame)
            phase = phases[(tid, event["name"])]
            phase["calls"] += 1
            phase["inclusive_us"] += duration
        for frame in frames:
            if frame["self_us"] < 0:
                raise ValueError("negative exclusive span duration")
            phases[(tid, frame["name"])]["self_us"] += frame["self_us"]
            if frame["self_us"]:
                folded[";".join([f"thread-{tid}", *frame["path"]])] += frame["self_us"]
    counters = Counter()
    for thread in document["threads"]:
        counters.update(thread["counters"])
    summary = {
        "capture_duration_us": document["capture_duration_us"],
        "dropped_spans": sum(t["dropped_spans"] for t in document["threads"]),
        "refused_threads": document["refused_threads"],
        "incomplete_spans": document["incomplete_spans"],
        "counters": dict(sorted(counters.items())),
        "phases": [{"tid": tid, "name": name, **values}
                   for (tid, name), values in sorted(phases.items())],
    }
    return summary, folded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--folded", action="store_true")
    parser.add_argument("--thread", type=int, help="restrict folded stacks to one thread ID")
    args = parser.parse_args()
    if args.thread is not None and not args.folded:
        parser.error("--thread requires --folded")
    summary, folded = report(json.loads(args.trace.read_text()))
    if any(summary[key] for key in ("dropped_spans", "refused_threads", "incomplete_spans")):
        print("Warning: partial capture; missing child spans inflate reported self time.", file=sys.stderr)
    if args.folded:
        for stack, duration in sorted(folded.items()):
            if args.thread is None or stack.startswith(f"thread-{args.thread};"):
                print(f"{stack} {duration}")
    else:
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
