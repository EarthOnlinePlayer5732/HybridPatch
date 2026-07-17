import json
import os
import random
import uuid


def build_relay_task_plan(possible_forward_states, num_round_trips, seed=None):
    rng = random.Random(seed)
    states = list(possible_forward_states)
    out = []
    while len(out) < num_round_trips:
        batch = list(states)
        rng.shuffle(batch)
        out.extend(batch)
    return out[:num_round_trips]


def save_relay_task_plan(path, plan):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"forward_state_sequence": plan}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def load_relay_task_plan(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)["forward_state_sequence"]
