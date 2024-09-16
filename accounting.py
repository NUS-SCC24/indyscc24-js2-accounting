import openstack
import dateutil.parser as date_parser

# We want to filter out events that are just noise
# e.g. volume attachments, snapshots, etc.
RELEVANT_EVENTS = [
    "create", 
    "delete", 
    "pause", 
    "resume",
    "shelve",
    "start",
    "stop",
    "suspend",
    "unpause",
    "unshelve"
]

# If the last event before the time in question was {key}, then the instance should be in the state {value}
ACTION_TO_STATE_LOOKUP = {
    "create": "active",
    "delete": "deleted",
    "pause": "suspended",
    "resume": "active",
    "shelve": "shelved_offloaded",
    "start": "active",
    "stop": "stopped",
    "suspend": "suspended",
    "unpause": "active",
    "unshelve": "active",
}

STATE_CHARGE_MULTIPLIERS = {
    "active": 1,
    "deleted": 0,
    "error": 0,
    "not_yet_created": 0,
    "paused": 0.75,
    "resized": 1,
    "shelved_offloaded": 0,
    "stopped": 0.5,
    "suspended": 0.75,
}

FLAVOR_TYPE_CHARGE_MULTIPLIERS = {
    "m3": 1,
    "g3": 2,
    "g3p": 0,
    "r3": 2,
    "p3": 1,
}

# Gets all event history for an instance given its UUID
def get_actions_for_instance(conn, instance_id):

    actions = []
    for action in conn.compute.server_actions(instance_id):
        actions.append({"action": action.action, "time": date_parser.parse(action.start_time + "Z")})

    actions = list(filter((lambda action: action["action"] in RELEVANT_EVENTS), actions))
    actions.sort(key=(lambda action: action["time"]))

    # If the instance has no create event, put one in
    if actions[0]["action"] != "create":
        actions = [{"action": "create", "time": date_parser.parse(conn.compute.get_server(instance_id).created_at)}] + actions

    return actions

# Returns a list of activity intervals, bound by start and end UTC datetimes.
# e.g. [{"start": datetime, "end": datetime, "state": "active"}, ...]
def get_charge_intervals_for_instance(conn, instance_id, start, end):
    if end < start:
        raise ValueError("End datetime cannot be before start datetime!")

    actions = get_actions_for_instance(conn, instance_id)
    
    # We don't care about any actions after `end`
    actions = list(filter(lambda action: action["time"] < end, actions))

    if len(actions) == 0:
        raise Exception("Action history is empty!")
    
    # Determine the starting state of the instance (state of the instance at datetime `start`)
    starting_state = ""
    # Look for the last action before `start`
    if start <= actions[0]["time"]:
        # Handle the special case where `start` is before the first action
        starting_state = "deleted"
    else:
        for action in actions:
            if action["time"] <= start:
                starting_state = ACTION_TO_STATE_LOOKUP[action["action"]]
            else:
                break

    # Filter out actions before `start`
    actions = list(filter(lambda action: action["time"] >= start, actions))

    intervals = []
    # Special case where there are no actions during the period (state stays the same).
    if len(actions) == 0:
        intervals = [{"start": start, "end": end, "state": starting_state}]
    else:
        # Create a first interval from `start` until the first action
        intervals.append({"start": start, "end": actions[0]["time"], "state": starting_state})

        for i in range(len(actions)):
            # If this is the last action, cap the interval's range at `end`
            if i == (len(actions) - 1):
                intervals.append({
                    "start": actions[i]["time"],
                    "end": end,
                    "state": ACTION_TO_STATE_LOOKUP[actions[i]["action"]]
                })
            else:
                # It's safe to take the i+1'th item since len > 0 and i != len
                intervals.append({
                    "start": actions[i]["time"],
                    "end": actions[i+1]["time"],
                    "state": ACTION_TO_STATE_LOOKUP[actions[i]["action"]]
                })
    return intervals

# IMPORTANT:
# Assumes the instance was never resized (changed flavors)
def get_total_charge_for_instance(conn, instance_id, start, end):
    intervals = get_charge_intervals_for_instance(conn, instance_id, start, end)

    server = conn.compute.get_server(instance_id)
    flavor_prefix = server.flavor.original_name[:server.flavor.original_name.index(".")]

    total_charge = 0
    for interval in intervals:
        # Calculate the interval's duration in hours, without rounding
        interval_duration = (interval["end"] - interval["start"]).total_seconds() / 3600
        total_multiplier = STATE_CHARGE_MULTIPLIERS[interval["state"]] * server.flavor.vcpus * FLAVOR_TYPE_CHARGE_MULTIPLIERS[flavor_prefix]

        total_charge += interval_duration * total_multiplier

    return total_charge