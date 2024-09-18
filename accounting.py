import openstack
import dateutil.parser as date_parser
import argparse
from datetime import datetime,timedelta,timezone

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IndySCC 2024 Accounting Script")
    parser.add_argument("team_id", type=str, help="Your team's submission ID (e.g. \"scc999\")")
    
    seven_days_ago_in_iso8601 = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    
    parser.add_argument(
        "--start",
        type=str,
        nargs="?",
        default=seven_days_ago_in_iso8601,
        help="An ISO-8601-formatted timestamp representing the start of the accounting period. (e.g. \"2024-09-11T14:28:45.955158+00:00\")"
    )
    parser.add_argument(
        "--end",
        type=str,
        nargs="?",
        default=(datetime.now(timezone.utc)).isoformat(),
        help="An ISO-8601-formatted timestamp representing the end of the accounting period. (e.g. \"2024-09-11T14:28:45.955158+00:00\")"
    )

    args = parser.parse_args()
    conn = openstack.connect()

    servers = list(conn.compute.servers())
    servers = list(filter(lambda server: server.name.startswith(args.team_id), servers))

    if len(servers) == 0:
        raise ValueError(f"No servers found for team \"{args.team_id}\"")
    
    start = ""
    end = ""
    try:
        start = date_parser.parse(args.start)
        end = date_parser.parse(args.end)
    except date_parser._parser.ParserError:
        print("Could not parse start/end timestamp.")
        exit()
    
    sum = 0
    for server in servers:
        charge = round(get_total_charge_for_instance(conn, server.id, start, end), 2)
        print(f"{server.name}: {charge} SUs")
        sum += charge

    print(f"Total: {sum} SUs")