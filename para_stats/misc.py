with open("all_round_ids.txt") as f:
    lines = f.read().strip().splitlines()

round_ids = list(map(int, lines))
reversed_ids = round_ids[::-1]
sequence_breaks = []

for i in range(len(reversed_ids) - 1):
    current_id = reversed_ids[i]
    expected_next_id = current_id + 1
    next_id = reversed_ids[i + 1]

    if expected_next_id != next_id:
        print(f"Sequence break detected between {current_id}->{next_id}")
        sequence_breaks.append((str(current_id), str(next_id), str(next_id - current_id)))

print(f"Found {len(sequence_breaks)}, listing...")

for i, j, k in sequence_breaks:
    print(f"{i}->{j} || diff: {k}")

print("Writing sequence breaks to file...")

with open("sequence_breaks.txt", "w") as f:
    for i in sequence_breaks:
        f.write(" ".join(i) + "\n")

print("Done!")