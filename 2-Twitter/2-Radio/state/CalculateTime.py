# Open the file
with open('execution_times.txt', 'r') as file:
    # Initialize sum
    total_seconds = 0
    # Iterate over each line
    for line in file:
        # Split the line by colon
        parts = line.split(':')
        # Extract the numerical value and convert it to float
        seconds = float(parts[1].strip().split()[0])
        # Add to the total sum
        total_seconds += seconds

# Print the sum
print("Total seconds:", total_seconds)

