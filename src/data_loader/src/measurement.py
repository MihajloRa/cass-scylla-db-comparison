import csv

def write_average_time_to_csv(file_path, operation_name, total_time, count, batch_size):
    # Calculate the average time per batch operation in seconds
    average_time = total_time / (count / batch_size) if count else 0
    
    # Open the CSV file for appending. If it doesn't exist, it will be created.
    with open(file_path, 'a+', newline='') as file:
        writer = csv.writer(file)
        
        # Check if the file is empty to write headers
        file.seek(0)  # Go to the start of the file
        if len(file.readline()) == 0:  # Check if the first line is empty
            # Write headers with a unit for clarity
            writer.writerow(['Operation Name', 'Average Time per Batch (seconds)', 'Total Operations', 'Total Time (seconds)', 'Batch Size'])
        
        # Write the data row
        writer.writerow([operation_name, average_time, count, total_time, batch_size])
