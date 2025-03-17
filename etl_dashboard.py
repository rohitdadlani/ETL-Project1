import os
from datetime import datetime
from flask import Flask, render_template

app = Flask(__name__)

# Path to your ETL log file
LOG_FILE_PATH = "etl_pipeline.log"  # if the file is in the same directory as etl_dashboard.py


def parse_log_file(log_path):
    
    if not os.path.exists(log_path):
        return None, None, []

    last_start_time = None
    last_end_time = None
    all_lines = []

    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            all_lines.append(line)

            if "Starting ETL pipeline" in line:
                timestamp_str = line.split(" - ")[0]  # e.g. "2025-03-17 12:00:00,123"
                try:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                    last_start_time = dt
                except ValueError:
                    pass

            elif "ETL pipeline completed successfully" in line:
                timestamp_str = line.split(" - ")[0]
                try:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                    last_end_time = dt
                except ValueError:
                    pass

    # Show the last 10 lines in the dashboard
    recent_lines = all_lines[-10:] if len(all_lines) > 10 else all_lines

    return last_start_time, last_end_time, recent_lines


@app.route("/")
def dashboard():
    last_start, last_end, recent_lines = parse_log_file(LOG_FILE_PATH)

    # Calculate execution time in minutes
    execution_time = None
    if last_start and last_end:
        duration = last_end - last_start  # timedelta
        execution_time = round(duration.total_seconds() / 60, 2)

    # Render the HTML template, passing the data
    return render_template(
        "dashboard.html",
        last_start=last_start,
        last_end=last_end,
        execution_time=execution_time,
        recent_lines=recent_lines
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
