import os
os.environ['CONNECTOR_NAME'] = 'park-attendance'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.attendance.attendance import process_attendance

def main():
    validate_environment()
    
    attendance_data = process_attendance()
    upload_data(attendance_data, "park_attendance")

if __name__ == "__main__":
    main()