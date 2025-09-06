import pyarrow as pa
from datetime import datetime
from utils import get, load_state, save_state
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd

def get_park_ids():
    url = "https://queue-times.com/parks"
    response = get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.select('a[href^="/parks/"]')
    ids = [link['href'].split('/')[-1] for link in links]
    return ids

def process_attendance() -> pa.Table:
    state = load_state("attendance")
    last_update = state.get("last_update")
    
    print("Fetching park IDs...")
    park_ids = get_park_ids()
    
    all_data = []
    
    for park_id in park_ids:
        print(f"Processing park: {park_id}")
        url = f"https://queue-times.com/parks/{park_id}/attendances"
        
        try:
            response = get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", {"class": "table"})
            title = soup.find("h1", {"class": "title"})
            
            if table is None:
                continue
            
            df = pd.read_html(StringIO(str(table)))[0]
            
            df['Attendance'] = df['Attendance'].str.split(' ').str[0]
            df['Attendance'] = df['Attendance'].str.replace(',', '')
            
            park_name = title.text.strip() if title else park_id
            
            for _, row in df.dropna().iterrows():
                all_data.append({
                    'park_id': park_id,
                    'park_name': park_name,
                    'year': int(row['Year']),
                    'attendance': int(row['Attendance'])
                })
                
        except Exception as e:
            print(f"Error processing park {park_id}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No attendance data found")
    
    table = pa.Table.from_pylist(all_data)
    
    save_state("attendance", {"last_update": datetime.now().isoformat()})
    
    return table