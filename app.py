import os
import sys
import pandas as pd
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

# --- Paths & Constants ---
DATA_DIR = "data"
CACHE_COMP = os.path.join(DATA_DIR, "competitors.parquet")
CACHE_SPEC = os.path.join(DATA_DIR, "specialists.parquet")

PERMISSIBLE_EXTRA = {'magic', 'mmagic', '333ft', '333mbo'}
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft", "fto"}

# Global State
DF_COMPETITORS = None
DF_SPECIALISTS = None
IS_LOADED = False

def load_data_sync():
    """Synchronous load for Vercel stability"""
    global DF_COMPETITORS, DF_SPECIALISTS, IS_LOADED
    
    if os.path.exists(CACHE_COMP) and os.path.exists(CACHE_SPEC):
        try:
            print("📦 Loading Parquet cache...", file=sys.stderr)
            DF_COMPETITORS = pd.read_parquet(CACHE_COMP)
            DF_SPECIALISTS = pd.read_parquet(CACHE_SPEC)
            IS_LOADED = True
            print("✅ Data Loaded.", file=sys.stderr)
        except Exception as e:
            print(f"❌ Cache Error: {e}", file=sys.stderr)
    else:
        print("❌ Critical: Parquet files NOT found in /data", file=sys.stderr)

# RUN THIS IMMEDIATELY - No threads.
# Vercel will wait for this to finish before marking the deployment as "Ready"
load_data_sync()

@app.route('/')
def index(): 
    return render_template('index.html')

@app.route('/specialist')
def specialist_page(): 
    return render_template('specialist.html')

@app.route('/api/competitors')
def api_comp():
    if not IS_LOADED: 
        return jsonify({"error": "Data still loading on server..."}), 503
    
    events = request.args.get("events", "").split(",")
    selected_set = {e.strip() for e in events if e.strip() and e.strip() not in PERMISSIBLE_EXTRA}
    
    if not selected_set: return jsonify([])

    mask = DF_COMPETITORS['event_id'].apply(lambda x: (set(x) - PERMISSIBLE_EXTRA) == selected_set)
    results = DF_COMPETITORS[mask].head(100)
    
    return jsonify([{
        "personId": r['wca_id'], 
        "personName": r['name'], 
        "personCountryId": r['country_id'], 
        "completed_events": list(r['event_id']) 
    } for _, r in results.iterrows()])

@app.route('/api/specialists')
def api_spec():
    if not IS_LOADED: 
        return jsonify({"error": "Data still loading on server..."}), 503
    
    target_events = set([e.strip() for e in request.args.get("events", "").split(",") if e.strip()])
    if not target_events: return jsonify([])
    
    grouped = DF_SPECIALISTS.groupby('person_id')
    matches = []
    for pid, group in grouped:
        if set(group['event_id'].unique()) == target_events:
            row = group.iloc[0]
            matches.append({
                "personName": row['name'],
                "personId": pid,
                "personCountryId": row['country_id'],
                "podiums": [{"eventId": r['event_id'], "count": int(r['count'])} for _, r in group.iterrows()]
            })
            if len(matches) >= 100: break
            
    return jsonify(matches)

if __name__ == "__main__":
    app.run(debug=True, port=5000)