import os
import sys
import pandas as pd
import tempfile
from threading import Lock, Thread
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

# --- Paths & Constants ---
DATA_DIR = "data"
# Vercel's writable directory for persistent cache during the session
CACHE_COMP = os.path.join(tempfile.gettempdir(), "competitors.parquet")
CACHE_SPEC = os.path.join(tempfile.gettempdir(), "specialists.parquet")

PERMISSIBLE_EXTRA = {'magic', 'mmagic', '333ft', '333mbo'}
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft", "fto"}

# Global State
DATA_LOCK = Lock()
DF_COMPETITORS = None
DF_SPECIALISTS = None
IS_LOADED = False

def load_data():
    global DF_COMPETITORS, DF_SPECIALISTS, IS_LOADED
    
    with DATA_LOCK:
        # 1. Check if Parquet Cache already exists to save time
        if os.path.exists(CACHE_COMP) and os.path.exists(CACHE_SPEC):
            print("📦 Loading data from Parquet cache...", file=sys.stderr)
            DF_COMPETITORS = pd.read_parquet(CACHE_COMP)
            DF_SPECIALISTS = pd.read_parquet(CACHE_SPEC)
            IS_LOADED = True
            print("✅ Cache loaded successfully.", file=sys.stderr)
            return

        # 2. Fallback: Process TSVs if cache is missing
        try:
            print("📂 Cache missing. Processing TSVs (this may take a minute)...", file=sys.stderr)
            
            # Map Persons (Using dtype=str to prevent mixed type warnings)
            p_df = pd.read_csv(
                os.path.join(DATA_DIR, "WCA_export_persons.tsv"), 
                sep='\t', 
                dtype={'wca_id': str, 'sub_id': int}
            )
            p_df = p_df[p_df['sub_id'] == 1][['wca_id', 'name', 'country_id']]
            
            # Process Competitors (Ranks)
            print("📊 Processing Ranks...", file=sys.stderr)
            # Use low_memory=False to stop the DtypeWarning on column 2 (person_id/rank)
            rs = pd.read_csv(os.path.join(DATA_DIR, "WCA_export_ranks_single.tsv"), sep='\t', low_memory=False)
            ra = pd.read_csv(os.path.join(DATA_DIR, "WCA_export_ranks_average.tsv"), sep='\t', low_memory=False)
            
            ranks = pd.concat([rs, ra])[['person_id', 'event_id']].drop_duplicates()
            
            # Group events into a list per person
            comp_final = ranks.groupby('person_id')['event_id'].apply(list).reset_index()
            comp_final = comp_final.merge(p_df, left_on='person_id', right_on='wca_id')
            
            # Process Specialists (Results)
            print("🏆 Processing Results for Specialists...", file=sys.stderr)
            res_df = pd.read_csv(os.path.join(DATA_DIR, "WCA_export_results.tsv"), sep='\t', low_memory=False)
            
            # Filter for Podiums in Finals (Round f or c, pos 1-3)
            podiums = res_df[
                (res_df['round_type_id'].isin(['f', 'c'])) & 
                (res_df['pos'] <= 3) & 
                (res_df['best'] > 0) & 
                (~res_df['event_id'].isin(REMOVED_EVENTS))
            ]
            
            # Group by person and event to get counts
            spec_counts = podiums.groupby(['person_id', 'event_id']).size().reset_index(name='count')
            spec_final = spec_counts.merge(p_df, left_on='person_id', right_on='wca_id')

            # 3. Save to Parquet for next time
            comp_final.to_parquet(CACHE_COMP)
            spec_final.to_parquet(CACHE_SPEC)
            
            DF_COMPETITORS = comp_final
            DF_SPECIALISTS = spec_final
            IS_LOADED = True
            print("✅ Data Parqueted and Loaded.", file=sys.stderr)
            
        except Exception as e:
            print(f"❌ Error during TSV processing: {e}", file=sys.stderr)

# --- Routes ---

@app.route('/')
def index(): 
    return render_template('index.html')

@app.route('/specialist')
def specialist_page(): 
    return render_template('specialist.html')

@app.route('/api/competitors')
def api_comp():
    if not IS_LOADED: return jsonify({"error": "Data is still loading..."}), 503
    
    events = request.args.get("events", "").split(",")
    selected_set = {e.strip() for e in events if e.strip() and e.strip() not in PERMISSIBLE_EXTRA}
    
    if not selected_set: return jsonify([])

    def match_events(evt_list):
        p_set = set(evt_list) - PERMISSIBLE_EXTRA
        return p_set == selected_set

    mask = DF_COMPETITORS['event_id'].apply(match_events)
    results = DF_COMPETITORS[mask].head(100)
    
    return jsonify([{
        "personId": r['wca_id'], 
        "personName": r['name'], 
        "personCountryId": r['country_id'], 
        "completed_events": r['event_id']
    } for _, r in results.iterrows()])

@app.route('/api/specialists')
def api_spec():
    if not IS_LOADED: return jsonify({"error": "Data is still loading..."}), 503
    
    target_events = set([e.strip() for e in request.args.get("events", "").split(",") if e.strip()])
    if not target_events: return jsonify([])
    
    # Filter for people who have podiums in the EXACT set of events selected
    grouped = DF_SPECIALISTS.groupby('person_id')
    
    matches = []
    for pid, group in grouped:
        p_events = set(group['event_id'].unique())
        if p_events == target_events:
            row = group.iloc[0]
            matches.append({
                "personName": row['name'],
                "personId": pid,
                "personCountryId": row['country_id'],
                "podiums": [{"eventId": r['event_id'], "count": int(r['count'])} for _, r in group.iterrows()]
            })
            if len(matches) >= 100: break
            
    return jsonify(matches)

# --- Startup ---
if __name__ == "__main__":
    # Start loading in background so the server starts immediately
    Thread(target=load_data, daemon=True).start()
    app.run(debug=True, port=5000)