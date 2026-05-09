import os
import sys
import pandas as pd
import threading
import time
from threading import Lock, Thread
from flask import Flask, jsonify, request, render_template, Blueprint

app = Flask(__name__)

# --- Paths & Constants ---
DATA_DIR = "data"
CACHE_COMP = os.path.join(DATA_DIR, "competitors.parquet")
CACHE_SPEC = os.path.join(DATA_DIR, "specialists.parquet")
CACHE_COMPS = os.path.join(DATA_DIR, "competitions.parquet")

# Source TSVs (Using ALL CAPS for WCA as requested)
TSV_COMPETITIONS = os.path.join(DATA_DIR, "WCA_export_competitions.tsv")
TSV_RESULTS = os.path.join(DATA_DIR, "WCA_export_results.tsv")
TSV_PERSONS = os.path.join(DATA_DIR, "WCA_export_persons.tsv")
TSV_RANKS_S = os.path.join(DATA_DIR, "WCA_export_ranks_single.tsv")
TSV_RANKS_A = os.path.join(DATA_DIR, "WCA_export_ranks_average.tsv")

PERMISSIBLE_EXTRA = {'magic', 'mmagic', '333ft', '333mbo'}
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft", "fto"}

# Global State
DATA_LOCK = Lock()
DF_COMPETITORS = None
DF_SPECIALISTS = None
DF_COMPETITIONS = None  # New global for Parquet-backed competitions
IS_LOADED = False

# --- Competition Blueprint Logic ---
competitions_bp = Blueprint('competitions', __name__)

def load_data():
    global DF_COMPETITORS, DF_SPECIALISTS, DF_COMPETITIONS, IS_LOADED
    
    with DATA_LOCK:
        # 1. Try loading all from Parquet Cache
        if os.path.exists(CACHE_COMP) and os.path.exists(CACHE_SPEC) and os.path.exists(CACHE_COMPS):
            try:
                print("📦 Loading all data from Parquet cache...", file=sys.stderr)
                DF_COMPETITORS = pd.read_parquet(CACHE_COMP)
                DF_SPECIALISTS = pd.read_parquet(CACHE_SPEC)
                DF_COMPETITIONS = pd.read_parquet(CACHE_COMPS)
                IS_LOADED = True
                print("✅ All data loaded successfully from Parquet.", file=sys.stderr)
                return
            except Exception as e:
                print(f"⚠️ Parquet Load Error: {e}", file=sys.stderr)

        # 2. Fallback: Process TSVs
        print("📂 Cache incomplete. Processing raw TSVs...", file=sys.stderr)
        try:
            os.makedirs(DATA_DIR, exist_ok=True)

            # --- Process Competitors & Specialists (Original Logic) ---
            p_df = pd.read_csv(TSV_PERSONS, sep='\t', dtype={'wca_id': str, 'sub_id': int})
            p_df = p_df[p_df['sub_id'] == 1][['wca_id', 'name', 'country_id']]
            
            rs = pd.read_csv(TSV_RANKS_S, sep='\t', low_memory=False)
            ra = pd.read_csv(TSV_RANKS_A, sep='\t', low_memory=False)
            ranks = pd.concat([rs, ra])[['person_id', 'event_id']].drop_duplicates()
            
            comp_final = ranks.groupby('person_id')['event_id'].apply(list).reset_index()
            comp_final = comp_final.merge(p_df, left_on='person_id', right_on='wca_id')
            
            res_df = pd.read_csv(TSV_RESULTS, sep='\t', low_memory=False)
            podiums = res_df[(res_df['round_type_id'].isin(['f', 'c'])) & (res_df['pos'] <= 3) & (res_df['best'] > 0) & (~res_df['event_id'].isin(REMOVED_EVENTS))]
            
            spec_counts = podiums.groupby(['person_id', 'event_id']).size().reset_index(name='count')
            spec_final = spec_counts.merge(p_df, left_on='person_id', right_on='wca_id')

            # --- Process Competitions (Parquet logic) ---
            if os.path.exists(TSV_COMPETITIONS):
                print("⚙️ Generating Competitions Parquet...", file=sys.stderr)
                c_df = pd.read_csv(TSV_COMPETITIONS, sep='\t')
                c_df = c_df[c_df['cancelled'] != 1]
                
                # Logic from competitions.py
                c_df['events'] = c_df['event_specs'].str.split(' ').apply(lambda x: [e for e in x if e and e.lower() != "fto"])
                c_df['date_from'] = c_df.apply(lambda r: f"{r['year']}-{int(r['month']):02d}-{int(r['day']):02d}", axis=1)
                c_df['date_till'] = c_df.apply(lambda r: f"{r['end_year']}-{int(r['end_month']):02d}-{int(r['end_day']):02d}", axis=1)
                
                # Keep relevant columns
                DF_COMPETITIONS = c_df[['id', 'name', 'city_name', 'country_id', 'events', 'date_from', 'date_till']]
                DF_COMPETITIONS.to_parquet(CACHE_COMPS)

            # Save the others
            comp_final.to_parquet(CACHE_COMP)
            spec_final.to_parquet(CACHE_SPEC)
            
            DF_COMPETITORS = comp_final
            DF_SPECIALISTS = spec_final
            IS_LOADED = True
            print("✅ New Parquet cache saved for all files.", file=sys.stderr)
            
        except Exception as e:
            print(f"❌ Error during TSV processing: {e}", file=sys.stderr)

# --- Competition Routes ---

@competitions_bp.route("/competitions")
def get_competitions_api():
    if not IS_LOADED:
        return jsonify({"error": "Data loading..."}), 503
    
    if not request.args:
        return render_template("competitions.html")

    partial = request.args.get("partial", "true").lower() == "true"
    events_param = request.args.get("events")
    
    # Use the Parquet-backed DataFrame
    sorted_comps = DF_COMPETITIONS.sort_values(by='date_from', ascending=False)

    def format_comp(row):
        return {
            "id": row['id'],
            "name": row['name'],
            "city": row['city_name'],
            "country": row['country_id'],
            "events": list(row['events']),
            "date": {"from": row['date_from'], "till": row['date_till']}
        }

    if not events_param:
        return jsonify([format_comp(r) for _, r in sorted_comps.head(100).iterrows()])

    target_events = set([e.strip() for e in events_param.split(",") if e.strip() and e.strip() != "fto"])

    if partial:
        mask = sorted_comps['events'].apply(lambda x: target_events.issubset(set(x)))
    else:
        mask = sorted_comps['events'].apply(lambda x: set(x) == target_events)
    
    filtered = sorted_comps[mask].head(100)
    return jsonify([format_comp(r) for _, r in filtered.iterrows()])

# Register Blueprint
app.register_blueprint(competitions_bp)

# --- Original Routes ---

@app.route('/')
def index(): return render_template('index.html')

@app.route('/specialist')
def specialist_page(): return render_template('specialist.html')

@app.route('/api/competitors')
def api_comp():
    if not IS_LOADED: return jsonify({"error": "Loading..."}), 503
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
    if not IS_LOADED: return jsonify({"error": "Loading..."}), 503
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

@app.route('/api/reload_competitions')
def reload_cache_route():
    # Trigger the full load_data again in a thread
    Thread(target=load_data, daemon=True).start()
    return jsonify({"message": "Background refresh initiated for all datasets."})

# Background loader
Thread(target=load_data, daemon=True).start()

if __name__ == "__main__":
    app.run(debug=True, port=5000)