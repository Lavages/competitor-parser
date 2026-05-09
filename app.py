import os
import sys
import pandas as pd
from flask import Flask, jsonify, request, render_template, Blueprint

app = Flask(__name__)

# --- Paths ---
DATA_DIR = "data"
CACHE_COMP = os.path.join(DATA_DIR, "competitors.parquet")
CACHE_SPEC = os.path.join(DATA_DIR, "specialists.parquet")
CACHE_COMPS = os.path.join(DATA_DIR, "competitions.parquet")

PERMISSIBLE_EXTRA = {'magic', 'mmagic', '333ft', '333mbo'}

# Global State
DF_COMPETITORS = None
DF_SPECIALISTS = None
DF_COMPETITIONS = None

def ensure_data_loaded():
    """Synchronous loader required for Serverless environments."""
    global DF_COMPETITORS, DF_SPECIALISTS, DF_COMPETITIONS
    
    # If already loaded, skip
    if DF_COMPETITORS is not None:
        return

    try:
        print("📦 Loading Parquet files...", file=sys.stderr)
        # Load only from Parquet. TSV processing is removed because 
        # Vercel's filesystem is read-only at runtime.
        if os.path.exists(CACHE_COMP):
            DF_COMPETITORS = pd.read_parquet(CACHE_COMP)
        if os.path.exists(CACHE_SPEC):
            DF_SPECIALISTS = pd.read_parquet(CACHE_SPEC)
        if os.path.exists(CACHE_COMPS):
            DF_COMPETITIONS = pd.read_parquet(CACHE_COMPS)
            
        print("✅ Data systems online.", file=sys.stderr)
    except Exception as e:
        print(f"❌ Load Error: {e}", file=sys.stderr)

# --- Competition Blueprint ---
competitions_bp = Blueprint('competitions', __name__)

@competitions_bp.route("/competitions")
def get_competitions_api():
    ensure_data_loaded()
    if DF_COMPETITIONS is None:
        return jsonify({"error": "Data unavailable"}), 500
    
    if not request.args:
        return render_template("competitions.html")

    partial = request.args.get("partial", "true").lower() == "true"
    events_param = request.args.get("events")
    
    sorted_comps = DF_COMPETITIONS.sort_values(by='date_from', ascending=False)

    def format_comp(row):
        return {
            "id": row['id'], "name": row['name'], "city": row['city_name'],
            "country": row['country_id'], "events": list(row['events']),
            "date": {"from": row['date_from'], "till": row['date_till']}
        }

    if not events_param:
        return jsonify([format_comp(r) for _, r in sorted_comps.head(100).iterrows()])

    target_events = set([e.strip() for e in events_param.split(",") if e.strip() and e.strip() != "fto"])
    mask = sorted_comps['events'].apply(lambda x: target_events.issubset(set(x)) if partial else set(x) == target_events)
    
    return jsonify([format_comp(r) for _, r in sorted_comps[mask].head(100).iterrows()])

app.register_blueprint(competitions_bp)

# --- Main Routes ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/competitors')
def api_comp():
    ensure_data_loaded()
    if DF_COMPETITORS is None: return jsonify({"error": "Loading..."}), 503
    
    events = request.args.get("events", "").split(",")
    selected_set = {e.strip() for e in events if e.strip() and e.strip() not in PERMISSIBLE_EXTRA}
    if not selected_set: return jsonify([])

    mask = DF_COMPETITORS['event_id'].apply(lambda x: (set(x) - PERMISSIBLE_EXTRA) == selected_set)
    results = DF_COMPETITORS[mask].head(100)
    
    return jsonify([{
        "personId": r['wca_id'], "personName": r['name'], 
        "personCountryId": r['country_id'], "completed_events": list(r['event_id']) 
    } for _, r in results.iterrows()])

@app.route('/api/specialists')
def api_spec():
    ensure_data_loaded()
    if DF_SPECIALISTS is None: return jsonify({"error": "Loading..."}), 503
    
    target_events = set([e.strip() for e in request.args.get("events", "").split(",") if e.strip()])
    if not target_events: return jsonify([])
    
    grouped = DF_SPECIALISTS.groupby('person_id')
    matches = []
    for pid, group in grouped:
        if set(group['event_id'].unique()) == target_events:
            row = group.iloc[0]
            matches.append({
                "personName": row['name'], "personId": pid, "personCountryId": row['country_id'],
                "podiums": [{"eventId": r['event_id'], "count": int(r['count'])} for _, r in group.iterrows()]
            })
            if len(matches) >= 100: break
    return jsonify(matches)

# Required for Vercel
app.debug = False
if __name__ == "__main__":
    app.run(debug=True, port=5000)