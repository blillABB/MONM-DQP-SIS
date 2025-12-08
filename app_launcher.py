import os
import subprocess
from pathlib import Path

# --- Locate directories ---
root = Path(__file__).parent
app_dir = root / "app"
index_path = app_dir / "Home.py"

# --- Ensure PYTHONPATH includes both project root and app ---
pythonpath = os.environ.get("PYTHONPATH", "")
new_pythonpath = os.pathsep.join(filter(None, [str(root), str(app_dir), pythonpath]))
os.environ["PYTHONPATH"] = new_pythonpath

# --- Run Streamlit with explicit env ---
subprocess.run(["streamlit", "run", str(index_path)], cwd=str(root), env=os.environ)
