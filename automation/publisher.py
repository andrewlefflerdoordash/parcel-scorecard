"""Publish the scorecard to GitHub Pages."""
import re
import subprocess
from datetime import date
from pathlib import Path


def update_landing_page(scorecard_dir: Path, op_date: date, hub_count: int, spoke_count: int):
    """Add the new date entry to the landing page and move the 'Latest' badge.
    
    Args:
        scorecard_dir: Root of the parcel-scorecard repo
        op_date: The operational date for the new dashboard
        hub_count: Number of hubs in this dashboard
        spoke_count: Number of spokes in this dashboard
    """
    index_path = scorecard_dir / "index.html"
    html = index_path.read_text()
    
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]
    
    date_folder = op_date.isoformat()
    date_display = f"{months[op_date.month - 1]} {op_date.day}, {op_date.year}"
    day_name = days[op_date.weekday()]
    
    # Remove existing 'Latest' badge from any card
    html = html.replace(' <span class="badge-latest">Latest</span>', '')
    
    # Remove any existing cards for this same date to prevent duplicates
    pattern = re.compile(
        r'\s*<a class="card" href="' + re.escape(date_folder) + r'/">\s*'
        r'<div>\s*<div class="card-date">.*?</div>\s*'
        r'<div class="card-day">.*?</div>\s*</div>\s*'
        r'<div class="card-arrow">.*?</div>\s*</a>',
        re.DOTALL
    )
    html = pattern.sub('', html)
    
    # Build the new card HTML
    new_card = f'''  <a class="card" href="{date_folder}/">
    <div>
      <div class="card-date">{date_display} <span class="badge-latest">Latest</span></div>
      <div class="card-day">{day_name} — {hub_count} Hubs · {spoke_count} Spokes</div>
    </div>
    <div class="card-arrow">→</div>
  </a>'''
    
    # Insert as the first card in the grid
    grid_marker = '<div class="card-grid">'
    if grid_marker in html:
        html = html.replace(grid_marker, f'{grid_marker}\n{new_card}')
    
    index_path.write_text(html)


def git_commit_and_push(scorecard_dir: Path, op_date: date, summary: str):
    """Stage changes, commit, and push to origin/main.
    
    Args:
        scorecard_dir: Root of the parcel-scorecard repo
        op_date: The operational date
        summary: Brief summary for the commit message
    """
    date_folder = op_date.isoformat()
    
    cmds = [
        ["git", "add", f"{date_folder}/", "index.html"],
        ["git", "commit", "-m", f"Add {date_folder} daily scorecard\n\n{summary}"],
        ["git", "push", "origin", "main"],
    ]
    
    for cmd in cmds:
        result = subprocess.run(
            cmd, cwd=str(scorecard_dir),
            capture_output=True, text=True
        )
        if result.returncode != 0 and "nothing to commit" not in result.stdout + result.stderr:
            print(f"  Warning: {' '.join(cmd)} returned {result.returncode}")
            print(f"  stderr: {result.stderr[:500]}")
