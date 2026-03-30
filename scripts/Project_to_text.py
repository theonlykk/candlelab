import os
import base64
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
SOURCE_DIR  = r"D:\candlelab"
OUTPUT_FILE = "Project_Dump.txt"

IGNORE_DIRS = {
    'venv', '__pycache__', '.git', '.idea',
    'precomputed', 'notebooks', 'vba_modules',
}

IGNORE_EXTS = {
    '.pyc', '.parquet', '.exe', '.db', '.zip',
    '.csv', '.json', '.png', '.jpg', '.gif',
    '.css', '.md', '.log',
    '.xlsm', '.xls', '.xlsx', '.pdf',
    '.ipynb', '.suo', '.sln', '.vbproj', '.vb',
}

IGNORE_FILES = {
    'Project_To_Bootstrap.py',
    'Project_Dump.txt',
    
    
}


def is_text_file(filepath):
    try:
        with open(filepath, 'tr') as f:
            f.read()
            return True
    except Exception:
        return False


def dump_project():
    src = Path(SOURCE_DIR)
    print(f"[*] Scanning {SOURCE_DIR}...")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as out:

        out.write(f"PROJECT DUMP: {SOURCE_DIR}\n")
        out.write("=" * 80 + "\n\n")

        # ── Folder structure ──────────────────────────────────────────────────
        out.write("FOLDER STRUCTURE\n")
        out.write("-" * 80 + "\n")
        for root, dirs, files in os.walk(src):
            dirs[:] = sorted(d for d in dirs if d not in IGNORE_DIRS)
            level = len(Path(root).relative_to(src).parts)
            indent = "    " * level
            folder_name = Path(root).name if level > 0 else "."
            out.write(f"{indent}{folder_name}/\n")
            file_indent = "    " * (level + 1)
            for file in sorted(files):
                ext = os.path.splitext(file)[1].lower()
                if file not in IGNORE_FILES and ext not in IGNORE_EXTS:
                    out.write(f"{file_indent}{file}\n")
        out.write("\n")

        # ── File contents ─────────────────────────────────────────────────────
        out.write("FILE CONTENTS\n")
        out.write("=" * 80 + "\n\n")

        for root, dirs, files in os.walk(src):
            dirs[:] = sorted(d for d in dirs if d not in IGNORE_DIRS)
            for file in sorted(files):
                if file in IGNORE_FILES:
                    continue
                ext = os.path.splitext(file)[1].lower()
                if ext in IGNORE_EXTS:
                    continue

                filepath = Path(root) / file
                rel_path = filepath.relative_to(src).as_posix()
                size_kb  = filepath.stat().st_size / 1024

                out.write(f"{'=' * 80}\n")
                out.write(f"FILE: {rel_path}  ({size_kb:.1f} KB)\n")
                out.write(f"{'=' * 80}\n")

                if is_text_file(filepath):
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        out.write(f.read())
                else:
                    out.write(f"[BINARY FILE — base64 encoded]\n")
                    with open(filepath, 'rb') as f:
                        out.write(base64.b64encode(f.read()).decode('utf-8'))

                out.write(f"\n\n")
                print(f"  [+] {rel_path:<60} ({size_kb:.1f} KB)")

    size_kb = Path(OUTPUT_FILE).stat().st_size / 1024
    print(f"\n[SUCCESS] Written to {OUTPUT_FILE} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    dump_project()
