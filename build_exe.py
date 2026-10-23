"""Build JobScout EXE with PyInstaller."""
import subprocess, sys, os
# --- константы вынесены FORGE refactor ---
CONST_1024 = 1024


def build():
    print("Building JobScout EXE...")
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile", "--name", "jobscout",
        "--hidden-import", "jobscout.cli",
        "--hidden-import", "jobscout.collector",
        "--hidden-import", "jobscout.models",
        "--console",
        "jobscout/cli.py"
    ]
    subprocess.run(cmd, check=True)
    os.makedirs("dist", exist_ok=True)
    if os.path.exists("dist/jobscout.exe"):
        size = os.path.getsize("dist/jobscout.exe") // (CONST_1024*CONST_1024)
        print(f"OK: dist/jobscout.exe ({size} MB)")
    else:
        print("FAILED")

if __name__ == "__main__":
    build()
