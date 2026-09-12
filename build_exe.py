"""Build JobScout EXE with PyInstaller."""
import subprocess, sys, os, shutil

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
        size = os.path.getsize("dist/jobscout.exe") // (1024*1024)
        print(f"OK: dist/jobscout.exe ({size} MB)")
    else:
        print("FAILED")

if __name__ == "__main__":
    build()
