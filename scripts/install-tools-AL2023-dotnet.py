#!/usr/bin/env python3
"""
Install tools needed to build and run s3-benchrunner-dotnet on Amazon Linux 2023.
"""
import subprocess
from pathlib import Path
import sys
import os

def run(cmd_args):
    """Run a subprocess"""
    print(f'{Path.cwd()}> {subprocess.list2cmdline(cmd_args)}', flush=True)
    subprocess.run(cmd_args, check=True)

def main():
    if sys.platform != 'linux':
        print('ERROR: This script is for Amazon Linux 2023')
        sys.exit(1)

    # Download the dotnet-install script
    run(['curl', '-L', 'https://dot.net/v1/dotnet-install.sh', '-o', 'dotnet-install.sh'])

    # Make the script executable
    run(['chmod', '+x', './dotnet-install.sh'])

    # Run the installer to get the latest .NET SDK
    run(['./dotnet-install.sh', '--version', 'latest'])

    # Clean up the installer script
    os.remove('dotnet-install.sh')

    # Add .dotnet/tools to PATH if it exists in home directory
    dotnet_tools = os.path.expanduser('~/.dotnet/tools')
    if os.path.exists(dotnet_tools):
        os.environ['PATH'] = f"{dotnet_tools}:{os.environ.get('PATH', '')}"

    # Verify installation
    run(['dotnet', '--version'])

if __name__ == '__main__':
    main()
