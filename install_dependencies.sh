#!/bin/bash

# Update package list
#apt-get update

# Install pip if not installed
if ! command -v pip &> /dev/null
then
    echo "pip not found, installing..."
    apt-get install -y python3-pip
fi

# Install required Python packages
pip install --upgrade mysql-connector-python jtop psutil

# Ensure jtop is installed
pip install jtop
