#!/bin/bash

# Get the current directory
CURRENT_DIR=$(basename "$PWD")

# Check if we are already in the uspto directory
if [ "$CURRENT_DIR" != "uspto" ]; then
    # Navigate to the uspto directory
    cd uspto || exit
fi

# Clone MathML to LaTeX converter
git clone https://github.com/baberabb/mathml-to-latex.git

# Navigate to the Node.js project directory
cd mathml-to-latex || exit

# Install Node.js dependencies
npm install

# Compile TypeScript to JavaScript
npx tsc

# Start the Node server in the background
node dist/server.js &

# Get the PID of the Node server
NODE_PID=$!

# Navigate back to the uspto directory
cd ..

# Run the Python script
python uspto-to-dolma.py

# Kill the Node server after the Python script completes
kill $NODE_PID

# Navigate back to the top-level directory
cd ..
