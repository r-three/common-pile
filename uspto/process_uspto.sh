#!/bin/bash

#!/bin/bash

# Get the current directory
CURRENT_DIR=$(basename "$PWD")

# Check if we are already in the uspto directory
if [ "$CURRENT_DIR" != "uspto" ]; then
    cd uspto || { echo "Failed to navigate to the uspto directory"; exit 1; }
fi

# Start the Node server in the background
cd mathml-to-latex || { echo "Failed to navigate to mathml-to-latex directory"; exit 1; }
node dist/server.js &
echo "MathML to LaTeX server started."

# Get the PID of the Node server
NODE_PID=$!
echo "MathML to LaTeX server started with PID: $NODE_PID"

# Navigate back to the uspto directory
cd ..

# Run the Python script
echo "Running the uspto-to-dolma.py script..."
python uspto-to-dolma.py
echo "uspto-to-dolma.py script completed."

# Kill the Node server after the Python script completes
kill $NODE_PID

# Navigate back to the top-level directory
cd ..
echo "MathML to LaTeX server stopped."
echo "Data processing complete."
