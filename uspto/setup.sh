#!/bin/bash

# Download HF Dataset repo
git clone git@hf.co:datasets/baber/USPTO ./data/uspto/raw
echo "HF Dataset repo cloned to ./data/uspto/raw."

# Clone MathML to LaTeX converter
echo "Cloning MathML to LaTeX converter..."
git clone https://github.com/baberabb/mathml-to-latex.git

# Navigate to the Node.js project directory
echo "Navigating to mathml-to-latex directory..."
cd mathml-to-latex || { echo "Failed to navigate to mathml-to-latex directory"; exit 1; }

# Install Node.js dependencies
npm install

# Compile TypeScript to JavaScript
npx tsc
echo "TypeScript compilation completed."

# Navigate back to the original directory
cd ..
echo "Setup complete."
