#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

echo "Installing dependencies..."
"$VENV_DIR/bin/pip" install --quiet --upgrade pip
"$VENV_DIR/bin/pip" install --quiet -r "$SCRIPT_DIR/requirements.txt"

echo ""
echo "Done."
echo ""
echo "Next steps:"
echo "  1. Copy and fill secrets:  cp .env.example .env && \$EDITOR .env"
echo "  2. Copy local config:      cp config.yaml config.local.yaml && \$EDITOR config.local.yaml"
echo "  3. Source secrets:         source .env"
echo "  4. Run:                    $VENV_DIR/bin/python $SCRIPT_DIR/pr_analytics.py --help"
echo "  5. Run tests:              $VENV_DIR/bin/pytest tests/"
echo ""
echo "Or activate the venv first:"
echo "  source $VENV_DIR/bin/activate"
