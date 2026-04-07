#!/usr/bin/env bash
#
# demo.sh — End-to-end tpfs demo
#
# Demonstrates a turbopuffer-backed filesystem:
#   1. Agent boots with no disk → initializes workspace
#   2. Writes project files
#   3. Navigates and inspects
#   4. Searches (literal grep, regex grep, BM25 full-text)
#   5. Edits code
#   6. Hydrates to local disk → modifies locally → syncs back
#   7. "Machine dies" → reboots → all state recovered
#
# Prerequisites:
#   pip install turbopuffer click
#   export TURBOPUFFER_API_KEY="..."
#
# Usage:
#   ./demo.sh                   # uses mount "solver-agent"
#   ./demo.sh my-custom-mount   # uses custom mount name

set -euo pipefail

MOUNT="${1:-solver-agent}"
TPFS="python3 $(dirname "$0")/tpfs.py --mount $MOUNT"
SANDBOX="/tmp/tpfs-demo-$$"
MANIFEST="/tmp/tpfs-manifest-$$.json"

# Colors
GREEN='\033[0;32m'
CYAN='\033[0;36m'
DIM='\033[2m'
BOLD='\033[1m'
RESET='\033[0m'

banner() { echo -e "\n${BOLD}${CYAN}═══ $1 ═══${RESET}\n"; }
run()    { echo -e "${DIM}\$ tpfs ${*}${RESET}"; $TPFS "$@"; echo; }

cleanup() {
    echo -e "\n${DIM}Cleaning up...${RESET}"
    rm -rf "$SANDBOX" "$MANIFEST"
    $TPFS delete-mount 2>/dev/null || true
}
trap cleanup EXIT

# ─────────────────────────────────────────────────────────────────────────────

banner "ACT 1: Agent boots on fresh compute — no disk, no state"

run init
run pwd

# ─────────────────────────────────────────────────────────────────────────────

banner "ACT 2: Agent writes a project"

$TPFS put /project/solver.py --text '"""Quadratic solver v0.1 — a numpy-based root finder."""
import numpy as np
from typing import Tuple

def solve_quadratic(a: float, b: float, c: float) -> Tuple[complex, complex]:
    """Solve ax^2 + bx + c = 0 using the quadratic formula."""
    discriminant = b**2 - 4*a*c
    sqrt_disc = np.sqrt(complex(discriminant))
    return (-b + sqrt_disc) / (2*a), (-b - sqrt_disc) / (2*a)

def solve_batch(equations: list[Tuple[float, float, float]]) -> list[Tuple[complex, complex]]:
    """Solve multiple quadratic equations."""
    return [solve_quadratic(a, b, c) for a, b, c in equations]

if __name__ == "__main__":
    roots = solve_quadratic(1, -5, 6)
    print(f"x² - 5x + 6 = 0  →  x₁={roots[0]:.2f}, x₂={roots[1]:.2f}")
'
echo -e "${DIM}\$ tpfs put /project/solver.py --text '...'${RESET}"
echo "✓ solver.py written"
echo

$TPFS put /project/tests/test_solver.py --text '"""Tests for the quadratic solver."""
import numpy as np
from solver import solve_quadratic, solve_batch

def test_real_roots():
    x1, x2 = solve_quadratic(1, -5, 6)
    assert abs(x1 - 3.0) < 1e-10
    assert abs(x2 - 2.0) < 1e-10

def test_complex_roots():
    x1, x2 = solve_quadratic(1, 0, 1)
    assert abs(x1 - 1j) < 1e-10
    assert abs(x2 + 1j) < 1e-10

def test_batch():
    results = solve_batch([(1, -5, 6), (1, 0, 1)])
    assert len(results) == 2
' > /dev/null
echo -e "${DIM}\$ tpfs put /project/tests/test_solver.py --text '...'${RESET}"
echo "✓ test_solver.py written"
echo

$TPFS put /project/README.md --text '# Quadratic Solver

A numpy-based equation solver for scientific computing.

## Usage

```python
from solver import solve_quadratic
roots = solve_quadratic(1, -5, 6)
```

## Version
v0.1 — Initial release
' > /dev/null
echo -e "${DIM}\$ tpfs put /project/README.md --text '...'${RESET}"
echo "✓ README.md written"
echo

# ─────────────────────────────────────────────────────────────────────────────

banner "ACT 3: Agent navigates and inspects"

run tree /
run ls .
run head solver.py -n 5
run wc solver.py

# ─────────────────────────────────────────────────────────────────────────────

banner "ACT 4: Agent searches the codebase"

echo -e "${GREEN}Literal grep — exact substring:${RESET}"
run grep "import" /

echo -e "${GREEN}Regex grep — pattern matching:${RESET}"
run grep "def.*solve" /project --mode regex

echo -e "${GREEN}BM25 — ranked full-text search:${RESET}"
run grep "quadratic equation roots" / --mode bm25

echo -e "${GREEN}Find — glob filter:${RESET}"
run find / --glob "*.py"

# ─────────────────────────────────────────────────────────────────────────────

banner "ACT 5: Agent edits code"

run replace /project/solver.py --search "v0.1" --replace "v0.2"
run head solver.py -n 1

# ─────────────────────────────────────────────────────────────────────────────

banner "ACT 6: Hydrate → run tools locally → sync back"

echo -e "${GREEN}Pull workspace to local disk:${RESET}"
run hydrate "$SANDBOX" --root /project --manifest-out "$MANIFEST"

echo -e "${DIM}Local files:${RESET}"
find "$SANDBOX" -type f | sort
echo

echo -e "${GREEN}Modify locally (simulate agent running tools):${RESET}"

# Upgrade solver
cat > "$SANDBOX/solver.py" << 'PYEOF'
"""Quadratic solver v0.3 — upgraded locally with new features."""
import numpy as np
from typing import Tuple

def solve_quadratic(a: float, b: float, c: float) -> Tuple[complex, complex]:
    """Solve ax^2 + bx + c = 0 using the quadratic formula."""
    discriminant = b**2 - 4*a*c
    sqrt_disc = np.sqrt(complex(discriminant))
    return (-b + sqrt_disc) / (2*a), (-b - sqrt_disc) / (2*a)

def solve_batch(equations: list[Tuple[float, float, float]]) -> list[Tuple[complex, complex]]:
    """Solve multiple quadratic equations."""
    return [solve_quadratic(a, b, c) for a, b, c in equations]

def discriminant(a: float, b: float, c: float) -> float:
    """Return the discriminant b² - 4ac."""
    return b**2 - 4*a*c

if __name__ == "__main__":
    roots = solve_quadratic(1, -5, 6)
    print(f"x² - 5x + 6 = 0  →  x₁={roots[0]:.2f}, x₂={roots[1]:.2f}")
PYEOF
echo "  • Modified solver.py (v0.2 → v0.3, added discriminant function)"

# Add new file
echo "# Code Review — LGTM" > "$SANDBOX/REVIEW.md"
echo "  • Created REVIEW.md"

# Delete file
rm "$SANDBOX/README.md"
echo "  • Deleted README.md"
echo

echo -e "${GREEN}Push changes back to turbopuffer:${RESET}"
run sync "$SANDBOX" --manifest "$MANIFEST"

echo -e "${GREEN}Verify in turbopuffer:${RESET}"
run tree /project
run head /project/solver.py -n 1

# ─────────────────────────────────────────────────────────────────────────────

banner "ACT 7: Machine dies. New machine boots."

echo -e "${DIM}  (imagine this is a brand new VM with no local state)${RESET}"
echo

run pwd
run tree /project
run head /project/solver.py -n 1

echo -e "${BOLD}${GREEN}  ✓ Everything recovered from turbopuffer. No local disk needed.${RESET}"
echo
