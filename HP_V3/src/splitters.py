"""
AnchorPatch candidate splitters.

A splitter maps raw document bytes -> ordered list of Block, where blocks are
non-overlapping, fully covering (concatenation == original bytes), and each
block carries a stable byte range + content hash for anchoring.

Candidates (2 baselines + 2 candidates), spanning the granularity spectrum:
    fixed   : fixed K-line windows           (simplest baseline, structure-blind)
    blank   : split on blank-line runs       (simple structural baseline)
    line    : one block per physical line    (finest-granularity reference)
    struct  : adaptive marker/bracket/indent/intra-line  (the proposed candidate)

All splitters operate on bytes to guarantee byte-exact coverage.
"""
import hashlib
from dataclasses import dataclass
from typing import List
import re


@dataclass
class Block:
    block_id: int        # sequential index within the document
    start: int           # byte offset (inclusive)
    end: int             # byte offset (exclusive)
    data: bytes          # raw bytes data[start:end]

    @property
    def hash(self) -> str:
        return hashlib.sha1(self.data).hexdigest()[:12]

    @property
    def anchor(self) -> str:
        # content-addressed anchor: what a patch references to relocate a block
        return self.hash


# ---------------------------------------------------------------------------
# byte-line helpers (keep trailing "\n" so concatenation is exact)
# ---------------------------------------------------------------------------
def to_lines(data: bytes) -> List[bytes]:
    """Split into lines, each keeping its trailing b'\\n'. join(lines)==data."""
    out, start = [], 0
    for i, b in enumerate(data):
        if b == 0x0A:  # '\n'
            out.append(data[start:i + 1])
            start = i + 1
    if start < len(data):
        out.append(data[start:])
    elif len(data) == 0:
        out = []
    return out


def _finalize(pieces: List[bytes]) -> List[Block]:
    """Turn an ordered list of byte-pieces into Blocks with offsets."""
    blocks, off = [], 0
    for idx, p in enumerate(pieces):
        blocks.append(Block(idx, off, off + len(p), p))
        off += len(p)
    return blocks


def _verify(blocks: List[Block], data: bytes) -> bool:
    if not blocks:
        return data == b""
    if blocks[0].start != 0 or blocks[-1].end != len(data):
        return False
    for a, b in zip(blocks, blocks[1:]):
        if a.end != b.start:
            return False
    return b"".join(bl.data for bl in blocks) == data


# ---------------------------------------------------------------------------
# fixed  — K-line windows
# ---------------------------------------------------------------------------
def split_fixed(data: bytes, k: int = 8) -> List[Block]:
    lines = to_lines(data)
    if not lines:
        return _finalize([data]) if data else []
    pieces = [b"".join(lines[i:i + k]) for i in range(0, len(lines), k)]
    return _finalize(pieces)


# ---------------------------------------------------------------------------
# blank  — split on runs of blank lines (blank run attaches to preceding block)
# ---------------------------------------------------------------------------
def _is_blank(line: bytes) -> bool:
    return line.strip(b" \t\r\n") == b""


def split_blank(data: bytes) -> List[Block]:
    lines = to_lines(data)
    if not lines:
        return _finalize([data]) if data else []
    pieces, cur = [], []
    seen_content = False
    for ln in lines:
        cur.append(ln)
        if _is_blank(ln):
            # close block at the end of a blank run, only if it had content
            if seen_content:
                pieces.append(b"".join(cur))
                cur, seen_content = [], False
        else:
            seen_content = True
    if cur:
        pieces.append(b"".join(cur))
    # merge any leading blank-only piece into the next (keep coverage, avoid empties)
    return _finalize(pieces if pieces else [data])


# ---------------------------------------------------------------------------
# line  — one block per physical line
# ---------------------------------------------------------------------------
def split_line(data: bytes) -> List[Block]:
    lines = to_lines(data)
    return _finalize(lines if lines else ([data] if data else []))


# ---------------------------------------------------------------------------
# struct — adaptive: blank-record / marker / bracket-indent / intra-line, capped
# ---------------------------------------------------------------------------
CAP = 1200            # byte cap above which a block is recursively sub-split
GIANT_RATIO = 0.30    # one line >= this fraction of doc => giant-line mode


def _blank_segments(lines: List[bytes]) -> List[bytes]:
    """Same policy as split_blank, returns list of byte pieces."""
    pieces, cur, seen = [], [], False
    for ln in lines:
        cur.append(ln)
        if _is_blank(ln):
            if seen:
                pieces.append(b"".join(cur)); cur, seen = [], False
        else:
            seen = True
    if cur:
        pieces.append(b"".join(cur))
    return pieces or [b"".join(lines)]


def _detect_marker(lines: List[bytes]):
    """Find a repeating full-line marker that partitions the doc.
    Returns (marker_bytes, mode) where mode is 'after' (terminator) or
    'before' (initiator prefix), or (None, None)."""
    import collections
    # terminator: exact full lines repeating >=3x and short
    full = collections.Counter(l.strip() for l in lines if l.strip())
    cand = [(k, v) for k, v in full.items() if v >= 3 and len(k) <= 12]
    if cand:
        marker = max(cand, key=lambda kv: kv[1])[0]
        return marker, "after"
    # initiator prefix: line-start token repeating (BEGIN:, level-0 GEDCOM "0 ",
    # XML open tag) — use first token prefix
    prefixes = collections.Counter()
    for l in lines:
        s = l.lstrip()
        if not s.strip():
            continue
        m = re.match(rb"(BEGIN:|0 |<[A-Za-z][\w:]*\b)", s)
        if m:
            prefixes[m.group(1)] += 1
    if prefixes:
        pfx, n = prefixes.most_common(1)[0]
        if n >= 3:
            return pfx, "before"
    return None, None


def _split_by_marker(lines, marker, mode) -> List[bytes]:
    pieces, cur = [], []
    for ln in lines:
        s = ln.strip()
        if mode == "after":
            cur.append(ln)
            if s == marker:
                pieces.append(b"".join(cur)); cur = []
        else:  # before: start a new block when line begins with marker prefix
            if ln.lstrip().startswith(marker) and cur:
                pieces.append(b"".join(cur)); cur = []
            cur.append(ln)
    if cur:
        pieces.append(b"".join(cur))
    return pieces or [b"".join(lines)]


def _split_bracket_indent(piece: bytes) -> List[bytes]:
    """Sub-split a brace/indent-structured byte piece (json/xml/yaml/code).
    Cut after a line where bracket depth returns to its running base level."""
    lines = to_lines(piece)
    if len(lines) <= 1:
        return _split_intraline(piece)
    pieces, cur, depth = [], [], 0
    base = None
    for ln in lines:
        cur.append(ln)
        for c in ln:
            if c in (0x7B, 0x5B):      # { [
                depth += 1
            elif c in (0x7D, 0x5D):    # } ]
                depth -= 1
        if base is None:
            base = depth
        # cut at a line that returns to base depth and ends an element
        stripped = ln.rstrip()
        if depth <= base and stripped[-1:] in (b",", b"}", b"]", b">"):
            pieces.append(b"".join(cur)); cur = []
    if cur:
        pieces.append(b"".join(cur))
    return pieces if len(pieces) > 1 else lines


def _split_intraline(piece: bytes) -> List[bytes]:
    """Sub-split a single very long line by the best repeating delimiter."""
    # candidate delimiters; keep delimiter attached to the LEFT piece
    candidates = [
        rb"(?<=\s)(?=\d+\.\s)",     # chess move numbers "12. "
        rb"(?<=')(?=[A-Z]{2,}\+)",  # edifact-ish segment ends "'"
        rb"(?<=\})(?=,?\s*\{)",     # json objects on one line
        rb",",                       # csv-ish
        rb";\s*",                    # statements
        rb"\t",                      # tabular
    ]
    best = None
    for pat in candidates:
        parts = re.split(pat, piece)
        # re.split with lookarounds keeps content; rebuild to stay exact
        if pat in (rb",", rb";\s*", rb"\t"):
            # these consume the delimiter; rejoin by re-finding
            segs = _split_keep(piece, pat)
        else:
            segs = parts
        segs = [s for s in segs if s]
        if len(segs) >= 4 and b"".join(segs) == piece:
            if best is None or len(segs) > len(best):
                best = segs
    return best if best else [piece]


def _split_keep(piece: bytes, pat: bytes) -> List[bytes]:
    """Split keeping the delimiter attached to the preceding segment."""
    out, last = [], 0
    for m in re.finditer(pat, piece):
        out.append(piece[last:m.end()])
        last = m.end()
    if last < len(piece):
        out.append(piece[last:])
    return out


def _cap_split(piece: bytes) -> List[bytes]:
    """Ensure no piece exceeds CAP; recursively apply the gentlest sub-split."""
    if len(piece) <= CAP:
        return [piece]
    lines = to_lines(piece)
    if len(lines) == 1:
        sub = _split_intraline(piece)
    else:
        sub = _split_bracket_indent(piece)
        if len(sub) <= 1:
            sub = lines  # fall back to line split
    if len(sub) <= 1:
        return [piece]   # irreducible
    out = []
    for s in sub:
        out.extend(_cap_split(s) if len(s) > CAP else [s])
    return out


def split_struct(data: bytes) -> List[Block]:
    if not data:
        return []
    lines = to_lines(data)
    total = len(data)

    # giant-line mode: a single physical line dominates
    max_line = max((len(l) for l in lines), default=0)
    if max_line / max(1, total) >= GIANT_RATIO:
        primary = []
        for ln in lines:
            if len(ln) > CAP:
                primary.extend(_split_intraline(ln))
            else:
                primary.append(ln)
    else:
        # primary = blank segmentation; if blanks fail to partition, use marker/bracket
        primary = _blank_segments(lines)
        biggest = max((len(p) for p in primary), default=0)
        if biggest / max(1, total) > 0.60:
            marker, mode = _detect_marker(lines)
            if marker is not None:
                primary = _split_by_marker(lines, marker, mode)
            else:
                primary = _split_bracket_indent(data)

    # enforce size cap everywhere
    pieces = []
    for p in primary:
        pieces.extend(_cap_split(p))
    return _finalize(pieces)


def split_struct2(data: bytes) -> List[Block]:
    """Round-2 revision: when a record marker partitions the doc into >=2x more
    units than blank-line splitting (i.e. blank UNDER-segments but a marker
    exists), prefer the marker. Fixes struct's degeneration to blank on
    marker-bearing docs whose blank-segments stay under the 60% mega-block guard.
    Everything else identical to struct."""
    if not data:
        return []
    lines = to_lines(data)
    total = len(data)
    max_line = max((len(l) for l in lines), default=0)
    if max_line / max(1, total) >= GIANT_RATIO:
        primary = []
        for ln in lines:
            primary.extend(_split_intraline(ln) if len(ln) > CAP else [ln])
    else:
        primary = _blank_segments(lines)
        biggest = max((len(p) for p in primary), default=0)
        marker, mode = _detect_marker(lines)
        use_marker = False
        if biggest / max(1, total) > 0.60:
            use_marker = marker is not None
        elif marker is not None:
            mseg = _split_by_marker(lines, marker, mode)
            # prefer marker only if it segments substantially finer AND uniformly
            if len(mseg) >= 2 * max(1, len(primary)) and len(mseg) >= 4:
                primary = mseg
                use_marker = None  # already applied
        if use_marker is True:
            primary = _split_by_marker(lines, marker, mode)
        elif use_marker is False and biggest / max(1, total) > 0.60:
            primary = _split_bracket_indent(data)
    pieces = []
    for p in primary:
        pieces.extend(_cap_split(p))
    return _finalize(pieces)


# ---------------------------------------------------------------------------
# v2 multi-scale self-index (additive; split_struct2 stays the canonical/coarse
# partition used for output assembly + preservation accounting; medium/fine are
# ADDRESSING-ONLY scales the executor resolves back to the owning coarse block)
# ---------------------------------------------------------------------------
MEDIUM_CAP = 600
FINE_CAP = 200


def split_medium(data: bytes) -> List[Block]:
    """Medium scale: paragraph / blank-segment grouping, capped at MEDIUM_CAP."""
    if not data:
        return []
    pieces = []
    for seg in _blank_segments(to_lines(data)):
        if len(seg) <= MEDIUM_CAP:
            pieces.append(seg)
            continue
        sub = _split_bracket_indent(seg)
        if len(sub) <= 1:
            sub = to_lines(seg)
        buf = b""
        for s in sub:
            if buf and len(buf) + len(s) > MEDIUM_CAP:
                pieces.append(buf); buf = b""
            buf += s
        if buf:
            pieces.append(buf)
    return _finalize(pieces or [data])


def split_fine(data: bytes) -> List[Block]:
    """Fine scale: line-level spans; consecutive short lines merged up to
    FINE_CAP, giant single lines sub-split intra-line."""
    if not data:
        return []
    pieces, buf = [], b""
    for ln in to_lines(data):
        if len(ln) > FINE_CAP:
            if buf:
                pieces.append(buf); buf = b""
            pieces.extend(_split_intraline(ln))
            continue
        if buf and len(buf) + len(ln) > FINE_CAP:
            pieces.append(buf); buf = b""
        buf += ln
    if buf:
        pieces.append(buf)
    return _finalize(pieces or [data])


SCALE_SPLITTERS = {"coarse": split_struct2, "medium": split_medium, "fine": split_fine}


def build_multiscale_index(data: bytes):
    """{scale: [Block]} over the same bytes; every scale fully covers the doc."""
    return {scale: fn(data) for scale, fn in SCALE_SPLITTERS.items()}


SPLITTERS = {
    "fixed": split_fixed,
    "blank": split_blank,
    "line": split_line,
    "struct": split_struct,
}

# extended set including the round-2 revision (for A/B comparison only)
SPLITTERS_AB = dict(SPLITTERS, struct2=split_struct2)


if __name__ == "__main__":
    # self-test: coverage exactness on a handful of real docs
    import glob, os, sys
    _samples = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            os.pardir, "data", "samples_delegate52")
    paths = []
    for pat in ["accounting1", "json1", "chess2", "molecule1", "calendar1",
                "genealogy1", "python1", "subtitles2", "latex1", "geodata1"]:
        paths.extend(glob.glob(os.path.join(_samples, pat, "basic_state", "*")))
    if not paths:
        print("RESULT: FAIL (no sample documents found — self-test would test nothing)")
        sys.exit(1)
    bad = 0
    for p in paths:
        with open(p, "rb") as f:
            data = f.read()
        row = []
        for name, fn in {**SPLITTERS, "struct2": split_struct2,
                         "medium": split_medium, "fine": split_fine}.items():
            blocks = fn(data)
            ok = _verify(blocks, data)
            bad += 0 if ok else 1
            row.append(f"{name}={len(blocks):>4}{'' if ok else '!COV'}")
        print(f"{os.path.basename(p):28s} {' '.join(row)}")
    if bad:
        print(f"RESULT: FAIL ({bad} coverage violations)")
        sys.exit(1)
    print("RESULT: PASS (all splitters byte-exact coverage)")
