"""The model behind docs/checkpoint-cost.md: the bytes a writer puts in
the store, per checkpoint, under four designs.

    python3 docs/checkpoint_cost_model.py        # prints the tables of the document

It is arithmetic, not a test of the package: it reads no store and
writes no file. One run is one unfinished file, written for RUN_HOURS
at a constant row rate, with a checkpoint every CHECKPOINT_S.

The four designs:

  today        every checkpoint writes one checkpoint object: every
               finished row group not yet in a 5 MiB chunk, the whole
               unfinished row group, and the footer.
  steps_time   every checkpoint writes only the new rows, as a small
               object. Small objects merge in steps (class Steps). A
               finished row group is written once, as its own object.
               The file is cut by time.
  steps_bytes  steps_time and two byte rules: the file is also cut at
               FILE_CUT_KB, and a checkpoint whose new rows reach the
               S3 part minimum writes them straight into a chunk.
  groups_only  no checkpoints: a row group is written when it is full.
               Same objects and byte rules as steps_bytes.

Sizes are those of one stream, measured in its store on 2026-11-09:
55 columns, 100 rows per second, a checkpoint every 15 seconds. The
other row rates scale the rows only. See "How the numbers were made"
in the document.
"""

# ---- sizes, measured -------------------------------------------------------

ROW_GROUP_ROWS = 122_880            # rows in a full row group (DuckDB's size)
ROW_GROUP_KB = 831.0                # a finished row group: one 5.82 MB chunk held 7
KB_PER_ROW = ROW_GROUP_KB / ROW_GROUP_ROWS   # 6.8 bytes per row
FOOTER_BASE_KB = 4.7                # footers of four finished files with 1, 2, 3 and 9
FOOTER_ENTRY_KB = 4.95              #   row groups: 9.6, 14.5, 19.4 and 49.1 KB
S3_PART_MIN_KB = 5 * 1024 * 1.024   # 5 MiB: the smallest part S3 copies into a file

# ---- sizes, estimated ------------------------------------------------------

COLUMNS = 55
OBJECT_OVERHEAD_KB = 0.080 * COLUMNS   # page and column headers of one small object
LIST_ENTRY_KB = 0.05                   # one object's line in the checkpoint's list

# ---- the run ---------------------------------------------------------------

CHECKPOINT_S = 15.0
RUN_HOURS = 3.0                     # the file is cut by time at the end of the run
FILE_CUT_KB = 256_000.0             # steps_bytes, groups_only: also cut the file here
RUN_LIMIT = 16                      # the most objects of one size before they merge


def unfinished_kb(rows, overhead_kb=None):
    """One object holding `rows` rows of the unfinished row group."""
    if overhead_kb is None:
        overhead_kb = OBJECT_OVERHEAD_KB
    return overhead_kb + KB_PER_ROW * rows


def footer_kb(entries):
    """The Parquet footer of a file with `entries` row groups."""
    return FOOTER_BASE_KB + FOOTER_ENTRY_KB * entries


class Steps:
    """The objects of the unfinished row group, merged in steps.

    Every checkpoint adds one small object. Objects of one size form a
    run. A run merges into one object of the next size when the footer
    bytes the run has cost reach the size of the merged object, or when
    the run reaches RUN_LIMIT objects. The footer cost of a run is
    FOOTER_ENTRY_KB for each of its objects at each checkpoint, because
    every object is a row group with its own entry in the footer, and
    the footer is written at every checkpoint."""

    def __init__(self, overhead_kb=None):
        self.overhead_kb = overhead_kb
        self.runs = [[]]            # runs[k]: the row counts of the objects of size k
        self.paid = [0.0]           # footer KB the objects of runs[k] have cost

    def add(self, rows):
        """A checkpoint with `rows` new rows. Returns the KB written."""
        self.runs[0].append(rows)
        written, new_rows_written, k = 0.0, False, 0
        while k < len(self.runs):
            run = self.runs[k]
            merged_kb = unfinished_kb(sum(run), self.overhead_kb)
            if len(run) >= 2 and (self.paid[k] >= merged_kb or len(run) >= RUN_LIMIT):
                written += merged_kb
                new_rows_written = new_rows_written or k == 0
                if k + 1 == len(self.runs):
                    self.runs.append([])
                    self.paid.append(0.0)
                self.runs[k + 1].append(sum(run))
                self.runs[k], self.paid[k] = [], 0.0
            k += 1
        if not new_rows_written:
            written += unfinished_kb(rows, self.overhead_kb)
        return written

    def count(self):
        return sum(len(run) for run in self.runs)

    def charge_footer(self):
        for k, run in enumerate(self.runs):
            self.paid[k] += FOOTER_ENTRY_KB * len(run)

    def clear(self):
        self.runs, self.paid = [[]], [0.0]


def today(rate, parts=False):
    """Returns (KB per checkpoint, largest checkpoint object in KB).
    parts=True: instead the mean KB per checkpoint of the three parts
    of the checkpoint object (waiting row groups, unfinished rows,
    footer)."""
    new_rows = rate * CHECKPOINT_S
    checkpoints = int(RUN_HOURS * 3600 / CHECKPOINT_S)
    total, largest = 0.0, 0.0
    unfinished = 0.0                # rows of the unfinished row group
    waiting = 0                     # finished row groups not yet in a chunk
    groups = 0                      # finished row groups in the file
    split = [0.0, 0.0, 0.0]

    def checkpoint_object():
        return (waiting * ROW_GROUP_KB
                + (unfinished_kb(unfinished) if unfinished else 0.0)
                + footer_kb(groups + (1 if unfinished else 0)))

    for _ in range(checkpoints):
        unfinished += new_rows
        if unfinished >= ROW_GROUP_ROWS:
            full = int(unfinished // ROW_GROUP_ROWS)
            unfinished -= full * ROW_GROUP_ROWS
            waiting += full
            groups += full
            if waiting * ROW_GROUP_KB >= S3_PART_MIN_KB:
                total += waiting * ROW_GROUP_KB         # the chunk, written once
                waiting = 0
        total += checkpoint_object()
        largest = max(largest, checkpoint_object())
        split[0] += waiting * ROW_GROUP_KB
        split[1] += unfinished_kb(unfinished) if unfinished else 0.0
        split[2] += footer_kb(groups + (1 if unfinished else 0))
    total += checkpoint_object()    # the file cut: one last checkpoint object
    if parts:
        return [part / checkpoints for part in split]
    return total / checkpoints, largest


def steps(rate, byte_rules, with_checkpoints=True, overhead_kb=None):
    """Returns (KB per checkpoint, largest checkpoint object in KB, the
    most small objects the unfinished row group held, files written)."""
    new_rows = rate * CHECKPOINT_S
    checkpoints = int(RUN_HOURS * 3600 / CHECKPOINT_S)
    total, largest, most, files = 0.0, 0.0, 0, 1
    small = Steps(overhead_kb)
    unfinished = 0.0                # rows of the unfinished row group
    loose = 0                       # finished row group objects not yet in a chunk
    chunks = 0
    groups = 0                      # finished row groups in the file
    file_kb = 0.0

    def checkpoint_object(entries, listed):
        return footer_kb(entries) + LIST_ENTRY_KB * listed

    def cut_file():
        """The unfinished rows become one finished row group, and one
        last checkpoint object names it. S3 builds the file from the
        chunks and the objects left over: nothing else is written."""
        nonlocal total
        if unfinished:
            total += unfinished_kb(unfinished, overhead_kb)
        total += checkpoint_object(groups + (1 if unfinished else 0), chunks + loose + 1)

    for _ in range(checkpoints):
        unfinished += new_rows
        file_kb += new_rows * KB_PER_ROW
        write_checkpoint_object = with_checkpoints
        if unfinished >= ROW_GROUP_ROWS:
            full = int(unfinished // ROW_GROUP_ROWS)
            unfinished -= full * ROW_GROUP_ROWS
            groups += full
            small.clear()           # the finished row group replaces the small objects
            write_checkpoint_object = True
            if byte_rules and loose == 0 and full * ROW_GROUP_KB >= S3_PART_MIN_KB:
                total += full * ROW_GROUP_KB            # straight into a chunk, written once
                chunks += 1
            else:
                loose += full
                total += full * ROW_GROUP_KB            # each row group, as its own object
                if loose * ROW_GROUP_KB >= S3_PART_MIN_KB:
                    total += loose * ROW_GROUP_KB       # and again in the chunk S3 needs
                    loose = 0
                    chunks += 1
            if unfinished and with_checkpoints:
                total += small.add(unfinished)
        elif with_checkpoints:
            total += small.add(new_rows)
        most = max(most, small.count())
        if write_checkpoint_object:
            size = checkpoint_object(groups + small.count(), chunks + loose + small.count())
            total += size
            largest = max(largest, size)
        small.charge_footer()
        if byte_rules and file_kb >= FILE_CUT_KB:
            cut_file()
            files += 1
            small.clear()
            unfinished, loose, chunks, groups, file_kb = 0.0, 0, 0, 0, 0.0
    cut_file()
    return total / checkpoints, largest, most, files


def span(seconds):
    if seconds < 90:
        return f"{seconds:.0f} s"
    if seconds < 5400:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.0f} h"


def kb(value):
    return f"{value:,.1f}" if value < 10 else f"{value:,.0f}"


RATES = (1, 10, 100, 1_000, 10_000, 100_000)


def main():
    print(f"Sizes: {COLUMNS} columns, {KB_PER_ROW * 1000:.1f} bytes per row, a checkpoint every "
          f"{CHECKPOINT_S:.0f} s, one {RUN_HOURS:.0f}-hour run.\n")

    print("Table 1. Today: the checkpoint object.\n")
    print("| Rows/s | New rows per checkpoint, KB | Written per checkpoint, KB | Largest checkpoint object, KB |")
    print("|---|---|---|---|")
    for rate in RATES:
        mean, largest = today(rate)
        print(f"| {rate:,} | {kb(rate * CHECKPOINT_S * KB_PER_ROW)} | {kb(mean)} | {kb(largest)} |")

    print("\nTable 1b. Today, at 100 rows/s: the parts of the checkpoint object.\n")
    print("| Part | Mean KB per checkpoint | Share |")
    print("|---|---|---|")
    split = today(100, parts=True)
    for name, part in zip(("Finished row groups that wait for a 5 MiB chunk",
                           "The rows of the unfinished row group", "The footer"), split):
        print(f"| {name} | {kb(part)} | {100 * part / sum(split):.0f}% |")

    print("\nTable 2. KB put in the store per checkpoint interval.\n")
    print("| Rows/s | New rows | Today | Steps, by time | Steps, by time or bytes | Row groups only |")
    print("|---|---|---|---|---|---|")
    for rate in RATES:
        print(f"| {rate:,} | {kb(rate * CHECKPOINT_S * KB_PER_ROW)} | {kb(today(rate)[0])} "
              f"| {kb(steps(rate, False)[0])} | {kb(steps(rate, True)[0])} "
              f"| {kb(steps(rate, True, with_checkpoints=False)[0])} |")

    print("\nTable 3. Rows lost if the writer dies, as time.\n")
    print("| Rows/s | With checkpoints | Row groups only |")
    print("|---|---|---|")
    for rate in RATES:
        fill = min(ROW_GROUP_ROWS / rate, RUN_HOURS * 3600)
        print(f"| {rate:,} | {span(CHECKPOINT_S)} | {span(fill)} |")

    print("\nTable 4. Steps, by time or bytes: what a reader meets.\n")
    print("| Rows/s | Most small objects in the unfinished row group | Largest checkpoint object, KB | Files in the run |")
    print("|---|---|---|---|")
    for rate in RATES:
        _, largest, most, files = steps(rate, True)
        print(f"| {rate:,} | {most} | {kb(largest)} | {files} |")

    print("\nCheck. The estimated object overhead, at 100 rows/s, steps by time or bytes:\n")
    print("| Overhead per column, bytes | KB per checkpoint |")
    print("|---|---|")
    for per_column in (0, 80, 160, 320):
        print(f"| {per_column} | {kb(steps(100, True, overhead_kb=per_column / 1000 * COLUMNS)[0])} |")


if __name__ == "__main__":
    main()
