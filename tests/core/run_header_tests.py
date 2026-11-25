#!/usr/bin/env python3
"""The three C cores talk to their hosts one way (src/core/report.h).
This test reads the three headers and checks every function they
declare against the rule:

  - A function that can fail, or that can have a line for the log,
    returns `int` and takes `Report *report` as its last parameter.
  - No function takes a buffer for an error text, a function of the
    host to call, or the time.
  - No function returns a pointer the caller must free: a pointer that
    is returned is `const` (it stays the object's).
  - No header declares a function that returns the last error.
  - A function returns one of a few plain types (not a status type of
    the header's own), and no struct of a header holds a function for
    the core to call.

And it checks the C sources: no core writes to a stream, sleeps, or
reads the clock except through report.h.

Run: python3 tests/core/run_header_tests.py
"""

import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CORE = os.path.join(REPO, "src", "core")

#: header -> the prefixes of the functions it declares
HEADERS = {"arrow_ring.h": ("ar_",), "pqc.h": ("pqc_",), "sp.h": ("sp_",)}
SOURCES = ("arrow_ring.c", "pqc.c", "pqc_mirror.c", "sp.c", "sp_catalog.c")

#: Functions that return `int` and take no Report: each answers a
#: question about a value the object holds, and cannot fail. A new one
#: is added here with its reason, or it takes a Report.
INT_WITHOUT_REPORT = {
    # pqc.h
    "pqc_reader_chunk": "reads the idx-th entry; -1 past the last",
    "pqc_ckpt_object_parse": "a yes or no, with the parts of the name",
    "pqc_versions_names": "a yes or no",
    "pqc_mirror_shipped": "1 and the values, or 0 for none",
    "pqc_finish_source": "1 and the values, or 0 past the last",
    "pqc_sweep_local": "what to do with a file",
    "pqc_sweep_remote": "what to do with a file",
    # sp.h: reads of a ticket, a plan, a store, a reader, a view
    "sp_ticket_field_kind": "the kind of a member; -1 for none",
    "sp_ticket_integer": "1 and the value, or 0 for an unknown member",
    "sp_ticket_number": "1 and the value, or 0 for an unknown member",
    "sp_ticket_file": "reads the index-th file; -1 past the last",
    "sp_ticket_is_stale": "a yes or no",
    "sp_writes_due": "the kind of write due",
    "sp_writes_commit": "the kind of write due",
    "sp_writes_on": "a yes or no",
    "sp_store_listed": "a yes or no",
    "sp_store_dropped": "a count",
    "sp_store_stored_at": "1 and the value, or 0 for none",
    "sp_reader_state": "the state",
    "sp_catalog_lists": "a yes or no",
    "sp_catalog_copy_due": "a yes or no",
}

#: What a function may return.
RETURNS = {"int", "void", "double", "int32_t", "int64_t", "uint32_t",
           "uint64_t", "const char *"}

#: The only functions that take a time: the ones that set the clock a
#: test sees.
TEST_CLOCKS = {"ar_test_set_clock", "pqc_test_set_clock", "sp_test_set_clock"}

passed = failed = 0


def check(name, problems):
    global passed, failed
    if problems:
        failed += 1
        print(f"  FAIL  {name}")
        for p in problems:
            print(f"          {p}")
    else:
        passed += 1
        print(f"  PASS  {name}")


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    return re.sub(r"//[^\n]*", " ", text)


def own_text(header):
    """A header's text without comments and without the Arrow C Data
    Interface structs, which are another project's: their members are
    function pointers by that interface."""
    text = strip_comments(open(os.path.join(CORE, header)).read())
    text = re.sub(r"#ifndef ARROW_C_DATA_INTERFACE.*?#endif\s", " ", text, flags=re.S)
    return re.sub(r"#ifndef ARROW_C_STREAM_INTERFACE.*?#endif\s", " ", text, flags=re.S)


def declarations(header):
    """[(return type, name, [parameter text])] of the functions a
    header declares."""
    text = own_text(header)
    text = re.sub(r"^\s*#[^\n]*(\\\n[^\n]*)*", " ", text, flags=re.M)   # preprocessor
    text = re.sub(r"static inline[^{;]*\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}", " ", text)
    text = re.sub(r"typedef\s+(struct|enum)[^{;]*\{(?:[^{}]|\{[^{}]*\})*\}[^;]*;", " ", text)
    text = re.sub(r'extern\s+"C"\s*\{', " ", text)
    out = []
    for m in re.finditer(r"([A-Za-z_][\w\s\*]*?)\b(\w+)\s*\(([^;{}]*)\)\s*;", text):
        ret, name, params = m.group(1).strip(), m.group(2), m.group(3)
        if ret.startswith("typedef") or not ret:
            continue
        params = [" ".join(p.split()) for p in params.split(",")]
        out.append((" ".join(ret.split()), name, params))
    return out


def test_headers():
    for header, prefixes in HEADERS.items():
        decls = declarations(header)
        names = [n for _, n, _ in decls]
        check(f"{header}: its functions are found ({len(decls)})",
              [] if len(decls) >= 20 and all(n.startswith(prefixes) for n in names)
              else [f"found {len(decls)}: {[n for n in names if not n.startswith(prefixes)]}"])

        problems = []
        for ret, name, params in decls:
            takes_report = any(re.search(r"\bReport\s*\*", p) for p in params)
            if takes_report:
                if not re.fullmatch(r"Report \*\s*report", params[-1]):
                    problems.append(f"{name}: `Report *report` is not its last parameter")
                if ret != "int":
                    problems.append(f"{name}: takes a Report and returns `{ret}`, not `int`")
            elif ret == "int" and name not in INT_WITHOUT_REPORT:
                problems.append(f"{name}: returns `int` with no Report (a status? "
                                f"then it takes one; a value? then list it in "
                                f"INT_WITHOUT_REPORT with the reason)")
        check(f"{header}: a function with a status takes `Report *report` last "
              f"and returns int", problems)

        problems = []
        for ret, name, params in decls:
            for p in params:
                if re.search(r"\berr(len|or)?\b|\bmsg(len)?\b|\berrlen\b", p):
                    problems.append(f"{name}: an error-text parameter `{p}`")
                if "(*" in p:
                    problems.append(f"{name}: takes a function to call `{p}`")
                if re.search(r"\b(now|mono|wall|block_ms)\b", p) and name not in TEST_CLOCKS \
                        and not re.fullmatch(r"double \*\s*wall", p):
                    problems.append(f"{name}: takes the time `{p}`")
        check(f"{header}: no error buffer, no function of the host, no time",
              problems)

        problems = []
        for ret, name, params in decls:
            if "*" in ret and not ret.startswith("const "):
                problems.append(f"{name}: returns `{ret}`: a pointer the caller "
                                f"would own comes back through an output parameter")
            if re.search(r"last_error|_message$", name):
                problems.append(f"{name}: a call that returns what an earlier call said")
        check(f"{header}: no owned pointer is returned, no last-error call",
              problems)

        problems = [f"{name}: returns `{ret}`" for ret, name, _ in decls
                    if ret not in RETURNS]
        for m in re.finditer(r"typedef\s+struct[^{;]*\{((?:[^{}]|\{[^{}]*\})*)\}\s*(\w+)",
                             own_text(header)):
            if "(*" in m.group(1):
                problems.append(f"struct {m.group(2)} holds a function to call")
        check(f"{header}: plain return types, no function in a struct", problems)

    stale = [n for n in INT_WITHOUT_REPORT
             if not any(n == d[1] for h in HEADERS for d in declarations(h))]
    check("the list of int functions with no Report names only functions "
          "that exist", [f"{n} is declared nowhere" for n in stale])


def test_sources():
    banned = {
        r"\b(f|v|vf|d)?printf\s*\(": "writes to a stream",
        r"\b(fputs|fputc|puts|putchar|fwrite|perror)\s*\(": "writes to a stream",
        r"\bstd(err|out)\b": "names a standard stream",
        r"\b(fopen|open|creat|socket|connect)\s*\(": "opens a file or a socket",
        r"\b(nanosleep|usleep|sleep|select|poll)\s*\(": "waits",
        r"\b(clock_gettime|gettimeofday|time)\s*\(": "reads the clock itself "
                                                      "(report.h reads it)",
    }
    for source in SOURCES:
        text = strip_comments(open(os.path.join(CORE, source)).read())
        text = re.sub(r'"(?:[^"\\\n]|\\.)*"', '""', text)        # string literals
        problems = []
        for pattern, why in banned.items():
            for m in re.finditer(pattern, text):
                line = text.count("\n", 0, m.start()) + 1
                problems.append(f"line {line}: `{m.group(0).strip()}` {why}")
        check(f"{source}: no output, no waiting, no clock but report.h's",
              problems)


if __name__ == "__main__":
    print("--- how the cores report (src/core/report.h) ---")
    test_headers()
    test_sources()
    print(f"\n{'=' * 50}\nResults: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
    print("All tests passed!")
