/* report.h -- how a core talks to its host. One way, in the three cores
 * (the ring arrow_ring.h, the checkpoint format pqc.h, the stream's
 * rules sp.h).
 *
 * THE RULE
 *
 * A call into a core returns a status. Everything else the call has to
 * say comes back in the Report the caller passed: the reason for a
 * failure, and the lines for the host's log. A core never writes
 * anywhere (no file, no socket, no standard error), never calls the
 * host (no callback), and never waits. The host does the input and
 * output, the waiting and the logging.
 *
 * WHAT THAT MEANS FOR A FUNCTION
 *
 *   - A function that can fail, or that can have a line for the log,
 *     returns `int` (a ReportStatus) and takes `Report *report` as its
 *     last parameter. What it makes comes back through an output
 *     parameter. The status it returns is also report->status.
 *   - Every other function cannot fail and says nothing: it reads a
 *     value the object holds, or frees something. It takes no Report.
 *   - No function takes a buffer for an error text, a function to call,
 *     or the time.
 *   - No function returns a pointer the caller must free: such a
 *     pointer comes back through an output parameter, with a status.
 *
 * THE REPORT
 *
 * The host owns it: one for each of its threads, used again for every
 * call. The core sets it at the start of each call and never keeps a
 * pointer to it. A caller MUST pass one; a call given none answers
 * REPORT_INVALID and does nothing.
 *
 * After a call the host looks at it:
 *   - status not REPORT_OK: the call failed and `error` says why, in
 *     words for a person. The object is as it was before the call,
 *     unless the function's own text says otherwise.
 *   - lines > 0: line[0 .. lines-1] are for the host's log, oldest
 *     first, each with its level and the time it was made. A call that
 *     worked can have lines, and so can one that failed. dropped counts
 *     the lines that did not fit.
 * A line made on a thread that must not do output (a writer's) is
 * handed to another thread by the host; the core has done no output
 * either way.
 *
 * TIME
 *
 * A core reads the clock itself: the wall clock (seconds since 1970,
 * the system's) and the monotonic clock (seconds, for intervals). It
 * decides by them (a ticket is stale, a file's old checkpoints are
 * due) and stamps each line with both. It never waits for a time: a
 * function says how long until something is due, and the host waits.
 * A timestamp with no time domain named is the system's wall time.
 *
 * For tests, each core that reads the clock has one function that sets
 * the time the core sees (ar_test_set_clock, sp_test_set_clock): a test
 * of "ten minutes later" then takes no time. Library code does not call
 * it. The checkpoint core reads no clock.
 */
#ifndef CORE_REPORT_H
#define CORE_REPORT_H

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    REPORT_OK = 0,
    REPORT_INVALID = 1,       /* the caller's arguments are not ones the
                                 function takes (a missing object, a
                                 number out of range, no Report) */
    REPORT_REFUSED = 2,       /* the data is not what it must be: bytes
                                 that are not a ticket or a checkpoint, a
                                 file whose column changed its type */
    REPORT_UNSUPPORTED = 3,   /* a type or a form this core does not take */
    REPORT_NO_MEMORY = 4,
    REPORT_STALE = 5,         /* the ring no longer holds the rows asked for */
    REPORT_BLOCKED = 6,       /* a reader's pin holds what the writer
                                 needs: wait, and call again */
    REPORT_NOT_NULLABLE = 7,  /* null given for a column that takes none */
    REPORT_WIDTH = 8,         /* a value of the wrong size for its column */
    REPORT_OVERFLOW = 9       /* a value or a count too large to hold */
} ReportStatus;

typedef enum {
    REPORT_INFO = 1,          /* a milestone */
    REPORT_WARNING = 2,       /* a condition someone should look at */
    REPORT_ERROR = 3          /* a fault that loses or delays data */
} ReportLevel;

#define REPORT_ERROR_TEXT 512
#define REPORT_LINES 4
#define REPORT_LINE_TEXT 1024

typedef struct {
    int32_t level;            /* ReportLevel */
    double wall;              /* when the line was made: wall clock */
    double mono;              /* and monotonic clock */
    char text[REPORT_LINE_TEXT];
} ReportLine;

typedef struct {
    int32_t status;           /* ReportStatus of the call */
    int32_t lines;            /* entries of line[] the call made */
    int32_t dropped;          /* lines that did not fit */
    char error[REPORT_ERROR_TEXT];
    ReportLine line[REPORT_LINES];
} Report;

/* ---- for the cores (a host needs nothing below) ------------------------ */

/* A core's clock: the system's, or the time a test set. */
typedef struct {
    int fixed;
    double wall, mono;
} ReportClock;

static inline double report_wall(const ReportClock *c) {
    if (c->fixed) return c->wall;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static inline double report_mono(const ReportClock *c) {
    if (c->fixed) return c->mono;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/* The start of every call that takes a Report: 0 when there is none. */
static inline int report_begin(Report *r) {
    if (!r) return 0;
    r->status = REPORT_OK;
    r->lines = 0;
    r->dropped = 0;
    r->error[0] = 0;
    return 1;
}

/* The call failed: the status and the reason. Returns the status. */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((format(printf, 3, 4)))
#endif
static inline int report_fail(Report *r, int status, const char *fmt, ...) {
    if (!r) return status;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(r->error, sizeof r->error, fmt, ap);
    va_end(ap);
    return r->status = status;
}

static inline void report_vline(Report *r, const ReportClock *clock,
                                int level, const char *fmt, va_list ap) {
    if (!r) return;
    if (r->lines >= REPORT_LINES) {
        r->dropped++;
        return;
    }
    ReportLine *l = &r->line[r->lines++];
    l->level = level;
    l->wall = report_wall(clock);
    l->mono = report_mono(clock);
    vsnprintf(l->text, sizeof l->text, fmt, ap);
}

/* A line for the host's log, stamped with the core's clock. */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((format(printf, 4, 5)))
#endif
static inline void report_line(Report *r, const ReportClock *clock,
                               int level, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    report_vline(r, clock, level, fmt, ap);
    va_end(ap);
}

#ifdef __cplusplus
}
#endif
#endif
