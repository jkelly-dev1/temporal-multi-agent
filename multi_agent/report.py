"""Pretty-printing for a FinalReport (used by the runnable entry points)."""

from .shared import FinalReport


def print_report(report: FinalReport) -> None:
    line = "=" * 78
    print(f"\n{line}\nTOPIC: {report.topic}\nprovider: {report.provider}\n{line}")
    print("EXECUTIVE SUMMARY\n" + report.summary + "\n")
    print("SECTIONS")
    for s in report.sections:
        print(
            f"  [{s.subtask_id}] {s.angle}  "
            f"(confidence={s.confidence}, attempts={s.attempts})"
        )
        print(f"      {s.finding}")
        print(f"      sources: {', '.join(s.sources)}")
    print("\nREVIEW\n  " + report.review.overall)
    print(f"  avg_confidence={report.review.avg_confidence}\n{line}")
