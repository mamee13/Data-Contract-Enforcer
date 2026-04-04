#!/usr/bin/env python3
"""
contracts/report_generator.py - Enforcer Report Generator

Auto-generates the Enforcer Report from live validation data.

Usage:
    python contracts/report_generator.py --output enforcer_report/report_data.json
"""

import json
import argparse
import glob
from datetime import datetime, timedelta
from pathlib import Path


try:
    from weasyprint import HTML as WeasyprintHTML
    WEASYPRINT_AVAILABLE = True
except Exception:
    WEASYPRINT_AVAILABLE = False

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib import colors
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


class ReportGenerator:
    def __init__(self, output_path: str):
        self.output_path = output_path
        self.validation_reports: list[dict] = []
        self.violations: list[dict] = []

    def load_validation_reports(self):
        """Load all validation reports from validation_reports/."""
        report_files = glob.glob("validation_reports/*.json")

        for rf in report_files:
            if "schema_evolution" in rf:
                continue

            with open(rf) as f:
                try:
                    report = json.load(f)
                    self.validation_reports.append(report)
                except json.JSONDecodeError:
                    pass

        print(f"Loaded {len(self.validation_reports)} validation reports")

    def load_violations(self):
        """Load violations from violation_log/."""
        violation_file = Path("violation_log/violations.jsonl")

        if not violation_file.exists():
            print("No violations file found")
            return

        with open(violation_file) as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    try:
                        self.violations.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

        print(f"Loaded {len(self.violations)} violations")

    def compute_health_score(self) -> tuple:
        """Compute data health score (0-100).

        Formula: (checks_passed / total_checks) * 100 - 20 per CRITICAL violation
        """
        total_checks = 0
        passed_checks = 0
        critical_violations = 0

        for report in self.validation_reports:
            results = report.get("results", [])
            total_checks += len(results)

            for result in results:
                if result.get("status") == "PASS":
                    passed_checks += 1
                elif result.get("status") == "FAIL":
                    severity = result.get("severity", "LOW")
                    if severity == "CRITICAL":
                        critical_violations += 1

        if total_checks == 0:
            return 100, "No validation data available"

        base_score = (passed_checks / total_checks) * 100
        final_score = max(0, base_score - (critical_violations * 20))
        final_score = min(100, round(final_score))

        if final_score >= 90:
            narrative = "Excellent data health. All critical checks passing."
        elif final_score >= 70:
            narrative = "Good data health. Some warnings need attention."
        elif final_score >= 50:
            narrative = "Moderate data health. Several violations require action."
        else:
            narrative = "Critical data health issues. Immediate attention required."

        return final_score, narrative

    def get_top_violations(self) -> list:
        """Get top 3 violations in plain language."""
        all_failures = []

        for report in self.validation_reports:
            for result in report.get("results", []):
                if result.get("status") == "FAIL":
                    all_failures.append(result)

        severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}

        all_failures.sort(key=lambda x: severity_order.get(x.get("severity", "LOW"), 4))

        impact_by_check: dict[str, str] = {}
        for v in self.violations:
            cid = v.get("check_id", "")
            blast = v.get("blast_radius", {})
            direct = blast.get("direct_subscribers", [])
            if cid and direct and cid not in impact_by_check:
                impacted = []
                for sub in direct:
                    sid = sub.get("subscriber_id", "unknown")
                    mode = sub.get("validation_mode", "AUDIT")
                    impacted.append(f"{sid} ({mode})")
                impact_by_check[cid] = ", ".join(impacted)

        top_violations = []

        for failure in all_failures[:3]:
            check_id = failure.get("check_id", "unknown")
            column = failure.get("column_name", "unknown")
            severity = failure.get("severity", "unknown")
            records_failing = failure.get("records_failing", 0)

            message = f"The {column} field failed its {check_id} check. "
            message += (
                f"Expected {failure.get('expected')} but found {failure.get('actual_value')}. "
            )
            impact = impact_by_check.get(check_id)
            if impact:
                message += f"This affects {records_failing} records and downstream consumers: {impact}."
            else:
                message += f"This affects {records_failing} records and downstream consumers."

            top_violations.append(
                {
                    "severity": severity,
                    "check_id": check_id,
                    "column_name": column,
                    "records_failing": records_failing,
                    "plain_language": message,
                }
            )

        return top_violations

    def get_violations_by_severity(self) -> dict:
        """Get count of violations by severity."""
        counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}

        for report in self.validation_reports:
            for result in report.get("results", []):
                if result.get("status") == "FAIL":
                    severity = result.get("severity", "LOW")
                    if severity in counts:
                        counts[severity] += 1

        return counts

    def get_schema_changes(self) -> list:
        """Get schema changes detected in past 7 days."""
        schema_report_files = glob.glob("validation_reports/*schema_evolution*.json")

        changes = []

        for sf in schema_report_files:
            with open(sf) as f:
                try:
                    data = json.load(f)
                    diff = data.get("schema_diff", {})

                    if diff.get("compatibility_verdict") == "BREAKING":
                        changes.append(
                            {
                                "contract_id": data.get("contract_id", "unknown"),
                                "verdict": diff.get("compatibility_verdict", "UNKNOWN"),
                                "breaking_count": len(diff.get("breaking_changes", [])),
                                "timestamp": diff.get("new_snapshot", "unknown"),
                            }
                        )
                except Exception:  # noqa: BLE001
                    pass

        return changes

    def get_ai_assessment(self) -> dict:
        """Get AI system risk assessment."""
        ai_report_files = glob.glob("validation_reports/*ai_extensions*.json")

        if not ai_report_files:
            return {
                "embedding_drift_score": None,
                "llm_output_violation_rate": None,
                "status": "UNKNOWN",
                "message": "No AI extension results available",
            }

        with open(ai_report_files[0]) as f:
            ai_data = json.load(f)

        extensions = ai_data.get("extensions", {})

        embedding_drift = extensions.get("embedding_drift", {})
        llm_output = extensions.get("llm_output_schema", {})

        return {
            "embedding_drift_score": embedding_drift.get("drift_score"),
            "embedding_drift_status": embedding_drift.get("status"),
            "llm_output_violation_rate": llm_output.get("violation_rate"),
            "llm_output_trend": llm_output.get("trend"),
            "status": ai_data.get("overall_status", "UNKNOWN"),
        }

    def get_recommendations(self) -> list[dict]:
        """Get recommended actions derived from live violation data."""
        recommendations: list[dict] = []

        # Build a lookup of violation blame chains keyed by check_id for path resolution
        blame_by_check: dict[str, str] = {}
        for v in self.violations:
            cid = v.get("check_id", "")
            chain = v.get("blame_chain", [])
            if chain and cid:
                top = chain[0]
                fp = top.get("file_path", "")
                if fp and fp != "unknown":
                    blame_by_check[cid] = fp

        for report in self.validation_reports:
            contract_id = report.get("contract_id", "unknown")
            for result in report.get("results", []):
                if result.get("status") != "FAIL":
                    continue
                severity = result.get("severity", "")
                if severity != "CRITICAL":
                    continue

                check_id = result.get("check_id", "")
                column = result.get("column_name", "")

                # Resolve the upstream file from the blame chain; fall back to contract path
                upstream_file = blame_by_check.get(
                    check_id,
                    f"generated_contracts/{contract_id}.yaml",
                )

                recommendations.append(
                    {
                        "priority": 1,
                        "action": (
                            f"Resolve CRITICAL violation on '{column}' "
                            f"(check: {check_id}) in contract '{contract_id}'"
                        ),
                        "file": upstream_file,
                        "contract_clause": check_id,
                        "reason": (
                            f"CRITICAL failure: {result.get('message', '')} "
                            f"[expected: {result.get('expected', '')}, "
                            f"actual: {result.get('actual_value', '')}]"
                        ),
                    }
                )

        # Deduplicate by file+clause, keep highest priority
        seen: set[str] = set()
        deduped: list[dict] = []
        for rec in recommendations:
            key = f"{rec['file']}::{rec['contract_clause']}"
            if key not in seen:
                seen.add(key)
                deduped.append(rec)

        deduped.sort(key=lambda x: x.get("priority", 99))

        if not deduped:
            deduped.append(
                {
                    "priority": 3,
                    "action": "Re-run ContractGenerator to refresh contracts from latest data",
                    "file": "contracts/generator.py",
                    "contract_clause": "N/A",
                    "reason": "No CRITICAL violations detected — routine maintenance",
                }
            )

        return deduped[:3]

    def generate_pdf(self, report: dict, pdf_path: str):
        """Generate PDF from report_data.json using weasyprint or reportlab."""
        if WEASYPRINT_AVAILABLE:
            self._generate_pdf_weasyprint(report, pdf_path)
        elif REPORTLAB_AVAILABLE:
            self._generate_pdf_reportlab(report, pdf_path)
        else:
            raise RuntimeError("Neither weasyprint nor reportlab is installed. Run: uv add weasyprint OR uv add reportlab")

    def _generate_pdf_weasyprint(self, report: dict, pdf_path: str):
        """Generate PDF via weasyprint from an HTML template."""
        score = report.get("data_health_score", 0)
        score_color = "#27ae60" if score >= 70 else "#e67e22" if score >= 50 else "#e74c3c"

        violations = report.get("violations_this_week", {})
        by_sev = violations.get("by_severity", {})
        top3 = violations.get("top_3", [])

        top3_rows = "".join(
            f"<tr><td>{v.get('severity','')}</td><td>{v.get('plain_language','')}</td></tr>"
            for v in top3
        ) or "<tr><td colspan='2'>No violations</td></tr>"

        ai = report.get("ai_system_risk", {})
        recs = report.get("recommendations", [])
        rec_items = "".join(f"<li>{r.get('action','')} — <code>{r.get('file','')}</code></li>" for r in recs)

        schema_changes = report.get("schema_changes", [])
        schema_rows = "".join(
            f"<tr><td>{c.get('contract_id','')}</td><td>{c.get('verdict','')}</td><td>{c.get('breaking_count',0)}</td></tr>"
            for c in schema_changes
        ) or "<tr><td colspan='3'>No schema changes detected</td></tr>"

        html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'>
<style>
  body {{ font-family: Arial, sans-serif; margin: 2cm; color: #333; }}
  h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 8px; }}
  h2 {{ color: #2980b9; margin-top: 24px; }}
  .score {{ font-size: 48px; font-weight: bold; color: {score_color}; }}
  table {{ border-collapse: collapse; width: 100%; margin: 12px 0; }}
  th {{ background: #3498db; color: white; padding: 8px; text-align: left; }}
  td {{ border: 1px solid #ddd; padding: 8px; }}
  tr:nth-child(even) {{ background: #f9f9f9; }}
  .meta {{ color: #888; font-size: 12px; }}
  ul {{ padding-left: 20px; }}
</style>
</head><body>
<h1>Data Contract Enforcer Report</h1>
<p class='meta'>Generated: {report.get('generated_at','')} | Period: {report.get('period','')}</p>

<h2>1. Data Health Score</h2>
<div class='score'>{score}/100</div>
<p>{report.get('health_narrative','')}</p>

<h2>2. Violations This Week</h2>
<p>CRITICAL: {by_sev.get('CRITICAL',0)} | HIGH: {by_sev.get('HIGH',0)} | MEDIUM: {by_sev.get('MEDIUM',0)} | LOW: {by_sev.get('LOW',0)}</p>
<table><tr><th>Severity</th><th>Summary</th></tr>{top3_rows}</table>

<h2>3. Schema Changes (Past 7 Days)</h2>
<table><tr><th>Contract</th><th>Verdict</th><th>Breaking Changes</th></tr>{schema_rows}</table>

<h2>4. AI System Risk Assessment</h2>
<table>
  <tr><th>Metric</th><th>Value</th><th>Status</th></tr>
  <tr><td>Embedding Drift Score</td><td>{ai.get('embedding_drift_score','N/A')}</td><td>{ai.get('embedding_drift_status','N/A')}</td></tr>
  <tr><td>LLM Output Violation Rate</td><td>{ai.get('llm_output_violation_rate','N/A')}</td><td>{ai.get('llm_output_trend','N/A')}</td></tr>
  <tr><td>Overall AI Status</td><td colspan='2'>{ai.get('status','N/A')}</td></tr>
</table>

<h2>5. Recommended Actions</h2>
<ul>{rec_items or '<li>No critical actions required</li>'}</ul>

</body></html>"""

        Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
        WeasyprintHTML(string=html).write_pdf(pdf_path)
        print(f"PDF written to {pdf_path}")

    def _generate_pdf_reportlab(self, report: dict, pdf_path: str):
        """Generate PDF via reportlab."""
        Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
        doc = SimpleDocTemplate(pdf_path, pagesize=A4,
                                leftMargin=2*cm, rightMargin=2*cm,
                                topMargin=2*cm, bottomMargin=2*cm)
        styles = getSampleStyleSheet()
        story = []

        story.append(Paragraph("Data Contract Enforcer Report", styles["Title"]))
        story.append(Paragraph(f"Generated: {report.get('generated_at','')} | Period: {report.get('period','')}", styles["Normal"]))
        story.append(Spacer(1, 0.4*cm))

        story.append(Paragraph("1. Data Health Score", styles["Heading2"]))
        score = report.get("data_health_score", 0)
        story.append(Paragraph(f"<b>{score}/100</b> — {report.get('health_narrative','')}", styles["Normal"]))
        story.append(Spacer(1, 0.3*cm))

        story.append(Paragraph("2. Violations This Week", styles["Heading2"]))
        by_sev = report.get("violations_this_week", {}).get("by_severity", {})
        story.append(Paragraph(f"CRITICAL: {by_sev.get('CRITICAL',0)} | HIGH: {by_sev.get('HIGH',0)} | MEDIUM: {by_sev.get('MEDIUM',0)} | LOW: {by_sev.get('LOW',0)}", styles["Normal"]))
        top3 = report.get("violations_this_week", {}).get("top_3", [])
        for v in top3:
            story.append(Paragraph(f"• [{v.get('severity','')}] {v.get('plain_language','')}", styles["Normal"]))
        story.append(Spacer(1, 0.3*cm))

        story.append(Paragraph("3. Schema Changes (Past 7 Days)", styles["Heading2"]))
        schema_changes = report.get("schema_changes", [])
        if schema_changes:
            data = [["Contract", "Verdict", "Breaking"]] + [[c.get("contract_id",""), c.get("verdict",""), str(c.get("breaking_count",0))] for c in schema_changes]
            t = Table(data, hAlign="LEFT")
            t.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor("#3498db")),
                                   ("TEXTCOLOR", (0,0), (-1,0), colors.white),
                                   ("GRID", (0,0), (-1,-1), 0.5, colors.grey)]))
            story.append(t)
        else:
            story.append(Paragraph("No schema changes detected.", styles["Normal"]))
        story.append(Spacer(1, 0.3*cm))

        story.append(Paragraph("4. AI System Risk Assessment", styles["Heading2"]))
        ai = report.get("ai_system_risk", {})
        ai_data = [
            ["Metric", "Value", "Status"],
            ["Embedding Drift Score", str(ai.get("embedding_drift_score","N/A")), str(ai.get("embedding_drift_status","N/A"))],
            ["LLM Output Violation Rate", str(ai.get("llm_output_violation_rate","N/A")), str(ai.get("llm_output_trend","N/A"))],
            ["Overall AI Status", ai.get("status","N/A"), ""],
        ]
        t2 = Table(ai_data, hAlign="LEFT")
        t2.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor("#3498db")),
                                ("TEXTCOLOR", (0,0), (-1,0), colors.white),
                                ("GRID", (0,0), (-1,-1), 0.5, colors.grey)]))
        story.append(t2)
        story.append(Spacer(1, 0.3*cm))

        story.append(Paragraph("5. Recommended Actions", styles["Heading2"]))
        for r in report.get("recommendations", []):
            story.append(Paragraph(f"• {r.get('action','')} — {r.get('file','')}", styles["Normal"]))

        doc.build(story)
        print(f"PDF written to {pdf_path}")

    def run(self, generate_pdf: bool = True):
        """Generate the enforcer report."""
        print("Loading validation reports...")
        self.load_validation_reports()

        print("Loading violations...")
        self.load_violations()

        health_score, health_narrative = self.compute_health_score()

        top_violations = self.get_top_violations()
        severity_counts = self.get_violations_by_severity()
        schema_changes = self.get_schema_changes()
        ai_assessment = self.get_ai_assessment()
        recommendations = self.get_recommendations()

        period_end = datetime.utcnow()
        period_start = period_end - timedelta(days=7)

        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "period": f"{period_start.date()} to {period_end.date()}",
            "data_health_score": health_score,
            "health_narrative": health_narrative,
            "violations_this_week": {
                "by_severity": severity_counts,
                "total": sum(severity_counts.values()),
                "top_3": top_violations,
            },
            "schema_changes": schema_changes,
            "ai_system_risk": ai_assessment,
            "recommendations": recommendations,
            "total_validation_reports": len(self.validation_reports),
            "total_violations_logged": len(self.violations),
        }

        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_path, "w") as f:
            json.dump(report, f, indent=2)

        if generate_pdf:
            date_str = datetime.utcnow().strftime("%Y%m%d")
            pdf_path = str(Path(self.output_path).parent / f"report_{date_str}.pdf")
            try:
                self.generate_pdf(report, pdf_path)
            except Exception as e:
                print(f"Warning: PDF generation failed: {e}")

        print(f"\n{'=' * 60}")
        print("ENFORCER REPORT SUMMARY")
        print(f"{'=' * 60}")
        print(f"Health Score: {health_score}/100")
        print(f"Narrative: {health_narrative}")
        print(f"Total Violations: {sum(severity_counts.values())}")
        print(f"Schema Changes: {len(schema_changes)}")
        print(f"AI Status: {ai_assessment.get('status', 'UNKNOWN')}")
        print(f"{'=' * 60}")
        print(f"Output: {self.output_path}")

        return report


def main():
    parser = argparse.ArgumentParser(description="Generate Enforcer Report")
    parser.add_argument(
        "--output", default="enforcer_report/report_data.json", help="Path to output JSON report"
    )
    parser.add_argument("--no-pdf", action="store_true", help="Skip PDF generation")

    args = parser.parse_args()

    generator = ReportGenerator(args.output)
    generator.run(generate_pdf=not args.no_pdf)


if __name__ == "__main__":
    main()
