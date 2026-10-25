from __future__ import annotations

import ipaddress
import re
from typing import Any

from app.schemas.threat import IOCFinding
from app.utils.metrics import IOC_ENRICHMENTS_TOTAL

IP_PATTERN = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
DOMAIN_PATTERN = re.compile(r"\b[a-zA-Z0-9.-]+\.(?:com|net|org|io|ru|cn|info)\b")
HASH_PATTERN = re.compile(r"\b[a-fA-F0-9]{32,64}\b")


class IOCEnricher:
    high_risk_countries = {"North Korea", "Russia", "Iran"}
    suspicious_ranges = [ipaddress.ip_network("45.83.0.0/16"), ipaddress.ip_network("185.220.0.0/16"), ipaddress.ip_network("198.51.100.0/24")]
    tor_exit_nodes = {"185.220.101.1", "185.220.101.2", "45.83.64.1"}
    malware_hashes = {"44d88612fea8a8f36de82e1278abb02f", "e99a18c428cb38d5f260853678922e03"}
    watched_users = {"admin", "root", "svc_backup", "security_admin"}

    def enrich(self, payload: dict[str, Any]) -> tuple[list[IOCFinding], str | None, str | None]:
        text = str(payload)
        findings: list[IOCFinding] = []
        for ip in sorted(set(IP_PATTERN.findall(text))):
            findings.extend(self._enrich_ip(ip))
        for domain in sorted(set(DOMAIN_PATTERN.findall(text))):
            if domain.endswith((".ru", ".cn")):
                findings.append(IOCFinding(value=domain, ioc_type="domain", feed_name="domain-watchlist", confidence=0.66, severity="medium", description="Domain TLD is in local elevated-risk watchlist."))
        for hash_value in sorted(set(HASH_PATTERN.findall(text))):
            if hash_value.lower() in self.malware_hashes:
                findings.append(IOCFinding(value=hash_value, ioc_type="hash", feed_name="malware-signatures", confidence=0.94, severity="critical", description="Hash matches local malware signature feed."))
        username = str(payload.get("username") or payload.get("user_id") or "").lower()
        if username in self.watched_users:
            findings.append(IOCFinding(value=username, ioc_type="username", feed_name="privileged-account-watchlist", confidence=0.72, severity="high", description="Privileged or sensitive account observed in suspicious event."))
        country = payload.get("country") or payload.get("geo_country")
        if country in self.high_risk_countries:
            findings.append(IOCFinding(value=str(country), ioc_type="country", feed_name="geo-risk-feed", confidence=0.64, severity="medium", description="Country is in local high-risk geography watchlist."))
        for finding in findings:
            IOC_ENRICHMENTS_TOTAL.labels(finding.ioc_type, finding.severity).inc()
        return findings, str(country) if country else None, self._asn_for_payload(payload)

    def _enrich_ip(self, value: str) -> list[IOCFinding]:
        try:
            address = ipaddress.ip_address(value)
        except ValueError:
            return []
        findings: list[IOCFinding] = []
        if value in self.tor_exit_nodes:
            findings.append(IOCFinding(value=value, ioc_type="ip", feed_name="tor-exit-nodes", confidence=0.90, severity="high", description="IP matches local TOR exit node feed."))
        if any(address in network for network in self.suspicious_ranges):
            findings.append(IOCFinding(value=value, ioc_type="ip", feed_name="suspicious-ip-ranges", confidence=0.78, severity="high", description="IP belongs to a suspicious local reputation range."))
        return findings

    @staticmethod
    def _asn_for_payload(payload: dict[str, Any]) -> str | None:
        source_ip = str(payload.get("source_ip") or payload.get("ip") or "")
        if source_ip.startswith("10.") or source_ip.startswith("192.168."):
            return "AS64512 Internal Lab Network"
        if source_ip.startswith("185.220."):
            return "AS-TOR Local Intelligence"
        if source_ip:
            return "AS-LOCAL Synthetic Telemetry"
        return None
