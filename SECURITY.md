# Security Policy

## Reporting a vulnerability

Do not report suspected vulnerabilities in public issues or pull requests.
Report them privately to the repository security contacts configured by
PLOMID Technologies INDIA, or use the repository host's private security advisory mechanism.
Include the affected component, impact, reproduction steps, and any suggested
mitigation. Do not include secrets or unnecessary customer data.

If no private security contact is configured yet, contact the PLOMID Technologies INDIA
project maintainers directly (Author: Sainath Sapa <info.djsai@gmail.com>,
https://github.com/sainathsapa) and request a private channel before sharing
sensitive details.

## Response

Maintainers will acknowledge a report, assess severity and affected releases,
coordinate a fix and disclosure timeline with the reporter, and document the
resolution. Reporters should allow reasonable time for remediation before
public disclosure.

Security-sensitive changes must include relevant tests and must not weaken the
workspace dependency, unsafe-code, authentication, audit, or data-protection
policies.

## Response SLA

| Severity (CVSS-style) | Acknowledge | Fix + release | Disclosure |
|---|---|---|---|
| Critical (RCE, auth bypass, data loss) | 1 business day | 7 days | Coordinated, after fix ships |
| High (privilege escalation, significant DoS) | 2 business days | 30 days | Coordinated, after fix ships |
| Medium / Low | 5 business days | Next scheduled release | With the release notes |

## Supported versions

Only the latest beta/stable release line receives security fixes. The
version table below is updated on every release; anything older is
best-effort and may require upgrading first.

| Version | Supported |
|---|---|
| 0.1.0-beta.2 (current) | Yes |
| < 0.1.0-beta.2 | No — upgrade to the current line |

## Supply chain

- Dependencies are locked (`Cargo.lock`); `cargo deny check`
  (advisories, licenses, bans, sources) gates CI, and Dependabot opens
  weekly Cargo + GitHub Actions updates.
- Release artifacts ship an SPDX SBOM and keyless Sigstore signatures
  (see `.github/workflows/release.yml`); CI actions are SHA-pinned.
