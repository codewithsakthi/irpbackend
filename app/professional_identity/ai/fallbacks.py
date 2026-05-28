"""SPICS — AI fallback summaries.
When AI is unavailable, these static templates provide a degraded but functional experience.
"""
from typing import Optional


def generate_fallback_summary(
    project_count: int = 0,
    skill_count: int = 0,
    cert_count: int = 0,
    has_github: bool = False,
    primary_domain: Optional[str] = None,
) -> dict:
    """
    Generates a rule-based summary when the AI API is unavailable.
    Returns the same shape as AI-generated insights.
    """
    domain_label = (primary_domain or "technology").replace("_", " ").title()
    readiness_score = min(
        (project_count * 15) + (skill_count * 5) + (cert_count * 10) + (20 if has_github else 0),
        100,
    )

    strengths = []
    if project_count >= 2:
        strengths.append(f"Portfolio demonstrates initiative with {project_count} projects")
    if has_github:
        strengths.append("Active GitHub presence signals coding consistency")
    if cert_count >= 1:
        strengths.append(f"Certified in {cert_count} area(s) — shows commitment to learning")
    if skill_count >= 5:
        strengths.append(f"Broad technical skill set with {skill_count} skills listed")
    if not strengths:
        strengths = ["Profile is being built — early-stage professional identity"]

    improvement_areas = []
    if project_count < 2:
        improvement_areas.append("Add more project entries with GitHub links")
    if not has_github:
        improvement_areas.append("Link GitHub account to show coding activity")
    if cert_count == 0:
        improvement_areas.append("Earn NPTEL/Coursera certifications to validate domain skills")
    if skill_count < 5:
        improvement_areas.append("Expand skill matrix to at least 5 core technologies")

    career_fit_roles = _infer_roles(primary_domain)

    return {
        "technical_depth_score":  min(project_count * 20 + skill_count * 3, 80),
        "communication_score":    50.0,
        "innovation_score":       min(project_count * 15, 75),
        "collaboration_score":    50.0,
        "project_maturity_score": min(project_count * 25, 85),
        "career_readiness_score": float(readiness_score),
        "ai_summary": (
            f"This student is building a {domain_label} profile with {project_count} project(s), "
            f"{skill_count} skill(s), and {cert_count} certification(s). "
            "AI-powered analysis will be generated when the AI engine is available."
        ),
        "strengths":          strengths,
        "improvement_areas":  improvement_areas,
        "career_fit_roles":   career_fit_roles,
        "missing_skills":     _infer_missing_skills(primary_domain),
        "model_used":         "rule-based-fallback",
    }


def _infer_roles(domain: Optional[str]) -> list:
    domain_role_map = {
        "frontend":     [{"role": "Frontend Developer", "match": "High"}, {"role": "UI Engineer", "match": "Medium"}],
        "backend":      [{"role": "Backend Developer", "match": "High"}, {"role": "API Engineer", "match": "High"}],
        "fullstack":    [{"role": "Full Stack Developer", "match": "High"}, {"role": "Software Engineer", "match": "High"}],
        "data":         [{"role": "Data Analyst", "match": "High"}, {"role": "Business Intelligence Analyst", "match": "Medium"}],
        "ml":           [{"role": "ML Engineer", "match": "High"}, {"role": "Data Scientist", "match": "Medium"}],
        "devops":       [{"role": "DevOps Engineer", "match": "High"}, {"role": "Site Reliability Engineer", "match": "Medium"}],
        "mobile":       [{"role": "Mobile Developer", "match": "High"}, {"role": "Android/iOS Engineer", "match": "Medium"}],
        "cybersecurity":[{"role": "Security Analyst", "match": "High"}, {"role": "Penetration Tester", "match": "Medium"}],
    }
    return domain_role_map.get(domain or "", [{"role": "Software Developer", "match": "Medium"}])


def _infer_missing_skills(domain: Optional[str]) -> list:
    domain_skills = {
        "frontend":  ["React Testing Library", "TypeScript", "Web Accessibility"],
        "backend":   ["Docker", "Redis", "System Design"],
        "fullstack": ["CI/CD", "Cloud (AWS/GCP)", "Database Optimization"],
        "data":      ["Tableau/PowerBI", "SQL Advanced", "Statistics"],
        "ml":        ["MLOps", "PyTorch/TensorFlow", "Feature Engineering"],
        "devops":    ["Kubernetes", "Terraform", "Monitoring & Alerting"],
        "mobile":    ["Firebase", "Testing (Espresso/XCTest)", "App Store Deployment"],
    }
    return domain_skills.get(domain or "", ["System Design", "Cloud Computing", "Testing Best Practices"])
