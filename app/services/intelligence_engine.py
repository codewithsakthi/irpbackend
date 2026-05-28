from __future__ import annotations

import json
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from sqlalchemy import select, text, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from .. import models, schemas
from ..services.ai_service import generate
from ..professional_identity.models.models import AIProfessionalInsight, AIStatus
from ..services.student_service import StudentService
from ..core.constants import CURRICULUM_CREDITS

logger = logging.getLogger(__name__)

# Configurable Weights for SPI (Student Potential Index)
DEFAULT_ASIE_WEIGHTS = {
    "academic": 0.25,
    "technical": 0.20,
    "communication": 0.15,
    "leadership": 0.10,
    "sports": 0.10,
    "discipline": 0.10,
    "consistency": 0.10
}

class StudentIntelligenceEngine:
    @staticmethod
    def get_weights(program_code: Optional[str] = None) -> Dict[str, float]:
        """
        Returns configurable scoring weights. In the future, this can load weights
        from database settings/configurations.
        """
        # Overrides can be implemented here based on program (e.g., MCA vs MBA)
        return DEFAULT_ASIE_WEIGHTS

    @classmethod
    async def compute_capability_scores(
        cls, student_id: int, db: AsyncSession
    ) -> Tuple[Dict[str, float], str, float]:
        """
        Calculates 10-dimensional performance capabilities deterministically.
        Returns:
            - Dict containing all 10 capability scores
            - Profile classification string
            - Placement probability (base deterministic prediction)
        """
        # Fetch Student record along with assessments and program
        student_res = await db.execute(
            select(models.Student)
            .options(
                joinedload(models.Student.program),
                joinedload(models.Student.assessments).joinedload(models.StudentAssessment.subject)
            )
            .filter(models.Student.id == student_id)
        )
        student = student_res.scalars().first()
        if not student:
            raise ValueError(f"Student with ID {student_id} not found")

        # Fetch extra curriculars
        ec_res = await db.execute(
            select(models.ExtraCurricular).filter(models.ExtraCurricular.student_id == student_id)
        )
        extracurriculars = ec_res.scalars().all()

        # Fetch counselor logs
        diary_res = await db.execute(
            select(models.CounselorDiary).filter(models.CounselorDiary.student_id == student_id)
        )
        counselor_logs = diary_res.scalars().all()

        # Fetch overall attendance
        att_res = await db.execute(
            text("SELECT * FROM v_attendance_summary WHERE student_id = :sid"),
            {"sid": student_id}
        )
        att_rows = att_res.mappings().all()

        total_present = sum((row.get("present") or 0) + (row.get("on_duty") or 0) for row in att_rows)
        total_periods = sum(row.get("total_periods") or 0 for row in att_rows)
        attendance_pct = (total_present / total_periods * 100.0) if total_periods > 0 else 75.0

        # Fetch historical marks and calculate CGPA/SGPA details
        analytics = await StudentService.calculate_analytics(student, db)
        cgpa = analytics.average_grade_points
        backlogs = analytics.total_backlogs

        # ----------------------------------------------------
        # 1. ACADEMIC SCORE
        # ----------------------------------------------------
        # Max CGPA is 10.0. Scale directly to 100. Penalize by -10 per active backlog.
        academic_score = max(0.0, min(100.0, (cgpa * 10.0) - (backlogs * 10.0)))

        # ----------------------------------------------------
        # 2. TECHNICAL SCORE
        # ----------------------------------------------------
        tech_keywords = [
            "programming", "data structure", "database", "java", "python", "c++", 
            "html", "css", "javascript", "web", "network", "operating system", "os", 
            "algorithms", "software", "cloud", "machine learning", "ai", "cybersecurity", 
            "cryptography", "security", "mca", "bca", "computer", "information", "lab", "project"
        ]
        
        tech_marks: List[float] = []
        for ass in student.assessments:
            if not ass.subject:
                continue
            name_lower = (ass.subject.name or "").lower()
            code_lower = (ass.subject.course_code or "").lower()
            if any(kw in name_lower or kw in code_lower for kw in tech_keywords):
                # Use total computed mark if available, else derive from marks
                val = float(ass.marks or 0.0)
                if val > 0:
                    tech_marks.append(val)

        base_tech_avg = sum(tech_marks) / len(tech_marks) if tech_marks else 60.0
        # Extracurricular boost (+5 per tech activity)
        tech_activities = [ec for ec in extracurriculars if (ec.category or "").lower() in ["technical", "hackathon", "coding", "project"]]
        technical_score = min(100.0, base_tech_avg + (len(tech_activities) * 5.0))

        # ----------------------------------------------------
        # 3. COMMUNICATION SCORE
        # ----------------------------------------------------
        comm_keywords = ["english", "communication", "technical communication", "humanities", "social", "management", "behavior", "professional"]
        comm_marks: List[float] = []
        for ass in student.assessments:
            if not ass.subject:
                continue
            name_lower = (ass.subject.name or "").lower()
            if any(kw in name_lower for kw in comm_keywords):
                val = float(ass.marks or 0.0)
                if val > 0:
                    comm_marks.append(val)
        
        base_comm_avg = sum(comm_marks) / len(comm_marks) if comm_marks else 65.0
        
        # Parse counselor diaries for communication keywords
        comm_boost = 0.0
        comm_search = ["communication", "speak", "speech", "express", "presentation", "language", "english", "interaction", "active", "articulate"]
        for diary in counselor_logs:
            remarks_lower = (diary.remarks or "").lower()
            if any(kw in remarks_lower for kw in comm_search):
                comm_boost += 3.0
                
        communication_score = min(100.0, base_comm_avg + comm_boost)

        # ----------------------------------------------------
        # 4. LEADERSHIP SCORE
        # ----------------------------------------------------
        leadership_activities = 0
        leader_search = ["leadership", "coordinator", "organizer", "president", "secretary", "representative", "lead", "head", "captain", "manager", "volunteered", "committee", "team"]
        for ec in extracurriculars:
            desc_lower = (ec.description or "").lower()
            cat_lower = (ec.category or "").lower()
            if any(kw in desc_lower or kw in cat_lower for kw in leader_search):
                leadership_activities += 1
                
        for diary in counselor_logs:
            remarks_lower = (diary.remarks or "").lower()
            if any(kw in remarks_lower for kw in ["leadership", "coordinated", "organized", "team", "lead"]):
                leadership_activities += 1

        leadership_score = min(100.0, 50.0 + (leadership_activities * 10.0))

        # ----------------------------------------------------
        # 5. SPORTS SCORE
        # ----------------------------------------------------
        sports_activities = 0
        sports_search = ["sports", "athletic", "tournament", "game", "football", "cricket", "basketball", "volleyball", "badminton", "tennis", "athletics", "race", "captain", "sportsman", "champion", "gold medal", "silver medal", "runner", "winner"]
        for ec in extracurriculars:
            desc_lower = (ec.description or "").lower()
            cat_lower = (ec.category or "").lower()
            if any(kw in desc_lower or kw in cat_lower for kw in sports_search) or cat_lower == "sports":
                sports_activities += 1

        sports_score = min(100.0, 50.0 + (sports_activities * 15.0))

        # ----------------------------------------------------
        # 6. CREATIVITY SCORE
        # ----------------------------------------------------
        creativity_activities = 0
        creative_search = ["creative", "cultural", "music", "dance", "drama", "arts", "design", "innovation", "patent", "publication", "innovative", "original"]
        for ec in extracurriculars:
            desc_lower = (ec.description or "").lower()
            cat_lower = (ec.category or "").lower()
            if any(kw in desc_lower or kw in cat_lower for kw in creative_search) or cat_lower in ["creative", "cultural"]:
                creativity_activities += 1
                
        for diary in counselor_logs:
            remarks_lower = (diary.remarks or "").lower()
            if any(kw in remarks_lower for kw in ["creative", "innovative", "original", "idea"]):
                creativity_activities += 1

        creativity_score = min(100.0, 55.0 + (creativity_activities * 10.0))

        # ----------------------------------------------------
        # 7. DISCIPLINE SCORE
        # ----------------------------------------------------
        if attendance_pct >= 90:
            base_discipline = 100.0
        elif attendance_pct >= 75:
            base_discipline = 80.0 + (attendance_pct - 75.0) * 1.33
        else:
            base_discipline = attendance_pct

        discipline_penalties = 0.0
        disciplinary_terms = ["warning", "warned", "disciplinary", "absenteeism", "irregular", "discipline", "late", "suspended"]
        for diary in counselor_logs:
            remarks_lower = (diary.remarks or "").lower()
            cat_lower = (diary.remark_category or "").lower()
            if any(kw in remarks_lower or kw in cat_lower for kw in disciplinary_terms):
                discipline_penalties += 15.0
                
        discipline_score = max(0.0, base_discipline - discipline_penalties)

        # ----------------------------------------------------
        # 8. CONSISTENCY SCORE
        # ----------------------------------------------------
        sem_performances = analytics.semester_performance
        if len(sem_performances) < 2:
            consistency_score = 85.0
        else:
            sgpas = [float(p.average_grade_points) for p in sem_performances]
            mean_sgpa = sum(sgpas) / len(sgpas)
            variance = sum((x - mean_sgpa) ** 2 for x in sgpas) / len(sgpas)
            std_dev = variance ** 0.5
            consistency_score = max(0.0, min(100.0, 100.0 - (std_dev * 25.0)))

        # ----------------------------------------------------
        # 9. GROWTH SCORE
        # ----------------------------------------------------
        if len(sem_performances) < 2:
            growth_score = 75.0
        else:
            sorted_perf = sorted(sem_performances, key=lambda x: x.semester)
            latest_gpa = float(sorted_perf[-1].average_grade_points)
            prev_gpa = float(sorted_perf[-2].average_grade_points)
            gpa_delta = latest_gpa - prev_gpa
            growth_score = max(0.0, min(100.0, 50.0 + (gpa_delta * 15.0)))

        # ----------------------------------------------------
        # 10. PLACEMENT SCORE & BASE PROBABILITY
        # ----------------------------------------------------
        placement_score = max(
            0.0,
            (academic_score * 0.35 + technical_score * 0.35 + communication_score * 0.20 + (attendance_pct * 0.10))
            - (backlogs * 20.0)
        )
        
        # Base Placement Probability: calculated from placement_score and backlog severity
        placement_probability = max(0.0, min(100.0, placement_score))
        if backlogs > 0:
            placement_probability = min(40.0, placement_probability) # Hard limit if active backlogs exist

        # Load weights and calculate Student Potential Index (SPI)
        program_code = student.program.code if student.program else None
        weights = cls.get_weights(program_code)
        
        spi_score = (
            academic_score * weights["academic"] +
            technical_score * weights["technical"] +
            communication_score * weights["communication"] +
            leadership_score * weights["leadership"] +
            sports_score * weights["sports"] +
            discipline_score * weights["discipline"] +
            consistency_score * weights["consistency"]
        )

        # ----------------------------------------------------
        # AUTOMATIC PROFILE CLASSIFICATION
        # ----------------------------------------------------
        if cgpa >= 8.5 and backlogs == 0:
            profile_type = "Academic Performer"
        elif technical_score >= 85:
            profile_type = "Technical Specialist"
        elif placement_score >= 80 and backlogs == 0:
            profile_type = "Placement Ready"
        elif leadership_score >= 80:
            profile_type = "Leadership Candidate"
        elif sports_score >= 80:
            profile_type = "Sports Excellence"
        elif creativity_score >= 80:
            profile_type = "Creative Innovator"
        elif cgpa >= 7.5 and technical_score >= 75 and creativity_score >= 75:
            profile_type = "Research Oriented"
        elif cgpa < 6.5 and (technical_score >= 80 or leadership_score >= 80 or sports_score >= 80):
            profile_type = "High Potential" # Hidden Talent
        elif academic_score >= 65 and technical_score >= 65 and communication_score >= 65:
            profile_type = "Balanced Performer"
        else:
            profile_type = "Needs Support"

        scores = {
            "academic_score": academic_score,
            "communication_score": communication_score,
            "leadership_score": leadership_score,
            "technical_score": technical_score,
            "creativity_score": creativity_score,
            "sports_score": sports_score,
            "discipline_score": discipline_score,
            "consistency_score": consistency_score,
            "placement_score": placement_score,
            "growth_score": growth_score,
            "spi_score": spi_score
        }

        return scores, profile_type, placement_probability

    @classmethod
    async def analyze_and_cache_student(
        cls, student_id: int, db: AsyncSession, bypass_ai: bool = False
    ) -> schemas.StudentDNAResponse:
        """
        Executes capability scoring, updates database records, triggers the NVIDIA AI summarizer,
        and saves cached values in student_capability_scores and ai_professional_insights.
        """
        # Fetch student roll no and name
        res = await db.execute(select(models.Student).filter(models.Student.id == student_id))
        student = res.scalars().first()
        if not student:
            raise ValueError(f"Student with ID {student_id} not found")

        # 1. Calculate deterministic scores
        scores, profile_type, placement_probability = await cls.compute_capability_scores(student_id, db)

        # 2. Save capability scores in DB
        cap_score_rec = await db.get(models.StudentCapabilityScore, student_id)
        if not cap_score_rec:
            cap_score_rec = models.StudentCapabilityScore(student_id=student_id)
            db.add(cap_score_rec)
            
        for k, v in scores.items():
            setattr(cap_score_rec, k, v)
        cap_score_rec.profile_type = profile_type
        cap_score_rec.placement_probability = placement_probability
        cap_score_rec.confidence_score = 0.90
        cap_score_rec.updated_at = datetime.utcnow()
        cap_score_rec.computed_at = datetime.utcnow()

        # Update growth history record for the student's current semester
        semester = student.current_semester or 1
        growth_rec_res = await db.execute(
            select(models.StudentGrowthHistory)
            .filter(
                and_(
                    models.StudentGrowthHistory.student_id == student_id,
                    models.StudentGrowthHistory.semester == semester
                )
            )
        )
        growth_rec = growth_rec_res.scalars().first()

        # Calculate growth delta (SPI current - SPI previous)
        prev_growth_res = await db.execute(
            select(models.StudentGrowthHistory)
            .filter(
                and_(
                    models.StudentGrowthHistory.student_id == student_id,
                    models.StudentGrowthHistory.semester == semester - 1
                )
            )
        )
        prev_growth = prev_growth_res.scalars().first()
        prev_spi = float(prev_growth.spi_score) if prev_growth else scores["spi_score"]
        growth_delta = scores["spi_score"] - prev_spi

        if not growth_rec:
            growth_rec = models.StudentGrowthHistory(
                student_id=student_id,
                semester=semester,
                spi_score=scores["spi_score"],
                growth_delta=growth_delta,
                generated_at=datetime.utcnow()
            )
            db.add(growth_rec)
        else:
            growth_rec.spi_score = scores["spi_score"]
            growth_rec.growth_delta = growth_delta
            growth_rec.generated_at = datetime.utcnow()

        await db.flush()

        ai_profile_data = None

        # 3. Call NVIDIA DeepSeek AI Service (unless bypassed)
        if not bypass_ai:
            # Fetch extracurricular list and counselor remarks to provide to the AI as context
            ec_res = await db.execute(select(models.ExtraCurricular).filter(models.ExtraCurricular.student_id == student_id))
            ecs = ec_res.scalars().all()
            ecs_text = "; ".join([f"{ec.category}: {ec.description}" for ec in ecs]) if ecs else "None recorded"

            counselor_res = await db.execute(select(models.CounselorDiary).filter(models.CounselorDiary.student_id == student_id))
            diaries = counselor_res.scalars().all()
            diaries_text = "; ".join([diary.remarks for diary in diaries if diary.remarks]) if diaries else "None recorded"

            system_prompt = (
                "You are SPARK ASIE (AI Student Intelligence Engine), an advanced academic "
                "analytics interpretative layer. You generate supportive, highly detailed, "
                "growth-oriented developmental profiles of students. "
                "CRITICAL: Do NOT generate insult-based or discouraging language. Always focus "
                "on supportive recommendations. You must output JSON only, conforming to the exact schema requested."
            )

            prompt = f"""
Analyze this student's multi-dimensional capabilities and provide professional, growth-oriented interpretations.

Student Details:
- Name: {student.name}
- Program Roll No: {student.roll_no}
- Profile Category (Deterministic): {profile_type}

Calculated Capability Scores (out of 100):
- Academic Score: {scores['academic_score']:.1f}
- Technical Skill: {scores['technical_score']:.1f}
- Communication Score: {scores['communication_score']:.1f}
- Leadership Index: {scores['leadership_score']:.1f}
- Sports Excellence: {scores['sports_score']:.1f}
- Creativity/Innovation: {scores['creativity_score']:.1f}
- Discipline/Attendance: {scores['discipline_score']:.1f}
- Consistency: {scores['consistency_score']:.1f}
- Placement Readiness: {scores['placement_score']:.1f}
- Growth Momentum: {scores['growth_score']:.1f}
- Student Potential Index (SPI): {scores['spi_score']:.1f}

Extracurricular Logs:
{ecs_text}

Counselor Notes:
{diaries_text}

Calculated Base Placement Probability: {placement_probability:.1f}%

Provide the qualitative analysis as a single JSON object. Do not include markdown wraps or anything except the JSON.
Required Schema:
{{
  "primary_identity": "E.g., High-Potential Tech Lead",
  "secondary_identity": "E.g., Creative Problem Solver",
  "strengths": ["Strength 1", "Strength 2", "Strength 3"],
  "weaknesses": ["Area of growth 1", "Area of growth 2"],
  "recommendations": ["Developmental path 1", "Actionable improvement 2"],
  "career_fit": [
    {{
      "domain": "Software Engineering",
      "match_percentage": 85.0,
      "explanation": "Brief explanation based on strong technical score and sports coordination."
    }},
    {{
      "domain": "Product Management",
      "match_percentage": 70.0,
      "explanation": "Supported by good leadership extracurriculars and active counselor diary representation."
    }}
  ],
  "placement_probability": {placement_probability:.1f},
  "summary": "Compassionate, highly professional 2-paragraph summary highlighting the student's hidden potential, consistency, and recommended supportive path."
}}
"""
            # AI Retry Wrapper & Schema Fallback Validation
            ai_success = False
            for attempt in range(3):
                try:
                    response_text = await generate(prompt, system=system_prompt, thinking=False, retries=1)
                    
                    # Safe parsing
                    if "```json" in response_text:
                        json_str = response_text.split("```json")[-1].split("```")[0].strip()
                    else:
                        json_str = response_text.strip()

                    parsed = json.loads(json_str)
                    
                    # Validate schema attributes
                    if "primary_identity" in parsed and "strengths" in parsed and "career_fit" in parsed:
                        ai_profile_data = parsed
                        ai_success = True
                        break
                except Exception as e:
                    logger.error(f"ASIE AI generation attempt {attempt+1} failed: {e}")
                    await asyncio.sleep(1)

            if not ai_success:
                # Fallback template to prevent hallucination / crashes and ensure supportive terminology
                ai_profile_data = {
                    "primary_identity": f"Promising {profile_type}",
                    "secondary_identity": "Active Learner",
                    "strengths": ["Dedicated focus area", "Core curricular commitment", "Receptive to counseling"],
                    "weaknesses": ["Opportunity to enhance consistency", "Potential for specialized growth"],
                    "recommendations": ["Engage in additional collaborative tech assignments", "Participate in communication/English seminars"],
                    "career_fit": [
                        {"domain": "Software Engineering", "match_percentage": max(50.0, float(scores["technical_score"])), "explanation": "Reflects primary technical skills and class assessments."},
                        {"domain": "Operations Support", "match_percentage": max(50.0, float(scores["discipline_score"])), "explanation": "Backed by attendance records and stable academic discipline."}
                    ],
                    "placement_probability": placement_probability,
                    "summary": f"{student.name} is classified as a {profile_type} with a Student Potential Index of {scores['spi_score']:.1f}. Active coaching and targeted skill-development programs will help them achieve their career goals."
                }

            # 4. Save AI profile in DB (ai_professional_insights)
            ai_profile_res = await db.execute(
                select(AIProfessionalInsight).where(AIProfessionalInsight.student_id == student_id)
            )
            ai_profile_rec = ai_profile_res.scalars().first()
            if not ai_profile_rec:
                ai_profile_rec = AIProfessionalInsight(student_id=student_id)
                db.add(ai_profile_rec)

            ai_profile_rec.strengths = ai_profile_data.get("strengths")
            ai_profile_rec.improvement_areas = ai_profile_data.get("weaknesses")
            ai_profile_rec.missing_skills = ai_profile_data.get("recommendations")
            ai_profile_rec.career_fit_roles = ai_profile_data.get("career_fit")
            ai_profile_rec.ai_summary = ai_profile_data.get("summary")
            ai_profile_rec.ai_status = AIStatus.COMPLETED
            ai_profile_rec.generated_at = datetime.utcnow()

        await db.commit()

        # Build schema response
        cap_score_dto = schemas.StudentCapabilityScoreResponse.model_validate(cap_score_rec)
        ai_profile_dto = None
        if ai_profile_data:
            ai_profile_dto = schemas.StudentAIProfileResponse(
                primary_identity=ai_profile_data.get("primary_identity") or f"Promising {profile_type}",
                secondary_identity=ai_profile_data.get("secondary_identity") or "Active Learner",
                strengths=ai_profile_data.get("strengths", []),
                weaknesses=ai_profile_data.get("weaknesses", []),
                recommendations=ai_profile_data.get("recommendations", []),
                placement_probability=float(ai_profile_data.get("placement_probability", placement_probability)),
                career_fit=[schemas.CareerFitItem(**item) for item in ai_profile_data.get("career_fit", [])],
                ai_summary=ai_profile_data.get("summary"),
                confidence_score=0.90,
                generated_at=datetime.utcnow()
            )

        return schemas.StudentDNAResponse(
            roll_no=student.roll_no,
            student_name=student.name,
            capability_scores=cap_score_dto,
            ai_profile=ai_profile_dto,
            spi_score=float(scores["spi_score"]),
            profile_type=profile_type
        )
