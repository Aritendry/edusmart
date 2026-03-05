"""
seed.py — Génération de données de test pour EduSmart (avec Faker)
Lancer : python seed.py
"""

from database import SessionLocal
from models import (User, Student, Teacher, Course, Enrollment,
                    Grade, Semester, AuditLog, EnrollmentStatus)
from auth import get_password_hash
from datetime import datetime, timezone, timedelta
from faker import Faker
import random

fake = Faker("fr_FR")
db  = SessionLocal()

print("🌱 Début du seeding...")


# ─── NETTOYAGE ────────────────────────────────────────────────────────────────

db.query(AuditLog).delete()
db.query(Grade).delete()
db.query(Enrollment).delete()
db.query(Course).delete()
db.query(Semester).delete()
db.query(Student).delete()
db.query(Teacher).delete()
db.query(User).filter(User.role != "admin").delete()
db.commit()
print("🧹 Tables nettoyées")


# ─── ADMIN ────────────────────────────────────────────────────────────────────

admin = db.query(User).filter(User.email == "admin@edusmart.com").first()
if not admin:
    admin = User(
        email="admin@edusmart.com",
        hashed_password=get_password_hash("admin123"),
        role="admin"
    )
    db.add(admin)
    db.commit()
print("👤 Admin : admin@edusmart.com / admin123")


# ─── SEMESTRES ────────────────────────────────────────────────────────────────

now = datetime.now(timezone.utc)

semesters_raw = [
    {
        "name": "S1 2023-2024",
        "start_date":       datetime(2023, 9,  1, tzinfo=timezone.utc),
        "end_date":         datetime(2024, 1, 31, tzinfo=timezone.utc),
        "enroll_deadline":  datetime(2023, 9, 15, tzinfo=timezone.utc),
        "exam_start":       datetime(2024, 1, 10, tzinfo=timezone.utc),
        "exam_end":         datetime(2024, 1, 25, tzinfo=timezone.utc),
    },
    {
        "name": "S2 2023-2024",
        "start_date":       datetime(2024, 2,  1, tzinfo=timezone.utc),
        "end_date":         datetime(2024, 6, 30, tzinfo=timezone.utc),
        "enroll_deadline":  datetime(2024, 2, 15, tzinfo=timezone.utc),
        "exam_start":       datetime(2024, 6,  5, tzinfo=timezone.utc),
        "exam_end":         datetime(2024, 6, 25, tzinfo=timezone.utc),
    },
    {
        "name": "S1 2024-2025",
        "start_date":       datetime(2024, 9,  1, tzinfo=timezone.utc),
        "end_date":         datetime(2025, 1, 31, tzinfo=timezone.utc),
        "enroll_deadline":  now + timedelta(days=30),   # future → inscriptions possibles
        "exam_start":       datetime(2025, 1, 10, tzinfo=timezone.utc),
        "exam_end":         datetime(2025, 1, 25, tzinfo=timezone.utc),
    },
]

semesters = []
for s in semesters_raw:
    sem = Semester(**s)
    db.add(sem)
    semesters.append(sem)
db.commit()
print(f"📅 {len(semesters)} semestres créés")


# ─── FORMATEURS ───────────────────────────────────────────────────────────────

specialties = [
    "Informatique",
    "Mathématiques",
    "Réseau & Sécurité",
    "Base de données",
    "Intelligence Artificielle",
]

teachers = []
for spec in specialties:
    first = fake.first_name()
    last  = fake.last_name()
    email = f"{first.lower()}.{last.lower()}@edusmart.com"
    user  = User(
        email=email,
        hashed_password=get_password_hash("form123"),
        role="formateur"
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    teacher = Teacher(
        user_id=user.id,
        first_name=first,
        last_name=last,
        specialty=spec,
        phone=fake.phone_number()
    )
    db.add(teacher)
    db.commit()
    db.refresh(teacher)
    teachers.append(teacher)

print(f"👨‍🏫 {len(teachers)} formateurs créés  (mot de passe : form123)")


# ─── COURS ────────────────────────────────────────────────────────────────────

courses_raw = [
    # (code, nom, crédits, quota, idx_teacher, idx_semester)
    ("INF101", "Introduction à la Programmation",    3, 35, 0, 0),
    ("MAT101", "Algèbre Linéaire",                   3, 30, 1, 0),
    ("RES101", "Réseaux Informatiques",               3, 25, 2, 0),
    ("BDD101", "Introduction aux Bases de Données",  3, 30, 3, 0),
    ("INF201", "Programmation Orientée Objet",        4, 30, 0, 1),
    ("MAT201", "Probabilités et Statistiques",        3, 28, 1, 1),
    ("RES201", "Sécurité des Réseaux",                3, 20, 2, 1),
    ("BDD201", "SQL Avancé et NoSQL",                 4, 25, 3, 1),
    ("INF301", "Développement Web Full-Stack",        4, 30, 0, 2),
    ("IA301",  "Machine Learning",                    4, 20, 4, 2),
    ("BDD301", "Data Warehouse & BI",                 3, 25, 3, 2),
]

courses = []
for code, name, credits, quota, t_idx, s_idx in courses_raw:
    course = Course(
        code=code,
        name=name,
        credits=credits,
        quota=quota,
        teacher_id=teachers[t_idx].id,
        semester_id=semesters[s_idx].id,
        description=fake.sentence(nb_words=12)
    )
    db.add(course)
    db.commit()
    db.refresh(course)
    courses.append(course)

print(f"📚 {len(courses)} cours créés")


# ─── ÉTUDIANTS ────────────────────────────────────────────────────────────────

NB_STUDENTS = 30
students     = []
used_emails  = set()

for _ in range(NB_STUDENTS):
    first = fake.first_name()
    last  = fake.last_name()

    # email unique garanti
    base  = f"{first.lower()}.{last.lower()}@etu.edusmart.com"
    email = base
    n     = 1
    while email in used_emails:
        email = f"{first.lower()}.{last.lower()}{n}@etu.edusmart.com"
        n += 1
    used_emails.add(email)

    user = User(
        email=email,
        hashed_password=get_password_hash("etu123"),
        role="etudiant"
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    student = Student(
        user_id=user.id,
        first_name=first,
        last_name=last,
        phone=fake.phone_number(),
        address=fake.address().replace("\n", ", "),
        birth_date=datetime.combine(
        fake.date_of_birth(minimum_age=18, maximum_age=26),
            datetime.min.time()
        ).replace(tzinfo=timezone.utc)
    )
    db.add(student)
    db.commit()
    db.refresh(student)
    students.append(student)

print(f"🎓 {len(students)} étudiants créés  (mot de passe : etu123)")


# ─── INSCRIPTIONS + NOTES — semestres passés ─────────────────────────────────

past_courses     = courses[:8]
enrollment_count = 0
grade_count      = 0

# Chaque étudiant a un profil fixe → cohérence pour l'IA plus tard
profiles = random.choices(
    ["bon", "moyen", "risque"],
    weights=[65, 25, 10],
    k=len(students)
)

for student, profile in zip(students, profiles):
    nb   = random.randint(3, 6)
    pool = random.sample(past_courses, min(nb, len(past_courses)))

    for course in pool:
        enrollment = Enrollment(
            student_id=student.id,
            course_id=course.id,
            status=EnrollmentStatus.validee
        )
        db.add(enrollment)
        db.commit()
        db.refresh(enrollment)
        enrollment_count += 1

        if profile == "bon":
            score = round(random.uniform(12, 20), 1)
        elif profile == "moyen":
            score = round(random.uniform(7, 14),  1)
        else:
            score = round(random.uniform(0, 8),   1)

        grade = Grade(
            enrollment_id=enrollment.id,
            score=score,
            comment=fake.sentence(nb_words=6)
        )
        db.add(grade)
        db.commit()
        grade_count += 1

print(f"📝 {enrollment_count} inscriptions validées")
print(f"⭐ {grade_count} notes attribuées")


# ─── INSCRIPTIONS — semestre courant ─────────────────────────────────────────

current_courses = courses[8:]
pending_count   = 0

for student in students[:20]:
    nb   = random.randint(1, 3)
    pool = random.sample(current_courses, min(nb, len(current_courses)))
    for course in pool:
        exists = db.query(Enrollment).filter(
            Enrollment.student_id == student.id,
            Enrollment.course_id  == course.id
        ).first()
        if not exists:
            enrollment = Enrollment(
                student_id=student.id,
                course_id=course.id,
                status=random.choice([
                    EnrollmentStatus.en_attente,
                    EnrollmentStatus.validee
                ])
            )
            db.add(enrollment)
            db.commit()
            pending_count += 1

print(f"⏳ {pending_count} inscriptions semestre courant")


# ─── AUDIT LOGS ───────────────────────────────────────────────────────────────

actions = [
    "CREATE_ENROLLMENT",
    "UPDATE_GRADE",
    "UPDATE_ENROLLMENT_STATUS:validee",
    "UPDATE_ENROLLMENT_STATUS:annulee",
    "CREATE_GRADE",
]
for _ in range(30):
    db.add(AuditLog(
        user_id=admin.id,
        action=random.choice(actions),
        entity=random.choice(["Enrollment", "Grade"]),
        entity_id=random.randint(1, enrollment_count),
        detail=fake.sentence(nb_words=8)
    ))
db.commit()
print("📋 30 logs d'audit créés")


# ─── RÉSUMÉ ───────────────────────────────────────────────────────────────────

print("\n✅ Seeding terminé !")
print(f"   👤 Admin      → admin@edusmart.com      / admin123")
print(f"   👨‍🏫 Formateurs → <prenom.nom>@edusmart.com / form123")
print(f"   🎓 Étudiants  → <prenom.nom>@etu.edusmart.com / etu123")
print(f"   📅 {len(semesters)} semestres  |  📚 {len(courses)} cours")
print(f"   📝 {enrollment_count} inscriptions passées  |  ⏳ {pending_count} en cours")
print(f"   ⭐ {grade_count} notes")

db.close()