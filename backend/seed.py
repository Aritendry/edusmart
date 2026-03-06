"""
seed.py -- Generateur de donnees de test EduSmart (Faker)
---------------------------------------------------------
Usage :
  python seed.py                        # valeurs par defaut
  python seed.py --students 50          # 50 etudiants
  python seed.py --students 100 --teachers 8 --semesters 4 --audit 50
  python seed.py --help
"""

import argparse
import random
import unicodedata
from datetime import datetime, timezone, timedelta

from faker import Faker

from database import SessionLocal
from models import (AuditLog, Course, Enrollment, EnrollmentStatus,
                    Grade, Semester, Student, Teacher, User)
from auth import get_password_hash

# ---------------------------------------------------------------------------
#  ARGS
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description="Genere des donnees de test pour EduSmart."
)
parser.add_argument("--students",  type=int, default=30,  help="Nombre d'etudiants (defaut: 30)")
parser.add_argument("--teachers",  type=int, default=5,   help="Nombre de formateurs (defaut: 5)")
parser.add_argument("--semesters", type=int, default=3,   help="Nombre de semestres (defaut: 3)")
parser.add_argument("--audit",     type=int, default=30,  help="Nombre de logs d'audit (defaut: 30)")
args = parser.parse_args()

NB_STUDENTS  = max(1,  args.students)
NB_TEACHERS  = max(1,  args.teachers)
NB_SEMESTERS = max(1,  args.semesters)
NB_AUDIT     = max(0,  args.audit)

# ---------------------------------------------------------------------------
#  INIT
# ---------------------------------------------------------------------------

fake = Faker("fr_FR")
db   = SessionLocal()

SEP = "-" * 60

def slugify(text):
    """Convertit un texte en slug sans accents ni espaces pour les emails."""
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.replace(" ", ".").replace("'", "").replace("-", ".")
    return text

def unique_email(first, last, domain, used):
    base = f"{slugify(first)}.{slugify(last)}@{domain}"
    email, n = base, 1
    while email in used:
        email = f"{slugify(first)}.{slugify(last)}{n}@{domain}"
        n += 1
    used.add(email)
    return email

def to_aware(d):
    if hasattr(d, "date"):
        return d
    return datetime.combine(d, datetime.min.time()).replace(tzinfo=timezone.utc)

print(SEP)
print("  EDUSMART -- SEEDING")
print(f"  etudiants={NB_STUDENTS}  formateurs={NB_TEACHERS}  "
      f"semestres={NB_SEMESTERS}  audit={NB_AUDIT}")
print(SEP)

# ---------------------------------------------------------------------------
#  NETTOYAGE
# ---------------------------------------------------------------------------

print("[CLEAN ] Suppression des donnees existantes...")
db.query(AuditLog).delete()
db.query(Grade).delete()
db.query(Enrollment).delete()
db.query(Course).delete()
db.query(Semester).delete()
db.query(Student).delete()
db.query(Teacher).delete()
db.query(User).filter(User.role != "admin").delete()
db.commit()

# ---------------------------------------------------------------------------
#  ADMIN
# ---------------------------------------------------------------------------

admin = db.query(User).filter(User.email == "admin@edusmart.com").first()
if not admin:
    admin = User(
        email="admin@edusmart.com",
        hashed_password=get_password_hash("admin123"),
        role="admin"
    )
    db.add(admin)
    db.commit()
print("[ADMIN ] admin@edusmart.com / admin123")

# ---------------------------------------------------------------------------
#  SEMESTRES  (generés dynamiquement selon NB_SEMESTERS)
# ---------------------------------------------------------------------------

now = datetime.now(timezone.utc)
semesters = []

# Point de départ : NB_SEMESTERS semestres en arrière depuis aujourd'hui
# Chaque semestre dure ~6 mois, le dernier a une deadline future
semester_starts = []
base = datetime(2023, 9, 1, tzinfo=timezone.utc)
for i in range(NB_SEMESTERS):
    month_offset = i * 6
    y = base.year + (base.month + month_offset - 1) // 12
    m = (base.month + month_offset - 1) % 12 + 1
    semester_starts.append(datetime(y, m, 1, tzinfo=timezone.utc))

for i, start in enumerate(semester_starts):
    end      = start + timedelta(days=180)
    is_last  = (i == NB_SEMESTERS - 1)
    deadline = now + timedelta(days=30) if is_last else start + timedelta(days=14)

    # Nom : S1 / S2 selon le mois de début
    half = "S1" if start.month < 7 else "S2"
    name = f"{half} {start.year}-{start.year + 1}" if half == "S2" else f"{half} {start.year - 1}-{start.year}"

    sem = Semester(
        name=name,
        start_date=start,
        end_date=end,
        enroll_deadline=deadline,
        exam_start=end - timedelta(days=21),
        exam_end=end - timedelta(days=5),
    )
    db.add(sem)
    semesters.append(sem)
db.commit()
print(f"[SEM   ] {len(semesters)} semestres crees")

# ---------------------------------------------------------------------------
#  FORMATEURS
# ---------------------------------------------------------------------------

specialties_pool = [
    "Informatique", "Mathematiques", "Reseau & Securite",
    "Base de donnees", "Intelligence Artificielle",
    "Developpement Web", "Cybersecurite", "Data Science",
    "Systemes embarques", "Gestion de projet",
]

teachers     = []
used_t_emails = set()

for i in range(NB_TEACHERS):
    first = fake.first_name()
    last  = fake.last_name()
    email = unique_email(first, last, "edusmart.com", used_t_emails)
    spec  = specialties_pool[i % len(specialties_pool)]

    user = User(email=email, hashed_password=get_password_hash("form123"), role="formateur")
    db.add(user); db.commit(); db.refresh(user)

    teacher = Teacher(
        user_id=user.id,
        first_name=first, last_name=last,
        specialty=spec, phone=fake.phone_number()
    )
    db.add(teacher); db.commit(); db.refresh(teacher)
    teachers.append(teacher)

print(f"[TEACH ] {len(teachers)} formateurs crees  (mdp: form123)")

# ---------------------------------------------------------------------------
#  COURS  (2-3 cours par semestre selon quota de teachers)
# ---------------------------------------------------------------------------

course_templates = [
    ("INF", "Programmation"),
    ("MAT", "Mathematiques"),
    ("RES", "Reseaux"),
    ("BDD", "Bases de donnees"),
    ("IA",  "Intelligence Artificielle"),
    ("WEB", "Developpement Web"),
    ("SEC", "Securite informatique"),
    ("STA", "Statistiques"),
    ("AGL", "Genie logiciel"),
    ("SYS", "Systemes d'exploitation"),
]

courses       = []
courses_count = 0

for s_idx, sem in enumerate(semesters):
    nb_courses = random.randint(2, min(4, len(teachers)))
    for c_idx in range(nb_courses):
        tpl    = course_templates[courses_count % len(course_templates)]
        level  = (s_idx + 1) * 100 + c_idx + 1
        code   = f"{tpl[0]}{level}"
        name   = f"{tpl[1]} — Niveau {level}"

        course = Course(
            code=code,
            name=name,
            credits=random.choice([3, 4]),
            quota=random.randint(20, 40),
            teacher_id=teachers[c_idx % len(teachers)].id,
            semester_id=sem.id,
            description=fake.sentence(nb_words=10)
        )
        db.add(course); db.commit(); db.refresh(course)
        courses.append(course)
        courses_count += 1

print(f"[COURS ] {len(courses)} cours crees sur {len(semesters)} semestres")

# ---------------------------------------------------------------------------
#  ETUDIANTS
# ---------------------------------------------------------------------------

students      = []
used_s_emails = set()

for _ in range(NB_STUDENTS):
    first = fake.first_name()
    last  = fake.last_name()
    email = unique_email(first, last, "etu.edusmart.com", used_s_emails)

    user = User(email=email, hashed_password=get_password_hash("etu123"), role="etudiant")
    db.add(user); db.commit(); db.refresh(user)

    student = Student(
        user_id=user.id,
        first_name=first, last_name=last,
        phone=fake.phone_number(),
        address=fake.address().replace("\n", ", "),
        birth_date=to_aware(fake.date_of_birth(minimum_age=18, maximum_age=26))
    )
    db.add(student); db.commit(); db.refresh(student)
    students.append(student)

print(f"[ETU   ] {len(students)} etudiants crees  (mdp: etu123)")

# ---------------------------------------------------------------------------
#  INSCRIPTIONS + NOTES — semestres passes
# ---------------------------------------------------------------------------

past_courses     = [c for c in courses if c.semester_id != semesters[-1].id]
enrollment_count = 0
grade_count      = 0

# Profil fixe par etudiant pour coherence IA
profiles = random.choices(
    ["bon", "moyen", "risque"],
    weights=[65, 25, 10],
    k=len(students)
)

for student, profile in zip(students, profiles):
    if not past_courses:
        break
    nb   = random.randint(2, min(6, len(past_courses)))
    pool = random.sample(past_courses, nb)

    for course in pool:
        enr = Enrollment(
            student_id=student.id,
            course_id=course.id,
            status=EnrollmentStatus.validee
        )
        db.add(enr); db.commit(); db.refresh(enr)
        enrollment_count += 1

        if profile == "bon":
            score = round(random.uniform(12, 20), 1)
        elif profile == "moyen":
            score = round(random.uniform(7, 14), 1)
        else:
            score = round(random.uniform(0, 8), 1)

        db.add(Grade(
            enrollment_id=enr.id,
            score=score,
            comment=fake.sentence(nb_words=6)
        ))
        db.commit()
        grade_count += 1

print(f"[ENR   ] {enrollment_count} inscriptions passees validees")
print(f"[GRADE ] {grade_count} notes attribuees")

# ---------------------------------------------------------------------------
#  INSCRIPTIONS — semestre courant (dernier)
# ---------------------------------------------------------------------------

current_courses = [c for c in courses if c.semester_id == semesters[-1].id]
pending_count   = 0
sample_students = students[:min(len(students), max(1, int(len(students) * 0.7)))]

for student in sample_students:
    if not current_courses:
        break
    nb   = random.randint(1, min(3, len(current_courses)))
    pool = random.sample(current_courses, nb)
    for course in pool:
        exists = db.query(Enrollment).filter(
            Enrollment.student_id == student.id,
            Enrollment.course_id  == course.id
        ).first()
        if not exists:
            db.add(Enrollment(
                student_id=student.id,
                course_id=course.id,
                status=random.choice([
                    EnrollmentStatus.en_attente,
                    EnrollmentStatus.validee
                ])
            ))
            db.commit()
            pending_count += 1

print(f"[ENR   ] {pending_count} inscriptions semestre courant")

# ---------------------------------------------------------------------------
#  AUDIT LOGS
# ---------------------------------------------------------------------------

actions = [
    "CREATE_ENROLLMENT", "UPDATE_GRADE",
    "UPDATE_ENROLLMENT_STATUS:validee",
    "UPDATE_ENROLLMENT_STATUS:annulee",
    "CREATE_GRADE",
]

for _ in range(NB_AUDIT):
    db.add(AuditLog(
        user_id=admin.id,
        action=random.choice(actions),
        entity=random.choice(["Enrollment", "Grade"]),
        entity_id=random.randint(1, max(1, enrollment_count)),
        detail=fake.sentence(nb_words=8)
    ))
db.commit()
print(f"[AUDIT ] {NB_AUDIT} logs crees")

# ---------------------------------------------------------------------------
#  RESUME
# ---------------------------------------------------------------------------

print(SEP)
print("  SEEDING TERMINE")
print(SEP)
print(f"  Admin       : admin@edusmart.com / admin123")
print(f"  Formateurs  : <prenom.nom>@edusmart.com / form123")
print(f"  Etudiants   : <prenom.nom>@etu.edusmart.com / etu123")
print(f"  Semestres   : {len(semesters)}")
print(f"  Cours       : {len(courses)}")
print(f"  Inscriptions: {enrollment_count} passees + {pending_count} en cours")
print(f"  Notes       : {grade_count}")
print(f"  Audit logs  : {NB_AUDIT}")
print(SEP)

db.close()