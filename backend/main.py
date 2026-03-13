from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from database import engine, get_db, SessionLocal, DATABASE_URL
from models import Base, User
import crud, schemas, auth
import os, sys, random, unicodedata
from datetime import datetime, timezone, timedelta

# ── INIT ──────────────────────────────────────────────────────────────────────

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="EduSmart ERP",
    description="Système ERP pour centre de formation",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── STARTUP LOG ───────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup_log():
    SEP = "=" * 55
    print(f"\n{SEP}")
    print("  EDUSMART API  —  démarrage")
    print(SEP)
    print(f"  Base de données : {DATABASE_URL}")
    print(f"  Docs            : http://localhost:8000/docs")
    print(f"  Interface       : ouvrir index.html dans le navigateur")
    print(SEP)
    print("  COMPTES DISPONIBLES")
    print(f"  {'Rôle':<14} {'Email':<35} {'Mot de passe'}")
    print(f"  {'-'*14} {'-'*35} {'-'*12}")

    db = SessionLocal()
    try:
        # Admin
        admin = db.query(User).filter(User.role == "admin").first()
        if admin:
            print(f"  {'admin':<14} {admin.email:<35} admin123")

        # Formateurs (3 max)
        teachers = db.query(User).filter(User.role == "formateur").limit(3).all()
        for t in teachers:
            print(f"  {'formateur':<14} {t.email:<35} form123")
        if not teachers:
            print(f"  {'formateur':<14} {'(aucun — lancez seed)':<35}")

        # Étudiants (3 max)
        students = db.query(User).filter(User.role == "etudiant").limit(3).all()
        for s in students:
            print(f"  {'etudiant':<14} {s.email:<35} etu123")
        if not students:
            print(f"  {'etudiant':<14} {'(aucun — lancez seed)':<35}")

        total_s = db.query(User).filter(User.role == "etudiant").count()
        total_t = db.query(User).filter(User.role == "formateur").count()
        if total_s > 3 or total_t > 3:
            print(f"  ... et {total_s} étudiants, {total_t} formateurs au total")
    finally:
        db.close()

    print(SEP + "\n")


# ── AUTH ──────────────────────────────────────────────────────────────────────

@app.post("/api/v1/auth/login", response_model=schemas.Token, tags=["Auth"])
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = crud.get_user_by_email(db, form.username)
    if not user or not auth.verify_password(form.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect")
    token = auth.create_access_token({"sub": user.email, "role": user.role})
    return {"access_token": token, "token_type": "bearer"}

@app.get("/api/v1/auth/me", response_model=schemas.UserOut, tags=["Auth"])
def me(current_user=Depends(auth.get_current_user)):
    return current_user


# ── USERS ─────────────────────────────────────────────────────────────────────

@app.post("/api/v1/users", response_model=schemas.UserOut, tags=["Users"])
def create_user(data: schemas.UserCreate, db: Session = Depends(get_db),
                _=Depends(auth.require_role("admin"))):
    if crud.get_user_by_email(db, data.email):
        raise HTTPException(status_code=400, detail="Email déjà utilisé")
    return crud.create_user(db, data)

@app.get("/api/v1/users", response_model=list[schemas.UserOut], tags=["Users"])
def list_users(db: Session = Depends(get_db), _=Depends(auth.require_role("admin", "scolarite"))):
    return crud.get_users(db)


# ── SEMESTERS ─────────────────────────────────────────────────────────────────

@app.post("/api/v1/semesters", response_model=schemas.SemesterOut, tags=["Semesters"])
def create_semester(data: schemas.SemesterCreate, db: Session = Depends(get_db),
                    _=Depends(auth.require_role("admin", "scolarite"))):
    return crud.create_semester(db, data)

@app.get("/api/v1/semesters", response_model=list[schemas.SemesterOut], tags=["Semesters"])
def list_semesters(db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    return crud.get_semesters(db)


# ── STUDENTS ──────────────────────────────────────────────────────────────────

@app.post("/api/v1/students", response_model=schemas.StudentOut, tags=["Students"])
def create_student(data: schemas.StudentCreate, db: Session = Depends(get_db),
                   _=Depends(auth.require_role("admin", "scolarite"))):
    return crud.create_student(db, data)

@app.get("/api/v1/students", response_model=list[schemas.StudentOut], tags=["Students"])
def list_students(db: Session = Depends(get_db), _=Depends(auth.require_role("admin", "scolarite", "formateur"))):
    return crud.get_students(db)

@app.get("/api/v1/students/{student_id}", response_model=schemas.StudentOut, tags=["Students"])
def get_student(student_id: int, db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    s = crud.get_student(db, student_id)
    if not s:
        raise HTTPException(status_code=404, detail="Étudiant introuvable")
    return s


# ── TEACHERS ──────────────────────────────────────────────────────────────────

@app.post("/api/v1/teachers", response_model=schemas.TeacherOut, tags=["Teachers"])
def create_teacher(data: schemas.TeacherCreate, db: Session = Depends(get_db),
                   _=Depends(auth.require_role("admin"))):
    return crud.create_teacher(db, data)

@app.get("/api/v1/teachers", response_model=list[schemas.TeacherOut], tags=["Teachers"])
def list_teachers(db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    return crud.get_teachers(db)


# ── COURSES ───────────────────────────────────────────────────────────────────

@app.post("/api/v1/courses", response_model=schemas.CourseOut, tags=["Courses"])
def create_course(data: schemas.CourseCreate, db: Session = Depends(get_db),
                  _=Depends(auth.require_role("admin", "scolarite"))):
    # Vérification FK explicite avec message clair
    from models import Teacher, Semester
    if not db.query(Teacher).filter(Teacher.id == data.teacher_id).first():
        raise HTTPException(status_code=400, detail=f"Formateur ID {data.teacher_id} introuvable")
    if not db.query(Semester).filter(Semester.id == data.semester_id).first():
        raise HTTPException(status_code=400, detail=f"Semestre ID {data.semester_id} introuvable")
    return crud.create_course(db, data)

@app.get("/api/v1/courses", response_model=list[schemas.CourseOut], tags=["Courses"])
def list_courses(db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    return crud.get_courses(db)

@app.get("/api/v1/courses/{course_id}", response_model=schemas.CourseOut, tags=["Courses"])
def get_course(course_id: int, db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    c = crud.get_course(db, course_id)
    if not c:
        raise HTTPException(status_code=404, detail="Cours introuvable")
    return c


# ── ENROLLMENTS ───────────────────────────────────────────────────────────────

@app.post("/api/v1/enrollments", response_model=schemas.EnrollmentOut, tags=["Enrollments"])
def create_enrollment(data: schemas.EnrollmentCreate, db: Session = Depends(get_db),
                      current_user=Depends(auth.require_role("admin", "scolarite", "etudiant"))):
    return crud.create_enrollment(db, data, current_user.id)

@app.patch("/api/v1/enrollments/{enrollment_id}", response_model=schemas.EnrollmentOut, tags=["Enrollments"])
def update_enrollment(enrollment_id: int, data: schemas.EnrollmentUpdate,
                      db: Session = Depends(get_db),
                      current_user=Depends(auth.require_role("admin", "scolarite"))):
    return crud.update_enrollment_status(db, enrollment_id, data, current_user.id)

@app.get("/api/v1/enrollments", response_model=list[schemas.EnrollmentOut], tags=["Enrollments"])
def list_enrollments(student_id: int = None, course_id: int = None,
                     db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    return crud.get_enrollments(db, student_id, course_id)


# ── GRADES ────────────────────────────────────────────────────────────────────

@app.post("/api/v1/grades", response_model=schemas.GradeOut, tags=["Grades"])
def create_grade(data: schemas.GradeCreate, db: Session = Depends(get_db),
                 current_user=Depends(auth.require_role("admin", "formateur"))):
    return crud.create_grade(db, data, current_user.id)

@app.patch("/api/v1/grades/{grade_id}", response_model=schemas.GradeOut, tags=["Grades"])
def update_grade(grade_id: int, data: schemas.GradeUpdate,
                 db: Session = Depends(get_db),
                 current_user=Depends(auth.require_role("admin", "formateur"))):
    return crud.update_grade(db, grade_id, data, current_user.id)

@app.get("/api/v1/grades", response_model=list[schemas.GradeOut], tags=["Grades"])
def list_grades(enrollment_id: int = None, db: Session = Depends(get_db),
                _=Depends(auth.get_current_user)):
    return crud.get_grades(db, enrollment_id)


# ── AUDIT ─────────────────────────────────────────────────────────────────────

@app.get("/api/v1/audit", tags=["Audit"])
def get_audit_logs(db: Session = Depends(get_db),
                   _=Depends(auth.require_role("admin"))):
    return crud.get_audit_logs(db)


# ── SEED ──────────────────────────────────────────────────────────────────────

class SeedStatus:
    running = False
    log     = []
    done    = False
    error   = None

seed_status = SeedStatus()

def run_seed(nb_students: int, nb_teachers: int, nb_semesters: int, nb_audit: int):
    from faker import Faker
    from models import (AuditLog, Course, Enrollment, EnrollmentStatus,
                        Grade, Semester, Student, Teacher)
    from auth import get_password_hash

    seed_status.running = True
    seed_status.done    = False
    seed_status.error   = None
    seed_status.log     = []

    def emit(msg):
        seed_status.log.append(msg)
        print(f"[SEED] {msg}")

    db = SessionLocal()
    try:
        fake = Faker("fr_FR")

        def slugify(text):
            text = text.lower().strip()
            text = unicodedata.normalize("NFD", text)
            text = "".join(c for c in text if unicodedata.category(c) != "Mn")
            return text.replace(" ", ".").replace("'", "").replace("-", ".")

        def unique_email(first, last, domain, used):
            base = f"{slugify(first)}.{slugify(last)}@{domain}"
            email, n = base, 1
            while email in used:
                email = f"{slugify(first)}.{slugify(last)}{n}@{domain}"
                n += 1
            used.add(email)
            return email

        # Nettoyage
        emit("Nettoyage des tables...")
        db.query(AuditLog).delete()
        db.query(Grade).delete()
        db.query(Enrollment).delete()
        db.query(Course).delete()
        db.query(Semester).delete()
        db.query(Student).delete()
        db.query(Teacher).delete()
        db.query(User).filter(User.role != "admin").delete()
        db.commit()

        # Admin
        admin = db.query(User).filter(User.email == "admin@edusmart.com").first()
        if not admin:
            admin = User(email="admin@edusmart.com",
                         hashed_password=get_password_hash("admin123"), role="admin")
            db.add(admin); db.commit()
        emit("Admin : admin@edusmart.com / admin123")

        # Semestres
        now = datetime.now(timezone.utc)
        semesters, base_dt = [], datetime(2023, 9, 1, tzinfo=timezone.utc)
        specialties_pool = [
            "Informatique", "Mathematiques", "Reseau & Securite",
            "Base de donnees", "Intelligence Artificielle",
            "Developpement Web", "Cybersecurite", "Data Science",
        ]
        for i in range(nb_semesters):
            mo = i * 6
            y  = base_dt.year  + (base_dt.month + mo - 1) // 12
            m  = (base_dt.month + mo - 1) % 12 + 1
            start = datetime(y, m, 1, tzinfo=timezone.utc)
            end   = start + timedelta(days=180)
            is_last  = (i == nb_semesters - 1)
            deadline = now + timedelta(days=30) if is_last else start + timedelta(days=14)
            half = "S1" if start.month < 7 else "S2"
            name = f"{half} {start.year-1}-{start.year}" if half == "S1" else f"{half} {start.year}-{start.year+1}"
            sem = Semester(name=name, start_date=start, end_date=end,
                           enroll_deadline=deadline,
                           exam_start=end - timedelta(days=21),
                           exam_end=end   - timedelta(days=5))
            db.add(sem); semesters.append(sem)
        db.commit()
        emit(f"{len(semesters)} semestres créés")

        # Formateurs
        teachers, used_t = [], set()
        for i in range(nb_teachers):
            first = fake.first_name(); last = fake.last_name()
            email = unique_email(first, last, "edusmart.com", used_t)
            spec  = specialties_pool[i % len(specialties_pool)]
            user  = User(email=email, hashed_password=get_password_hash("form123"), role="formateur")
            db.add(user); db.commit(); db.refresh(user)
            t = Teacher(user_id=user.id, first_name=first, last_name=last,
                        specialty=spec, phone=fake.phone_number())
            db.add(t); db.commit(); db.refresh(t); teachers.append(t)
        emit(f"{len(teachers)} formateurs créés (mdp: form123)")

        # Cours
        course_templates = [
            ("INF","Programmation"), ("MAT","Mathematiques"), ("RES","Reseaux"),
            ("BDD","Bases de donnees"), ("IA","Intelligence Artificielle"),
            ("WEB","Developpement Web"), ("SEC","Securite"), ("STA","Statistiques"),
        ]
        courses, cc = [], 0
        for s_idx, sem in enumerate(semesters):
            for c_idx in range(random.randint(2, min(4, len(teachers)))):
                tpl   = course_templates[cc % len(course_templates)]
                level = (s_idx + 1) * 100 + c_idx + 1
                course = Course(
                    code=f"{tpl[0]}{level}", name=f"{tpl[1]} — Niveau {level}",
                    credits=random.choice([3, 4]), quota=random.randint(20, 40),
                    teacher_id=teachers[c_idx % len(teachers)].id,
                    semester_id=sem.id, description=fake.sentence(nb_words=10)
                )
                db.add(course); db.commit(); db.refresh(course)
                courses.append(course); cc += 1
        emit(f"{len(courses)} cours créés")

        # Étudiants
        students, used_s = [], set()
        for _ in range(nb_students):
            first = fake.first_name(); last = fake.last_name()
            email = unique_email(first, last, "etu.edusmart.com", used_s)
            user  = User(email=email, hashed_password=get_password_hash("etu123"), role="etudiant")
            db.add(user); db.commit(); db.refresh(user)
            bd = fake.date_of_birth(minimum_age=18, maximum_age=26)
            s  = Student(user_id=user.id, first_name=first, last_name=last,
                         phone=fake.phone_number(),
                         address=fake.address().replace("\n", ", "),
                         birth_date=datetime.combine(bd, datetime.min.time()).replace(tzinfo=timezone.utc))
            db.add(s); db.commit(); db.refresh(s); students.append(s)
        emit(f"{len(students)} étudiants créés (mdp: etu123)")

        # Inscriptions + notes
        past_courses = [c for c in courses if c.semester_id != semesters[-1].id]
        profiles     = random.choices(["bon","moyen","risque"], weights=[65,25,10], k=len(students))
        enr_count, grade_count = 0, 0

        for student, profile in zip(students, profiles):
            if not past_courses: break
            pool = random.sample(past_courses, random.randint(2, min(6, len(past_courses))))
            for course in pool:
                enr = Enrollment(student_id=student.id, course_id=course.id,
                                 status=EnrollmentStatus.validee)
                db.add(enr); db.commit(); db.refresh(enr); enr_count += 1
                score = (round(random.uniform(12,20),1) if profile=="bon" else
                         round(random.uniform(7,14),1)  if profile=="moyen" else
                         round(random.uniform(0,8),1))
                db.add(Grade(enrollment_id=enr.id, score=score,
                             comment=fake.sentence(nb_words=6)))
                db.commit(); grade_count += 1

        # Inscriptions semestre courant
        current_courses = [c for c in courses if c.semester_id == semesters[-1].id]
        pend_count = 0
        for student in students[:max(1, int(len(students)*0.7))]:
            if not current_courses: break
            for course in random.sample(current_courses, random.randint(1, min(3, len(current_courses)))):
                exists = db.query(Enrollment).filter(
                    Enrollment.student_id == student.id,
                    Enrollment.course_id  == course.id).first()
                if not exists:
                    db.add(Enrollment(student_id=student.id, course_id=course.id,
                                      status=random.choice([EnrollmentStatus.en_attente,
                                                            EnrollmentStatus.validee])))
                    db.commit(); pend_count += 1

        emit(f"{enr_count} inscriptions + {grade_count} notes")
        emit(f"{pend_count} inscriptions semestre courant")

        # Audit
        actions = ["CREATE_ENROLLMENT","UPDATE_GRADE",
                   "UPDATE_ENROLLMENT_STATUS:validee","CREATE_GRADE"]
        for _ in range(nb_audit):
            db.add(AuditLog(user_id=admin.id, action=random.choice(actions),
                            entity=random.choice(["Enrollment","Grade"]),
                            entity_id=random.randint(1, max(1, enr_count)),
                            detail=fake.sentence(nb_words=8)))
        db.commit()
        emit(f"{nb_audit} logs d'audit")
        emit("Seeding terminé avec succès.")
        seed_status.done = True

    except Exception as e:
        seed_status.error = str(e)
        emit(f"ERREUR : {e}")
    finally:
        db.close()
        seed_status.running = False


@app.post("/api/v1/seed", tags=["Admin"])
def trigger_seed(
    nb_students:  int = 30,
    nb_teachers:  int = 5,
    nb_semesters: int = 3,
    nb_audit:     int = 30,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    _=Depends(auth.require_role("admin"))
):
    if seed_status.running:
        raise HTTPException(status_code=409, detail="Seed déjà en cours")
    background_tasks.add_task(run_seed, nb_students, nb_teachers, nb_semesters, nb_audit)
    return {"status": "started", "message": f"Génération de {nb_students} étudiants lancée"}

@app.get("/api/v1/seed/status", tags=["Admin"])
def get_seed_status(_=Depends(auth.require_role("admin"))):
    return {
        "running": seed_status.running,
        "done":    seed_status.done,
        "error":   seed_status.error,
        "log":     seed_status.log,
    }


# ── PREDICT ───────────────────────────────────────────────────────────────────

@app.get("/api/v1/predict/dropout", tags=["IA"])
def predict_dropout(
    threshold: float = 0.35,
    _=Depends(auth.require_role("admin")),
    db: Session = Depends(get_db)
):
    from sqlalchemy import text

    try:
        import numpy as np
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        from sklearn.impute import SimpleImputer
    except ImportError:
        raise HTTPException(status_code=500, detail="scikit-learn non installé")

    rows = db.execute(text("""
        SELECT
            f.student_id,
            st.first_name, st.last_name, st.age,
            COUNT(*)                                         AS nb_cours,
            ROUND(AVG(f.score)::NUMERIC, 3)                 AS avg_score,
            ROUND(MIN(f.score)::NUMERIC, 3)                 AS min_score,
            ROUND(MAX(f.score)::NUMERIC, 3)                 AS max_score,
            ROUND(COALESCE(STDDEV(f.score),0)::NUMERIC, 3)  AS std_score,
            SUM(CASE WHEN f.is_passing   THEN 1 ELSE 0 END) AS nb_passing,
            SUM(CASE WHEN f.is_at_risk   THEN 1 ELSE 0 END) AS nb_at_risk,
            SUM(CASE WHEN f.is_excellent THEN 1 ELSE 0 END) AS nb_excellent,
            ROUND(100.0 * SUM(CASE WHEN f.is_passing THEN 1 ELSE 0 END)
                  / COUNT(*), 2)                            AS pass_rate,
            ROUND(100.0 * SUM(CASE WHEN f.is_at_risk THEN 1 ELSE 0 END)
                  / COUNT(*), 2)                            AS risk_rate
        FROM dw.fact_performance f
        JOIN dw.dim_student st ON st.student_id = f.student_id
        WHERE f.enrollment_status = 'validee'
        GROUP BY f.student_id, st.first_name, st.last_name, st.age
        HAVING COUNT(*) >= 2
    """)).mappings().all()

    if len(rows) < 5:
        raise HTTPException(
            status_code=400,
            detail="Pas assez de données dans le DW. Lancez d'abord l'ETL depuis le terminal."
        )

    # Build features + labels
    X, y, meta = [], [], []
    for r in rows:
        avg    = float(r["avg_score"]  or 0)
        mn     = float(r["min_score"]  or 0)
        mx     = float(r["max_score"]  or 0)
        std    = float(r["std_score"]  or 0)
        nb     = int(r["nb_cours"]     or 0)
        pass_r = float(r["pass_rate"]  or 0)
        risk_r = float(r["risk_rate"]  or 0)
        nb_risk= int(r["nb_at_risk"]   or 0)
        nb_exc = int(r["nb_excellent"] or 0)

        X.append([avg, mn, mx, std, nb, pass_r, risk_r, nb_risk, nb_exc,
                  mx - mn, int(r["nb_passing"] or 0) - nb_risk])
        y.append(int(avg < 10 or pass_r < 50 or nb_risk >= 2))
        meta.append({
            "student_id": int(r["student_id"]),
            "name":       f"{r['first_name']} {r['last_name']}",
            "avg_score":  round(avg, 1),
            "pass_rate":  round(pass_r, 1),
            "nb_at_risk": nb_risk,
        })

    X = np.array(X, dtype=float)
    y = np.array(y, dtype=int)

    if y.sum() == 0:
        raise HTTPException(status_code=400, detail="Aucun étudiant à risque détecté dans les données.")

    pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
        ("clf",     RandomForestClassifier(n_estimators=200, max_depth=6,
                                           class_weight="balanced", random_state=42)),
    ])
    pipeline.fit(X, y)
    probs = pipeline.predict_proba(X)[:, 1]

    results = []
    for i, prob in enumerate(probs):
        level = ("Critique" if prob >= 0.7 else "Élevé" if prob >= 0.5
                 else "Modéré" if prob >= 0.35 else "Faible")
        results.append({
            **meta[i],
            "dropout_prob": round(float(prob), 3),
            "at_risk":      bool(prob >= threshold),
            "risk_level":   level,
        })

    results.sort(key=lambda x: x["dropout_prob"], reverse=True)
    at_risk = [r for r in results if r["at_risk"]]

    return {
        "total":     len(results),
        "at_risk":   len(at_risk),
        "threshold": threshold,
        "model":     "RandomForest",
        "predictions": results,
    }


# ── HEALTH ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {"status": "EduSmart API running", "docs": "/docs"}