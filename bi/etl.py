"""
etl.py -- Pipeline ETL + EDA EduSmart
--------------------------------------
Etapes :
  0. Init   : creation du schema DW (DDL embarque)
  1. Extract: lecture depuis l'ERP (schema public)
  2. Transform: nettoyage, metriques, enrichissement
  3. Load   : chargement dans le DW (schema dw)
  4. EDA    : analyse exploratoire directement en console

Lancer : python etl.py
"""

from sqlalchemy import create_engine, text
from datetime import date
from dotenv import load_dotenv
import os
import sys

# ---------------------------------------------------------------------------
#  CONFIG
# ---------------------------------------------------------------------------

load_dotenv(dotenv_path=os.path.join(
    os.path.dirname(__file__), "..", ".env"
))

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:secret@localhost:5432/edusmart"
)

# ---------------------------------------------------------------------------
#  HELPERS
# ---------------------------------------------------------------------------

SEP1 = "=" * 72
SEP2 = "-" * 72

def log(step, msg):
    print(f"[{step:<10}] {msg}")

def to_date(val):
    if val is None:
        return None
    return val.date() if hasattr(val, "date") else val

def get_age(birth_date):
    if not birth_date:
        return None
    today = date.today()
    bd = to_date(birth_date)
    return today.year - bd.year - (
        (today.month, today.day) < (bd.month, bd.day)
    )

def semester_label(d):
    if not d:
        return "Inconnu"
    d = to_date(d)
    if d.month < 7:
        return f"S1 {d.year - 1}-{d.year}"
    return f"S2 {d.year}-{d.year + 1}"

def build_time_record(d):
    MONTHS = [
        "Janvier", "Fevrier", "Mars", "Avril", "Mai", "Juin",
        "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"
    ]
    return {
        "full_date":  d,
        "day":        d.day,
        "month":      d.month,
        "month_name": MONTHS[d.month - 1],
        "quarter":    (d.month - 1) // 3 + 1,
        "year":       d.year,
        "semester":   semester_label(d),
    }

def bar(value, total, width=30):
    """Barre de progression ASCII."""
    filled = int(width * value / total) if total > 0 else 0
    return "[" + "#" * filled + "." * (width - filled) + "]"

# ---------------------------------------------------------------------------
#  ETAPE 0 -- INIT WAREHOUSE (DDL embarque)
# ---------------------------------------------------------------------------

DDL = [
    "CREATE SCHEMA IF NOT EXISTS dw",

    """CREATE TABLE IF NOT EXISTS dw.dim_time (
        time_id    SERIAL PRIMARY KEY,
        full_date  DATE        NOT NULL UNIQUE,
        day        INTEGER     NOT NULL,
        month      INTEGER     NOT NULL,
        month_name VARCHAR(20) NOT NULL,
        quarter    INTEGER     NOT NULL,
        year       INTEGER     NOT NULL,
        semester   VARCHAR(20) NOT NULL
    )""",

    """CREATE TABLE IF NOT EXISTS dw.dim_student (
        student_id INTEGER      PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name  VARCHAR(100) NOT NULL,
        email      VARCHAR(200) NOT NULL,
        age        INTEGER
    )""",

    """CREATE TABLE IF NOT EXISTS dw.dim_teacher (
        teacher_id INTEGER      PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name  VARCHAR(100) NOT NULL,
        specialty  VARCHAR(100)
    )""",

    """CREATE TABLE IF NOT EXISTS dw.dim_semester (
        semester_id   INTEGER     PRIMARY KEY,
        name          VARCHAR(50) NOT NULL,
        start_time_id INTEGER     REFERENCES dw.dim_time(time_id),
        end_time_id   INTEGER     REFERENCES dw.dim_time(time_id)
    )""",

    """CREATE TABLE IF NOT EXISTS dw.dim_course (
        course_id   INTEGER      PRIMARY KEY,
        code        VARCHAR(20)  NOT NULL,
        name        VARCHAR(200) NOT NULL,
        credits     INTEGER,
        quota       INTEGER,
        teacher_id  INTEGER REFERENCES dw.dim_teacher(teacher_id),
        semester_id INTEGER REFERENCES dw.dim_semester(semester_id)
    )""",

    """CREATE TABLE IF NOT EXISTS dw.fact_performance (
        perf_id           SERIAL  PRIMARY KEY,
        student_id        INTEGER NOT NULL REFERENCES dw.dim_student(student_id),
        course_id         INTEGER NOT NULL REFERENCES dw.dim_course(course_id),
        semester_id       INTEGER NOT NULL REFERENCES dw.dim_semester(semester_id),
        teacher_id        INTEGER NOT NULL REFERENCES dw.dim_teacher(teacher_id),
        graded_time_id    INTEGER REFERENCES dw.dim_time(time_id),
        score             FLOAT   NOT NULL,
        is_passing        BOOLEAN NOT NULL,
        is_excellent      BOOLEAN NOT NULL,
        is_at_risk        BOOLEAN NOT NULL,
        enrollment_status VARCHAR(20) NOT NULL,
        loaded_at         TIMESTAMP DEFAULT NOW()
    )""",

    """CREATE OR REPLACE VIEW dw.v_success_rate_by_course AS
        SELECT
            c.code,
            c.name  AS course_name,
            s.name  AS semester,
            COUNT(*) AS total_students,
            SUM(CASE WHEN f.is_passing  THEN 1 ELSE 0 END) AS passing,
            SUM(CASE WHEN f.is_at_risk  THEN 1 ELSE 0 END) AS at_risk,
            ROUND(100.0 * SUM(CASE WHEN f.is_passing THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS success_rate_pct,
            ROUND(AVG(f.score)::NUMERIC, 2) AS avg_score,
            ROUND(MIN(f.score)::NUMERIC, 2) AS min_score,
            ROUND(MAX(f.score)::NUMERIC, 2) AS max_score
        FROM dw.fact_performance f
        JOIN dw.dim_course   c ON f.course_id   = c.course_id
        JOIN dw.dim_semester s ON f.semester_id = s.semester_id
        GROUP BY c.code, c.name, s.name
        ORDER BY success_rate_pct DESC""",

    """CREATE OR REPLACE VIEW dw.v_success_rate_by_teacher AS
        SELECT
            t.first_name || ' ' || t.last_name AS teacher,
            t.specialty,
            COUNT(*)  AS total_notes,
            ROUND(AVG(f.score)::NUMERIC, 2)    AS avg_score,
            ROUND(MIN(f.score)::NUMERIC, 2)    AS min_score,
            ROUND(MAX(f.score)::NUMERIC, 2)    AS max_score,
            ROUND(100.0 * SUM(CASE WHEN f.is_passing  THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS success_rate_pct,
            ROUND(100.0 * SUM(CASE WHEN f.is_at_risk  THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS at_risk_pct
        FROM dw.fact_performance f
        JOIN dw.dim_teacher t ON f.teacher_id = t.teacher_id
        GROUP BY t.teacher_id, t.first_name, t.last_name, t.specialty
        ORDER BY avg_score DESC""",

    """CREATE OR REPLACE VIEW dw.v_at_risk_students AS
        SELECT
            st.first_name || ' ' || st.last_name AS student,
            st.email,
            COUNT(*)  AS courses_at_risk,
            ROUND(AVG(f.score)::NUMERIC, 2) AS avg_score,
            ROUND(MIN(f.score)::NUMERIC, 2) AS min_score
        FROM dw.fact_performance f
        JOIN dw.dim_student st ON f.student_id = st.student_id
        WHERE f.is_at_risk = TRUE
        GROUP BY st.student_id, st.first_name, st.last_name, st.email
        ORDER BY avg_score ASC""",

    """CREATE OR REPLACE VIEW dw.v_enrollment_evolution AS
        SELECT
            s.name  AS semester,
            COUNT(DISTINCT f.student_id) AS nb_students,
            COUNT(*)  AS nb_inscriptions,
            ROUND(AVG(f.score)::NUMERIC, 2) AS avg_score,
            ROUND(100.0 * SUM(CASE WHEN f.is_passing  THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS success_rate_pct
        FROM dw.fact_performance f
        JOIN dw.dim_semester s ON f.semester_id = s.semester_id
        GROUP BY s.semester_id, s.name
        ORDER BY s.semester_id""",

    """CREATE OR REPLACE VIEW dw.v_student_summary AS
        SELECT
            st.first_name || ' ' || st.last_name AS student,
            st.email,
            st.age,
            COUNT(*)  AS total_courses,
            ROUND(AVG(f.score)::NUMERIC, 2)  AS avg_score,
            ROUND(MIN(f.score)::NUMERIC, 2)  AS min_score,
            ROUND(MAX(f.score)::NUMERIC, 2)  AS max_score,
            SUM(CASE WHEN f.is_passing   THEN 1 ELSE 0 END) AS passed,
            SUM(CASE WHEN f.is_at_risk   THEN 1 ELSE 0 END) AS at_risk,
            SUM(CASE WHEN f.is_excellent THEN 1 ELSE 0 END) AS excellent,
            CASE
                WHEN AVG(f.score) < 8  THEN 'A risque'
                WHEN AVG(f.score) < 12 THEN 'Moyen'
                ELSE 'Bon'
            END AS profil
        FROM dw.fact_performance f
        JOIN dw.dim_student st ON f.student_id = st.student_id
        GROUP BY st.student_id, st.first_name, st.last_name, st.email, st.age
        ORDER BY avg_score DESC""",
]

def init_warehouse(conn):
    log("INIT", "Creation du schema DW...")
    for stmt in DDL:
        conn.execute(text(stmt))
    conn.commit()
    log("INIT", f"{len(DDL)} statements executes -- schema DW pret")

# ---------------------------------------------------------------------------
#  ETAPE 1 -- EXTRACT
# ---------------------------------------------------------------------------

def extract(conn):
    log("EXTRACT", "Lecture des tables ERP...")

    students = conn.execute(text("""
        SELECT s.id, s.first_name, s.last_name, u.email, s.birth_date
        FROM   students s
        JOIN   users    u ON u.id = s.user_id
    """)).mappings().all()

    teachers = conn.execute(text("""
        SELECT id, first_name, last_name, specialty
        FROM   teachers
    """)).mappings().all()

    semesters = conn.execute(text("""
        SELECT id, name, start_date, end_date
        FROM   semesters
    """)).mappings().all()

    courses = conn.execute(text("""
        SELECT id, code, name, credits, quota, teacher_id, semester_id
        FROM   courses
    """)).mappings().all()

    performances = conn.execute(text("""
        SELECT
            s.id          AS student_id,
            c.id          AS course_id,
            c.semester_id AS semester_id,
            c.teacher_id  AS teacher_id,
            g.score       AS score,
            g.graded_at   AS graded_at,
            e.status      AS enrollment_status
        FROM   enrollments e
        JOIN   students    s ON s.id = e.student_id
        JOIN   courses     c ON c.id = e.course_id
        JOIN   grades      g ON g.enrollment_id = e.id
        WHERE  g.score IS NOT NULL
    """)).mappings().all()

    log("EXTRACT", (
        f"etudiants={len(students)}  formateurs={len(teachers)}  "
        f"semestres={len(semesters)}  cours={len(courses)}  "
        f"performances={len(performances)}"
    ))
    return students, teachers, semesters, courses, performances

# ---------------------------------------------------------------------------
#  ETAPE 2 -- TRANSFORM
# ---------------------------------------------------------------------------

def transform(students, teachers, semesters, courses, performances):
    log("TRANSFORM", "Calcul des metriques...")

    dates_needed = set()
    for sem in semesters:
        for key in ("start_date", "end_date"):
            d = to_date(sem[key])
            if d:
                dates_needed.add(d)
    for p in performances:
        d = to_date(p["graded_at"])
        if d:
            dates_needed.add(d)

    time_records = {d: build_time_record(d) for d in dates_needed}

    t_students = [
        {
            "student_id": s["id"],
            "first_name": (s["first_name"] or "").strip(),
            "last_name":  (s["last_name"]  or "").strip(),
            "email":      (s["email"]      or "").lower().strip(),
            "age":         get_age(s["birth_date"]),
        }
        for s in students
    ]

    t_teachers = [
        {
            "teacher_id": t["id"],
            "first_name": (t["first_name"] or "").strip(),
            "last_name":  (t["last_name"]  or "").strip(),
            "specialty":  (t["specialty"]  or "Non renseigne").strip(),
        }
        for t in teachers
    ]

    t_semesters = [
        {
            "semester_id": s["id"],
            "name":        s["name"],
            "start_date":  to_date(s["start_date"]),
            "end_date":    to_date(s["end_date"]),
        }
        for s in semesters
    ]

    t_courses = [
        {
            "course_id":   c["id"],
            "code":        c["code"],
            "name":        c["name"],
            "credits":     c["credits"],
            "quota":       c["quota"],
            "teacher_id":  c["teacher_id"],
            "semester_id": c["semester_id"],
        }
        for c in courses
    ]

    t_performances = [
        {
            "student_id":        p["student_id"],
            "course_id":         p["course_id"],
            "semester_id":       p["semester_id"],
            "teacher_id":        p["teacher_id"],
            "score":             round(float(p["score"]), 1),
            "is_passing":        float(p["score"]) >= 10,
            "is_excellent":      float(p["score"]) >= 16,
            "is_at_risk":        float(p["score"]) <   8,
            "enrollment_status": str(p["enrollment_status"]),
            "graded_date":       to_date(p["graded_at"]),
        }
        for p in performances
    ]

    log("TRANSFORM", f"dim_time={len(time_records)}  fact={len(t_performances)}")
    return time_records, t_students, t_teachers, t_semesters, t_courses, t_performances

# ---------------------------------------------------------------------------
#  ETAPE 3 -- LOAD
# ---------------------------------------------------------------------------

def load(conn, time_records, t_students, t_teachers,
         t_semesters, t_courses, t_performances):
    log("LOAD", "Chargement dans le schema dw...")

    for tbl in (
        "dw.fact_performance", "dw.dim_course", "dw.dim_semester",
        "dw.dim_teacher",      "dw.dim_student", "dw.dim_time"
    ):
        conn.execute(text(f"TRUNCATE {tbl} CASCADE"))
    conn.commit()

    for rec in time_records.values():
        conn.execute(text("""
            INSERT INTO dw.dim_time
                (full_date, day, month, month_name, quarter, year, semester)
            VALUES
                (:full_date, :day, :month, :month_name, :quarter, :year, :semester)
            ON CONFLICT (full_date) DO NOTHING
        """), rec)
    conn.commit()

    rows     = conn.execute(text("SELECT time_id, full_date FROM dw.dim_time")).fetchall()
    time_map = {str(r[1]): r[0] for r in rows}

    for s in t_students:
        conn.execute(text("""
            INSERT INTO dw.dim_student (student_id, first_name, last_name, email, age)
            VALUES (:student_id, :first_name, :last_name, :email, :age)
            ON CONFLICT (student_id) DO UPDATE SET
                first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
                email=EXCLUDED.email, age=EXCLUDED.age
        """), s)
    conn.commit()

    for t in t_teachers:
        conn.execute(text("""
            INSERT INTO dw.dim_teacher (teacher_id, first_name, last_name, specialty)
            VALUES (:teacher_id, :first_name, :last_name, :specialty)
            ON CONFLICT (teacher_id) DO UPDATE SET specialty=EXCLUDED.specialty
        """), t)
    conn.commit()

    for s in t_semesters:
        conn.execute(text("""
            INSERT INTO dw.dim_semester (semester_id, name, start_time_id, end_time_id)
            VALUES (:semester_id, :name, :start_time_id, :end_time_id)
            ON CONFLICT (semester_id) DO UPDATE SET name=EXCLUDED.name
        """), {
            "semester_id":   s["semester_id"],
            "name":          s["name"],
            "start_time_id": time_map.get(str(s["start_date"])),
            "end_time_id":   time_map.get(str(s["end_date"])),
        })
    conn.commit()

    for c in t_courses:
        conn.execute(text("""
            INSERT INTO dw.dim_course
                (course_id, code, name, credits, quota, teacher_id, semester_id)
            VALUES
                (:course_id, :code, :name, :credits, :quota, :teacher_id, :semester_id)
            ON CONFLICT (course_id) DO UPDATE SET
                name=EXCLUDED.name, credits=EXCLUDED.credits
        """), c)
    conn.commit()

    for p in t_performances:
        conn.execute(text("""
            INSERT INTO dw.fact_performance (
                student_id, course_id, semester_id, teacher_id,
                graded_time_id, score, is_passing, is_excellent,
                is_at_risk, enrollment_status
            ) VALUES (
                :student_id, :course_id, :semester_id, :teacher_id,
                :graded_time_id, :score, :is_passing, :is_excellent,
                :is_at_risk, :enrollment_status
            )
        """), {**p, "graded_time_id": time_map.get(str(p["graded_date"]))})
    conn.commit()

    log("LOAD", (
        f"dim_time={len(time_records)}  dim_student={len(t_students)}  "
        f"dim_teacher={len(t_teachers)}  dim_semester={len(t_semesters)}  "
        f"dim_course={len(t_courses)}  fact={len(t_performances)}"
    ))

# ---------------------------------------------------------------------------
#  ETAPE 4 -- EDA (Analyse Exploratoire des Donnees)
# ---------------------------------------------------------------------------

def eda(conn):
    print(f"\n{SEP1}")
    print("  EDA -- ANALYSE EXPLORATOIRE DES DONNEES")
    print(SEP1)

    # ── 1. Vue d'ensemble ──────────────────────────────────────────────────
    print(f"\n{'[ 1 ] VUE D ENSEMBLE':^72}")
    print(SEP2)

    row = conn.execute(text("""
        SELECT
            COUNT(DISTINCT student_id)  AS nb_etudiants,
            COUNT(DISTINCT course_id)   AS nb_cours,
            COUNT(DISTINCT teacher_id)  AS nb_formateurs,
            COUNT(DISTINCT semester_id) AS nb_semestres,
            COUNT(*)                    AS total_notes,
            ROUND(AVG(score)::NUMERIC, 2) AS moy_generale,
            ROUND(MIN(score)::NUMERIC, 2) AS note_min,
            ROUND(MAX(score)::NUMERIC, 2) AS note_max,
            SUM(CASE WHEN is_passing  THEN 1 ELSE 0 END) AS nb_reussi,
            SUM(CASE WHEN is_at_risk  THEN 1 ELSE 0 END) AS nb_risque,
            SUM(CASE WHEN is_excellent THEN 1 ELSE 0 END) AS nb_excellent
        FROM dw.fact_performance
    """)).fetchone()

    total = row[4]
    print(f"  Etudiants     : {row[0]}")
    print(f"  Cours         : {row[1]}")
    print(f"  Formateurs    : {row[2]}")
    print(f"  Semestres     : {row[3]}")
    print(f"  Total notes   : {total}")
    print(f"  Moyenne gen.  : {row[5]} / 20")
    print(f"  Note min/max  : {row[6]} / {row[7]}")
    print(f"  Reussi (>=10) : {row[8]:>4}  {bar(row[8], total)}  "
          f"{round(100*row[8]/total,1)} %")
    print(f"  A risque (<8) : {row[9]:>4}  {bar(row[9], total)}  "
          f"{round(100*row[9]/total,1)} %")
    print(f"  Excellent(>=16): {row[10]:>3}  {bar(row[10], total)}  "
          f"{round(100*row[10]/total,1)} %")

    # ── 2. Distribution des notes par tranche ─────────────────────────────
    print(f"\n{'[ 2 ] DISTRIBUTION DES NOTES':^72}")
    print(SEP2)

    tranches = [
        ("0  - 4  (Echec grave)",   0,  4),
        ("5  - 7  (Insuffisant)",   5,  7),
        ("8  - 9  (Limite)",        8,  9),
        ("10 - 12 (Passable)",     10, 12),
        ("13 - 15 (Assez bien)",   13, 15),
        ("16 - 18 (Bien)",         16, 18),
        ("19 - 20 (Tres bien)",    19, 20),
    ]
    for label, lo, hi in tranches:
        count = conn.execute(text(
            "SELECT COUNT(*) FROM dw.fact_performance "
            "WHERE score >= :lo AND score <= :hi"
        ), {"lo": lo, "hi": hi}).scalar()
        pct = round(100 * count / total, 1) if total else 0
        print(f"  {label:<25}  {count:>4}  {bar(count, total)}  {pct} %")

    # ── 3. Reussite par cours ─────────────────────────────────────────────
    print(f"\n{'[ 3 ] REUSSITE PAR COURS':^72}")
    print(SEP2)
    print(f"  {'Code':<8} {'Cours':<32} {'Sem.':<8} {'Moy':>5}  {'Reussite':>9}  {'A risque':>9}")
    print(f"  {'-'*8} {'-'*32} {'-'*8} {'-'*5}  {'-'*9}  {'-'*9}")
    rows = conn.execute(text(
        "SELECT code, course_name, semester, avg_score, "
        "success_rate_pct, at_risk, total_students "
        "FROM dw.v_success_rate_by_course"
    )).fetchall()
    for r in rows:
        at_risk_pct = round(100 * r[5] / r[6], 1) if r[6] else 0
        print(f"  {r[0]:<8} {r[1][:32]:<32} {r[2][:8]:<8} {r[3]:>5}  "
              f"{str(r[4])+' %':>9}  {str(at_risk_pct)+' %':>9}")

    # ── 4. Performance par formateur ──────────────────────────────────────
    print(f"\n{'[ 4 ] PERFORMANCE PAR FORMATEUR':^72}")
    print(SEP2)
    print(f"  {'Formateur':<26} {'Specialite':<22} {'Moy':>5}  "
          f"{'Reussite':>9}  {'A risque':>9}")
    print(f"  {'-'*26} {'-'*22} {'-'*5}  {'-'*9}  {'-'*9}")
    rows = conn.execute(text(
        "SELECT teacher, specialty, avg_score, success_rate_pct, at_risk_pct "
        "FROM dw.v_success_rate_by_teacher"
    )).fetchall()
    for r in rows:
        print(f"  {r[0]:<26} {r[1]:<22} {r[2]:>5}  "
              f"{str(r[3])+' %':>9}  {str(r[4])+' %':>9}")

    # ── 5. Profils etudiants ──────────────────────────────────────────────
    print(f"\n{'[ 5 ] PROFILS ETUDIANTS':^72}")
    print(SEP2)

    profil_rows = conn.execute(text("""
        SELECT profil, COUNT(*) AS nb
        FROM dw.v_student_summary
        GROUP BY profil
        ORDER BY nb DESC
    """)).fetchall()
    nb_total_students = sum(r[1] for r in profil_rows)
    for r in profil_rows:
        pct = round(100 * r[1] / nb_total_students, 1) if nb_total_students else 0
        print(f"  {r[0]:<12}  {r[1]:>3} etudiants  "
              f"{bar(r[1], nb_total_students)}  {pct} %")

    # ── 6. Etudiants a risque ─────────────────────────────────────────────
    print(f"\n{'[ 6 ] ETUDIANTS A RISQUE (score moyen < 8)':^72}")
    print(SEP2)
    rows = conn.execute(text(
        "SELECT student, avg_score, min_score, courses_at_risk "
        "FROM dw.v_at_risk_students"
    )).fetchall()
    if rows:
        print(f"  {'Etudiant':<30} {'Moy':>6}  {'Min':>6}  {'Cours en diff.':>14}")
        print(f"  {'-'*30} {'-'*6}  {'-'*6}  {'-'*14}")
        for r in rows:
            print(f"  {r[0]:<30} {r[1]:>6}  {r[2]:>6}  {r[3]:>14}")
    else:
        print("  Aucun etudiant a risque detecte.")

    # ── 7. Evolution par semestre ─────────────────────────────────────────
    print(f"\n{'[ 7 ] EVOLUTION PAR SEMESTRE':^72}")
    print(SEP2)
    print(f"  {'Semestre':<20} {'Etudiants':>10}  {'Notes':>6}  "
          f"{'Moy':>5}  {'Reussite':>9}")
    print(f"  {'-'*20} {'-'*10}  {'-'*6}  {'-'*5}  {'-'*9}")
    rows = conn.execute(text(
        "SELECT semester, nb_students, nb_inscriptions, "
        "avg_score, success_rate_pct "
        "FROM dw.v_enrollment_evolution"
    )).fetchall()
    for r in rows:
        print(f"  {r[0]:<20} {r[1]:>10}  {r[2]:>6}  "
              f"{r[3]:>5}  {str(r[4])+' %':>9}")

    # ── 8. Top 5 meilleurs et derniers etudiants ──────────────────────────
    print(f"\n{'[ 8 ] CLASSEMENT ETUDIANTS':^72}")
    print(SEP2)

    print("  Top 5 meilleurs :")
    rows = conn.execute(text(
        "SELECT student, avg_score, total_courses, profil "
        "FROM dw.v_student_summary ORDER BY avg_score DESC LIMIT 5"
    )).fetchall()
    for i, r in enumerate(rows, 1):
        print(f"    {i}. {r[0]:<28}  moy={r[1]}  cours={r[2]}  [{r[3]}]")

    print("  Top 5 en difficulte :")
    rows = conn.execute(text(
        "SELECT student, avg_score, total_courses, profil "
        "FROM dw.v_student_summary ORDER BY avg_score ASC LIMIT 5"
    )).fetchall()
    for i, r in enumerate(rows, 1):
        print(f"    {i}. {r[0]:<28}  moy={r[1]}  cours={r[2]}  [{r[3]}]")

    # ── 9. Cours avec le plus fort taux d'echec ───────────────────────────
    print(f"\n{'[ 9 ] COURS LES PLUS DIFFICILES':^72}")
    print(SEP2)
    rows = conn.execute(text("""
        SELECT code, course_name, avg_score, success_rate_pct, min_score, max_score
        FROM dw.v_success_rate_by_course
        ORDER BY success_rate_pct ASC
        LIMIT 5
    """)).fetchall()
    print(f"  {'Code':<8} {'Cours':<32} {'Moy':>5}  {'Reussite':>9}  "
          f"{'Min':>5}  {'Max':>5}")
    print(f"  {'-'*8} {'-'*32} {'-'*5}  {'-'*9}  {'-'*5}  {'-'*5}")
    for r in rows:
        print(f"  {r[0]:<8} {r[1][:32]:<32} {r[2]:>5}  "
              f"{str(r[3])+' %':>9}  {r[4]:>5}  {r[5]:>5}")

    print(f"\n{SEP1}\n")

# ---------------------------------------------------------------------------
#  MAIN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(SEP1)
    print("  ETL EDUSMART")
    print(f"  Source    : {DATABASE_URL}")
    print(f"  DW schema : dw  (meme base PostgreSQL)")
    print(SEP1 + "\n")

    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            init_warehouse(conn)
            students, teachers, semesters, courses, performances = extract(conn)
            (time_records, t_students, t_teachers,
             t_semesters, t_courses, t_performances) = transform(
                students, teachers, semesters, courses, performances
            )
            load(conn, time_records, t_students, t_teachers,
                 t_semesters, t_courses, t_performances)
            eda(conn)

        print("[DONE] ETL + EDA termines avec succes.")

    except Exception as e:
        log("ERROR", str(e))
        sys.exit(1)